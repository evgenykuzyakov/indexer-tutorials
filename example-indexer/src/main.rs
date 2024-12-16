mod models;
mod retriable;
mod schema;

#[macro_use]
extern crate diesel;

use actix_diesel::dsl::AsyncRunQueryDsl;
use actix_diesel::Database;
use bigdecimal::ToPrimitive;
use diesel::{ExpressionMethods, PgConnection, QueryDsl};
use dotenv::dotenv;
use std::collections::HashSet;
use std::convert::TryFrom;
use std::env;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use fastnear_neardata_fetcher::fetcher;
use fastnear_primitives::block_with_tx_hash::BlockWithTxHashes;
use fastnear_primitives::near_primitives::types::BlockHeight;
use fastnear_primitives::near_primitives::views::{ActionView, ExecutionOutcomeView, ExecutionStatusView, ReceiptEnumView, ReceiptView};
use fastnear_primitives::types::ChainId;
use tokio::sync::mpsc;
use tracing_subscriber::EnvFilter;

use crate::models::enums::ExecutionOutcomeStatus;
use crate::models::events::Event;
use crate::models::social::Receipt;

fn get_database_credentials() -> String {
    env::var("DATABASE_URL").expect("DATABASE_URL must be set in .env file")
}

fn establish_connection() -> actix_diesel::Database<PgConnection> {
    let database_url = get_database_credentials();
    actix_diesel::Database::builder()
        .pool_max_size(30)
        .open(&database_url)
}

const PROJECT_ID: &str = "social_indexer";
const INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);
const MAX_DELAY_TIME: std::time::Duration = std::time::Duration::from_secs(120);

fn main() {
    openssl_probe::init_ssl_cert_env_vars();
    dotenv().ok();

    let is_running = Arc::new(AtomicBool::new(true));
    let ctrl_c_running = is_running.clone();

    ctrlc::set_handler(move || {
        ctrl_c_running.store(false, Ordering::SeqCst);
        println!("Received Ctrl+C, starting shutdown...");
    })
        .expect("Error setting Ctrl+C handler");

    let whitelisted_accounts = HashSet::from(["social.near".to_string()]);
    let stream_events: bool = env::var("STREAM_EVENTS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(true);

    let args: Vec<String> = std::env::args().collect();

    let mut env_filter = EnvFilter::new(
        "tokio_reactor=info,neardata-fetcher=info,social_indexer=info",
    );

    if let Ok(rust_log) = std::env::var("RUST_LOG") {
        if !rust_log.is_empty() {
            for directive in rust_log.split(',').filter_map(|s| match s.parse() {
                Ok(directive) => Some(directive),
                Err(err) => {
                    eprintln!("Ignoring directive `{}`: {}", s, err);
                    None
                }
            }) {
                env_filter = env_filter.add_directive(directive);
            }
        }
    }

    tracing_subscriber::fmt::Subscriber::builder()
        .with_env_filter(env_filter)
        .with_writer(std::io::stderr)
        .init();

    tracing::log::info!(
        target: PROJECT_ID,
        "Starting indexer. Whitelisted accounts: {:?}. Streaming events: {}",
        whitelisted_accounts,
        stream_events
    );

    let command = args
        .get(1)
        .map(|arg| arg.as_str())
        .expect("You need to provide a command: `init`, `run` or `redis_run` as arg");

    match command {
        "fetcher_run" => {
            let pool = establish_connection();

            let sys = actix::System::new();
            sys.block_on(async move {
                let client = reqwest::Client::new();
                let chain_id = ChainId::try_from(std::env::var("CHAIN_ID").expect("CHAIN_ID is not set"))
                    .expect("Invalid chain id");
                let first_block_height = fetcher::fetch_first_block(&client, chain_id)
                    .await
                    .expect("First block doesn't exists")
                    .block
                    .header
                    .height;
                tracing::log::info!(target: PROJECT_ID, "First block {}", first_block_height);

                // Select 1 Receipt from the database ordered by block_height descending using Diesel 1.4.8
                let receipt: Receipt = schema::receipts::table
                    .order(schema::receipts::block_height.desc())
                    .first_async(&pool)
                    .await
                    .expect("Failed to get the last indexed block");

                let last_block_height: BlockHeight = receipt.block_height.to_u64().unwrap();

                tracing::log::info!(target: PROJECT_ID, "Resuming from {}", last_block_height);

                let num_threads = std::env::var("NUM_THREADS")
                    .map(|s| s.parse::<u64>().expect("Failed to parse NUM_THREADS"))
                    .unwrap_or(4);

                let (sender, receiver) = mpsc::channel(100);
                let config = fetcher::FetcherConfig {
                    num_threads,
                    start_block_height: last_block_height + 1,
                    chain_id,
                };
                tokio::spawn(fetcher::start_fetcher(
                    Some(client),
                    config,
                    sender,
                    is_running,
                ));

                listen_blocks(receiver, pool, &whitelisted_accounts, stream_events).await;

                actix::System::current().stop();
            });
            sys.run().unwrap();
        }
        _ => panic!("You have to pass `init`, `run` or `redis_run` arg"),
    }
}

async fn listen_blocks(
    mut stream: tokio::sync::mpsc::Receiver<BlockWithTxHashes>,
    pool: Database<PgConnection>,
    whitelisted_accounts: &HashSet<String>,
    stream_events: bool,
) {
    while let Some(streamer_message) = stream.recv().await {
        extract_info(&pool, streamer_message, whitelisted_accounts, stream_events)
            .await
            .unwrap();
    }
}

const EVENT_LOG_PREFIX: &str = "EVENT_JSON:";

async fn extract_info(
    pool: &Database<PgConnection>,
    msg: BlockWithTxHashes,
    whitelisted_accounts: &HashSet<String>,
    stream_events: bool,
) -> anyhow::Result<()> {
    let block_height = msg.block.header.height;
    let block_hash = msg.block.header.hash.to_string();
    let block_timestamp = msg.block.header.timestamp_nanosec;
    let block_epoch_id = msg.block.header.epoch_id.to_string();

    let mut events = vec![];
    let mut receipts = vec![];
    for shard in msg.shards {
        for (outcome_index, outcome) in shard.receipt_execution_outcomes.into_iter().enumerate() {
            let ReceiptView {
                predecessor_id,
                receiver_id: account_id,
                receipt_id,
                receipt,
            } = outcome.receipt;
            let predecessor_id = predecessor_id.to_string();
            let account_id = account_id.to_string();
            let receipt_id = receipt_id.to_string();
            let ExecutionOutcomeView { logs, status, .. } = outcome.execution_outcome.outcome;
            let status = match status {
                ExecutionStatusView::Unknown => ExecutionOutcomeStatus::Failure,
                ExecutionStatusView::Failure(_) => ExecutionOutcomeStatus::Failure,
                ExecutionStatusView::SuccessValue(_) => ExecutionOutcomeStatus::Success,
                ExecutionStatusView::SuccessReceiptId(_) => ExecutionOutcomeStatus::Success,
            };
            if stream_events {
                for (log_index, log) in logs.into_iter().enumerate() {
                    if log.starts_with(EVENT_LOG_PREFIX) {
                        events.push(Event {
                            block_height: block_height.into(),
                            block_hash: block_hash.clone(),
                            block_timestamp: block_timestamp.into(),
                            block_epoch_id: block_epoch_id.clone(),
                            receipt_id: receipt_id.clone(),
                            log_index: log_index as i32,
                            predecessor_id: predecessor_id.clone(),
                            account_id: account_id.clone(),
                            status,
                            event: log.as_str()[EVENT_LOG_PREFIX.len()..].to_string(),
                        })
                    }
                }
            }
            if whitelisted_accounts.contains(&account_id) {
                match receipt {
                    ReceiptEnumView::Action {
                        signer_id,
                        signer_public_key,
                        actions,
                        ..
                    } => {
                        for (index_in_receipt, action) in actions.into_iter().enumerate() {
                            match action {
                                ActionView::FunctionCall {
                                    method_name,
                                    args,
                                    gas,
                                    deposit,
                                } => {
                                    if let Ok(args) = String::from_utf8(args.to_vec()) {
                                        receipts.push(Receipt {
                                            block_height: block_height.into(),
                                            block_hash: block_hash.clone(),
                                            block_timestamp: block_timestamp.into(),
                                            block_epoch_id: block_epoch_id.clone(),
                                            outcome_index: outcome_index as i32,
                                            receipt_id: receipt_id.clone(),
                                            index_in_receipt: index_in_receipt as i32,
                                            signer_public_key: signer_public_key.to_string(),
                                            signer_id: signer_id.to_string(),
                                            predecessor_id: predecessor_id.clone(),
                                            account_id: account_id.clone(),
                                            status,
                                            deposit: bigdecimal::BigDecimal::from_str(
                                                deposit.to_string().as_str(),
                                            )
                                                .unwrap(),
                                            gas: gas.into(),
                                            method_name,
                                            args,
                                        });
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    ReceiptEnumView::Data { .. } => {}
                }
            }
        }
    }

    if !events.is_empty() {
        crate::await_retry_or_panic!(
            diesel::insert_into(schema::events::table)
                .values(events.clone())
                .on_conflict_do_nothing()
                .execute_async(&pool),
            10,
            "Events insert foilureee".to_string(),
            &events
        );
    }

    if !receipts.is_empty() {
        crate::await_retry_or_panic!(
            diesel::insert_into(schema::receipts::table)
                .values(receipts.clone())
                .on_conflict_do_nothing()
                .execute_async(&pool),
            10,
            "Receipts insert foilureee".to_string(),
            &receipts
        );
    }

    tracing::log::info!(target: PROJECT_ID, "Processed block {}", block_height);

    Ok(())
}
