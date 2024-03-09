mod models;
mod redis_db;
mod retriable;
mod schema;

#[macro_use]
extern crate diesel;

use actix_diesel::dsl::AsyncRunQueryDsl;
use actix_diesel::Database;
use bigdecimal::ToPrimitive;
use diesel::{ExpressionMethods, PgConnection, QueryDsl};
use dotenv::dotenv;
use near_indexer::near_primitives::types::BlockHeight;
use near_indexer::near_primitives::views::{
    ActionView, ExecutionOutcomeView, ExecutionStatusView, ReceiptEnumView, ReceiptView,
};
use near_indexer::StreamerMessage;
use std::collections::HashSet;
use std::env;
use std::str::FromStr;
use tokio::sync::mpsc;
use tracing_subscriber::EnvFilter;

use crate::models::enums::ExecutionOutcomeStatus;
use crate::models::events::Event;
use crate::models::social::Receipt;
use crate::redis_db::RedisDB;

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

const FINAL_BLOCKS_KEY: &str = "final_blocks";
const BLOCK_KEY: &str = "block";
const SAFE_OFFSET: u64 = 100;

fn main() {
    openssl_probe::init_ssl_cert_env_vars();
    dotenv().ok();

    let whitelisted_accounts = HashSet::from(["social.near".to_string()]);
    let stream_events: bool = env::var("STREAM_EVENTS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(true);

    let args: Vec<String> = std::env::args().collect();
    let home_dir = std::path::PathBuf::from(near_indexer::get_default_home());

    let mut env_filter = EnvFilter::new(
        "redis=info,tokio_reactor=info,near=info,stats=info,telemetry=info,indexer=info,aggregated=info",
    );

    env_filter = env_filter.add_directive(
        "social_indexer=info"
            .parse()
            .expect("Failed to parse directive"),
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
        "init" => {
            let config_args = near_indexer::InitConfigArgs {
                chain_id: None,
                account_id: None,
                test_seed: None,
                num_shards: 4,
                fast: false,
                genesis: None,
                download_genesis: false,
                download_genesis_url: None,
                download_records_url: None,
                download_config: false,
                download_config_url: Some("https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore-deploy/mainnet/config.json".to_string()),
                boot_nodes: None,
                max_gas_burnt_view: None,
            };
            near_indexer::indexer_init_configs(&home_dir, config_args).unwrap();
        }
        "redis_run" => {
            let pool = establish_connection();

            let sys = actix::System::new();
            sys.block_on(async move {
                let mut read_redis_db = RedisDB::new(None).await;
                let (id, _key_values) = read_redis_db
                    .xread(1, FINAL_BLOCKS_KEY, "0")
                    .await
                    .expect("Failed to get the first block from Redis")
                    .into_iter()
                    .next()
                    .unwrap();
                let first_block_height: BlockHeight =
                    id.split_once("-").unwrap().0.parse().unwrap();
                tracing::log::info!(target: PROJECT_ID, "First redis block {}", first_block_height);

                // Select 1 Receipt from the database ordered by block_height descending using Diesel 1.4.8
                let receipt: Receipt = schema::receipts::table
                    .order(schema::receipts::block_height.desc())
                    .first_async(&pool)
                    .await
                    .expect("Failed to get the last indexed block");

                let last_block_height: BlockHeight = receipt.block_height.to_u64().unwrap();

                if first_block_height + SAFE_OFFSET > last_block_height {
                    panic!("The first block in the redis is too close to the last indexed block");
                }

                let last_id = format!("{}-0", last_block_height);
                tracing::log::info!(target: PROJECT_ID, "Resuming from {}", last_block_height);

                let stream = streamer(last_id, read_redis_db);
                listen_blocks(stream, pool, &whitelisted_accounts, stream_events).await;

                actix::System::current().stop();
            });
            sys.run().unwrap();
        }
        "run" => {
            let pool = establish_connection();
            let indexer_config = near_indexer::IndexerConfig {
                home_dir: std::path::PathBuf::from(near_indexer::get_default_home()),
                sync_mode: near_indexer::SyncModeEnum::FromInterruption,
                await_for_node_synced: near_indexer::AwaitForNodeSyncedEnum::WaitForFullSync,
                validate_genesis: false,
            };
            let sys = actix::System::new();
            sys.block_on(async move {
                let indexer = near_indexer::Indexer::new(indexer_config).unwrap();
                let stream = indexer.streamer();
                listen_blocks(stream, pool, &whitelisted_accounts, stream_events).await;

                actix::System::current().stop();
            });
            sys.run().unwrap();
        }
        _ => panic!("You have to pass `init`, `run` or `redis_run` arg"),
    }
}

async fn start(
    mut last_id: String,
    mut redis_db: RedisDB,
    blocks_sink: mpsc::Sender<StreamerMessage>,
) {
    loop {
        let res = redis_db.xread(1, FINAL_BLOCKS_KEY, &last_id).await;
        let res = match res {
            Ok(res) => res,
            Err(err) => {
                tracing::log::error!(target: PROJECT_ID, "Error: {}", err);
                tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
                let _ = redis_db.reconnect().await;
                continue;
            }
        };
        let (id, key_values) = res.into_iter().next().unwrap();
        assert_eq!(key_values.len(), 1, "Expected 1 key-value pair");
        let (key, value) = key_values.into_iter().next().unwrap();
        assert_eq!(key, BLOCK_KEY, "Expected key to be block");
        let streamer_message: StreamerMessage = serde_json::from_str(&value).unwrap();
        blocks_sink.send(streamer_message).await.unwrap();
        last_id = id;
    }
}

pub fn streamer(last_id: String, redis_db: RedisDB) -> mpsc::Receiver<StreamerMessage> {
    let (sender, receiver) = mpsc::channel(100);
    tokio::spawn(start(last_id, redis_db, sender));
    receiver
}

async fn listen_blocks(
    mut stream: tokio::sync::mpsc::Receiver<near_indexer::StreamerMessage>,
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
    msg: near_indexer::StreamerMessage,
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
