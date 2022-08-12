mod models;
mod retriable;
mod schema;

#[macro_use]
extern crate diesel;

use actix_diesel::dsl::AsyncRunQueryDsl;
use actix_diesel::Database;
use diesel::PgConnection;
use dotenv::dotenv;
use near_indexer::near_primitives::views::{
    ExecutionOutcomeView, ExecutionStatusView, ReceiptView,
};
use std::env;
use tracing_subscriber::EnvFilter;

use crate::models::enums::ExecutionOutcomeStatus;
use crate::models::events::Event;

// TODO: remove all db related
fn get_database_credentials() -> String {
    dotenv().ok();

    env::var("DATABASE_URL").expect("DATABASE_URL must be set in .env file")
}

fn establish_connection() -> actix_diesel::Database<PgConnection> {
    let database_url = get_database_credentials();
    actix_diesel::Database::builder()
        .pool_max_size(30)
        .open(&database_url)
}

const SCAM_PROJECT: &str = "scam_project";
const INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);
const MAX_DELAY_TIME: std::time::Duration = std::time::Duration::from_secs(120);

fn main() {
    openssl_probe::init_ssl_cert_env_vars();

    let args: Vec<String> = std::env::args().collect();
    let home_dir = std::path::PathBuf::from(near_indexer::get_default_home());

    let mut env_filter = EnvFilter::new(
        "tokio_reactor=info,near=info,stats=info,telemetry=info,indexer=info,aggregated=info",
    );

    env_filter = env_filter.add_directive(
        "scam_project=info"
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

    let command = args
        .get(1)
        .map(|arg| arg.as_str())
        .expect("You need to provide a command: `init` or `run` as arg");

    // TODO: add reading the config from ENV
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
                download_config: false,
                download_config_url: Some("https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore-deploy/testnet/config.json".to_string()),
                boot_nodes: None,
                max_gas_burnt_view: None
            };
            near_indexer::indexer_init_configs(&home_dir, config_args).unwrap();
        }
        "run" => {
            let pool = establish_connection();
            let indexer_config = near_indexer::IndexerConfig {
                home_dir: std::path::PathBuf::from(near_indexer::get_default_home()),
                sync_mode: near_indexer::SyncModeEnum::FromInterruption,
                await_for_node_synced: near_indexer::AwaitForNodeSyncedEnum::WaitForFullSync,
            };
            let sys = actix::System::new();
            sys.block_on(async move {
                let indexer = near_indexer::Indexer::new(indexer_config).unwrap();
                let stream = indexer.streamer();
                listen_blocks(stream, pool).await;

                actix::System::current().stop();
            });
            sys.run().unwrap();
        }
        _ => panic!("You have to pass `init` or `run` arg"),
    }
}

async fn listen_blocks(
    mut stream: tokio::sync::mpsc::Receiver<near_indexer::StreamerMessage>,
    pool: Database<PgConnection>,
) {
    while let Some(streamer_message) = stream.recv().await {
        extract_events(&pool, streamer_message).await.unwrap();
    }
}

async fn extract_events(
    pool: &Database<PgConnection>,
    msg: near_indexer::StreamerMessage,
) -> anyhow::Result<()> {
    let block_height = msg.block.header.height;
    let block_hash = msg.block.header.hash.to_string();
    let block_timestamp = msg.block.header.timestamp_nanosec;
    let block_epoch_id = msg.block.header.epoch_id.to_string();

    println!("BLOCK HEIGHT: {}", block_height);

    let mut events = vec![];
    for shard in msg.shards {
        for outcome in shard.receipt_execution_outcomes {
            let ReceiptView {
                predecessor_id,
                receiver_id: account_id,
                receipt_id,
                ..
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
            for (log_index, log) in logs.into_iter().enumerate() {
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

                // TODO: send all events to MQ
            }
        }
    }

    // TODO: leaving it here for testing purposes, but we can remove this code since we'll forward all events to MQ, not storing to DB
    crate::await_retry_or_panic!(
        diesel::insert_into(schema::events::table)
            .values(events.clone())
            .on_conflict_do_nothing()
            .execute_async(&pool),
        10,
        "Scam insert foilureee".to_string(),
        &events
    );

    Ok(())
}
