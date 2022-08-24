mod models;

use nats::jetstream::JetStream;
use near_indexer::near_primitives::views::{
    ExecutionOutcomeView, ExecutionStatusView, ReceiptView,
};
use tracing_subscriber::EnvFilter;

use crate::models::{Event, EventType, ExecutionOutcomeStatus};

fn establish_connection() -> JetStream {
    let nats_url = if let Ok(nats_url) = std::env::var("NATS_URL") {
        nats_url
    } else {
        "127.0.0.1:4222".to_string()
    };
    let nc = nats::connect(nats_url);
    let js = nats::jetstream::new(nc.unwrap());
    js.add_stream("events").unwrap();

    return js;
}

fn main() {
    openssl_probe::init_ssl_cert_env_vars();

    let args: Vec<String> = std::env::args().collect();
    let home_dir = std::path::PathBuf::from(near_indexer::get_default_home());

    let mut env_filter = EnvFilter::new(
        "tokio_reactor=info,near=info,stats=info,telemetry=info,indexer=info,aggregated=info",
    );

    env_filter = env_filter.add_directive(
        "events_indexer=info"
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

    match command {
        "init" => {
            let config_args = near_indexer::InitConfigArgs {
                chain_id: read_env("CHAIN_ID"),
                account_id: None,
                test_seed: None,
                num_shards: 4, // TODO make configurable
                fast: false,
                genesis: None,
                download_genesis: false,
                download_genesis_url: read_env("DOWNLOAD_GENESIS_URL"),
                download_config: false,
                download_config_url: read_env("DOWNLOAD_CONFIG_URL"),
                boot_nodes: None,
                max_gas_burnt_view: None
            };
            near_indexer::indexer_init_configs(&home_dir, config_args).unwrap();
        }
        "run" => {
            let queue = establish_connection();
            let indexer_config = near_indexer::IndexerConfig {
                home_dir: std::path::PathBuf::from(near_indexer::get_default_home()),
                sync_mode: near_indexer::SyncModeEnum::FromInterruption,
                await_for_node_synced: near_indexer::AwaitForNodeSyncedEnum::StreamWhileSyncing,
            };
            let sys = actix::System::new();
            sys.block_on(async move {
                let indexer = near_indexer::Indexer::new(indexer_config).unwrap();
                let stream = indexer.streamer();
                listen_blocks(stream, queue).await;

                actix::System::current().stop();
            });
            sys.run().unwrap();
        }
        _ => panic!("You have to pass `init` or `run` arg"),
    }
}

fn read_env(key: &str) -> Option<String> {
    if let Ok(value) = std::env::var(key) {
        Some(value)
    } else {
        None
    }
}

async fn listen_blocks(
    mut stream: tokio::sync::mpsc::Receiver<near_indexer::StreamerMessage>,
    queue: JetStream,
) {
    while let Some(streamer_message) = stream.recv().await {
        extract_events(&queue, streamer_message).await.unwrap();
    }
}
const EVENT_LOG_PREFIX: &str = "EVENT_JSON:";
const CALIMERO_EVENT_LOG_PREFIX: &str = "CALIMERO_EVENT:";

async fn extract_events(
    queue: &JetStream,
    msg: near_indexer::StreamerMessage,
) -> anyhow::Result<()> {
    let block_height = msg.block.header.height;
    let block_hash = msg.block.header.hash.to_string();
    let block_timestamp = msg.block.header.timestamp_nanosec;
    let block_epoch_id = msg.block.header.epoch_id.to_string();

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
                if log.starts_with(EVENT_LOG_PREFIX) || log.starts_with(CALIMERO_EVENT_LOG_PREFIX) {
                    let event_type = if log.starts_with(EVENT_LOG_PREFIX) {
                        EventType::Near
                    } else {
                        EventType::Calimero
                    };
                    let subject = read_env("NATS_SUBJECT").unwrap_or("events".to_string());
                    let event = Event {
                        block_height: block_height.into(),
                        block_hash: block_hash.clone(),
                        block_timestamp: block_timestamp.into(),
                        block_epoch_id: block_epoch_id.clone(),
                        receipt_id: receipt_id.clone(),
                        log_index: log_index as i32,
                        predecessor_id: predecessor_id.clone(),
                        account_id: account_id.clone(),
                        status: status.clone(),
                        event: log.as_str().to_string(),
                        event_type,
                    };
                    // TODO handle Err
                    queue
                        .publish(&subject, serde_json::to_vec(&event).unwrap())
                        .ok();
                }
            }
        }
    }

    Ok(())
}
