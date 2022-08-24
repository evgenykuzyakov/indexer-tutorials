use serde::{Serialize, Deserialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ExecutionOutcomeStatus {
    Failure,
    Success,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum EventType {
    Near,
    Calimero,
}


#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Event {
    pub block_height: u64,
    pub block_hash: String,
    pub block_timestamp: u64,
    pub block_epoch_id: String,
    pub receipt_id: String,
    pub log_index: i32,
    pub predecessor_id: String,
    pub account_id: String,
    pub status: ExecutionOutcomeStatus,
    pub event: String,
    pub event_type: EventType,
}
