use crate::ExecutionOutcomeStatus;
use bigdecimal::BigDecimal;

use crate::schema;
use schema::receipts;

#[derive(Insertable, Clone, Debug, Queryable)]
pub struct Receipt {
    pub block_height: BigDecimal,
    pub block_hash: String,
    pub block_timestamp: BigDecimal,
    pub block_epoch_id: String,
    pub outcome_index: i32,
    pub receipt_id: String,
    pub index_in_receipt: i32,
    pub signer_public_key: String,
    pub signer_id: String,
    pub predecessor_id: String,
    pub account_id: String,
    pub status: ExecutionOutcomeStatus,
    pub deposit: BigDecimal,
    pub gas: BigDecimal,
    pub method_name: String,
    pub args: String,
}
