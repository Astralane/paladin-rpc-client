use bytes::Bytes;
use solana_sdk::signature::Signature;
use std::time::SystemTime;

pub struct VerifiedTransaction {
    pub wire_transaction: Bytes,
    pub revert_protect: bool,
    signature: Option<Signature>,
    api_key: Option<String>,
    pub recv_timestamp: SystemTime,
    recv_slot: u64,
}

impl VerifiedTransaction {
    pub fn from_transaction(
        transaction: Vec<u8>,
        revert_protect: bool,
        signature: Option<Signature>,
        api_key: Option<String>,
        recv_slot: u64,
    ) -> Self {
        VerifiedTransaction {
            wire_transaction: Bytes::from(transaction),
            revert_protect,
            signature,
            api_key,
            recv_timestamp: SystemTime::now(),
            recv_slot,
        }
    }
}
