use crate::leader_tracker::types::PaladinSocketAddrs;
use auto_impl::auto_impl;
use paladin_lockup_program::state::LockupPool;
use solana_sdk::pubkey::Pubkey;
use spl_discriminator::SplDiscriminate;
use std::collections::HashMap;

#[auto_impl(Arc)]
pub trait PalidatorTracker {
    fn next_leaders(&self, lookahead_leaders: usize) -> Vec<PaladinSocketAddrs>;
}

pub fn try_deserialize_lockup_pool(data: &[u8]) -> Option<&LockupPool> {
    if data.len() < 8 || &data[0..8] != LockupPool::SPL_DISCRIMINATOR.as_slice() {
        return None;
    }
    bytemuck::try_from_bytes::<LockupPool>(data).ok()
}

pub fn get_stakes(pool: &LockupPool) -> HashMap<Pubkey, u64> {
    let mut stake_total = 0;
    let stakes = pool
        .entries
        .iter()
        .take_while(|entry| {
            stake_total += entry.amount;

            // Take while lockup is initialized and the locked amount exceeds
            // 1% of total stake.
            entry.lockup != Pubkey::default()
                && entry.amount > 0
                && entry.amount * 100 / stake_total > 1
        })
        .filter(|entry| entry.metadata != [0; 32])
        .map(|entry| (Pubkey::new_from_array(entry.metadata), entry.amount))
        .fold(
            HashMap::with_capacity(pool.entries_len),
            |mut map, (key, stake)| {
                *map.entry(key).or_default() += stake;

                map
            },
        );
    stakes
}