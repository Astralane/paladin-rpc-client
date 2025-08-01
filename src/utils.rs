use crate::leader_tracker::types::PaladinSocketAddrs;

pub trait PalidatorTracker {
    fn next_leaders(&self, lookahead_leaders: usize) -> Vec<PaladinSocketAddrs>;
    fn stop(&mut self);
}
