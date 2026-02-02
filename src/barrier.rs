//! Idempotency Barrier (Exactly-Once Processing)
//!
//! **Optimizations**:
//! - Per-sender sequence tracking (O(1) lookup)
//! - Gap detection for NACK requests
//! - Zero allocation on hot path
//!
//! > "Exactly-Once Delivery is impossible. Exactly-Once Processing is not."

use std::collections::HashMap;

/// Sender identifier (could be public key hash, node ID, etc.)
pub type SenderId = u64;

/// Sequence number
pub type SeqNum = u64;

/// Gap detection result
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GapResult {
    /// Sequence is next expected - process it
    Accept,
    /// Sequence is a duplicate - drop it
    Duplicate,
    /// Sequence is ahead - we have a gap, need to NACK for missing
    Gap { missing_start: SeqNum, missing_end: SeqNum },
}

/// Per-sender sequence tracker
#[derive(Debug, Default)]
struct SenderState {
    /// Last processed sequence number
    last_seen: SeqNum,
    /// Initialized flag (first message sets the baseline)
    initialized: bool,
}

/// Idempotency Barrier
///
/// Tracks the last processed sequence number per sender.
/// Detects duplicates and gaps.
pub struct IdempotencyBarrier {
    /// Per-sender state
    senders: HashMap<SenderId, SenderState>,
}

impl IdempotencyBarrier {
    /// Create new barrier
    pub fn new() -> Self {
        Self {
            senders: HashMap::new(),
        }
    }

    /// Create with pre-allocated capacity
    pub fn with_capacity(sender_count: usize) -> Self {
        Self {
            senders: HashMap::with_capacity(sender_count),
        }
    }

    /// Check if a message should be processed
    ///
    /// Returns the gap result indicating whether to accept, drop, or NACK
    #[inline]
    pub fn check(&self, sender: SenderId, seq: SeqNum) -> GapResult {
        match self.senders.get(&sender) {
            None => {
                // First message from this sender
                GapResult::Accept
            }
            Some(state) => {
                if !state.initialized {
                    GapResult::Accept
                } else if seq <= state.last_seen {
                    GapResult::Duplicate
                } else if seq == state.last_seen + 1 {
                    GapResult::Accept
                } else {
                    // Gap detected
                    GapResult::Gap {
                        missing_start: state.last_seen + 1,
                        missing_end: seq,
                    }
                }
            }
        }
    }

    /// Mark a message as processed
    ///
    /// Call this AFTER successfully processing the message
    #[inline]
    pub fn mark_processed(&mut self, sender: SenderId, seq: SeqNum) {
        let state = self.senders.entry(sender).or_default();
        if !state.initialized || seq > state.last_seen {
            state.last_seen = seq;
            state.initialized = true;
        }
    }

    /// Check and mark in one operation (for simple cases)
    ///
    /// Returns true if message should be processed
    #[inline]
    pub fn check_and_mark(&mut self, sender: SenderId, seq: SeqNum) -> GapResult {
        let result = self.check(sender, seq);
        if matches!(result, GapResult::Accept) {
            self.mark_processed(sender, seq);
        }
        result
    }

    /// Get last seen sequence for a sender
    pub fn last_seen(&self, sender: SenderId) -> Option<SeqNum> {
        self.senders
            .get(&sender)
            .filter(|s| s.initialized)
            .map(|s| s.last_seen)
    }

    /// Reset state for a sender
    pub fn reset_sender(&mut self, sender: SenderId) {
        self.senders.remove(&sender);
    }

    /// Clear all state
    pub fn clear(&mut self) {
        self.senders.clear();
    }

    /// Number of tracked senders
    pub fn sender_count(&self) -> usize {
        self.senders.len()
    }
}

impl Default for IdempotencyBarrier {
    fn default() -> Self {
        Self::new()
    }
}

/// Batch idempotency checker for high-throughput scenarios
///
/// Processes multiple messages from the same sender efficiently
pub struct BatchBarrier {
    /// Inner barrier
    barrier: IdempotencyBarrier,
    /// Pending marks (deferred until batch commit)
    pending: Vec<(SenderId, SeqNum)>,
}

impl BatchBarrier {
    pub fn new() -> Self {
        Self {
            barrier: IdempotencyBarrier::new(),
            pending: Vec::new(),
        }
    }

    /// Check a message (does not mark as processed yet)
    #[inline]
    pub fn check(&self, sender: SenderId, seq: SeqNum) -> GapResult {
        self.barrier.check(sender, seq)
    }

    /// Mark a message for processing (will be committed in batch)
    #[inline]
    pub fn mark(&mut self, sender: SenderId, seq: SeqNum) {
        self.pending.push((sender, seq));
    }

    /// Commit all pending marks
    pub fn commit(&mut self) {
        for (sender, seq) in self.pending.drain(..) {
            self.barrier.mark_processed(sender, seq);
        }
    }

    /// Rollback pending marks (on error)
    pub fn rollback(&mut self) {
        self.pending.clear();
    }

    /// Get inner barrier reference
    pub fn inner(&self) -> &IdempotencyBarrier {
        &self.barrier
    }
}

impl Default for BatchBarrier {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_barrier_basic() {
        let mut barrier = IdempotencyBarrier::new();
        let sender = 1;

        // First message
        assert_eq!(barrier.check_and_mark(sender, 1), GapResult::Accept);

        // Sequential messages
        assert_eq!(barrier.check_and_mark(sender, 2), GapResult::Accept);
        assert_eq!(barrier.check_and_mark(sender, 3), GapResult::Accept);

        // Duplicate
        assert_eq!(barrier.check(sender, 2), GapResult::Duplicate);
        assert_eq!(barrier.check(sender, 1), GapResult::Duplicate);
    }

    #[test]
    fn test_barrier_gap_detection() {
        let mut barrier = IdempotencyBarrier::new();
        let sender = 1;

        barrier.check_and_mark(sender, 1);
        barrier.check_and_mark(sender, 2);

        // Skip 3, 4, send 5
        let result = barrier.check(sender, 5);
        assert_eq!(
            result,
            GapResult::Gap {
                missing_start: 3,
                missing_end: 5
            }
        );

        // Fill the gap
        barrier.check_and_mark(sender, 3);
        barrier.check_and_mark(sender, 4);
        barrier.check_and_mark(sender, 5);

        assert_eq!(barrier.last_seen(sender), Some(5));
    }

    #[test]
    fn test_barrier_multiple_senders() {
        let mut barrier = IdempotencyBarrier::new();

        // Different senders have independent sequences
        assert_eq!(barrier.check_and_mark(1, 1), GapResult::Accept);
        assert_eq!(barrier.check_and_mark(2, 1), GapResult::Accept);
        assert_eq!(barrier.check_and_mark(1, 2), GapResult::Accept);
        assert_eq!(barrier.check_and_mark(2, 2), GapResult::Accept);

        assert_eq!(barrier.last_seen(1), Some(2));
        assert_eq!(barrier.last_seen(2), Some(2));
        assert_eq!(barrier.sender_count(), 2);
    }

    #[test]
    fn test_barrier_reset() {
        let mut barrier = IdempotencyBarrier::new();
        let sender = 1;

        barrier.check_and_mark(sender, 1);
        barrier.check_and_mark(sender, 2);

        barrier.reset_sender(sender);

        // After reset, can start fresh
        assert_eq!(barrier.check_and_mark(sender, 1), GapResult::Accept);
    }

    #[test]
    fn test_batch_barrier() {
        let mut barrier = BatchBarrier::new();
        let sender = 1;

        // Check without committing
        assert_eq!(barrier.check(sender, 1), GapResult::Accept);
        barrier.mark(sender, 1);

        // Before commit, duplicate check should still accept
        // (because mark is pending)
        // Actually, check uses inner barrier which isn't updated yet

        barrier.commit();

        // After commit, duplicate should be detected
        assert_eq!(barrier.check(sender, 1), GapResult::Duplicate);
    }

    #[test]
    fn test_batch_barrier_rollback() {
        let mut barrier = BatchBarrier::new();
        let sender = 1;

        barrier.mark(sender, 1);
        barrier.mark(sender, 2);
        barrier.rollback();

        // After rollback, nothing was committed
        assert_eq!(barrier.check(sender, 1), GapResult::Accept);
    }
}
