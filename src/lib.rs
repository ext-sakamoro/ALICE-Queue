//! # ALICE-Queue (Optimized Edition)
//!
//! **Deterministic Zero-Copy Message Log**
//!
//! > "Time is just an increasing integer."
//!
//! ## Architecture
//!
//! - **Ring Buffer**: Lock-free SPSC with cache-line padding (Disruptor style)
//! - **WAL Journal**: Memory-mapped append-only log (zero-copy persistence)
//! - **Idempotency**: Per-sender sequence tracking for exactly-once processing
//! - **Vector Clock**: Causal ordering across distributed nodes
//! - **Message ID**: BLAKE3(sender + seq + payload) for deterministic deduplication
//!
//! ## Performance
//!
//! | Operation | Complexity | Notes |
//! |-----------|------------|-------|
//! | Push | O(1) | Lock-free atomic |
//! | Pop | O(1) | Lock-free atomic |
//! | Journal Write | O(1) | mmap memcpy |
//! | Dedup Check | O(1) | Seq comparison |
//!
//! ## Example
//!
//! ```
//! use alice_queue::{RingBuffer, Message, IdempotencyBarrier, GapResult};
//!
//! // Create ring buffer
//! let ring: RingBuffer<Message, 1024> = RingBuffer::new();
//!
//! // Create message
//! let sender = [1u8; 32];
//! let msg = Message::new(sender, 1, b"hello".to_vec());
//!
//! // Push to ring
//! ring.try_push(msg.clone()).unwrap();
//!
//! // Pop and check idempotency
//! let mut barrier = IdempotencyBarrier::new();
//! if let Some(msg) = ring.try_pop() {
//!     let sender_id = u64::from_le_bytes(msg.header.sender[0..8].try_into().unwrap());
//!     match barrier.check_and_mark(sender_id, msg.header.seq) {
//!         GapResult::Accept => println!("Process: {:?}", msg.payload),
//!         GapResult::Duplicate => println!("Drop duplicate"),
//!         GapResult::Gap { .. } => println!("Request retransmit"),
//!     }
//! }
//! ```

#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

pub mod barrier;
pub mod clock;
pub mod journal;
pub mod message;
pub mod ring;

#[cfg(feature = "crypto")]
pub mod crypto_bridge;

#[cfg(feature = "text")]
pub mod text_bridge;

// Re-exports
pub use barrier::{BatchBarrier, GapResult, IdempotencyBarrier, SenderId, SeqNum};
pub use clock::{HybridClock, VectorClock, MAX_NODES};
pub use message::{flags, Message, MessageBuilder, MessageHeader, MessageId, SenderKey};
pub use ring::{BatchConsumer, PaddedAtomicU64, RingBuffer, RingSlot};

#[cfg(feature = "std")]
pub use journal::{Journal, JournalIter};

/// Version
pub const VERSION: &str = "0.1.0-optimized";

/// Queue combining ring buffer with idempotency barrier
pub struct AliceQueue<const N: usize> {
    /// Ring buffer for in-memory queueing
    pub ring: RingBuffer<Message, N>,
    /// Idempotency barrier for exactly-once processing
    pub barrier: IdempotencyBarrier,
}

impl<const N: usize> AliceQueue<N> {
    /// Create new queue
    pub fn new() -> Self {
        Self {
            ring: RingBuffer::new(),
            barrier: IdempotencyBarrier::new(),
        }
    }

    /// Enqueue a message
    #[inline]
    pub fn enqueue(&self, msg: Message) -> Result<u64, Message> {
        self.ring.try_push(msg)
    }

    /// Dequeue and process with idempotency check
    ///
    /// Returns `Some((message, GapResult))` if a message was available
    #[inline]
    pub fn dequeue(&mut self) -> Option<(Message, GapResult)> {
        let msg = self.ring.try_pop()?;
        let sender_id = Self::sender_to_id(&msg.header.sender);
        let result = self.barrier.check_and_mark(sender_id, msg.header.seq);
        Some((msg, result))
    }

    /// Peek without removing (checks next message)
    pub fn peek_result(&self) -> Option<GapResult> {
        // Can't easily peek ring buffer, so this is a no-op
        // In real implementation, would need separate peek
        None
    }

    /// Convert sender key to ID (first 8 bytes as u64)
    #[inline]
    fn sender_to_id(sender: &SenderKey) -> SenderId {
        u64::from_le_bytes(sender[0..8].try_into().unwrap())
    }

    /// Queue length
    pub fn len(&self) -> usize {
        self.ring.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.ring.is_empty()
    }
}

impl<const N: usize> Default for AliceQueue<N> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_sender(id: u8) -> SenderKey {
        let mut key = [0u8; 32];
        key[0] = id;
        key
    }

    #[test]
    fn test_queue_basic() {
        let mut queue = AliceQueue::<16>::new();
        let sender = test_sender(1);

        // Enqueue messages
        let msg1 = Message::new(sender, 1, b"first".to_vec());
        let msg2 = Message::new(sender, 2, b"second".to_vec());

        queue.enqueue(msg1).unwrap();
        queue.enqueue(msg2).unwrap();

        assert_eq!(queue.len(), 2);

        // Dequeue with idempotency
        let (msg, result) = queue.dequeue().unwrap();
        assert_eq!(result, GapResult::Accept);
        assert_eq!(msg.payload, b"first");

        let (msg, result) = queue.dequeue().unwrap();
        assert_eq!(result, GapResult::Accept);
        assert_eq!(msg.payload, b"second");

        assert!(queue.is_empty());
    }

    #[test]
    fn test_queue_duplicate() {
        let mut queue = AliceQueue::<16>::new();
        let sender = test_sender(1);

        // Same message twice
        let msg1 = Message::new(sender, 1, b"data".to_vec());
        let msg2 = Message::new(sender, 1, b"data".to_vec()); // duplicate

        queue.enqueue(msg1).unwrap();
        queue.enqueue(msg2).unwrap();

        let (_, result1) = queue.dequeue().unwrap();
        assert_eq!(result1, GapResult::Accept);

        let (_, result2) = queue.dequeue().unwrap();
        assert_eq!(result2, GapResult::Duplicate);
    }

    #[test]
    fn test_queue_gap() {
        let mut queue = AliceQueue::<16>::new();
        let sender = test_sender(1);

        // Send seq 1, then skip to 3
        let msg1 = Message::new(sender, 1, b"one".to_vec());
        let msg3 = Message::new(sender, 3, b"three".to_vec());

        queue.enqueue(msg1).unwrap();
        queue.enqueue(msg3).unwrap();

        let (_, result1) = queue.dequeue().unwrap();
        assert_eq!(result1, GapResult::Accept);

        let (_, result3) = queue.dequeue().unwrap();
        assert!(matches!(result3, GapResult::Gap { missing_start: 2, missing_end: 3 }));
    }

    #[test]
    fn test_integration() {
        let ring: RingBuffer<Message, 1024> = RingBuffer::new();

        let sender = [1u8; 32];
        let msg = Message::new(sender, 1, b"hello".to_vec());

        ring.try_push(msg.clone()).unwrap();

        let mut barrier = IdempotencyBarrier::new();
        if let Some(msg) = ring.try_pop() {
            let sender_id = u64::from_le_bytes(msg.header.sender[0..8].try_into().unwrap());
            match barrier.check_and_mark(sender_id, msg.header.seq) {
                GapResult::Accept => assert_eq!(msg.payload, b"hello"),
                _ => panic!("Expected Accept"),
            }
        }
    }
}
