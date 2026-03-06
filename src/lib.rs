// テストコードではformat!やリテラル等でpedantic警告が出るため抑制
#![cfg_attr(
    test,
    allow(
        clippy::uninlined_format_args,
        clippy::unreadable_literal,
        clippy::identity_op,
        clippy::manual_assert,
        clippy::range_plus_one,
        unused_must_use,
    )
)]
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::cast_lossless,
    clippy::similar_names,
    clippy::many_single_char_names,
    clippy::module_name_repetitions,
    clippy::inline_always,
    clippy::too_many_lines
)]

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

#[cfg(feature = "std")]
pub mod recovery;
#[cfg(feature = "std")]
pub mod replay;

#[cfg(feature = "crypto")]
pub mod crypto_bridge;

#[cfg(feature = "text")]
pub mod text_bridge;

// Re-exports
pub use barrier::{BatchBarrier, GapResult, IdempotencyBarrier, SenderId, SeqNum};
pub use clock::{HybridClock, VectorClock, MAX_NODES};
pub use message::{flags, Message, MessageBuilder, MessageHeader, MessageId, SenderKey};
pub use ring::{BatchConsumer, MpmcBatchConsumer, MpmcRing, PaddedAtomicU64, RingBuffer, RingSlot};

#[cfg(feature = "std")]
pub use journal::{Journal, JournalIter};

#[cfg(feature = "std")]
pub use recovery::{recover, validate_journal, RecoveryResult, ValidationResult};
#[cfg(feature = "std")]
pub use replay::{replay_journal, replay_to_vec, ReplayStats};

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
    #[must_use]
    pub fn new() -> Self {
        Self {
            ring: RingBuffer::new(),
            barrier: IdempotencyBarrier::new(),
        }
    }

    /// Enqueue a message
    ///
    /// # Errors
    ///
    /// Returns the message back if the ring buffer is full.
    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn enqueue(&self, msg: Message) -> Result<u64, Message> {
        self.ring.try_push(msg)
    }

    /// Dequeue and process with idempotency check
    ///
    /// Returns `Ok(Some((message, GapResult)))` if a message was available,
    /// `Ok(None)` if the queue is empty, or `Err` if the message header is
    /// malformed (e.g. sender key too short to derive a `SenderId`).
    ///
    /// # Errors
    ///
    /// Returns an error string if the sender key cannot be converted to a `SenderId`.
    #[inline]
    pub fn dequeue(&mut self) -> Result<Option<(Message, GapResult)>, &'static str> {
        let Some(msg) = self.ring.try_pop() else {
            return Ok(None);
        };
        let sender_id = Self::sender_to_id(&msg.header.sender)?;
        let result = self.barrier.check_and_mark(sender_id, msg.header.seq);
        Ok(Some((msg, result)))
    }

    /// Peek without removing (checks next message)
    pub const fn peek_result(&self) -> Option<GapResult> {
        // Can't easily peek ring buffer, so this is a no-op
        // In real implementation, would need separate peek
        None
    }

    /// Convert sender key to ID (first 8 bytes as u64)
    ///
    /// Returns an error if the sender key is shorter than 8 bytes.
    /// In practice `SenderKey` is always `[u8; 32]`, so this never fails
    /// for well-formed keys; the `Result` protects callers from future
    /// type changes and avoids a panic on the hot dequeue path.
    #[inline]
    fn sender_to_id(sender: &SenderKey) -> Result<SenderId, &'static str> {
        sender[0..8]
            .try_into()
            .map(u64::from_le_bytes)
            .map_err(|_| "sender key too short to derive SenderId")
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
        let (msg, result) = queue.dequeue().unwrap().unwrap();
        assert_eq!(result, GapResult::Accept);
        assert_eq!(msg.payload, b"first");

        let (msg, result) = queue.dequeue().unwrap().unwrap();
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

        let (_, result1) = queue.dequeue().unwrap().unwrap();
        assert_eq!(result1, GapResult::Accept);

        let (_, result2) = queue.dequeue().unwrap().unwrap();
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

        let (_, result1) = queue.dequeue().unwrap().unwrap();
        assert_eq!(result1, GapResult::Accept);

        let (_, result3) = queue.dequeue().unwrap().unwrap();
        assert!(matches!(
            result3,
            GapResult::Gap {
                missing_start: 2,
                missing_end: 3
            }
        ));
    }

    #[test]
    fn test_integration() {
        let ring: RingBuffer<Message, 1024> = RingBuffer::new();

        let sender = [1u8; 32];
        let msg = Message::new(sender, 1, b"hello".to_vec());

        ring.try_push(msg).unwrap();

        let mut barrier = IdempotencyBarrier::new();
        if let Some(msg) = ring.try_pop() {
            let sender_id = u64::from_le_bytes(msg.header.sender[0..8].try_into().unwrap());
            match barrier.check_and_mark(sender_id, msg.header.seq) {
                GapResult::Accept => assert_eq!(msg.payload, b"hello"),
                _ => panic!("Expected Accept"),
            }
        }
    }

    #[test]
    fn test_queue_default_trait() {
        let queue = AliceQueue::<16>::default();
        assert!(queue.is_empty());
        assert_eq!(queue.len(), 0);
    }

    #[test]
    fn test_queue_dequeue_empty() {
        let mut queue = AliceQueue::<16>::new();
        let result = queue.dequeue().unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_queue_peek_result_returns_none() {
        let queue = AliceQueue::<16>::new();
        assert!(queue.peek_result().is_none());

        // Even after enqueue
        let sender = test_sender(1);
        queue
            .enqueue(Message::new(sender, 1, b"data".to_vec()))
            .unwrap();
        assert!(queue.peek_result().is_none());
    }

    #[test]
    fn test_queue_multiple_senders() {
        let mut queue = AliceQueue::<16>::new();
        let sender_a = test_sender(1);
        let sender_b = test_sender(2);

        queue
            .enqueue(Message::new(sender_a, 1, b"a1".to_vec()))
            .unwrap();
        queue
            .enqueue(Message::new(sender_b, 1, b"b1".to_vec()))
            .unwrap();
        queue
            .enqueue(Message::new(sender_a, 2, b"a2".to_vec()))
            .unwrap();

        let (msg, result) = queue.dequeue().unwrap().unwrap();
        assert_eq!(result, GapResult::Accept);
        assert_eq!(msg.payload, b"a1");

        let (msg, result) = queue.dequeue().unwrap().unwrap();
        assert_eq!(result, GapResult::Accept);
        assert_eq!(msg.payload, b"b1");

        let (msg, result) = queue.dequeue().unwrap().unwrap();
        assert_eq!(result, GapResult::Accept);
        assert_eq!(msg.payload, b"a2");
    }

    #[test]
    fn test_queue_enqueue_returns_position() {
        let queue = AliceQueue::<16>::new();
        let sender = test_sender(1);

        let pos0 = queue
            .enqueue(Message::new(sender, 1, b"first".to_vec()))
            .unwrap();
        let pos1 = queue
            .enqueue(Message::new(sender, 2, b"second".to_vec()))
            .unwrap();

        assert_eq!(pos0, 0);
        assert_eq!(pos1, 1);
    }

    #[test]
    fn test_queue_full_rejects() {
        let queue = AliceQueue::<4>::new();
        let sender = test_sender(1);

        for i in 0..4 {
            queue
                .enqueue(Message::new(sender, i + 1, b"data".to_vec()))
                .unwrap();
        }

        let result = queue.enqueue(Message::new(sender, 5, b"overflow".to_vec()));
        assert!(result.is_err());
    }

    #[test]
    fn test_version_constant() {
        assert!(!VERSION.is_empty());
        assert!(VERSION.contains("0.1.0"));
    }

    #[test]
    fn test_sender_to_id_consistency() {
        // 最初の 8 バイトが同じなら同一 ID にマップされる
        let mut sender1 = [0u8; 32];
        sender1[0] = 42;
        let mut sender2 = [0u8; 32];
        sender2[0] = 42;
        sender2[31] = 99; // 先頭 8 バイト以降が異なる

        let id1 = AliceQueue::<4>::sender_to_id(&sender1).unwrap();
        let id2 = AliceQueue::<4>::sender_to_id(&sender2).unwrap();
        assert_eq!(id1, id2);

        // 先頭 8 バイトが異なれば別の ID になる
        let mut sender3 = [0u8; 32];
        sender3[0] = 43;
        let id3 = AliceQueue::<4>::sender_to_id(&sender3).unwrap();
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_sender_to_id_little_endian() {
        // バイト順が little-endian であることを確認する
        let mut sender = [0u8; 32];
        sender[0] = 0x01;
        // 先頭 8 バイトは [0x01, 0x00, 0x00, ...] → LE u64 = 1
        let id = AliceQueue::<4>::sender_to_id(&sender).unwrap();
        assert_eq!(id, 1u64);
    }

    #[test]
    fn test_queue_drain_then_refill() {
        // キューを空にした後に再度エンキューできる
        let mut queue = AliceQueue::<8>::new();
        let sender = test_sender(1);

        for i in 1u64..=4 {
            queue
                .enqueue(Message::new(sender, i, b"first-round".to_vec()))
                .unwrap();
        }
        while queue.dequeue().unwrap().is_some() {}
        assert!(queue.is_empty());

        // 2 ラウンド目: seq を続きから送る
        for i in 5u64..=8 {
            queue
                .enqueue(Message::new(sender, i, b"second-round".to_vec()))
                .unwrap();
        }
        assert_eq!(queue.len(), 4);

        let (msg, result) = queue.dequeue().unwrap().unwrap();
        assert_eq!(result, GapResult::Accept);
        assert_eq!(msg.header.seq, 5);
    }

    #[test]
    fn test_queue_gap_and_fill() {
        // ギャップを検出した後、欠落分を埋めると再び Accept される
        let mut queue = AliceQueue::<16>::new();
        let sender = test_sender(5);

        queue
            .enqueue(Message::new(sender, 1, b"one".to_vec()))
            .unwrap();
        queue
            .enqueue(Message::new(sender, 4, b"four".to_vec()))
            .unwrap(); // seq 2, 3 が欠落

        let (_, r1) = queue.dequeue().unwrap().unwrap();
        assert_eq!(r1, GapResult::Accept);

        let (_, r4) = queue.dequeue().unwrap().unwrap();
        assert!(matches!(
            r4,
            GapResult::Gap {
                missing_start: 2,
                missing_end: 4
            }
        ));

        // 欠落分を埋める（barrier は Gap を返すだけで last_seen を進めないため）
        queue
            .enqueue(Message::new(sender, 2, b"two".to_vec()))
            .unwrap();
        let (_, r2) = queue.dequeue().unwrap().unwrap();
        assert_eq!(r2, GapResult::Accept);
    }
}
