//! Lock-Free Ring Buffer (Disruptor Style)
//!
//! **Optimizations**:
//! - Cache-line padding (64 bytes) to prevent False Sharing
//! - Single Producer, Multiple Consumer (SPMC) or MPMC modes
//! - No locks, pure atomic operations
//! - Pre-allocated fixed memory region
//!
//! > "Context switches are the enemy of latency."

use core::cell::UnsafeCell;
use core::sync::atomic::{AtomicU64, Ordering};

/// Cache line size (64 bytes on most modern CPUs)
const CACHE_LINE: usize = 64;

/// Padded atomic counter to prevent false sharing
#[repr(C, align(64))]
pub struct PaddedAtomicU64 {
    value: AtomicU64,
    _pad: [u8; CACHE_LINE - 8],
}

impl PaddedAtomicU64 {
    #[must_use]
    pub const fn new(v: u64) -> Self {
        Self {
            value: AtomicU64::new(v),
            _pad: [0u8; CACHE_LINE - 8],
        }
    }

    #[inline]
    pub fn load(&self, ordering: Ordering) -> u64 {
        self.value.load(ordering)
    }

    #[inline]
    pub fn store(&self, val: u64, ordering: Ordering) {
        self.value.store(val, ordering);
    }

    #[inline]
    pub fn fetch_add(&self, val: u64, ordering: Ordering) -> u64 {
        self.value.fetch_add(val, ordering)
    }

    /// Compare-and-swap the counter value.
    ///
    /// # Errors
    ///
    /// Returns the current value if the exchange fails.
    #[inline]
    pub fn compare_exchange(
        &self,
        current: u64,
        new: u64,
        success: Ordering,
        failure: Ordering,
    ) -> Result<u64, u64> {
        self.value.compare_exchange(current, new, success, failure)
    }
}

/// Slot status for MPMC coordination
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SlotState {
    Empty = 0,
    Writing = 1,
    Ready = 2,
    Reading = 3,
}

/// Ring buffer entry with state tracking
#[repr(C)]
pub struct RingSlot<T> {
    /// Sequence number for this slot
    sequence: AtomicU64,
    /// Slot data
    data: UnsafeCell<Option<T>>,
}

impl<T> RingSlot<T> {
    pub const fn empty(seq: u64) -> Self {
        Self {
            sequence: AtomicU64::new(seq),
            data: UnsafeCell::new(None),
        }
    }
}

/// Lock-Free Ring Buffer (Disruptor Pattern)
///
/// Single-Producer Single-Consumer (SPSC) for maximum performance.
/// Use multiple rings for MPMC scenarios.
pub struct RingBuffer<T, const N: usize> {
    /// Producer cursor (next write position)
    /// Cache-line padded to prevent false sharing
    head: PaddedAtomicU64,
    /// Consumer cursor (next read position)
    /// Cache-line padded to prevent false sharing
    tail: PaddedAtomicU64,
    /// Ring buffer slots (power of 2 for fast modulo)
    slots: Box<[RingSlot<T>; N]>,
    /// Mask for fast modulo (N - 1)
    mask: u64,
}

impl<T, const N: usize> RingBuffer<T, N> {
    /// Create new ring buffer
    ///
    /// N must be a power of 2.
    ///
    /// # Panics
    ///
    /// Panics if `N` is not a power of two or is less than 2.
    #[must_use]
    pub fn new() -> Self {
        assert!(N.is_power_of_two(), "Ring size must be power of 2");
        assert!(N >= 2, "Ring size must be at least 2");

        // Initialize slots with sequence numbers.
        // The Vec is constructed with exactly N elements, so converting the
        // boxed slice to a `Box<[RingSlot<T>; N]>` is guaranteed to succeed.
        // We use `expect` here instead of a silent `unwrap` so that any future
        // mismatch (e.g. a const-generic change) produces an actionable message.
        let slots: Vec<RingSlot<T>> = (0..N).map(|i| RingSlot::empty(i as u64)).collect();
        let slots: Box<[RingSlot<T>; N]> = slots
            .into_boxed_slice()
            .try_into()
            .unwrap_or_else(|_| panic!("RingBuffer: slot Vec length must equal const N"));

        Self {
            head: PaddedAtomicU64::new(0),
            tail: PaddedAtomicU64::new(0),
            slots,
            mask: (N - 1) as u64,
        }
    }

    /// Try to publish a value (non-blocking)
    ///
    /// # Errors
    ///
    /// Returns `Err(value)` if buffer is full.
    #[inline]
    pub fn try_push(&self, value: T) -> Result<u64, T> {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Acquire);

        // Check if buffer is full
        if head - tail >= N as u64 {
            return Err(value);
        }

        let slot_idx = (head & self.mask) as usize;
        let slot = &self.slots[slot_idx];

        // Write data
        unsafe {
            *slot.data.get() = Some(value);
        }

        // Publish: update sequence to signal readiness
        slot.sequence.store(head + 1, Ordering::Release);

        // Advance head
        self.head.store(head + 1, Ordering::Release);

        Ok(head)
    }

    /// Try to consume a value (non-blocking)
    ///
    /// Returns `None` if buffer is empty
    #[inline]
    pub fn try_pop(&self) -> Option<T> {
        let tail = self.tail.load(Ordering::Relaxed);
        let slot_idx = (tail & self.mask) as usize;
        let slot = &self.slots[slot_idx];

        // Check if slot is ready
        let seq = slot.sequence.load(Ordering::Acquire);
        if seq != tail + 1 {
            return None; // Not ready yet
        }

        // Read data
        let value = unsafe { (*slot.data.get()).take() };

        // Reset sequence for next round
        slot.sequence.store(tail + N as u64, Ordering::Release);

        // Advance tail
        self.tail.store(tail + 1, Ordering::Release);

        value
    }

    /// Current number of items in buffer
    #[inline]
    pub fn len(&self) -> usize {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Relaxed);
        (head - tail) as usize
    }

    /// Check if buffer is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Check if buffer is full
    #[inline]
    pub fn is_full(&self) -> bool {
        self.len() >= N
    }

    /// Buffer capacity
    #[inline]
    pub const fn capacity(&self) -> usize {
        N
    }

    /// Current head position (next write)
    #[inline]
    pub fn head_position(&self) -> u64 {
        self.head.load(Ordering::Relaxed)
    }

    /// Current tail position (next read)
    #[inline]
    pub fn tail_position(&self) -> u64 {
        self.tail.load(Ordering::Relaxed)
    }
}

impl<T, const N: usize> Default for RingBuffer<T, N> {
    fn default() -> Self {
        Self::new()
    }
}

// Safety: RingBuffer is safe to share across threads
unsafe impl<T: Send, const N: usize> Send for RingBuffer<T, N> {}
unsafe impl<T: Send, const N: usize> Sync for RingBuffer<T, N> {}

/// Batch consumer for amortized syscall overhead
pub struct BatchConsumer<'a, T, const N: usize> {
    ring: &'a RingBuffer<T, N>,
    batch_size: usize,
}

impl<'a, T, const N: usize> BatchConsumer<'a, T, N> {
    pub const fn new(ring: &'a RingBuffer<T, N>, batch_size: usize) -> Self {
        Self { ring, batch_size }
    }

    /// Consume up to `batch_size` items
    pub fn consume_batch(&self, buffer: &mut Vec<T>) {
        buffer.clear();
        for _ in 0..self.batch_size {
            match self.ring.try_pop() {
                Some(item) => buffer.push(item),
                None => break,
            }
        }
    }
}

/// MPMC 対応リングバッファ。
///
/// CAS ループによるマルチプロデューサ・マルチコンシューマ対応。
/// SPSC より若干オーバーヘッドが大きいが、複数スレッドから
/// 同時に push/pop が可能。
pub struct MpmcRing<T, const N: usize> {
    head: PaddedAtomicU64,
    tail: PaddedAtomicU64,
    slots: Box<[RingSlot<T>; N]>,
    mask: u64,
}

impl<T, const N: usize> MpmcRing<T, N> {
    /// 新しい MPMC リングバッファを作成する。
    ///
    /// # Panics
    ///
    /// `N` が 2 のべき乗でない、または 2 未満の場合。
    #[must_use]
    pub fn new() -> Self {
        assert!(N.is_power_of_two(), "Ring size must be power of 2");
        assert!(N >= 2, "Ring size must be at least 2");

        let slots: Vec<RingSlot<T>> = (0..N).map(|i| RingSlot::empty(i as u64)).collect();
        let slots: Box<[RingSlot<T>; N]> = slots
            .into_boxed_slice()
            .try_into()
            .unwrap_or_else(|_| panic!("MpmcRing: slot Vec length must equal const N"));

        Self {
            head: PaddedAtomicU64::new(0),
            tail: PaddedAtomicU64::new(0),
            slots,
            mask: (N - 1) as u64,
        }
    }

    /// CAS ループで push（複数プロデューサ安全）。
    ///
    /// # Errors
    ///
    /// バッファ満杯時に値を返す。
    #[inline]
    pub fn try_push(&self, value: T) -> Result<u64, T> {
        loop {
            let head = self.head.load(Ordering::Acquire);
            let slot_idx = (head & self.mask) as usize;
            let slot = &self.slots[slot_idx];

            let seq = slot.sequence.load(Ordering::Acquire);
            if seq != head {
                // スロットが利用不可（満杯 or 別スレッドが書き込み中）
                return Err(value);
            }

            // CAS で head を獲得
            if self
                .head
                .compare_exchange(head, head + 1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                unsafe {
                    *slot.data.get() = Some(value);
                }
                slot.sequence.store(head + 1, Ordering::Release);
                return Ok(head);
            }
            // 別のプロデューサが先にヘッドを進めた → リトライ
            core::hint::spin_loop();
        }
    }

    /// CAS ループで pop（複数コンシューマ安全）。
    #[inline]
    pub fn try_pop(&self) -> Option<T> {
        loop {
            let tail = self.tail.load(Ordering::Acquire);
            let slot_idx = (tail & self.mask) as usize;
            let slot = &self.slots[slot_idx];

            let seq = slot.sequence.load(Ordering::Acquire);
            if seq != tail + 1 {
                // スロットが未書き込み（空 or 書き込み中）
                return None;
            }

            // CAS で tail を獲得
            if self
                .tail
                .compare_exchange(tail, tail + 1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                let value = unsafe { (*slot.data.get()).take() };
                slot.sequence.store(tail + N as u64, Ordering::Release);
                return value;
            }
            // 別のコンシューマが先に tail を進めた → リトライ
            core::hint::spin_loop();
        }
    }

    /// 現在のアイテム数。
    #[inline]
    pub fn len(&self) -> usize {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Relaxed);
        (head - tail) as usize
    }

    /// 空かどうか。
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 満杯かどうか。
    #[inline]
    pub fn is_full(&self) -> bool {
        self.len() >= N
    }

    /// キャパシティ。
    #[inline]
    pub const fn capacity(&self) -> usize {
        N
    }
}

impl<T, const N: usize> Default for MpmcRing<T, N> {
    fn default() -> Self {
        Self::new()
    }
}

unsafe impl<T: Send, const N: usize> Send for MpmcRing<T, N> {}
unsafe impl<T: Send, const N: usize> Sync for MpmcRing<T, N> {}

/// MPMC 対応バッチコンシューマ。
pub struct MpmcBatchConsumer<'a, T, const N: usize> {
    ring: &'a MpmcRing<T, N>,
    batch_size: usize,
}

impl<'a, T, const N: usize> MpmcBatchConsumer<'a, T, N> {
    pub const fn new(ring: &'a MpmcRing<T, N>, batch_size: usize) -> Self {
        Self { ring, batch_size }
    }

    /// 最大 `batch_size` 件を一括消費する。
    pub fn consume_batch(&self, buffer: &mut Vec<T>) {
        buffer.clear();
        for _ in 0..self.batch_size {
            match self.ring.try_pop() {
                Some(item) => buffer.push(item),
                None => break,
            }
        }
    }

    /// キュー内の全アイテムを消費する。
    pub fn consume_all(&self, buffer: &mut Vec<T>) {
        buffer.clear();
        while let Some(item) = self.ring.try_pop() {
            buffer.push(item);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ring_basic() {
        let ring: RingBuffer<u64, 8> = RingBuffer::new();

        assert!(ring.is_empty());
        assert_eq!(ring.capacity(), 8);

        // Push some values
        assert!(ring.try_push(1).is_ok());
        assert!(ring.try_push(2).is_ok());
        assert!(ring.try_push(3).is_ok());

        assert_eq!(ring.len(), 3);

        // Pop values
        assert_eq!(ring.try_pop(), Some(1));
        assert_eq!(ring.try_pop(), Some(2));
        assert_eq!(ring.try_pop(), Some(3));
        assert_eq!(ring.try_pop(), None);

        assert!(ring.is_empty());
    }

    #[test]
    fn test_ring_full() {
        let ring: RingBuffer<u64, 4> = RingBuffer::new();

        // Fill the buffer
        assert!(ring.try_push(1).is_ok());
        assert!(ring.try_push(2).is_ok());
        assert!(ring.try_push(3).is_ok());
        assert!(ring.try_push(4).is_ok());

        assert!(ring.is_full());

        // Should fail when full
        assert!(ring.try_push(5).is_err());

        // Pop one
        assert_eq!(ring.try_pop(), Some(1));

        // Now can push again
        assert!(ring.try_push(5).is_ok());
    }

    #[test]
    fn test_ring_wraparound() {
        let ring: RingBuffer<u64, 4> = RingBuffer::new();

        // Fill and drain multiple times
        for round in 0..10 {
            for i in 0..4 {
                assert!(ring.try_push(round * 4 + i).is_ok());
            }
            for i in 0..4 {
                assert_eq!(ring.try_pop(), Some(round * 4 + i));
            }
        }
    }

    #[test]
    fn test_ring_concurrent() {
        use std::sync::Arc;
        use std::thread;

        let ring = Arc::new(RingBuffer::<u64, 1024>::new());
        let ring_producer = Arc::clone(&ring);
        let ring_consumer = Arc::clone(&ring);

        let producer = thread::spawn(move || {
            for i in 0..10000 {
                while ring_producer.try_push(i).is_err() {
                    std::hint::spin_loop();
                }
            }
        });

        let consumer = thread::spawn(move || {
            let mut count = 0u64;
            let mut last = None;
            while count < 10000 {
                if let Some(val) = ring_consumer.try_pop() {
                    // Verify ordering
                    if let Some(prev) = last {
                        assert_eq!(val, prev + 1, "Out of order!");
                    }
                    last = Some(val);
                    count += 1;
                } else {
                    std::hint::spin_loop();
                }
            }
        });

        producer.join().unwrap();
        consumer.join().unwrap();
    }

    #[test]
    fn test_cache_line_padding() {
        // Verify padding works
        assert_eq!(
            std::mem::size_of::<PaddedAtomicU64>(),
            CACHE_LINE,
            "PaddedAtomicU64 should be cache-line sized"
        );
    }

    #[test]
    fn test_batch_consumer() {
        let ring: RingBuffer<u64, 16> = RingBuffer::new();

        for i in 0..10 {
            ring.try_push(i).unwrap();
        }

        let consumer = BatchConsumer::new(&ring, 4);
        let mut batch = Vec::new();

        consumer.consume_batch(&mut batch);
        assert_eq!(batch, vec![0, 1, 2, 3]);

        consumer.consume_batch(&mut batch);
        assert_eq!(batch, vec![4, 5, 6, 7]);

        consumer.consume_batch(&mut batch);
        assert_eq!(batch, vec![8, 9]);

        consumer.consume_batch(&mut batch);
        assert!(batch.is_empty());
    }

    #[test]
    fn test_pop_empty_returns_none() {
        let ring: RingBuffer<u64, 4> = RingBuffer::new();
        assert_eq!(ring.try_pop(), None);
        assert_eq!(ring.try_pop(), None);
    }

    #[test]
    fn test_push_returns_sequence() {
        let ring: RingBuffer<u64, 8> = RingBuffer::new();

        assert_eq!(ring.try_push(100).unwrap(), 0);
        assert_eq!(ring.try_push(200).unwrap(), 1);
        assert_eq!(ring.try_push(300).unwrap(), 2);
    }

    #[test]
    fn test_ring_minimum_size() {
        let ring: RingBuffer<u64, 2> = RingBuffer::new();
        assert_eq!(ring.capacity(), 2);

        assert!(ring.try_push(1).is_ok());
        assert!(ring.try_push(2).is_ok());
        assert!(ring.is_full());
        assert!(ring.try_push(3).is_err());

        assert_eq!(ring.try_pop(), Some(1));
        assert!(ring.try_push(3).is_ok());
        assert_eq!(ring.try_pop(), Some(2));
        assert_eq!(ring.try_pop(), Some(3));
        assert!(ring.is_empty());
    }

    #[test]
    fn test_len_is_full_is_empty_consistency() {
        let ring: RingBuffer<u64, 4> = RingBuffer::new();

        assert!(ring.is_empty());
        assert!(!ring.is_full());
        assert_eq!(ring.len(), 0);

        ring.try_push(1).unwrap();
        assert!(!ring.is_empty());
        assert!(!ring.is_full());
        assert_eq!(ring.len(), 1);

        ring.try_push(2).unwrap();
        ring.try_push(3).unwrap();
        ring.try_push(4).unwrap();
        assert!(!ring.is_empty());
        assert!(ring.is_full());
        assert_eq!(ring.len(), 4);

        ring.try_pop();
        assert!(!ring.is_empty());
        assert!(!ring.is_full());
        assert_eq!(ring.len(), 3);
    }

    #[test]
    fn test_head_tail_position_tracking() {
        let ring: RingBuffer<u64, 8> = RingBuffer::new();

        assert_eq!(ring.head_position(), 0);
        assert_eq!(ring.tail_position(), 0);

        ring.try_push(10).unwrap();
        ring.try_push(20).unwrap();
        assert_eq!(ring.head_position(), 2);
        assert_eq!(ring.tail_position(), 0);

        ring.try_pop();
        assert_eq!(ring.head_position(), 2);
        assert_eq!(ring.tail_position(), 1);
    }

    #[test]
    fn test_push_full_returns_original_value() {
        let ring: RingBuffer<String, 2> = RingBuffer::new();
        ring.try_push("a".to_string()).unwrap();
        ring.try_push("b".to_string()).unwrap();

        let err = ring.try_push("overflow".to_string()).unwrap_err();
        assert_eq!(err, "overflow");
    }

    #[test]
    fn test_default_trait() {
        let ring: RingBuffer<u64, 4> = RingBuffer::default();
        assert!(ring.is_empty());
        assert_eq!(ring.capacity(), 4);
    }

    #[test]
    fn test_batch_consumer_larger_than_available() {
        let ring: RingBuffer<u64, 16> = RingBuffer::new();
        ring.try_push(1).unwrap();
        ring.try_push(2).unwrap();

        let consumer = BatchConsumer::new(&ring, 100);
        let mut batch = Vec::new();
        consumer.consume_batch(&mut batch);
        assert_eq!(batch, vec![1, 2]);
    }

    #[test]
    fn test_batch_consumer_zero_batch_size() {
        let ring: RingBuffer<u64, 16> = RingBuffer::new();
        ring.try_push(1).unwrap();

        let consumer = BatchConsumer::new(&ring, 0);
        let mut batch = Vec::new();
        consumer.consume_batch(&mut batch);
        assert!(batch.is_empty());
        // アイテムはリングに残っているままのはず
        assert_eq!(ring.len(), 1);
    }

    #[test]
    fn test_padded_atomic_fetch_add() {
        // fetch_add は旧値を返し、内部は更新される
        let counter = PaddedAtomicU64::new(10);
        let old = counter.fetch_add(5, Ordering::Relaxed);
        assert_eq!(old, 10);
        assert_eq!(counter.load(Ordering::Relaxed), 15);
    }

    #[test]
    fn test_padded_atomic_compare_exchange_success() {
        // 期待値と一致する場合は交換成功
        let val = PaddedAtomicU64::new(42);
        let result = val.compare_exchange(42, 100, Ordering::SeqCst, Ordering::SeqCst);
        assert_eq!(result, Ok(42));
        assert_eq!(val.load(Ordering::Relaxed), 100);
    }

    #[test]
    fn test_padded_atomic_compare_exchange_failure() {
        // 期待値と不一致の場合は交換失敗し、現在値が Err で返る
        let val = PaddedAtomicU64::new(42);
        let result = val.compare_exchange(99, 100, Ordering::SeqCst, Ordering::SeqCst);
        assert_eq!(result, Err(42));
        assert_eq!(val.load(Ordering::Relaxed), 42);
    }

    #[test]
    fn test_padded_atomic_store_load() {
        let val = PaddedAtomicU64::new(0);
        val.store(u64::MAX, Ordering::Release);
        assert_eq!(val.load(Ordering::Acquire), u64::MAX);
    }

    #[test]
    fn test_slot_state_discriminant_values() {
        // ディスクリミナント値はプロトコルの一部のため固定化する
        assert_eq!(SlotState::Empty as u8, 0);
        assert_eq!(SlotState::Writing as u8, 1);
        assert_eq!(SlotState::Ready as u8, 2);
        assert_eq!(SlotState::Reading as u8, 3);
    }

    #[test]
    fn test_head_tail_advance_past_capacity() {
        // N 件以上 push/pop しても head/tail は単調増加する（ラップしない）
        let ring: RingBuffer<u64, 4> = RingBuffer::new();
        for i in 0..12u64 {
            ring.try_push(i).unwrap();
            ring.try_pop().unwrap();
        }
        assert_eq!(ring.head_position(), 12);
        assert_eq!(ring.tail_position(), 12);
        assert!(ring.is_empty());
    }

    #[test]
    fn test_ring_interleaved_push_pop() {
        // push と pop を交互に行い、スロットシーケンシングを検証する
        let ring: RingBuffer<u64, 8> = RingBuffer::new();
        for i in 0..32u64 {
            ring.try_push(i).unwrap();
            assert_eq!(ring.try_pop(), Some(i));
        }
        assert!(ring.is_empty());
    }

    // ================================================================
    // MPMC リングバッファ テスト
    // ================================================================

    #[test]
    fn test_mpmc_basic() {
        let ring: MpmcRing<u64, 8> = MpmcRing::new();
        assert!(ring.is_empty());
        assert_eq!(ring.capacity(), 8);

        ring.try_push(1).unwrap();
        ring.try_push(2).unwrap();
        ring.try_push(3).unwrap();
        assert_eq!(ring.len(), 3);

        assert_eq!(ring.try_pop(), Some(1));
        assert_eq!(ring.try_pop(), Some(2));
        assert_eq!(ring.try_pop(), Some(3));
        assert_eq!(ring.try_pop(), None);
        assert!(ring.is_empty());
    }

    #[test]
    fn test_mpmc_full() {
        let ring: MpmcRing<u64, 4> = MpmcRing::new();
        ring.try_push(1).unwrap();
        ring.try_push(2).unwrap();
        ring.try_push(3).unwrap();
        ring.try_push(4).unwrap();
        assert!(ring.is_full());
        assert!(ring.try_push(5).is_err());

        ring.try_pop();
        assert!(!ring.is_full());
        ring.try_push(5).unwrap();
    }

    #[test]
    fn test_mpmc_wraparound() {
        let ring: MpmcRing<u64, 4> = MpmcRing::new();
        for round in 0..10u64 {
            for i in 0..4 {
                ring.try_push(round * 4 + i).unwrap();
            }
            for i in 0..4 {
                assert_eq!(ring.try_pop(), Some(round * 4 + i));
            }
        }
    }

    #[test]
    fn test_mpmc_concurrent_spsc() {
        use std::sync::Arc;
        use std::thread;

        let ring = Arc::new(MpmcRing::<u64, 1024>::new());
        let r1 = Arc::clone(&ring);
        let r2 = Arc::clone(&ring);

        let producer = thread::spawn(move || {
            for i in 0..10000u64 {
                while r1.try_push(i).is_err() {
                    std::hint::spin_loop();
                }
            }
        });

        let consumer = thread::spawn(move || {
            let mut count = 0u64;
            let mut last = None;
            while count < 10000 {
                if let Some(val) = r2.try_pop() {
                    if let Some(prev) = last {
                        assert_eq!(val, prev + 1, "Out of order!");
                    }
                    last = Some(val);
                    count += 1;
                } else {
                    std::hint::spin_loop();
                }
            }
        });

        producer.join().unwrap();
        consumer.join().unwrap();
    }

    #[test]
    fn test_mpmc_concurrent_multi_producer() {
        use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
        use std::sync::Arc;
        use std::thread;

        let ring = Arc::new(MpmcRing::<u64, 1024>::new());
        let total = Arc::new(AtomicU64::new(0));
        let num_producers = 4;
        let items_per_producer = 500u64;
        let expected_total = num_producers as u64 * items_per_producer;

        // コンシューマを先に起動
        let t = Arc::clone(&total);
        let r = Arc::clone(&ring);
        let consumer = thread::spawn(move || {
            while t.load(AtomicOrdering::Acquire) < expected_total {
                if r.try_pop().is_some() {
                    t.fetch_add(1, AtomicOrdering::Release);
                } else {
                    std::hint::spin_loop();
                }
            }
        });

        // プロデューサを起動
        let mut handles = Vec::new();
        for _ in 0..num_producers {
            let r = Arc::clone(&ring);
            handles.push(thread::spawn(move || {
                for i in 0..items_per_producer {
                    while r.try_push(i).is_err() {
                        std::hint::spin_loop();
                    }
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
        consumer.join().unwrap();

        assert_eq!(total.load(AtomicOrdering::Relaxed), expected_total);
    }

    #[test]
    fn test_mpmc_default_trait() {
        let ring: MpmcRing<u64, 4> = MpmcRing::default();
        assert!(ring.is_empty());
        assert_eq!(ring.capacity(), 4);
    }

    #[test]
    fn test_mpmc_batch_consumer() {
        let ring: MpmcRing<u64, 16> = MpmcRing::new();
        for i in 0..10 {
            ring.try_push(i).unwrap();
        }

        let consumer = MpmcBatchConsumer::new(&ring, 4);
        let mut batch = Vec::new();

        consumer.consume_batch(&mut batch);
        assert_eq!(batch.len(), 4);

        consumer.consume_batch(&mut batch);
        assert_eq!(batch.len(), 4);

        consumer.consume_batch(&mut batch);
        assert_eq!(batch.len(), 2);

        consumer.consume_batch(&mut batch);
        assert!(batch.is_empty());
    }

    #[test]
    fn test_mpmc_batch_consume_all() {
        let ring: MpmcRing<u64, 16> = MpmcRing::new();
        for i in 0..7 {
            ring.try_push(i).unwrap();
        }

        let consumer = MpmcBatchConsumer::new(&ring, 4);
        let mut batch = Vec::new();

        consumer.consume_all(&mut batch);
        assert_eq!(batch.len(), 7);
        assert!(ring.is_empty());
    }

    #[test]
    fn test_mpmc_pop_empty() {
        let ring: MpmcRing<u64, 4> = MpmcRing::new();
        assert_eq!(ring.try_pop(), None);
    }
}
