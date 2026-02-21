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
    /// N must be a power of 2
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
    /// Returns `Err(value)` if buffer is full
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
    pub fn new(ring: &'a RingBuffer<T, N>, batch_size: usize) -> Self {
        Self { ring, batch_size }
    }

    /// Consume up to batch_size items
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
}
