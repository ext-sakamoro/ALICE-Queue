# ALICE-Queue "Deep Fried"

**Deterministic Zero-Copy Message Log**

> "Time is just an increasing integer."

## Features

| Component | Algorithm | Complexity |
|-----------|-----------|------------|
| Ring Buffer | Lock-Free SPSC (Disruptor) | O(1), cache-line padded |
| WAL Journal | mmap Append-Only | O(1), zero-copy |
| Idempotency | Per-Sender Seq Tracking | O(1) duplicate detection |
| Ordering | Vector Clock / HLC | O(N) merge, N = nodes |
| Message ID | BLAKE3(sender + seq + payload) | O(len) deterministic |

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      AliceQueue                             │
├─────────────────────────────────────────────────────────────┤
│  ┌──────────────────────────────────────────────────────┐   │
│  │              RingBuffer<Message, N>                  │   │
│  │  [Head: PaddedAtomicU64] ←64B pad→ [Tail: PaddedU64] │   │
│  │         ↓ push                    ↓ pop              │   │
│  │  ┌─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┐   │   │
│  │  │Slot0│Slot1│Slot2│Slot3│Slot4│Slot5│Slot6│Slot7│   │   │
│  │  └─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┘   │   │
│  └──────────────────────────────────────────────────────┘   │
│                           │                                 │
│                           ▼                                 │
│  ┌──────────────────────────────────────────────────────┐   │
│  │              IdempotencyBarrier                      │   │
│  │  HashMap<SenderId, LastSeenSeq>                      │   │
│  │  Accept / Duplicate / Gap(missing_start, end)        │   │
│  └──────────────────────────────────────────────────────┘   │
├─────────────────────────────────────────────────────────────┤
│  ┌──────────────────────────────────────────────────────┐   │
│  │              Journal (WAL)                           │   │
│  │  mmap file → [Header][Entry1][Entry2][Entry3]...     │   │
│  │  Entry: [Len:u32][CRC32:u32][Data...]                │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

## Deep Fried Optimizations

### 1. Lock-Free Ring Buffer (Disruptor Style)
- **Cache-Line Padding**: Head and Tail cursors are 64 bytes apart to prevent false sharing
- **Power-of-2 Size**: Fast modulo via bitmask (`idx & (N-1)`)
- **No Locks**: Pure `AtomicU64` operations with acquire/release ordering

### 2. Memory-Mapped WAL
- **Zero Syscalls**: Data written directly to mmap region
- **OS Page Cache**: Let the kernel handle write-back
- **Crash Recovery**: Append-only format, CRC32 checksums

### 3. Exactly-Once Processing
- **Idempotency Barrier**: Tracks `LastSeenSeq` per sender
- **Gap Detection**: Missing sequences trigger NACK for retransmit
- **Deterministic ID**: `BLAKE3(sender || seq || payload)` ensures identical messages have identical IDs

### 4. Causal Ordering
- **Vector Clock**: `[u64; 16]` inline array, no heap allocation
- **Hybrid Logical Clock**: Bounded skew with physical time

## Installation

```toml
[dependencies]
alice-queue = "0.1"
```

## Usage

### Basic Queue

```rust
use alice_queue::{AliceQueue, Message, GapResult};

let mut queue = AliceQueue::<1024>::new();
let sender = [0u8; 32]; // Your sender key

// Enqueue
let msg = Message::new(sender, 1, b"hello world".to_vec());
queue.enqueue(msg).unwrap();

// Dequeue with exactly-once semantics
while let Some((msg, result)) = queue.dequeue() {
    match result {
        GapResult::Accept => {
            println!("Process: {:?}", msg.payload);
        }
        GapResult::Duplicate => {
            println!("Drop duplicate seq={}", msg.header.seq);
        }
        GapResult::Gap { missing_start, missing_end } => {
            println!("NACK: need seq {} to {}", missing_start, missing_end);
        }
    }
}
```

### Persistent WAL

```rust
use alice_queue::Journal;

// Open or create journal (1GB pre-allocated)
let mut journal = Journal::open("data.wal", 1024 * 1024 * 1024)?;

// Append entries
let offset = journal.append(b"transaction data")?;

// Read back
let (data, next_offset) = journal.read_at(offset)?;

// Iterate all entries
for entry in journal.iter() {
    let (offset, data) = entry?;
    println!("Entry at {}: {:?}", offset, data);
}

// Sync to disk
journal.sync()?;
```

### Vector Clock

```rust
use alice_queue::VectorClock;

let mut vc1 = VectorClock::new(0, 3); // Node 0, 3 total nodes
let mut vc2 = VectorClock::new(1, 3); // Node 1

// Local event
vc1.tick();

// Send message (include vc1 in message)
let msg_clock = vc1;

// Receive and merge
vc2.merge(&msg_clock);

// Check causality
assert!(vc1.happened_before(&vc2));
```

### Message with BLAKE3 ID

```rust
use alice_queue::{Message, MessageBuilder, VectorClock};

let sender = [1u8; 32];

// Simple message
let msg = Message::new(sender, 1, b"payload".to_vec());
assert!(msg.verify_id()); // BLAKE3 verification

// With vector clock
let vc = VectorClock::new(0, 4);
let msg = MessageBuilder::new(sender, 2)
    .payload(b"data".to_vec())
    .vclock(vc)
    .requires_ack()
    .build();
```

## API Reference

### `AliceQueue<N>`

```rust
fn new() -> Self;
fn enqueue(&self, msg: Message) -> Result<u64, Message>;
fn dequeue(&mut self) -> Option<(Message, GapResult)>;
fn len(&self) -> usize;
fn is_empty(&self) -> bool;
```

### `RingBuffer<T, N>`

```rust
fn new() -> Self;
fn try_push(&self, value: T) -> Result<u64, T>;
fn try_pop(&self) -> Option<T>;
fn len(&self) -> usize;
fn is_full(&self) -> bool;
fn capacity(&self) -> usize;
```

### `Journal`

```rust
fn open(path: &Path, capacity: u64) -> io::Result<Self>;
fn append(&mut self, data: &[u8]) -> io::Result<u64>;
fn read_at(&self, offset: u64) -> io::Result<(Vec<u8>, u64)>;
fn iter(&self) -> JournalIter;
fn sync(&self) -> io::Result<()>;
```

### `IdempotencyBarrier`

```rust
fn new() -> Self;
fn check(&self, sender: SenderId, seq: SeqNum) -> GapResult;
fn mark_processed(&mut self, sender: SenderId, seq: SeqNum);
fn check_and_mark(&mut self, sender: SenderId, seq: SeqNum) -> GapResult;
```

### `VectorClock`

```rust
fn new(node_id: u8, num_nodes: u8) -> Self;
fn tick(&mut self) -> u64;
fn merge(&mut self, other: &VectorClock);
fn happened_before(&self, other: &VectorClock) -> bool;
fn to_bytes(&self) -> Vec<u8>;
fn from_bytes(bytes: &[u8]) -> Option<Self>;
```

## Performance

### Ring Buffer (SPSC)
- **Throughput**: ~50M ops/sec (single producer, single consumer)
- **Latency**: ~20ns per operation
- **Memory**: Fixed N slots, no runtime allocation

### Journal (mmap WAL)
- **Write**: Memory bandwidth limited (~10GB/s on modern SSDs)
- **Read**: Zero-copy, direct memory access
- **Sync**: Depends on fsync latency

### Idempotency Check
- **Time**: O(1) HashMap lookup
- **Space**: 16 bytes per sender

## License

**GNU AGPLv3**

## Author

Moroya Sakamoto
