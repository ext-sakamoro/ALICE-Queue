# Contributing to ALICE-Queue

## Build

```bash
cargo build
```

## Test

```bash
cargo test
```

## Lint

```bash
cargo clippy -- -W clippy::all
cargo fmt -- --check
cargo doc --no-deps 2>&1 | grep warning
```

## Design Constraints

- **Lock-free SPSC**: ring buffer uses atomic operations with cache-line padding to avoid false sharing.
- **Zero-copy persistence**: WAL journal uses mmap for append-only writes without serialization overhead.
- **Exactly-once processing**: per-sender sequence tracking detects duplicates and gaps in O(1).
- **Deterministic message IDs**: BLAKE3(sender + seq + payload) for content-addressable deduplication.
- **Causal ordering**: vector clocks with fixed-size inline arrays (no heap allocation).
- **`no_std` core**: ring buffer, barrier, and clock work without `std`; journal requires `std` feature.
