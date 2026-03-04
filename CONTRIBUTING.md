# Contributing to ALICE-Queue

## Prerequisites

- Rust 1.70+ (stable)
- `clippy`, `rustfmt` コンポーネント (`rustup component add clippy rustfmt`)

## Code Style

- `cargo fmt` 準拠（CI で `--check` 実行）
- `cargo clippy --lib --tests -- -W clippy::all -W clippy::pedantic` 警告ゼロ
- パブリック関数には `#[must_use]` を付与
- `Result` 返却関数には `# Errors` docセクション必須
- `no_std` 互換: `std` は feature gate 経由（`alloc` のみ）
- コード内コメント: 日本語

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
