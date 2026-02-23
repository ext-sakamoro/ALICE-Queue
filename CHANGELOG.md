# Changelog

All notable changes to ALICE-Queue will be documented in this file.

## [0.1.0] - 2026-02-23

### Added
- `ring` — Lock-free SPSC `RingBuffer` with cache-line padding (Disruptor style), `BatchConsumer`
- `message` — `Message`, `MessageHeader`, `MessageBuilder`, BLAKE3-based `MessageId`, flag constants
- `barrier` — `IdempotencyBarrier` per-sender sequence tracking, `BatchBarrier`, `GapResult` (Accept/Duplicate/Gap)
- `clock` — `VectorClock` (fixed-size inline array, O(N) merge), `HybridClock` (physical + logical)
- `journal` — Memory-mapped append-only WAL `Journal` with `JournalIter` (std feature)
- `AliceQueue` composite struct combining ring buffer + idempotency barrier
- Zero-copy persistence via mmap
- 88 unit tests + 1 doc-test

### Fixed
- `[i]` in doc comments interpreted as intra-doc links (clock.rs)
