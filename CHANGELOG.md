# Changelog

All notable changes to ALICE-Queue will be documented in this file.

## [Unreleased]

### Changed
- **License: `AGPL-3.0` → `AGPL-3.0 OR LicenseRef-Commercial` (dual-licensed、2026-09-27)** AGPL 側の条件は変更なし (既存 AGPL 利用者への影響ゼロ)、商用という選択肢が追加されただけ SPDX が AGPL 単独だと cargo-deny / FOSSA / SBOM に「商用オプションなし」と見えるため宣言を dual に 変更点: SPDX / `LICENSE` → `LICENSE-AGPL` / `LICENSE-COMMERCIAL.md` (商用トリガー 6 条件 = クローズド製品・商用 SaaS・エッジ / ファームウェア配布・plugin 再配布・プラットフォーム NDA・保証、社内利用は AGPL 側で無償と明記) / README の選択肢表 商用窓口は法人 `contact@extoria.co.jp`

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
