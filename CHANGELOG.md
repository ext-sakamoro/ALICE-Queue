# Changelog

All notable changes to ALICE-Queue will be documented in this file.

## [Unreleased]

### Security
- CI: `security-audit.yml` を追加 `.github/alice-siblings` に固定した実物の sibling (Crypto / Text / Zip) と毎回新しい解決で `cargo audit` と `cargo deny --all-features check all` を走らせ、解決と deny の graph がこの crate と path の sibling を全て含むこと (0 件は失敗) を検査する 週次は金曜 02:53 UTC
- bincode 1.3.3 (RUSTSEC-2025-0141、unmaintained、alice-text 経由) は `scripts/advisory_holds.toml` の期限付きの保留 (2026-11-14 まで) とし、`scripts/advisory_hold_guard.py` が版と依存元の一致と期限を検査する それ以外の勧告は保留せず失敗する
- `deny.toml`: alice-queue と alice-crypto を crate 単位の exception で許可、alice-crypto が固定 commit で取る ALICE-Sign の git source を許可

### Changed
- CI: `.github/actions/alice-siblings/action.yml` を共有の版 (ALICE-Analytics の main と同じ bytes) に揃える `.github/alice-siblings-private` に載せた非公開の隣を読み取り専用の鍵で clone できるようになった (この repository の隣はすべて公開なので、clone の仕方は変わらない)
- Licence: `AGPL-3.0` → `LicenseRef-Proprietary`
- 依存: sibling の ALICE-Crypto を `4ab0f0e` に (deny の例外は `LicenseRef-Proprietary`)
- `alice-sign` の git rev を 8a1b00d に上げた (HMAC-SHA256 の追加のみ、使っている API と byte 列は同じ)
- Ed25519 の署名・検証を `alice-sign` (MIT OR Apache-2.0、公開) から取るようにした 鍵・署名の byte 列は同じ CI は非公開の依存を取得しなくなった

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
