# Changelog

All notable changes to ALICE-Queue will be documented in this file.

## [Unreleased]

### Security
- CI: `security-audit.yml` を追加 `.github/alice-siblings` に固定した実物の sibling (Crypto / Text / Zip) と毎回新しい解決で `cargo audit` と `cargo deny --all-features check all` を走らせ、解決と deny の graph がこの crate と path の sibling を全て含むこと (0 件は失敗) を検査する 週次は金曜 02:53 UTC
- bincode 1.3.3 (RUSTSEC-2025-0141、unmaintained、alice-text 経由) は `scripts/advisory_holds.toml` の期限付きの保留 (2026-11-14 まで) とし、`scripts/advisory_hold_guard.py` が版と依存元の一致と期限を検査する それ以外の勧告は保留せず失敗する
- `deny.toml`: alice-queue と alice-crypto の AGPL 側を crate 単位の exception で許可、alice-crypto が固定 commit で取る ALICE-Sign の git source を許可

### Changed

- `alice-sign` の git rev を 8a1b00d に上げた (HMAC-SHA256 の追加のみ、使っている API と byte 列は同じ)
- License の SPDX 識別子を非推奨の `AGPL-3.0` から `AGPL-3.0-only` に (`Cargo.toml` の `AGPL-3.0-only OR LicenseRef-Commercial`、README の表記、`deny.toml` の exception) 条文 (`LICENSE-AGPL` / `LICENSE-COMMERCIAL.md`) と利用条件は不変
- Ed25519 の署名・検証を `alice-sign` (MIT OR Apache-2.0、公開) から取るようにした 鍵・署名の byte 列は同じ CI は非公開の依存を取得しなくなった
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
