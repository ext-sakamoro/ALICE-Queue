//! ジャーナルリプレイロジック。
//!
//! WAL（Write-Ahead Log）のエントリを順にイテレートし、
//! リングバッファへ再適用する。チェックポイントからの
//! 部分リプレイもサポート。

use crate::journal::Journal;
use crate::message::Message;
use crate::ring::RingBuffer;
use std::io;

/// リプレイ結果の統計情報。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplayStats {
    /// リプレイしたエントリ数。
    pub replayed: u64,
    /// スキップしたエントリ数（チェックポイント前）。
    pub skipped: u64,
    /// デシリアライズ失敗数。
    pub failed: u64,
    /// リングバッファ満杯で投入できなかった数。
    pub dropped: u64,
}

/// ジャーナルの全エントリをリングバッファにリプレイする。
///
/// CRC32 が不正なエントリに到達した場合はそこで停止し、
/// それまでのリプレイ結果を返す。
///
/// # Errors
///
/// CRC32 不一致等の I/O エラーが発生した場合、
/// それまでのリプレイ結果とエラーを返す。
pub fn replay_journal<const N: usize>(
    journal: &Journal,
    ring: &RingBuffer<Message, N>,
) -> (ReplayStats, Option<io::Error>) {
    replay_from_checkpoint(journal, ring, 0)
}

/// 指定エントリインデックスからリングバッファにリプレイする。
///
/// `skip_count` 件のエントリをスキップしてから適用を開始する。
///
/// # 戻り値
///
/// `(ReplayStats, Option<io::Error>)` — エラーが無ければ `None`。
pub fn replay_from_checkpoint<const N: usize>(
    journal: &Journal,
    ring: &RingBuffer<Message, N>,
    skip_count: u64,
) -> (ReplayStats, Option<io::Error>) {
    let mut stats = ReplayStats {
        replayed: 0,
        skipped: 0,
        failed: 0,
        dropped: 0,
    };

    for result in journal {
        let (_offset, data) = match result {
            Ok(entry) => entry,
            Err(e) => return (stats, Some(e)),
        };

        // チェックポイント前のエントリはスキップ
        if stats.skipped < skip_count {
            stats.skipped += 1;
            continue;
        }

        // デシリアライズ
        let Some(msg) = Message::from_bytes(&data) else {
            stats.failed += 1;
            continue;
        };

        // リングバッファへ投入
        if ring.try_push(msg).is_err() {
            stats.dropped += 1;
        } else {
            stats.replayed += 1;
        }
    }

    (stats, None)
}

/// ジャーナルからメッセージの `Vec` としてリプレイする（リングバッファ不要）。
///
/// テストやインスペクション用途。
///
/// # Errors
///
/// CRC32 不一致等でイテレーション中にエラーが発生した場合。
pub fn replay_to_vec(journal: &Journal) -> io::Result<Vec<Message>> {
    let mut messages = Vec::new();
    for result in journal {
        let (_offset, data) = result?;
        if let Some(msg) = Message::from_bytes(&data) {
            messages.push(msg);
        }
    }
    Ok(messages)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_path(test_name: &str) -> std::path::PathBuf {
        let counter = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let mut path = std::env::temp_dir();
        path.push(format!("alice_replay_{}_{}.wal", test_name, counter));
        path
    }

    fn test_sender(id: u8) -> [u8; 32] {
        let mut key = [0u8; 32];
        key[0] = id;
        key
    }

    #[test]
    fn replay_empty_journal() {
        let path = temp_path("empty");
        let _ = fs::remove_file(&path);
        {
            let journal = Journal::open(&path, 4096).unwrap();
            let ring: RingBuffer<Message, 16> = RingBuffer::new();
            let (stats, err) = replay_journal(&journal, &ring);
            assert!(err.is_none());
            assert_eq!(stats.replayed, 0);
            assert_eq!(stats.skipped, 0);
            assert_eq!(stats.failed, 0);
            assert_eq!(stats.dropped, 0);
            assert!(ring.is_empty());
        }
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn replay_single_entry() {
        let path = temp_path("single");
        let _ = fs::remove_file(&path);
        {
            let mut journal = Journal::open(&path, 4096).unwrap();
            let msg = Message::new(test_sender(1), 1, b"hello".to_vec());
            journal.append(&msg.to_bytes()).unwrap();

            let ring: RingBuffer<Message, 16> = RingBuffer::new();
            let (stats, err) = replay_journal(&journal, &ring);
            assert!(err.is_none());
            assert_eq!(stats.replayed, 1);
            assert_eq!(ring.len(), 1);

            let popped = ring.try_pop().unwrap();
            assert_eq!(popped.payload, b"hello");
        }
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn replay_multiple_entries() {
        let path = temp_path("multi");
        let _ = fs::remove_file(&path);
        {
            let mut journal = Journal::open(&path, 1024 * 1024).unwrap();
            for i in 1u64..=5 {
                let msg = Message::new(test_sender(1), i, format!("msg{i}").into_bytes());
                journal.append(&msg.to_bytes()).unwrap();
            }

            let ring: RingBuffer<Message, 16> = RingBuffer::new();
            let (stats, err) = replay_journal(&journal, &ring);
            assert!(err.is_none());
            assert_eq!(stats.replayed, 5);
            assert_eq!(ring.len(), 5);
        }
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn replay_with_checkpoint_skip() {
        let path = temp_path("checkpoint");
        let _ = fs::remove_file(&path);
        {
            let mut journal = Journal::open(&path, 1024 * 1024).unwrap();
            for i in 1u64..=5 {
                let msg = Message::new(test_sender(1), i, format!("msg{i}").into_bytes());
                journal.append(&msg.to_bytes()).unwrap();
            }

            let ring: RingBuffer<Message, 16> = RingBuffer::new();
            let (stats, err) = replay_from_checkpoint(&journal, &ring, 3);
            assert!(err.is_none());
            assert_eq!(stats.skipped, 3);
            assert_eq!(stats.replayed, 2);
            assert_eq!(ring.len(), 2);

            // 最初に取り出すのは msg4
            let popped = ring.try_pop().unwrap();
            assert_eq!(popped.header.seq, 4);
        }
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn replay_ring_full_drops() {
        let path = temp_path("full");
        let _ = fs::remove_file(&path);
        {
            let mut journal = Journal::open(&path, 1024 * 1024).unwrap();
            for i in 1u64..=10 {
                let msg = Message::new(test_sender(1), i, format!("msg{i}").into_bytes());
                journal.append(&msg.to_bytes()).unwrap();
            }

            // キャパシティ 4 のリングに 10 件リプレイ
            let ring: RingBuffer<Message, 4> = RingBuffer::new();
            let (stats, err) = replay_journal(&journal, &ring);
            assert!(err.is_none());
            assert_eq!(stats.replayed, 4);
            assert_eq!(stats.dropped, 6);
        }
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn replay_invalid_data_counted_as_failed() {
        let path = temp_path("invalid");
        let _ = fs::remove_file(&path);
        {
            let mut journal = Journal::open(&path, 4096).unwrap();
            // 正常メッセージ
            let msg = Message::new(test_sender(1), 1, b"ok".to_vec());
            journal.append(&msg.to_bytes()).unwrap();
            // 不正データ（Message::from_bytes が None を返す）
            journal.append(b"not-a-valid-message").unwrap();
            // 正常メッセージ
            let msg2 = Message::new(test_sender(1), 2, b"ok2".to_vec());
            journal.append(&msg2.to_bytes()).unwrap();

            let ring: RingBuffer<Message, 16> = RingBuffer::new();
            let (stats, err) = replay_journal(&journal, &ring);
            assert!(err.is_none());
            assert_eq!(stats.replayed, 2);
            assert_eq!(stats.failed, 1);
        }
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn replay_to_vec_basic() {
        let path = temp_path("to_vec");
        let _ = fs::remove_file(&path);
        {
            let mut journal = Journal::open(&path, 1024 * 1024).unwrap();
            for i in 1u64..=3 {
                let msg = Message::new(test_sender(1), i, format!("v{i}").into_bytes());
                journal.append(&msg.to_bytes()).unwrap();
            }

            let messages = replay_to_vec(&journal).unwrap();
            assert_eq!(messages.len(), 3);
            assert_eq!(messages[0].header.seq, 1);
            assert_eq!(messages[2].header.seq, 3);
        }
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn replay_to_vec_empty() {
        let path = temp_path("to_vec_empty");
        let _ = fs::remove_file(&path);
        {
            let journal = Journal::open(&path, 4096).unwrap();
            let messages = replay_to_vec(&journal).unwrap();
            assert!(messages.is_empty());
        }
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn replay_stats_default_values() {
        let stats = ReplayStats {
            replayed: 0,
            skipped: 0,
            failed: 0,
            dropped: 0,
        };
        assert_eq!(
            stats.replayed + stats.skipped + stats.failed + stats.dropped,
            0
        );
    }

    #[test]
    fn replay_skip_all() {
        let path = temp_path("skip_all");
        let _ = fs::remove_file(&path);
        {
            let mut journal = Journal::open(&path, 1024 * 1024).unwrap();
            for i in 1u64..=3 {
                let msg = Message::new(test_sender(1), i, b"data".to_vec());
                journal.append(&msg.to_bytes()).unwrap();
            }

            let ring: RingBuffer<Message, 16> = RingBuffer::new();
            let (stats, err) = replay_from_checkpoint(&journal, &ring, 100);
            assert!(err.is_none());
            assert_eq!(stats.skipped, 3);
            assert_eq!(stats.replayed, 0);
            assert!(ring.is_empty());
        }
        let _ = fs::remove_file(&path);
    }
}
