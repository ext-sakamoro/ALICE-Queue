//! クラッシュリカバリ API。
//!
//! ジャーナルの整合性を検証し、破損エントリを切り詰め、
//! リングバッファに状態を復元する統合リカバリフロー。

use crate::journal::Journal;
use crate::message::Message;
use crate::replay::{replay_journal, ReplayStats};
use crate::ring::RingBuffer;
use std::io;

/// ジャーナル検証結果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationResult {
    /// 正常なエントリ数。
    pub valid_entries: u64,
    /// 破損を検出したか。
    pub corrupted: bool,
    /// 破損位置（バイトオフセット）。破損なしなら `None`。
    pub corrupt_offset: Option<u64>,
    /// エラーメッセージ（破損時のみ）。
    pub error_message: Option<String>,
}

/// ジャーナルの全エントリの CRC32 整合性を検証する。
///
/// 正常エントリ数と破損位置を返す。
pub fn validate_journal(journal: &Journal) -> ValidationResult {
    let mut valid_entries: u64 = 0;

    for result in journal {
        match result {
            Ok(_) => valid_entries += 1,
            Err(e) => {
                // 現在のイテレータオフセットは取得できないが、
                // ヘッダサイズ + 各エントリのサイズから推計
                return ValidationResult {
                    valid_entries,
                    corrupted: true,
                    corrupt_offset: None, // イテレータから正確なオフセットは取得不可
                    error_message: Some(e.to_string()),
                };
            }
        }
    }

    ValidationResult {
        valid_entries,
        corrupted: false,
        corrupt_offset: None,
        error_message: None,
    }
}

/// ジャーナルの全エントリを検証し、各エントリのオフセットも収集する。
///
/// 破損が無ければ全エントリの `(offset, data_len)` を返す。
/// 破損時は破損直前までのエントリ情報と破損オフセットを返す。
pub fn validate_journal_with_offsets(journal: &Journal) -> (Vec<(u64, usize)>, Option<u64>) {
    let mut entries = Vec::new();

    for result in journal {
        let Ok((offset, data)) = result else {
            // 破損位置を計算: 最後の正常エントリの直後
            let corrupt_at = entries
                .last()
                .map_or(64, |&(last_off, last_len)| last_off + 8 + last_len as u64);
            return (entries, Some(corrupt_at));
        };
        entries.push((offset, data.len()));
    }

    (entries, None)
}

/// 破損エントリ以降を切り詰め、ジャーナルヘッダを更新する。
///
/// `valid_count` 件の正常エントリのみを残し、
/// `write_pos` と `entry_count` を巻き戻す。
///
/// # Errors
///
/// ジャーナルを再オープンできない場合にエラー。
pub fn truncate_corrupted(journal: &mut Journal, valid_count: u64) -> io::Result<()> {
    if valid_count == 0 {
        // 全エントリ切り詰め: ヘッダ直後にリセット
        journal.reset_position(64, 0); // JOURNAL_HEADER_SIZE = 64
        return Ok(());
    }

    // write_pos を計算: valid_count 件分だけイテレート
    let mut count: u64 = 0;
    let mut last_next_offset: u64 = 64;

    for result in &*journal {
        let (offset, data) = result?;
        count += 1;
        // 次のエントリの開始位置 = offset + 8(header) + data.len
        last_next_offset = offset + 8 + data.len() as u64;
        if count >= valid_count {
            break;
        }
    }

    journal.reset_position(last_next_offset, valid_count);

    Ok(())
}

/// 統合リカバリ: validate → truncate → replay。
///
/// 1. ジャーナルの全エントリを CRC32 検証
/// 2. 破損があれば破損以降を切り詰め
/// 3. 正常エントリをリングバッファにリプレイ
///
/// # Errors
///
/// 検証・切り詰め・リプレイいずれかで致命的エラーが発生した場合。
pub fn recover<const N: usize>(
    journal: &mut Journal,
    ring: &RingBuffer<Message, N>,
) -> io::Result<RecoveryResult> {
    // 1. 検証
    let validation = validate_journal(journal);

    // 2. 破損があれば切り詰め
    let truncated = if validation.corrupted {
        truncate_corrupted(journal, validation.valid_entries)?;
        true
    } else {
        false
    };

    // 3. リプレイ
    let (replay_stats, replay_err) = replay_journal(journal, ring);
    if let Some(e) = replay_err {
        return Err(e);
    }

    Ok(RecoveryResult {
        validation,
        truncated,
        replay_stats,
    })
}

/// リカバリ統合結果。
#[derive(Debug)]
pub struct RecoveryResult {
    /// 検証結果。
    pub validation: ValidationResult,
    /// 切り詰めが実行されたか。
    pub truncated: bool,
    /// リプレイ統計。
    pub replay_stats: ReplayStats,
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
        path.push(format!("alice_recovery_{}_{}.wal", test_name, counter));
        path
    }

    fn test_sender(id: u8) -> [u8; 32] {
        let mut key = [0u8; 32];
        key[0] = id;
        key
    }

    #[test]
    fn validate_empty_journal() {
        let path = temp_path("validate_empty");
        let _ = fs::remove_file(&path);
        {
            let journal = Journal::open(&path, 4096).unwrap();
            let result = validate_journal(&journal);
            assert!(!result.corrupted);
            assert_eq!(result.valid_entries, 0);
            assert!(result.corrupt_offset.is_none());
            assert!(result.error_message.is_none());
        }
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn validate_healthy_journal() {
        let path = temp_path("validate_healthy");
        let _ = fs::remove_file(&path);
        {
            let mut journal = Journal::open(&path, 1024 * 1024).unwrap();
            for i in 1u64..=5 {
                let msg = Message::new(test_sender(1), i, b"data".to_vec());
                journal.append(&msg.to_bytes()).unwrap();
            }

            let result = validate_journal(&journal);
            assert!(!result.corrupted);
            assert_eq!(result.valid_entries, 5);
        }
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn validate_with_offsets_healthy() {
        let path = temp_path("offsets_healthy");
        let _ = fs::remove_file(&path);
        {
            let mut journal = Journal::open(&path, 1024 * 1024).unwrap();
            for i in 1u64..=3 {
                let msg = Message::new(test_sender(1), i, b"test".to_vec());
                journal.append(&msg.to_bytes()).unwrap();
            }

            let (entries, corrupt) = validate_journal_with_offsets(&journal);
            assert_eq!(entries.len(), 3);
            assert!(corrupt.is_none());
        }
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn recover_healthy_journal() {
        let path = temp_path("recover_healthy");
        let _ = fs::remove_file(&path);
        {
            let mut journal = Journal::open(&path, 1024 * 1024).unwrap();
            for i in 1u64..=3 {
                let msg = Message::new(test_sender(1), i, format!("r{i}").into_bytes());
                journal.append(&msg.to_bytes()).unwrap();
            }

            let ring: RingBuffer<Message, 16> = RingBuffer::new();
            let result = recover(&mut journal, &ring).unwrap();

            assert!(!result.truncated);
            assert!(!result.validation.corrupted);
            assert_eq!(result.replay_stats.replayed, 3);
            assert_eq!(ring.len(), 3);
        }
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn truncate_to_zero() {
        let path = temp_path("truncate_zero");
        let _ = fs::remove_file(&path);
        {
            let mut journal = Journal::open(&path, 1024 * 1024).unwrap();
            let msg = Message::new(test_sender(1), 1, b"data".to_vec());
            journal.append(&msg.to_bytes()).unwrap();
            assert_eq!(journal.entry_count(), 1);

            truncate_corrupted(&mut journal, 0).unwrap();
            assert_eq!(journal.entry_count(), 0);

            // イテレートしても何も出ない
            let entries: Vec<_> = journal.iter().collect::<Result<Vec<_>, _>>().unwrap();
            assert!(entries.is_empty());
        }
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn truncate_partial() {
        let path = temp_path("truncate_partial");
        let _ = fs::remove_file(&path);
        {
            let mut journal = Journal::open(&path, 1024 * 1024).unwrap();
            for i in 1u64..=5 {
                let msg = Message::new(test_sender(1), i, format!("e{i}").into_bytes());
                journal.append(&msg.to_bytes()).unwrap();
            }
            assert_eq!(journal.entry_count(), 5);

            truncate_corrupted(&mut journal, 3).unwrap();
            assert_eq!(journal.entry_count(), 3);

            let entries: Vec<_> = journal
                .iter()
                .map(|r| r.map(|(_, data)| data))
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(entries.len(), 3);
        }
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn validation_result_eq() {
        let a = ValidationResult {
            valid_entries: 5,
            corrupted: false,
            corrupt_offset: None,
            error_message: None,
        };
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn recover_empty() {
        let path = temp_path("recover_empty");
        let _ = fs::remove_file(&path);
        {
            let mut journal = Journal::open(&path, 4096).unwrap();
            let ring: RingBuffer<Message, 16> = RingBuffer::new();
            let result = recover(&mut journal, &ring).unwrap();
            assert!(!result.truncated);
            assert_eq!(result.replay_stats.replayed, 0);
            assert!(ring.is_empty());
        }
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn recover_after_append_preserves_data() {
        let path = temp_path("recover_preserve");
        let _ = fs::remove_file(&path);
        {
            let mut journal = Journal::open(&path, 1024 * 1024).unwrap();
            let msg = Message::new(test_sender(1), 1, b"preserved".to_vec());
            journal.append(&msg.to_bytes()).unwrap();
            journal.sync().unwrap();
        }
        // 再オープンしてリカバリ
        {
            let mut journal = Journal::open(&path, 1024 * 1024).unwrap();
            let ring: RingBuffer<Message, 16> = RingBuffer::new();
            let result = recover(&mut journal, &ring).unwrap();
            assert_eq!(result.replay_stats.replayed, 1);
            let popped = ring.try_pop().unwrap();
            assert_eq!(popped.payload, b"preserved");
        }
        let _ = fs::remove_file(&path);
    }
}
