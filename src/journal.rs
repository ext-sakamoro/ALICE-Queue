//! Memory-Mapped Append-Only Log (WAL)
//!
//! **Optimizations**:
//! - `mmap` for zero-copy I/O (no read/write syscalls)
//! - Append-only for crash safety
//! - OS page cache exploitation
//! - Pre-allocated file for no fragmentation
//!
//! > "The fastest I/O is the I/O you don't do."

use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use memmap2::{MmapMut, MmapOptions};

/// Journal entry header
/// Format: [Length: u32][Checksum: u32][Data...]
const HEADER_SIZE: usize = 8;

/// Magic bytes for journal file identification
const MAGIC: [u8; 4] = *b"ALWL"; // ALICE Write-ahead Log

/// Journal file header (at offset 0)
#[repr(C, packed)]
struct JournalHeader {
    /// Magic bytes
    magic: [u8; 4],
    /// Version
    version: u32,
    /// Write position (next write offset)
    write_pos: u64,
    /// Entry count
    entry_count: u64,
    /// Reserved for future use
    _reserved: [u8; 40],
}

const JOURNAL_HEADER_SIZE: usize = std::mem::size_of::<JournalHeader>();

/// CRC32 checksum (simple, fast)
#[inline]
fn crc32(data: &[u8]) -> u32 {
    // Simple polynomial CRC (not cryptographic, just for integrity)
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xEDB8_8320
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

/// Memory-Mapped Journal (Write-Ahead Log)
pub struct Journal {
    /// Memory-mapped file
    mmap: MmapMut,
    /// Current write position
    write_pos: AtomicU64,
    /// Entry count
    entry_count: AtomicU64,
    /// File handle (kept open for flush)
    file: File,
    /// Total capacity
    capacity: u64,
}

impl Journal {
    /// Create or open a journal file
    ///
    /// - `path`: File path
    /// - `capacity`: Pre-allocated size in bytes (will be rounded up to page size)
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the file cannot be opened, allocated, or memory-mapped.
    pub fn open<P: AsRef<Path>>(path: P, capacity: u64) -> io::Result<Self> {
        let path = path.as_ref();
        let exists = path.exists();

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        // Pre-allocate file
        if !exists || file.metadata()?.len() < capacity {
            file.set_len(capacity)?;
        }

        // Memory map the file
        let mut mmap = unsafe { MmapOptions::new().map_mut(&file)? };

        let (write_pos, entry_count) = if exists {
            // Read existing header
            Self::read_header(&mmap)?
        } else {
            // Initialize new journal
            Self::init_header(&mut mmap)?;
            (JOURNAL_HEADER_SIZE as u64, 0)
        };

        Ok(Self {
            mmap,
            write_pos: AtomicU64::new(write_pos),
            entry_count: AtomicU64::new(entry_count),
            file,
            capacity,
        })
    }

    /// Initialize journal header
    fn init_header(mmap: &mut MmapMut) -> io::Result<()> {
        if mmap.len() < JOURNAL_HEADER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "File too small for header",
            ));
        }

        // Write magic and version
        mmap[0..4].copy_from_slice(&MAGIC);
        mmap[4..8].copy_from_slice(&1u32.to_le_bytes()); // version 1
        mmap[8..16].copy_from_slice(&(JOURNAL_HEADER_SIZE as u64).to_le_bytes()); // write_pos
        mmap[16..24].copy_from_slice(&0u64.to_le_bytes()); // entry_count

        Ok(())
    }

    /// Read journal header
    fn read_header(mmap: &MmapMut) -> io::Result<(u64, u64)> {
        if mmap.len() < JOURNAL_HEADER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "File too small for header",
            ));
        }

        // Verify magic
        if mmap[0..4] != MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid journal magic",
            ));
        }

        // Read positions.
        // The bounds check above guarantees at least JOURNAL_HEADER_SIZE bytes,
        // so these fixed-width slices always have the expected length.
        // We use map_err+? instead of unwrap so any unexpected truncation
        // propagates as an io::Error rather than a panic.
        let write_pos =
            u64::from_le_bytes(mmap[8..16].try_into().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "corrupt write_pos field")
            })?);
        let entry_count = u64::from_le_bytes(mmap[16..24].try_into().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "corrupt entry_count field")
        })?);

        Ok((write_pos, entry_count))
    }

    /// Update header in mmap
    fn update_header(&mut self) {
        let write_pos = self.write_pos.load(Ordering::Relaxed);
        let entry_count = self.entry_count.load(Ordering::Relaxed);

        self.mmap[8..16].copy_from_slice(&write_pos.to_le_bytes());
        self.mmap[16..24].copy_from_slice(&entry_count.to_le_bytes());
    }

    /// Append an entry to the journal
    ///
    /// Returns the offset where entry was written.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the journal is full.
    pub fn append(&mut self, data: &[u8]) -> io::Result<u64> {
        let entry_size = HEADER_SIZE + data.len();
        let write_pos = self.write_pos.load(Ordering::Relaxed);

        // Check capacity
        if write_pos + entry_size as u64 > self.capacity {
            return Err(io::Error::new(io::ErrorKind::WriteZero, "Journal full"));
        }

        let offset = write_pos as usize;

        // Write length (u32)
        let len = data.len() as u32;
        self.mmap[offset..offset + 4].copy_from_slice(&len.to_le_bytes());

        // Write checksum (u32)
        let checksum = crc32(data);
        self.mmap[offset + 4..offset + 8].copy_from_slice(&checksum.to_le_bytes());

        // Write data
        self.mmap[offset + 8..offset + 8 + data.len()].copy_from_slice(data);

        // Update positions
        self.write_pos
            .store(write_pos + entry_size as u64, Ordering::Release);
        self.entry_count.fetch_add(1, Ordering::Relaxed);

        // Update header
        self.update_header();

        Ok(write_pos)
    }

    /// Read an entry at offset
    ///
    /// Returns the entry data and next offset.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the offset is past end or the checksum is invalid.
    pub fn read_at(&self, offset: u64) -> io::Result<(Vec<u8>, u64)> {
        let offset = offset as usize;

        if offset + HEADER_SIZE > self.mmap.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Offset past end of journal",
            ));
        }

        // Read length.
        // The bounds check above ensures `offset + HEADER_SIZE` fits inside the
        // mmap, so the slice is always 4 bytes wide. We propagate failure as an
        // io::Error rather than panicking.
        let len = u32::from_le_bytes(self.mmap[offset..offset + 4].try_into().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "corrupt entry length field")
        })?) as usize;

        if len == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Empty entry (end of log)",
            ));
        }

        // Read checksum.
        let stored_checksum =
            u32::from_le_bytes(self.mmap[offset + 4..offset + 8].try_into().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "corrupt entry checksum field")
            })?);

        // Read data
        let data_start = offset + HEADER_SIZE;
        let data_end = data_start + len;

        if data_end > self.mmap.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Entry extends past end of journal",
            ));
        }

        let data = self.mmap[data_start..data_end].to_vec();

        // Verify checksum
        let computed_checksum = crc32(&data);
        if computed_checksum != stored_checksum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Checksum mismatch",
            ));
        }

        Ok((data, data_end as u64))
    }

    /// Iterate over all entries
    pub const fn iter(&self) -> JournalIter<'_> {
        JournalIter {
            journal: self,
            offset: JOURNAL_HEADER_SIZE as u64,
        }
    }

    /// Sync to disk
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the flush or fsync fails.
    pub fn sync(&self) -> io::Result<()> {
        self.mmap.flush()?;
        self.file.sync_all()
    }

    /// Current write position
    pub fn write_position(&self) -> u64 {
        self.write_pos.load(Ordering::Relaxed)
    }

    /// Number of entries
    pub fn entry_count(&self) -> u64 {
        self.entry_count.load(Ordering::Relaxed)
    }

    /// Total capacity
    pub const fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Available space
    pub fn available(&self) -> u64 {
        self.capacity - self.write_pos.load(Ordering::Relaxed)
    }
}

/// Iterator over journal entries
pub struct JournalIter<'a> {
    journal: &'a Journal,
    offset: u64,
}

impl<'a> IntoIterator for &'a Journal {
    type Item = io::Result<(u64, Vec<u8>)>;
    type IntoIter = JournalIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl Iterator for JournalIter<'_> {
    type Item = io::Result<(u64, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let write_pos = self.journal.write_pos.load(Ordering::Acquire);

        if self.offset >= write_pos {
            return None;
        }

        let current_offset = self.offset;
        match self.journal.read_at(current_offset) {
            Ok((data, next_offset)) => {
                self.offset = next_offset;
                Some(Ok((current_offset, data)))
            }
            Err(e) => Some(Err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_path(test_name: &str) -> std::path::PathBuf {
        let counter = TEST_COUNTER.fetch_add(1, AtomicOrdering::SeqCst);
        let mut path = std::env::temp_dir();
        path.push(format!("alice_journal_{}_{}.wal", test_name, counter));
        path
    }

    #[test]
    fn test_journal_create() {
        let path = temp_path("create");
        let _ = fs::remove_file(&path);

        {
            let journal = Journal::open(&path, 1024 * 1024).unwrap();
            assert_eq!(journal.entry_count(), 0);
            assert_eq!(journal.write_position(), JOURNAL_HEADER_SIZE as u64);
        }

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_journal_append_read() {
        let path = temp_path("append_read");
        let _ = fs::remove_file(&path);

        {
            let mut journal = Journal::open(&path, 1024 * 1024).unwrap();

            let offset1 = journal.append(b"hello").unwrap();
            let offset2 = journal.append(b"world").unwrap();
            let offset3 = journal.append(b"test data 123").unwrap();

            assert_eq!(journal.entry_count(), 3);

            let (data1, _) = journal.read_at(offset1).unwrap();
            let (data2, _) = journal.read_at(offset2).unwrap();
            let (data3, _) = journal.read_at(offset3).unwrap();

            assert_eq!(data1, b"hello");
            assert_eq!(data2, b"world");
            assert_eq!(data3, b"test data 123");
        }

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_journal_iter() {
        let path = temp_path("iter");
        let _ = fs::remove_file(&path);

        {
            let mut journal = Journal::open(&path, 1024 * 1024).unwrap();

            journal.append(b"entry1").unwrap();
            journal.append(b"entry2").unwrap();
            journal.append(b"entry3").unwrap();

            let entries: Vec<_> = journal
                .iter()
                .map(|r| r.map(|(_, data)| data))
                .collect::<Result<Vec<_>, _>>()
                .unwrap();

            assert_eq!(entries.len(), 3);
            assert_eq!(entries[0], b"entry1");
            assert_eq!(entries[1], b"entry2");
            assert_eq!(entries[2], b"entry3");
        }

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_journal_persistence() {
        let path = temp_path("persistence");
        let _ = fs::remove_file(&path);

        // Write some entries
        {
            let mut journal = Journal::open(&path, 1024 * 1024).unwrap();
            journal.append(b"persistent1").unwrap();
            journal.append(b"persistent2").unwrap();
            journal.sync().unwrap();
        }

        // Reopen and verify
        {
            let journal = Journal::open(&path, 1024 * 1024).unwrap();
            assert_eq!(journal.entry_count(), 2);

            let entries: Vec<_> = journal
                .iter()
                .map(|r| r.map(|(_, data)| data))
                .collect::<Result<Vec<_>, _>>()
                .unwrap();

            assert_eq!(entries[0], b"persistent1");
            assert_eq!(entries[1], b"persistent2");
        }

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_crc32() {
        let data = b"hello world";
        let checksum = crc32(data);

        // Same data should produce same checksum
        assert_eq!(crc32(data), checksum);

        // Different data should produce different checksum
        assert_ne!(crc32(b"hello worlds"), checksum);
    }

    #[test]
    fn test_journal_full() {
        let path = temp_path("full");
        let _ = fs::remove_file(&path);

        {
            // Very small capacity: header (64 bytes) + minimal room
            let mut journal = Journal::open(&path, 128).unwrap();

            // Fill it up until we get an error
            let mut count = 0;
            loop {
                match journal.append(b"data") {
                    Ok(_) => count += 1,
                    Err(e) => {
                        assert_eq!(e.kind(), io::ErrorKind::WriteZero);
                        break;
                    }
                }
                if count > 100 {
                    panic!("Journal should have been full by now");
                }
            }
            assert!(count > 0);
        }

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_journal_read_at_invalid_offset() {
        let path = temp_path("invalid_offset");
        let _ = fs::remove_file(&path);

        {
            let journal = Journal::open(&path, 1024).unwrap();

            // Read at offset past end
            let result = journal.read_at(2000);
            assert!(result.is_err());

            // Read at start of header area (offset 0) - will get zero-length entry
            let result = journal.read_at(JOURNAL_HEADER_SIZE as u64);
            assert!(result.is_err()); // Empty entry since nothing written
        }

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_journal_available_space() {
        let path = temp_path("available");
        let _ = fs::remove_file(&path);

        {
            let mut journal = Journal::open(&path, 4096).unwrap();
            let initial_available = journal.available();
            assert!(initial_available > 0);
            assert_eq!(journal.capacity(), 4096);

            journal.append(b"some data here").unwrap();
            assert!(journal.available() < initial_available);
            assert_eq!(journal.capacity(), 4096);
        }

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_journal_write_position_advances() {
        let path = temp_path("write_pos");
        let _ = fs::remove_file(&path);

        {
            let mut journal = Journal::open(&path, 4096).unwrap();
            let pos0 = journal.write_position();

            journal.append(b"entry1").unwrap();
            let pos1 = journal.write_position();
            assert!(pos1 > pos0);

            journal.append(b"entry2").unwrap();
            let pos2 = journal.write_position();
            assert!(pos2 > pos1);

            // Entry size = HEADER_SIZE(8) + data.len
            let expected_advance = (HEADER_SIZE + 6) as u64; // "entry1" = 6 bytes
            assert_eq!(pos1 - pos0, expected_advance);
        }

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_crc32_empty_data() {
        let empty_checksum = crc32(b"");
        // CRC32 of empty data should be deterministic
        assert_eq!(crc32(b""), empty_checksum);
        // Should differ from non-empty
        assert_ne!(crc32(b"a"), empty_checksum);
    }

    #[test]
    fn test_crc32_known_value() {
        // CRC32 of "hello world" with polynomial 0xEDB88320 (reflected)
        // This is the standard CRC-32 (ISO 3309)
        let checksum = crc32(b"hello world");
        // The standard CRC-32 value for "hello world" is 0x0D4A1185
        assert_eq!(checksum, 0x0D4A1185);
    }

    #[test]
    fn test_journal_multiple_entries_iter_order() {
        let path = temp_path("iter_order");
        let _ = fs::remove_file(&path);

        {
            let mut journal = Journal::open(&path, 1024 * 1024).unwrap();

            for i in 0..20 {
                journal
                    .append(format!("entry_{:03}", i).as_bytes())
                    .unwrap();
            }

            assert_eq!(journal.entry_count(), 20);

            let entries: Vec<Vec<u8>> = journal
                .iter()
                .map(|r| r.map(|(_, data)| data))
                .collect::<Result<Vec<_>, _>>()
                .unwrap();

            assert_eq!(entries.len(), 20);
            for (i, entry) in entries.iter().enumerate() {
                assert_eq!(entry, format!("entry_{:03}", i).as_bytes());
            }
        }

        let _ = fs::remove_file(&path);
    }
}
