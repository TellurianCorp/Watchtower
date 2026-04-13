use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use prost::Message;
use tracing::{debug, error, info, warn};

use crate::proto::LogBatch;

/// Framing: each batch is stored as [len:u32 le][crc32:u32 le][protobuf bytes].
const HEADER_SIZE: usize = 8;

/// Disk-backed spillover buffer. When the in-memory pipeline channel is full,
/// batches are appended here instead of being dropped. A background task
/// replays them back into the pipeline when capacity frees up.
///
/// The file is append-only during writes and sequential-read during replay,
/// which is friendly to OS page cache and SSD write patterns.
pub struct SpilloverBuffer {
    path: PathBuf,
    writer: Mutex<Option<std::fs::File>>,
}

impl SpilloverBuffer {
    /// Create or open a spillover file at the given path.
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();

        // Ensure parent directory exists.
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        Ok(Self {
            path,
            writer: Mutex::new(None),
        })
    }

    /// Append a batch to the spillover file.
    pub fn append(&self, batch: &LogBatch) -> io::Result<()> {
        let data = batch.encode_to_vec();
        let len = data.len() as u32;
        let crc = crc32fast::hash(&data);

        let mut guard = self.writer.lock().unwrap();
        let file = match guard.as_mut() {
            Some(f) => f,
            None => {
                let f = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&self.path)?;
                *guard = Some(f);
                guard.as_mut().unwrap()
            }
        };

        file.write_all(&len.to_le_bytes())?;
        file.write_all(&crc.to_le_bytes())?;
        file.write_all(&data)?;
        file.flush()?;

        debug!(bytes = data.len(), "spilled batch to disk");
        Ok(())
    }

    /// Replay all batches from the spillover file, calling `f` for each.
    /// After successful replay the file is truncated.
    pub fn replay<F>(&self, mut f: F) -> io::Result<usize>
    where
        F: FnMut(LogBatch) -> bool,
    {
        // Close writer so we don't hold the file open for append.
        {
            let mut guard = self.writer.lock().unwrap();
            *guard = None;
        }

        if !self.path.exists() {
            return Ok(0);
        }

        let file = std::fs::File::open(&self.path)?;
        let file_len = file.metadata()?.len();
        if file_len == 0 {
            return Ok(0);
        }

        let mut reader = io::BufReader::new(file);
        let mut count = 0;
        let mut header = [0u8; HEADER_SIZE];

        loop {
            match reader.read_exact(&mut header) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }

            let len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
            let expected_crc = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);

            let mut data = vec![0u8; len];
            reader.read_exact(&mut data)?;

            let actual_crc = crc32fast::hash(&data);
            if actual_crc != expected_crc {
                warn!(expected_crc, actual_crc, "spillover CRC mismatch, skipping record");
                continue;
            }

            match LogBatch::decode(&data[..]) {
                Ok(batch) => {
                    count += batch.records.len();
                    if !f(batch) {
                        // Consumer signaled to stop (e.g., pipeline full again).
                        info!(replayed = count, "replay paused — pipeline full");
                        return Ok(count);
                    }
                }
                Err(e) => {
                    error!(error = %e, "failed to decode spilled batch, skipping");
                }
            }
        }

        // Truncate the file after successful full replay.
        std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.path)?;

        info!(replayed = count, "spillover replay complete");
        Ok(count)
    }

    /// Number of bytes currently on disk.
    pub fn disk_usage(&self) -> u64 {
        std::fs::metadata(&self.path)
            .map(|m| m.len())
            .unwrap_or(0)
    }

    /// Whether there are spilled records waiting to be replayed.
    pub fn has_pending(&self) -> bool {
        self.disk_usage() > 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{LogRecord, Severity};
    use prost_types::Timestamp;

    fn make_batch(n: usize) -> LogBatch {
        let records: Vec<LogRecord> = (0..n)
            .map(|i| LogRecord {
                timestamp: Some(Timestamp {
                    seconds: 1700000000 + i as i64,
                    nanos: 0,
                }),
                severity: Severity::Info as i32,
                body: format!("test log {i}"),
                ..Default::default()
            })
            .collect();
        LogBatch {
            records,
            metadata: Default::default(),
        }
    }

    #[test]
    fn test_spillover_write_and_replay() {
        let dir = std::env::temp_dir().join("watchtower_test_spillover");
        let _ = std::fs::remove_dir_all(&dir);
        let path = dir.join("spill.bin");

        let buf = SpilloverBuffer::new(&path).unwrap();

        // Write 3 batches.
        buf.append(&make_batch(5)).unwrap();
        buf.append(&make_batch(3)).unwrap();
        buf.append(&make_batch(2)).unwrap();

        assert!(buf.has_pending());
        assert!(buf.disk_usage() > 0);

        // Replay all.
        let mut total = 0;
        let replayed = buf
            .replay(|batch| {
                total += batch.records.len();
                true
            })
            .unwrap();

        assert_eq!(replayed, 10);
        assert_eq!(total, 10);

        // File should be truncated after full replay.
        assert!(!buf.has_pending());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_spillover_crc_integrity() {
        let dir = std::env::temp_dir().join("watchtower_test_crc");
        let _ = std::fs::remove_dir_all(&dir);
        let path = dir.join("spill.bin");

        let buf = SpilloverBuffer::new(&path).unwrap();
        buf.append(&make_batch(1)).unwrap();

        // Corrupt a byte in the data section.
        {
            let mut data = std::fs::read(&path).unwrap();
            if data.len() > HEADER_SIZE + 1 {
                data[HEADER_SIZE + 1] ^= 0xFF;
            }
            std::fs::write(&path, &data).unwrap();
        }

        // Replay should skip the corrupted record.
        let mut count = 0;
        buf.replay(|_batch| {
            count += 1;
            true
        })
        .unwrap();
        assert_eq!(count, 0);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
