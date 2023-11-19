mod codec;

use std::{
    fs::{File as StdFile, OpenOptions},
    io,
    path::PathBuf,
};

use fs2::FileExt;
use serde::Serialize;
use tokio::fs::File as TokioFile;
use tokio_util::codec::Framed;

use self::codec::LogCodec;

const SEGMENT_SIZE_BYTES: u64 = 64 * 1024 * 1024;

/// The log storage
#[derive(Debug)]
pub(super) struct LogStorage {
    /// The directory to store the log files
    dir: PathBuf,
    /// Locked files
    lfiles: Vec<TokioFile>,
}

impl LogStorage {
    /// Creates a new `LogStorage`
    pub(super) async fn new(dir: &str) -> io::Result<Self> {
        let dir = PathBuf::from(dir);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join(Self::log_name(0, 0)))?;
        file.try_lock_exclusive()?;

        Ok(Self {
            dir,
            lfiles: vec![TokioFile::from_std(file)],
        })
    }

    async fn test_file<C: Serialize>(&mut self) {
        let file = self.lfiles.pop().unwrap();
        let io = Framed::new(file, LogCodec::<C>::new());
    }

    /// Pre-allocates a file
    fn allocate_file(file: &mut StdFile, size_in_bytes: u64) -> io::Result<()> {
        if size_in_bytes == 0 {
            return Ok(());
        }

        file.allocate(size_in_bytes)
    }

    /// Gets the name of the log file
    fn log_name(seq: u64, log_index: u64) -> String {
        format!("{seq}-{log_index}.log")
    }
}
