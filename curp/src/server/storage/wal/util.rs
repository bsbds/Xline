use std::{
    fs::{File as StdFile, OpenOptions},
    io,
    path::PathBuf,
};

use fs2::FileExt;
use tokio::fs::File as TokioFile;

/// File that is exclusively locked
pub(super) struct LockedFile {
    /// The inner std file
    file: StdFile,
}

impl LockedFile {
    /// Open the file in read and append mode
    pub(super) fn open_read_append(path: PathBuf) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;
        file.try_lock_exclusive()?;

        Ok(Self { file })
    }

    /// Pre-allocates the file
    pub(super) fn preallocate(&mut self, size: u64) -> io::Result<()> {
        if size == 0 {
            return Ok(());
        }

        self.file.allocate(size)
    }

    pub(super) fn into_std(self) -> StdFile {
        self.file
    }

    pub(super) fn into_async(self) -> TokioFile {
        TokioFile::from_std(self.file)
    }
}
