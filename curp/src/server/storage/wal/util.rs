use std::{
    fs::{File as StdFile, OpenOptions},
    io,
    path::{Path, PathBuf},
};

use fs2::FileExt;
use sha2::{Digest, Sha256};
use tokio::fs::File as TokioFile;

/// File that is exclusively locked
// TODO: when the locked file is dropped, we should make sure that the file is properly deleted
#[derive(Debug)]
pub(super) struct LockedFile {
    /// The inner std file
    file: StdFile,
    /// The path of the file
    path: PathBuf,
}

impl LockedFile {
    /// Open the file in read and append mode
    pub(super) fn open_read_append(path: PathBuf) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path.as_path())?;
        file.try_lock_exclusive()?;

        Ok(Self { file, path })
    }

    /// Pre-allocates the file
    pub(super) fn preallocate(&mut self, size: u64) -> io::Result<()> {
        if size == 0 {
            return Ok(());
        }

        self.file.allocate(size)
    }

    /// Get the path of this file
    pub(super) fn path(&self) -> PathBuf {
        self.path.clone()
    }

    /// Rename the current file
    ///
    /// We will discard this file if the rename has failed
    /// TODO: GC the orignal file
    pub(super) fn rename(self, new_name: impl AsRef<Path>) -> io::Result<Self> {
        let mut new_path = self.path.clone();
        let _ignore = new_path.pop();
        new_path.push(new_name.as_ref());
        std::fs::rename(&self.path, new_path)?;
        Ok(Self {
            file: self.file,
            path: PathBuf::from(new_name.as_ref()),
        })
    }

    pub(super) fn into_std(self) -> StdFile {
        self.file
    }

    pub(super) fn into_async(self) -> TokioFile {
        TokioFile::from_std(self.file)
    }
}

/// Get the all files with the extension under the given folder
pub(super) fn get_file_paths_with_ext(
    dir: impl AsRef<Path>,
    ext: &str,
) -> io::Result<Vec<PathBuf>> {
    let mut files = vec![];
    for result in std::fs::read_dir(dir)? {
        let file = result?;
        if let Some(filename) = file.file_name().to_str() {
            if filename.ends_with(ext) {
                files.push(file.path());
            }
        }
    }
    Ok(files)
}

/// Get the checksum of the slice, we use Sha256 as the hash function
pub(super) fn get_checksum(data: &[u8]) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().into_iter().collect()
}

/// Validate the the data with the given checksum
pub(super) fn validate_data(data: &[u8], checksum: &[u8]) -> bool {
    get_checksum(data) == checksum
}
