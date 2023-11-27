use std::{
    fs::{File as StdFile, OpenOptions},
    io,
    path::{Path, PathBuf},
};

use fs2::FileExt;
use tokio::fs::File as TokioFile;

/// File that is exclusively locked
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
        std::fs::rename(&self.path, new_name.as_ref())?;
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
