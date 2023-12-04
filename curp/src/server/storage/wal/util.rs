use std::{
    fs::{File as StdFile, OpenOptions},
    io::{self, Seek, SeekFrom},
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
    pub(super) fn open_rw(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path.as_ref())?;
        file.try_lock_exclusive()?;

        Ok(Self {
            file,
            path: path.as_ref().into(),
        })
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

    /// Convert self to std file
    pub(super) fn into_std(self) -> StdFile {
        self.file
    }

    /// Convert self to tokio file
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

/// Checks whether the file exist
pub(super) fn is_exist(path: impl AsRef<Path>) -> bool {
    std::fs::metadata(path).is_ok()
}

/// Parse a u64 from u8 slice
pub(super) fn parse_u64(bytes_le: &[u8]) -> u64 {
    assert_eq!(bytes_le.len(), 8, "The slice passed should be 8 bytes long");
    u64::from_le_bytes(
        bytes_le
            .try_into()
            .unwrap_or_else(|_| unreachable!("This conversion should always exist")),
    )
}

#[cfg(test)]
mod tests {
    use std::{io::Read, process::Command};

    use super::*;

    #[test]
    fn file_open_is_exclusive() {
        let mut path = tempfile::tempdir().unwrap().into_path();
        path.push("file_open.test");
        let lfile = LockedFile::open_rw(&path).unwrap();
        let path_str = path.to_str().unwrap();

        let mut try_flock_output = Command::new("sh")
            .args(&[
                "-c",
                &format!("flock --nonblock {path_str} echo some_data >> {path_str}"),
            ])
            .output()
            .unwrap();
        assert_ne!(try_flock_output.status.code().unwrap(), 0);

        let mut file = lfile.into_std();
        let mut buf = String::new();
        file.read_to_string(&mut buf);
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn get_file_paths_with_ext_is_ok() {
        let dir = "/tmp/wal-test/";
        let num_paths = 10;
        let paths_create: Vec<_> = (0..num_paths)
            .map(|i| {
                let mut path = PathBuf::from(dir);
                path.push(format!("{i}.test"));
                std::fs::File::create(&path).unwrap();
                path
            })
            .collect();
        let mut paths = get_file_paths_with_ext(dir, ".test").unwrap();
        paths.sort();
        assert_eq!(paths.len(), num_paths);
        assert!(paths
            .into_iter()
            .zip(paths_create.into_iter())
            .all(|(x, y)| x.as_path() == y.as_path()));
    }
}
