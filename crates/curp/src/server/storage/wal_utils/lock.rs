use std::{
    fs::{File as StdFile, OpenOptions},
    io,
    path::{Path, PathBuf},
};

use fs2::FileExt;

use super::{is_exist, parent_dir, sync_parent_dir};

/// File that is exclusively locked
#[derive(Debug)]
pub(crate) struct LockedFile {
    /// The inner std file
    file: Option<StdFile>,
    /// The path of the file
    path: PathBuf,
}

impl LockedFile {
    /// Opens the file in read and append mode
    pub(crate) fn open_rw(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path.as_ref())?;
        file.try_lock_exclusive()?;

        Ok(Self {
            file: Some(file),
            path: path.as_ref().into(),
        })
    }

    /// Pre-allocates the file
    pub(crate) fn preallocate(&mut self, size: u64) -> io::Result<()> {
        if size == 0 {
            return Ok(());
        }

        self.file().allocate(size)
    }

    /// Gets the path of this file
    pub(crate) fn path(&self) -> PathBuf {
        self.path.clone()
    }

    /// Renames the current file
    ///
    /// We will discard this file if the rename has failed
    pub(crate) fn rename(mut self, new_name: impl AsRef<Path>) -> io::Result<Self> {
        let mut new_path = parent_dir(&self.path);
        new_path.push(new_name.as_ref());
        std::fs::rename(&self.path, &new_path)?;
        sync_parent_dir(&new_path)?;

        Ok(Self {
            file: self.file.take(),
            path: PathBuf::from(new_name.as_ref()),
        })
    }

    /// Converts self to std file
    pub(crate) fn into_std(self) -> StdFile {
        let mut this = std::mem::ManuallyDrop::new(self);
        this.file
            .take()
            .unwrap_or_else(|| unreachable!("File should always exist after creation"))
    }

    /// Gets the file wrapped inside an `Option`
    fn file(&mut self) -> &mut StdFile {
        self.file
            .as_mut()
            .unwrap_or_else(|| unreachable!("File should always exist after creation"))
    }
}

impl Drop for LockedFile {
    fn drop(&mut self) {
        if self.file.is_some() && is_exist(self.path()) {
            let _ignore = std::fs::remove_file(self.path());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use super::super::get_file_paths_with_ext;

    #[test]
    fn file_rename_is_ok() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut path = PathBuf::from(tempdir.path());
        path.push("file.test");
        let lfile = LockedFile::open_rw(&path).unwrap();
        let new_name = "new_name.test";
        let mut new_path = parent_dir(&path);
        new_path.push(new_name);
        lfile.rename(new_name).unwrap();
        assert!(!is_exist(path));
        assert!(is_exist(new_path));
    }

    #[test]
    #[allow(clippy::verbose_file_reads)] // false positive
    fn file_open_is_exclusive() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut path = PathBuf::from(tempdir.path());
        path.push("file.test");
        let _lfile = LockedFile::open_rw(&path).unwrap();
        assert!(
            LockedFile::open_rw(&path).is_err(),
            "acquire lock should failed"
        );
    }

    #[test]
    fn get_file_paths_with_ext_is_ok() {
        let dir = tempfile::tempdir().unwrap();
        let num_paths = 10;
        let paths_create: Vec<_> = (0..num_paths)
            .map(|i| {
                let mut path = PathBuf::from(dir.path());
                path.push(format!("{i}.test"));
                std::fs::File::create(&path).unwrap();
                path
            })
            .collect();
        let mut paths = get_file_paths_with_ext(dir.path(), ".test").unwrap();
        paths.sort();
        assert_eq!(paths.len(), num_paths);
        assert!(paths
            .into_iter()
            .zip(paths_create.into_iter())
            .all(|(x, y)| x.as_path() == y.as_path()));
    }
}
