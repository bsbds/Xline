/// File pipeline
pub(crate) mod pipeline;

/// Framed
pub(crate) mod framed;

/// File lock utils
pub(crate) mod lock;

use std::{
    io,
    path::{Path, PathBuf},
};

use sha2::{digest::Output, Digest, Sha256};

/// Gets the all files with the extension under the given folder
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

/// Gets the parent dir
pub(super) fn parent_dir(dir: impl AsRef<Path>) -> PathBuf {
    let mut parent = PathBuf::from(dir.as_ref());
    let _ignore = parent.pop();
    parent
}

/// Fsyncs the parent directory
pub(super) fn sync_parent_dir(dir: impl AsRef<Path>) -> io::Result<()> {
    let parent_dir = parent_dir(&dir);
    let parent = std::fs::File::open(parent_dir)?;
    parent.sync_all()?;

    Ok(())
}

/// Gets the checksum of the slice, we use Sha256 as the hash function
pub(super) fn get_checksum(data: &[u8]) -> Output<Sha256> {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize()
}

/// Validates the the data with the given checksum
pub(super) fn validate_data(data: &[u8], checksum: &[u8]) -> bool {
    AsRef::<[u8]>::as_ref(&get_checksum(data)) == checksum
}

/// Checks whether the file exist
pub(super) fn is_exist(path: impl AsRef<Path>) -> bool {
    std::fs::metadata(path).is_ok()
}

/// Parses a u64 from u8 slice
pub(super) fn parse_u64(bytes_le: &[u8]) -> u64 {
    assert_eq!(bytes_le.len(), 8, "The slice passed should be 8 bytes long");
    u64::from_le_bytes(
        bytes_le
            .try_into()
            .unwrap_or_else(|_| unreachable!("This conversion should always exist")),
    )
}
