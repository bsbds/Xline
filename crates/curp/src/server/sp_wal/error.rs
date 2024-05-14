use std::io;

use thiserror::Error;

/// Errors of the `WALStorage`
#[derive(Debug, Error)]
pub(crate) enum WALError {
    /// The WAL segment might reach on end
    ///
    /// NOTE: This exists because we cannot tell the difference between a corrupted WAL
    /// and a normally ended WAL, as the segment files are all preallocated with zeros
    #[error("WAL ended")]
    MaybeEnded,
    /// The WAL corrupt error
    #[error("WAL corrupted: {0}")]
    Corrupted(CorruptType),
    /// The IO error
    #[error("IO error: {0}")]
    IO(#[from] io::Error),
}

/// The type of the `Corrupted` error
#[derive(Debug, Error)]
pub(crate) enum CorruptType {
    /// Corrupt because of decode failure
    #[error("Error occurred when decoding WAL: {0}")]
    Codec(String),
    /// Corrupt because of checksum failure
    #[error("Checksumming for the file has failed")]
    Checksum,
    /// Corrupt because of some logs is missing
    #[error("The recovered logs are not continue")]
    LogNotContinue,
}

impl WALError {
    pub(super) fn io_or_corrupt(self) -> io::Result<CorruptType> {
        match self {
            WALError::Corrupted(e) => Ok(e),
            WALError::IO(e) => return Err(e),
            WALError::MaybeEnded => unreachable!("Should not call on WALError::MaybeEnded"),
        }
    }
}

impl From<WALError> for io::Error {
    fn from(err: WALError) -> Self {
        match err {
            WALError::MaybeEnded => unreachable!("Should not call on WALError::MaybeEnded"),
            WALError::Corrupted(_) => io::Error::new(io::ErrorKind::InvalidData, err.to_string()),
            WALError::IO(e) => e,
        }
    }
}
