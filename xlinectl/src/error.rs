use std::num::ParseIntError;

use thiserror::Error;
use xline_client::error::ClientError;

/// Command line client result
pub(crate) type Result<T> = std::result::Result<T, CtlError>;

/// Client Error
#[derive(Error, Debug)]
#[non_exhaustive]
pub(crate) enum CtlError {
    /// The input arguments are invalid
    #[error("Invalid arguments: {0}")]
    InvalidArgs(String),
    /// IO error
    #[error("IO error {0}")]
    IoError(#[from] std::io::Error),
    /// The Xline client's error
    #[error("Client error: {0}")]
    ClientError(#[from] ClientError),
}

impl From<ParseIntError> for CtlError {
    fn from(value: ParseIntError) -> Self {
        Self::InvalidArgs(value.to_string())
    }
}

impl From<clap::error::Error> for CtlError {
    fn from(value: clap::error::Error) -> Self {
        Self::InvalidArgs(value.to_string())
    }
}
