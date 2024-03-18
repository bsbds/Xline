use engine::EngineError;
use thiserror::Error;

use crate::{
    cmd::Command,
    log_entry::LogEntry,
    members::{ClusterInfo, ServerId},
    rpc::Member,
};

/// Storage layer error
#[derive(Error, Debug)]
#[allow(clippy::module_name_repetitions)]
#[non_exhaustive]
pub enum StorageError {
    /// Serialize or deserialize error
    #[error("codec error, {0}")]
    Codec(String),
    /// Rocksdb error
    #[error("rocksdb error, {0}")]
    RocksDB(#[from] EngineError),
    /// WAL error
    #[error("wal error, {0}")]
    WAL(#[from] std::io::Error),
}

impl From<bincode::Error> for StorageError {
    #[inline]
    fn from(e: bincode::Error) -> Self {
        Self::Codec(e.to_string())
    }
}

impl From<prost::DecodeError> for StorageError {
    #[inline]
    fn from(e: prost::DecodeError) -> Self {
        Self::Codec(e.to_string())
    }
}

/// Curp storage api
#[allow(clippy::module_name_repetitions)]
pub trait StorageApi: Send + Sync {
    /// Command
    type Command: Command;

    /// Put `voted_for` into storage, must be flushed on disk before returning
    fn flush_voted_for(&self, term: u64, voted_for: ServerId) -> Result<(), StorageError>;

    /// Put `Member` into storage
    fn put_member(&self, member: &Member) -> Result<(), StorageError>;

    /// Remove `Member` from storage
    fn remove_member(&self, id: ServerId) -> Result<(), StorageError>;

    /// Put `ClusterInfo` into storage
    fn put_cluster_info(&self, cluster_info: &ClusterInfo) -> Result<(), StorageError>;

    /// Recover `ClusterInfo` from storage
    fn recover_cluster_info(&self) -> Result<Option<ClusterInfo>, StorageError>;

    /// Put log entries in storage
    fn put_log_entries(&self, entry: &[&LogEntry<Self::Command>]) -> Result<(), StorageError>;

    /// Recover from persisted storage
    /// Return `voted_for` and all log entries
    fn recover(
        &self,
    ) -> Result<(Option<(u64, ServerId)>, Vec<LogEntry<Self::Command>>), StorageError>;
}

/// CURP `DB` storage implementation
pub(super) mod db;

/// CURP WAL storage implementation
pub(super) mod wal;
