use std::io;

use async_trait::async_trait;
use engine::EngineError;
use thiserror::Error;
use tokio::sync::Mutex;
use utils::config::{EngineConfig, WALConfig};

use crate::{cmd::Command, log_entry::LogEntry, members::ServerId};

use self::{db::DB, wal::WALStorage};

/// Storage layer error
#[derive(Error, Debug)]
pub(super) enum StorageError {
    /// Serialize or deserialize error
    #[error("bincode error, {0}")]
    Bincode(#[from] bincode::Error),
    /// Rocksdb error
    #[error("internal error, {0}")]
    Internal(#[from] EngineError),
    /// WAL IO error
    #[error("WAL IO error, {0}")]
    IO(#[from] io::Error),
}

/// Curp storage api
#[async_trait]
pub(super) trait StorageApi: Send + Sync {
    /// Command
    type Command: Command;

    /// Put `voted_for` in storage, must be flushed on disk before returning
    async fn flush_voted_for(&self, term: u64, voted_for: ServerId) -> Result<(), StorageError>;

    /// Put log entries in storage
    async fn put_log_entries(
        &self,
        entries: Vec<&LogEntry<Self::Command>>,
    ) -> Result<(), StorageError>;
}

/// CURP storage
pub(in crate::server) struct CurpStorage<C> {
    /// The DB storing vote
    db: DB<C>,
    /// The WAL storing log entries
    wal: Mutex<WALStorage<C>>,
}

impl<C: Command> CurpStorage<C> {
    /// Recover from persisted storage
    /// Return Self, `voted_for` and all log entries
    pub(in crate::server) async fn recover(
        engine_cfg: &EngineConfig,
        wal_cfg: &WALConfig,
    ) -> Result<(Self, Option<(u64, ServerId)>, Vec<LogEntry<C>>), StorageError> {
        let db = DB::open(engine_cfg)?;
        let vote = db.recover_vote()?;
        let (wal, entries) = WALStorage::new_or_recover(wal_cfg.clone()).await?;
        let storage = Self {
            db,
            wal: Mutex::new(wal),
        };

        Ok((storage, vote, entries))
    }
}

#[async_trait::async_trait]
impl<C: Command> StorageApi for CurpStorage<C> {
    type Command = C;

    async fn flush_voted_for(&self, term: u64, voted_for: ServerId) -> Result<(), StorageError> {
        self.db.flush_voted_for(term, voted_for)
    }

    async fn put_log_entries(
        &self,
        entries: Vec<&LogEntry<Self::Command>>,
    ) -> Result<(), StorageError> {
        self.wal
            .lock()
            .await
            .send_sync(entries)
            .await
            .map_err(Into::into)
    }
}

/// CURP `DB` storage implementation
mod db;

/// CURP WAL storage implementation
mod wal;
