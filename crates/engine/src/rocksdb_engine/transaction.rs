#![allow(clippy::module_name_repetitions)]

use std::{
    iter::repeat,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
};

use clippy_utilities::NumericCast;
use parking_lot::Mutex;
use rocksdb::{Direction, IteratorMode, OptimisticTransactionDB, Transaction};

use crate::{api::transaction_api::TransactionApi, error::EngineError, StorageOps, WriteOperation};

use super::RocksEngine;

/// Transaction type for `RocksDB`
pub struct RocksTransaction<'db> {
    /// The inner DB
    db: Arc<OptimisticTransactionDB>,
    /// A transaction of the DB
    ///
    /// We need a `Mutex` because `Transaction<'db, DB>` does not implement `Sync`
    txn: Mutex<Option<Transaction<'db, OptimisticTransactionDB>>>,
    /// The size of the engine
    engine_size: Arc<AtomicU64>,
    /// The size of the txn
    txn_size: AtomicUsize,
}

/// Write operation
/// This is an owned type of `WriteOperation`
#[non_exhaustive]
#[derive(Debug)]
enum WriteOperationOwned {
    /// `Put` operation
    Put {
        /// The table name
        table: String,
        /// Key
        key: Vec<u8>,
        /// Value
        value: Vec<u8>,
    },
    /// `Delete` operation
    Delete {
        /// The table name
        table: String,
        /// The target key
        key: Vec<u8>,
    },
    /// Delete range operation, it will remove the database entries in the range [from, to)
    DeleteRange {
        /// The table name
        table: String,
        /// The `from` key
        from: Vec<u8>,
        /// The `to` key
        to: Vec<u8>,
    },
}

impl<'db> RocksTransaction<'db> {
    /// Creates a new `RocksTransaction`
    pub(super) fn new(
        db: Arc<OptimisticTransactionDB>,
        txn: Transaction<'db, OptimisticTransactionDB>,
        engine_size: Arc<AtomicU64>,
    ) -> Self {
        Self {
            db,
            txn: Mutex::new(Some(txn)),
            engine_size,
            txn_size: AtomicUsize::new(0),
        }
    }
}

#[allow(clippy::unwrap_used, clippy::unwrap_in_result)] // txn is always `Some`
impl StorageOps for RocksTransaction<'_> {
    fn write(&self, op: WriteOperation<'_>, _sync: bool) -> Result<(), EngineError> {
        match op.into() {
            WriteOperationOwned::Put { table, key, value } => {
                let cf = self
                    .db
                    .cf_handle(table.as_ref())
                    .ok_or_else(|| EngineError::TableNotFound(table.clone()))?;
                self.txn
                    .lock()
                    .as_ref()
                    .unwrap()
                    .put_cf(&cf, &key, &value)
                    .map_err(EngineError::from)?;
                let _ignore = self.txn_size.fetch_add(
                    RocksEngine::max_write_size(table.len(), key.len(), value.len()),
                    Ordering::Relaxed,
                );
            }
            WriteOperationOwned::Delete { table, key } => {
                let cf = self
                    .db
                    .cf_handle(table.as_ref())
                    .ok_or_else(|| EngineError::TableNotFound(table.clone()))?;
                self.txn
                    .lock()
                    .as_ref()
                    .unwrap()
                    .delete_cf(&cf, key)
                    .map_err(EngineError::from)?;
            }
            WriteOperationOwned::DeleteRange { table, from, to } => {
                let cf = self
                    .db
                    .cf_handle(table.as_ref())
                    .ok_or_else(|| EngineError::TableNotFound(table.clone()))?;
                let mode = IteratorMode::From(&from, Direction::Forward);
                let kvs: Vec<_> = self
                    .txn
                    .lock()
                    .as_ref()
                    .unwrap()
                    .iterator_cf(&cf, mode)
                    .take_while(|res| {
                        res.as_ref()
                            .is_ok_and(|(key, _)| key.as_ref() < to.as_slice())
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                for (key, _) in kvs {
                    self.txn.lock().as_ref().unwrap().delete_cf(&cf, key)?;
                }
            }
        }

        Ok(())
    }

    fn write_multi(&self, ops: Vec<WriteOperation<'_>>, sync: bool) -> Result<(), EngineError> {
        for op in ops {
            self.write(op, sync)?;
        }
        Ok(())
    }

    fn get(&self, table: &str, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, EngineError> {
        let cf = self
            .db
            .cf_handle(table.as_ref())
            .ok_or_else(|| EngineError::TableNotFound(table.to_owned()))?;
        self.txn
            .lock()
            .as_ref()
            .unwrap()
            .get_cf(&cf, key)
            .map_err(EngineError::from)
    }

    fn get_multi(
        &self,
        table: &str,
        keys: &[impl AsRef<[u8]>],
    ) -> Result<Vec<Option<Vec<u8>>>, EngineError> {
        let cf = self
            .db
            .cf_handle(table.as_ref())
            .ok_or_else(|| EngineError::TableNotFound(table.to_owned()))?;
        self.txn
            .lock()
            .as_ref()
            .unwrap()
            .multi_get_cf(repeat(&cf).zip(keys.iter()))
            .into_iter()
            .collect::<Result<_, _>>()
            .map_err(EngineError::from)
    }
}

#[allow(clippy::unwrap_used, clippy::unwrap_in_result)] // txn is always `Some`
impl TransactionApi for RocksTransaction<'_> {
    fn commit(self) -> Result<(), EngineError> {
        let _ignore = self.engine_size.fetch_add(
            self.txn_size.load(Ordering::Relaxed).numeric_cast(),
            Ordering::Relaxed,
        );

        self.txn.lock().take().unwrap().commit().map_err(Into::into)
    }

    fn rollback(&self) -> Result<(), EngineError> {
        self.txn
            .lock()
            .as_ref()
            .unwrap()
            .rollback()
            .map_err(Into::into)
    }
}

impl std::fmt::Debug for RocksTransaction<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RocksTransaction")
            .field("db", &self.db)
            .finish()
    }
}

impl From<WriteOperation<'_>> for WriteOperationOwned {
    fn from(op: WriteOperation<'_>) -> Self {
        match op {
            WriteOperation::Put { table, key, value } => Self::Put {
                table: table.to_owned(),
                key,
                value,
            },
            WriteOperation::Delete { table, key } => Self::Delete {
                table: table.to_owned(),
                key: key.to_owned(),
            },
            WriteOperation::DeleteRange { table, from, to } => Self::DeleteRange {
                table: table.to_owned(),
                from: from.to_owned(),
                to: to.to_owned(),
            },
        }
    }
}
