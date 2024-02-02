use std::{
    iter::repeat,
    sync::{atomic::AtomicU64, Arc},
};

use clippy_utilities::{NumericCast, OverflowArithmetic};
use parking_lot::Mutex;
use rocksdb::{Direction, IteratorMode, OptimisticTransactionDB, Transaction};

use crate::{
    api::transaction_api::TransactionApi, error::EngineError, rocksdb_engine::RocksEngine,
    StorageOps, WriteOperation,
};

/// Transaction type for `RocksDB`
///
/// WARN: `db` should never be dropped before `txn`
pub struct RocksTransaction {
    /// The inner DB
    pub(super) db: Arc<OptimisticTransactionDB>,
    /// A transaction of the DB
    pub(super) txn: Mutex<Option<Transaction<'static, OptimisticTransactionDB>>>,
    /// The size of the engine
    pub(super) engine_size: Arc<AtomicU64>,
    /// The size of the txn
    pub(super) txn_size: AtomicU64,
}

impl std::fmt::Debug for RocksTransaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RocksTransaction")
            .field("db", &self.db)
            .finish()
    }
}

impl StorageOps for RocksTransaction {
    fn write(&self, op: WriteOperation<'_>, _sync: bool) -> Result<(), EngineError> {
        let txn_l = self.txn.lock();
        let txn = txn_l.as_ref().unwrap();
        let mut size = 0;
        #[allow(clippy::pattern_type_mismatch)] // can't be fixed
        match op {
            WriteOperation::Put { table, key, value } => {
                let cf = self
                    .db
                    .cf_handle(table.as_ref())
                    .ok_or_else(|| EngineError::TableNotFound(table.to_owned()))?;
                size = size.overflow_add(RocksEngine::max_write_size(
                    table.len(),
                    key.len(),
                    value.len(),
                ));
                txn.put_cf(&cf, key, value).map_err(EngineError::from)?;
            }
            WriteOperation::Delete { table, key } => {
                let cf = self
                    .db
                    .cf_handle(table.as_ref())
                    .ok_or_else(|| EngineError::TableNotFound(table.to_owned()))?;
                txn.delete_cf(&cf, key).map_err(EngineError::from)?;
            }
            WriteOperation::DeleteRange { table, from, to } => {
                let cf = self
                    .db
                    .cf_handle(table.as_ref())
                    .ok_or_else(|| EngineError::TableNotFound(table.to_owned()))?;
                let mode = IteratorMode::From(from, Direction::Forward);
                let kvs: Vec<_> = txn
                    .iterator_cf(&cf, mode)
                    .take_while(|res| res.as_ref().is_ok_and(|(key, _)| key.as_ref() < to))
                    .collect::<Result<Vec<_>, _>>()?;
                for (key, _) in kvs {
                    txn.delete_cf(&cf, key)?;
                }
            }
        };

        _ = self
            .txn_size
            .fetch_add(size.numeric_cast(), std::sync::atomic::Ordering::Relaxed);

        Ok(())
    }

    fn write_multi(&self, _ops: Vec<WriteOperation<'_>>, _sync: bool) -> Result<(), EngineError> {
        todo!()
    }

    fn get(&self, table: &str, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, EngineError> {
        let txn_l = self.txn.lock();
        let txn = txn_l.as_ref().unwrap();
        let cf = self
            .db
            .cf_handle(table.as_ref())
            .ok_or_else(|| EngineError::TableNotFound(table.to_owned()))?;
        txn.get_cf(&cf, key).map_err(EngineError::from)
    }

    fn get_multi(
        &self,
        table: &str,
        keys: &[impl AsRef<[u8]>],
    ) -> Result<Vec<Option<Vec<u8>>>, EngineError> {
        let txn_l = self.txn.lock();
        let txn = txn_l.as_ref().unwrap();
        let cf = self
            .db
            .cf_handle(table.as_ref())
            .ok_or_else(|| EngineError::TableNotFound(table.to_owned()))?;
        txn.multi_get_cf(repeat(&cf).zip(keys.iter()))
            .into_iter()
            .collect::<Result<_, _>>()
            .map_err(EngineError::from)
    }
}

#[allow(clippy::unwrap_used, clippy::unwrap_in_result)] // The transaction always exist
impl TransactionApi for RocksTransaction {
    fn commit(self) -> Result<(), EngineError> {
        self.txn.lock().take().unwrap().commit()?;
        let size = self.txn_size.load(std::sync::atomic::Ordering::Relaxed);
        let _ignore = self
            .engine_size
            .fetch_add(size, std::sync::atomic::Ordering::Relaxed);

        Ok(())
    }

    fn rollback(&self) -> Result<(), EngineError> {
        let txn_l = self.txn.lock();
        let txn = txn_l.as_ref().unwrap();
        txn.rollback().map_err(EngineError::from)
    }
}
