use std::marker::PhantomData;

use engine::{Engine, EngineType, StorageEngine, WriteOperation};
use utils::config::EngineConfig;

use super::StorageError;
use crate::members::ServerId;

/// Key for persisted state
const VOTE_FOR: &[u8] = b"VoteFor";

/// Column family name for curp storage
const CF: &str = "curp";

/// `DB` storage implementation
pub(super) struct DB<C> {
    /// DB handle
    db: Engine,
    /// Phantom
    phantom: PhantomData<C>,
}

impl<C> DB<C> {
    /// Create a new CURP `DB`
    pub(super) fn open(config: &EngineConfig) -> Result<Self, StorageError> {
        let engine_type = match *config {
            EngineConfig::Memory => EngineType::Memory,
            EngineConfig::RocksDB(ref path) => EngineType::Rocks(path.clone()),
            _ => unreachable!("Not supported storage type"),
        };
        let db = Engine::new(engine_type, &[CF])?;
        Ok(Self {
            db,
            phantom: PhantomData,
        })
    }

    pub(super) fn flush_voted_for(
        &self,
        term: u64,
        voted_for: ServerId,
    ) -> Result<(), StorageError> {
        let bytes = bincode::serialize(&(term, voted_for))?;
        let op = WriteOperation::new_put(CF, VOTE_FOR.to_vec(), bytes);
        self.db.write_batch(vec![op], true)?;

        Ok(())
    }

    /// Recovers vote infomations
    pub(super) fn recover_vote(&self) -> Result<Option<(u64, ServerId)>, StorageError> {
        let voted_for = self
            .db
            .get(CF, VOTE_FOR)?
            .map(|bytes| bincode::deserialize::<(u64, ServerId)>(&bytes))
            .transpose()?;

        Ok(voted_for)
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use curp_test_utils::{sleep_secs, test_cmd::TestCommand};
    use test_macros::abort_on_panic;
    use tokio::fs::remove_dir_all;

    use super::*;

    #[tokio::test]
    #[abort_on_panic]
    async fn create_and_recover() -> Result<(), Box<dyn Error>> {
        let db_dir = tempfile::tempdir().unwrap().into_path();
        let storage_cfg = EngineConfig::RocksDB(db_dir.clone());
        {
            let s = DB::<TestCommand>::open(&storage_cfg)?;
            s.flush_voted_for(1, 222)?;
            s.flush_voted_for(3, 111)?;
            sleep_secs(2).await;
        }

        {
            let s = DB::<TestCommand>::open(&storage_cfg)?;
            let voted_for = s.recover_vote()?;
            assert_eq!(voted_for, Some((3, 111)));
        }

        remove_dir_all(db_dir).await?;

        Ok(())
    }
}
