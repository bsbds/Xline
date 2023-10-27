use std::{collections::HashMap, sync::Arc};

use clippy_utilities::OverflowArithmetic;
use crossbeam_skiplist::SkipMap;
use parking_lot::RwLock;
use xlineapi::{command::KeyRange, DeleteRangeRequest, ExecuteError, KeyValue, PutRequest};

use super::{
    index::{Index, IndexOperate},
    storage_api::StorageApi,
    KvStore,
};

/// Execute state during speculative execute
/// This state is mainly designed for solving txn compare operation
#[derive(Debug, Default)]
pub(super) struct PrepareState {
    ro_cache: RwLock<HashMap<String, bool>>,
    inner: SkipMap<Vec<u8>, RwLock<KeyValue>>,
}

impl PrepareState {
    /// Update range using current state
    pub(super) fn update_range(
        &self,
        kvs: Vec<KeyValue>,
        key: &[u8],
        range_end: &[u8],
    ) -> Vec<KeyValue> {
        let key_range = KeyRange::new(key.to_vec(), range_end.to_vec());
        let state_range = self.inner.range(key_range);
        let mut kvs: HashMap<_, _> = kvs.into_iter().map(|kv| (kv.key.clone(), kv)).collect();
        kvs.extend(state_range.map(|entry| (entry.key().clone(), entry.value().read().clone())));
        kvs.into_iter()
            .filter_map(|(_, v)| (v != KeyValue::default()).then_some(v))
            .collect()
    }

    /// Execute a `PutRequest`
    pub(super) fn put(&self, req: &PutRequest, index: &Arc<Index>, revision: i64) {
        // If we update state on this key before, directly update it from current state
        if let Some(entry) = self.inner.get(&req.key) {
            let mut prev_kv = entry.value().write();
            if *prev_kv == KeyValue::default() {
                *prev_kv = KeyValue {
                    key: req.key.clone(),
                    value: req.value.clone(),
                    create_revision: revision,
                    mod_revision: revision,
                    version: 1,
                    lease: req.lease,
                };
            } else {
                *prev_kv = KeyValue {
                    key: req.key.clone(),
                    value: req.value.clone(),
                    create_revision: prev_kv.create_revision,
                    mod_revision: revision,
                    version: prev_kv.version.overflow_add(1),
                    lease: req.lease,
                };
            }
        } else
        // Other wise query from storage index
        {
            let new_rev = index.register_revision(&req.key, revision, 0);
            let _ignore = self.inner.insert(
                req.key.clone(),
                RwLock::new(KeyValue {
                    key: req.key.clone(),
                    value: req.value.clone(),
                    create_revision: new_rev.create_revision,
                    mod_revision: new_rev.mod_revision,
                    version: new_rev.version,
                    lease: req.lease,
                }),
            );
        }
    }

    /// Execute a `DeleteRangeRequest`
    /// TODO: double check the logic here
    pub(super) fn delete_range<DB: StorageApi>(
        &self,
        store: &KvStore<DB>,
        req: &DeleteRangeRequest,
        revision: i64,
    ) -> Result<(), ExecuteError> {
        let key_range = KeyRange::new(req.key.clone(), req.range_end.clone());
        let state_range = self.inner.range(key_range);
        let store_range = store.get_range(&req.key, &req.range_end, 0)?;

        let to_delete: HashMap<_, _> = state_range
            .map(|entry| entry.key().clone())
            .chain(store_range.into_iter().map(|kv| kv.key))
            // mark deleted by setting empty KeyValue
            .map(|key| {
                (
                    key.to_vec(),
                    KeyValue {
                        key: vec![],
                        create_revision: 0,
                        mod_revision: revision,
                        version: 0,
                        value: vec![],
                        lease: 0,
                    },
                )
            })
            .collect();

        for (k, v) in to_delete {
            let _ignore = self.inner.insert(k, RwLock::new(v));
        }

        Ok(())
    }

    pub(super) fn remove_key(&self, key: &[u8], revision: i64) {
        if let Some(entry) = self.inner.get(key) {
            if entry.value().read().mod_revision == revision {
                let _ignore = self.inner.remove(key);
            }
        }
    }

    pub(super) fn remove_key_range(&self, key: Vec<u8>, range_end: Vec<u8>, revision: i64) {
        let key_range = KeyRange::new(key, range_end);
        for entry in self.inner.range(key_range) {
            let value_r = entry.value().read();
            if value_r.mod_revision == revision {
                let _ignore = self.inner.remove(entry.key());
            }
        }
    }
}

impl PrepareState {
    pub(super) fn insert_ro(&self, id: String, is_read_only: bool) {
        let _ignore = self.ro_cache.write().insert(id, is_read_only);
    }

    pub(super) fn remove_ro(&self, id: &str) -> bool {
        self.ro_cache
            .write()
            .remove(id)
            .unwrap_or_else(|| unreachable!("remove_ro should only be called after insert_ro"))
    }
}
