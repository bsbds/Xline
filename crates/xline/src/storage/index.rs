#![allow(clippy::multiple_inherent_impl)]
#![allow(unused)]

use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use clippy_utilities::OverflowArithmetic;
use crossbeam_skiplist::{map::Entry, SkipMap};
use itertools::Itertools;
use parking_lot::RwLock;
use utils::parking_lot_lock::RwLockMap;
use xlineapi::command::KeyRange;

use super::revision::{KeyRevision, Revision};
use crate::server::command::RangeType;

/// A Key revisions map
type KeyRevMap = RwLock<BTreeMap<Vec<u8>, Vec<KeyRevision>>>;

/// Keys to revisions mapping
#[derive(Debug)]
pub(crate) struct Index {
    /// Inner struct of `Index`
    inner: Arc<KeyRevMap>,
}

/// A transaction for `Index`
#[derive(Debug)]
pub(crate) struct IndexTransaction {
    /// Inner struct of `Index`
    index_ref: Arc<KeyRevMap>,
    /// State current modification
    state: BTreeMap<Vec<u8>, Vec<KeyRevision>>,
}

impl Index {
    /// New `Index`
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    /// Creates a transaction
    pub(crate) fn transaction(&self) -> IndexTransaction {
        IndexTransaction {
            index_ref: Arc::clone(&self.inner),
            state: BTreeMap::new(),
        }
    }

    /// Filter out `KeyRevision` that is less than one revision and convert to `Revision`
    fn filter_revision(revs: &[KeyRevision], revision: i64) -> Vec<Revision> {
        revs.iter()
            .filter(|rev| rev.mod_revision >= revision)
            .map(KeyRevision::as_revision)
            .collect()
    }

    /// Get specified or last `KeyRevision` if the key is not deleted, and convert to `Revision`
    fn get_revision(revs: &[KeyRevision], revision: i64) -> Option<Revision> {
        let rev = if revision <= 0 {
            revs.last()
        } else {
            let idx = match revs.binary_search_by(|rev| rev.mod_revision.cmp(&revision)) {
                Ok(idx) => idx,
                Err(e) => {
                    if e == 0 {
                        return None;
                    }
                    e.overflow_sub(1)
                }
            };
            revs.get(idx)
        };
        rev.filter(|kr| !kr.is_deleted())
            .map(KeyRevision::as_revision)
    }

    /// Get all revisions that need to be kept after compact at the given revision
    pub(crate) fn keep(&self, at_rev: i64) -> HashSet<Revision> {
        let mut revs = HashSet::new();
        self.inner.read().values().for_each(|revisions| {
            if let Some(revision) = revisions.first() {
                if revision.mod_revision < at_rev {
                    let pivot = revisions.partition_point(|rev| rev.mod_revision <= at_rev);
                    let compacted_last_idx = pivot.overflow_sub(1);
                    let key_rev = revisions.get(compacted_last_idx).unwrap_or_else(|| {
                        unreachable!(
                            "Oops, the key revision at {compacted_last_idx} should not be None",
                        )
                    });
                    if !key_rev.is_deleted() {
                        _ = revs.insert(key_rev.as_revision());
                    }
                }
            }
        });
        revs
    }
}

impl Index {
    /// Get `Revision` of keys, get the latest `Revision` when revision <= 0
    pub(super) fn get(&self, key: &[u8], range_end: &[u8], revision: i64) -> Vec<Revision> {
        self.with_transaction(|txn| txn.get(key, range_end, revision))
    }

    /// Register a new `KeyRevision` of the given key
    pub(super) fn register_revision(
        &self,
        key: &[u8],
        revision: i64,
        sub_revision: i64,
    ) -> KeyRevision {
        self.with_transaction(|txn| txn.register_revision(key, revision, sub_revision))
    }

    /// Register a new `KeyRevision` of the given key
    pub(super) fn insert(&self, key_revisions: Vec<(Vec<u8>, KeyRevision)>) {
        self.with_transaction(|txn| txn.insert(key_revisions));
    }

    /// Mark keys as deleted and return latest revision before deletion and deletion revision
    /// return all revision pairs and all keys in range
    pub(super) fn delete(
        &self,
        key: &[u8],
        range_end: &[u8],
        revision: i64,
        sub_revision: i64,
    ) -> (Vec<(Revision, Revision)>, Vec<Vec<u8>>) {
        self.with_transaction(|txn| txn.delete(key, range_end, revision, sub_revision))
    }

    /// Executes a function with a transaction
    fn with_transaction<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut IndexTransaction) -> R,
    {
        let mut txn = self.transaction();
        let res = f(&mut txn);
        txn.commit();
        res
    }
}

impl IndexTransaction {
    /// Gets the revisions for a single key
    fn one_key_revisions(&self, key: &[u8]) -> Vec<KeyRevision> {
        self.index_ref
            .read()
            .get(key)
            .into_iter()
            .cloned()
            .chain(self.state.get(key).into_iter().cloned())
            .flatten()
            .collect()
    }

    /// Gets the revisions for a range of keys
    fn range_key_revisions(&self, range: KeyRange) -> BTreeMap<Vec<u8>, Vec<KeyRevision>> {
        let mut map: BTreeMap<Vec<u8>, Vec<KeyRevision>> = BTreeMap::new();
        for (key, value) in self
            .index_ref
            .read()
            .range(range.clone())
            .map(|(k, v)| (k.clone(), v.clone()))
            .chain(self.state.range(range).map(|(k, v)| (k.clone(), v.clone())))
        {
            let entry = map.entry(key.clone()).or_default();
            entry.extend(value);
        }
        map
    }

    /// Gets the revisions for all keys
    fn all_key_revisions(&self) -> BTreeMap<Vec<u8>, Vec<KeyRevision>> {
        let mut map: BTreeMap<Vec<u8>, Vec<KeyRevision>> = BTreeMap::new();
        for (key, value) in self
            .index_ref
            .read()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .chain(self.state.clone().into_iter())
        {
            let entry = map.entry(key.clone()).or_default();
            entry.extend(value.clone());
        }
        map
    }

    /// Deletes on key
    fn delete_one(
        &mut self,
        key: &[u8],
        revision: i64,
        sub_revision: i64,
    ) -> Option<((Revision, Revision), Vec<u8>)> {
        let revs = self.one_key_revisions(key);
        let last_available_rev = Index::get_revision(&revs, 0)?;
        let entry = self.state.entry(key.to_vec()).or_insert(vec![]);
        let del_rev = KeyRevision::new_deletion(revision, sub_revision);
        entry.push(del_rev);
        let pair = (last_available_rev, del_rev.as_revision());

        Some((pair, key.to_vec()))
    }

    /// Deletes a range of keys
    fn delete_range(
        &mut self,
        range: KeyRange,
        revision: i64,
        sub_revision: i64,
    ) -> (Vec<(Revision, Revision)>, Vec<Vec<u8>>) {
        self.range_key_revisions(range)
            .into_keys()
            .zip(0..)
            .filter_map(|(key, i)| self.delete_one(&key, revision, sub_revision.overflow_add(i)))
            .unzip()
    }

    /// Deletes all keys
    fn delete_all(
        &mut self,
        revision: i64,
        sub_revision: i64,
    ) -> (Vec<(Revision, Revision)>, Vec<Vec<u8>>) {
        self.all_key_revisions()
            .into_keys()
            .zip(0..)
            .filter_map(|(key, i)| self.delete_one(&key, revision, sub_revision.overflow_add(i)))
            .unzip()
    }

    /// Reads an entry
    #[allow(clippy::needless_pass_by_value)]
    fn entry_read(entry: Entry<Vec<u8>, RwLock<Vec<KeyRevision>>>) -> (Vec<u8>, Vec<KeyRevision>) {
        (entry.key().clone(), entry.value().read().clone())
    }
}

/// Operations of Index
pub(super) trait IndexOperate {
    /// Get `Revision` of keys from one revision
    fn get_from_rev(&self, key: &[u8], range_end: &[u8], revision: i64) -> Vec<Revision>;

    /// Restore `KeyRevision` of a key
    fn restore(
        &self,
        key: Vec<u8>,
        revision: i64,
        sub_revision: i64,
        create_revision: i64,
        version: i64,
    );

    /// Compact a `KeyRevision` by removing the versions with smaller or equal
    /// revision than the given atRev except the largest one (If the largest one is
    /// a tombstone, it will not be kept).
    fn compact(&self, at_rev: i64) -> Vec<KeyRevision>;
}

/// Transactional operations for `Index`
pub(crate) trait IndexTransactionOperate {
    /// Get `Revision` of keys, get the latest `Revision` when revision <= 0
    fn get(&self, key: &[u8], range_end: &[u8], revision: i64) -> Vec<Revision>;

    /// Register a new `KeyRevision` of the given key
    fn register_revision(&self, key: &[u8], revision: i64, sub_revision: i64) -> KeyRevision;

    /// Insert or update `KeyRevision`
    fn insert(&mut self, key_revisions: Vec<(Vec<u8>, KeyRevision)>);

    /// Mark keys as deleted and return latest revision before deletion and deletion revision
    /// return all revision pairs and all keys in range
    fn delete(
        &mut self,
        key: &[u8],
        range_end: &[u8],
        revision: i64,
        sub_revision: i64,
    ) -> (Vec<(Revision, Revision)>, Vec<Vec<u8>>);

    /// Commits all changes
    fn commit(self);

    /// Rollbacks all changes
    fn rollback(&mut self);
}

impl IndexOperate for Index {
    fn get_from_rev(&self, key: &[u8], range_end: &[u8], revision: i64) -> Vec<Revision> {
        match RangeType::get_range_type(key, range_end) {
            RangeType::OneKey => self
                .inner
                .read()
                .get(key)
                .map(|revs| Self::filter_revision(revs.as_ref(), revision))
                .unwrap_or_default(),
            RangeType::AllKeys => self
                .inner
                .read()
                .values()
                .flat_map(|revs| Self::filter_revision(revs.as_ref(), revision))
                .sorted()
                .collect(),
            RangeType::Range => self
                .inner
                .read()
                .range(KeyRange::new(key, range_end))
                .flat_map(|(_, revs)| Self::filter_revision(revs.as_ref(), revision))
                .sorted()
                .collect(),
        }
    }

    fn restore(
        &self,
        key: Vec<u8>,
        revision: i64,
        sub_revision: i64,
        create_revision: i64,
        version: i64,
    ) {
        let mut inner_w = self.inner.write();
        let revisions = inner_w.entry(key).or_default();
        revisions.push(KeyRevision::new(
            create_revision,
            version,
            revision,
            sub_revision,
        ));
    }

    fn compact(&self, at_rev: i64) -> Vec<KeyRevision> {
        let mut revs = Vec::new();
        let mut del_keys = Vec::new();

        self.inner.write().iter_mut().for_each(|(key, revisions)| {
            if let Some(revision) = revisions.first() {
                if revision.mod_revision < at_rev {
                    let pivot = revisions.partition_point(|rev| rev.mod_revision <= at_rev);
                    let compacted_last_idx = pivot.overflow_sub(1);
                    // There is at least 1 element in the first partition, so the key revision at `compacted_last_idx`
                    // must exist.
                    let key_rev = revisions.get(compacted_last_idx).unwrap_or_else(|| {
                        unreachable!(
                            "Oops, the key revision at {compacted_last_idx} should not be None",
                        )
                    });
                    let compact_revs = if key_rev.is_deleted() {
                        revisions.drain(..=compacted_last_idx)
                    } else {
                        revisions.drain(..compacted_last_idx)
                    };
                    revs.extend(compact_revs.into_iter());

                    if revisions.is_empty() {
                        del_keys.push(key.clone());
                    }
                }
            }
        });
        for key in del_keys {
            let _ignore = self.inner.write().remove(&key);
        }
        revs
    }
}

impl IndexTransactionOperate for IndexTransaction {
    fn get(&self, key: &[u8], range_end: &[u8], revision: i64) -> Vec<Revision> {
        match RangeType::get_range_type(key, range_end) {
            RangeType::OneKey => Index::get_revision(&self.one_key_revisions(key), revision)
                .map(|rev| vec![rev])
                .unwrap_or_default(),
            RangeType::AllKeys => self
                .all_key_revisions()
                .into_iter()
                .filter_map(|(_, revs)| Index::get_revision(revs.as_ref(), revision))
                .collect(),
            RangeType::Range => self
                .range_key_revisions(KeyRange::new(key, range_end))
                .into_iter()
                .filter_map(|(_, revs)| Index::get_revision(revs.as_ref(), revision))
                .collect(),
        }
    }

    fn register_revision(&self, key: &[u8], revision: i64, sub_revision: i64) -> KeyRevision {
        self.one_key_revisions(key).last().map_or(
            KeyRevision::new(revision, 1, revision, sub_revision),
            |rev| {
                if rev.is_deleted() {
                    KeyRevision::new(revision, 1, revision, sub_revision)
                } else {
                    KeyRevision::new(
                        rev.create_revision,
                        rev.version.overflow_add(1),
                        revision,
                        sub_revision,
                    )
                }
            },
        )
    }

    fn insert(&mut self, key_revisions: Vec<(Vec<u8>, KeyRevision)>) {
        for (key, revision) in key_revisions {
            if let Some(entry) = self.state.get_mut::<[u8]>(key.as_ref()) {
                entry.push(revision);
            } else {
                _ = self.state.insert(key, vec![revision]);
            }
        }
    }

    fn delete(
        &mut self,
        key: &[u8],
        range_end: &[u8],
        revision: i64,
        sub_revision: i64,
    ) -> (Vec<(Revision, Revision)>, Vec<Vec<u8>>) {
        let (pairs, keys) = match RangeType::get_range_type(key, range_end) {
            RangeType::OneKey => self
                .delete_one(key, revision, sub_revision)
                .into_iter()
                .unzip(),
            RangeType::AllKeys => self.delete_all(revision, sub_revision),
            RangeType::Range => {
                self.delete_range(KeyRange::new(key, range_end), revision, sub_revision)
            }
        };

        (pairs, keys)
    }

    fn commit(self) {
        let mut index_w = self.index_ref.write();
        for (key, state_revs) in self.state {
            let revs = index_w.entry(key).or_default();
            revs.extend(state_revs);
        }
    }

    fn rollback(&mut self) {
        self.state.clear();
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[allow(clippy::expect_used)]
    fn match_values(index: &Index, key: impl AsRef<[u8]>, expected_values: &[KeyRevision]) {
        let inner_r = index.inner.read();
        let revs = inner_r
            .get(key.as_ref())
            .expect("index entry should not be None");
        assert_eq!(*revs, expected_values);
    }

    fn init_and_test_insert() -> Index {
        let index = Index::new();
        let mut txn = index.transaction();

        txn.insert(vec![
            (b"key".to_vec(), txn.register_revision(b"key", 1, 3)),
            (b"foo".to_vec(), txn.register_revision(b"foo", 4, 5)),
            (b"bar".to_vec(), txn.register_revision(b"bar", 5, 4)),
        ]);

        txn.insert(vec![
            (b"key".to_vec(), txn.register_revision(b"key", 2, 2)),
            (b"foo".to_vec(), txn.register_revision(b"foo", 6, 6)),
            (b"bar".to_vec(), txn.register_revision(b"bar", 7, 7)),
        ]);
        txn.insert(vec![
            (b"key".to_vec(), txn.register_revision(b"key", 3, 1)),
            (b"foo".to_vec(), txn.register_revision(b"foo", 8, 8)),
            (b"bar".to_vec(), txn.register_revision(b"bar", 9, 9)),
        ]);
        txn.commit();

        match_values(
            &index,
            b"key",
            &[
                KeyRevision::new(1, 1, 1, 3),
                KeyRevision::new(1, 2, 2, 2),
                KeyRevision::new(1, 3, 3, 1),
            ],
        );

        match_values(
            &index,
            b"foo",
            &[
                KeyRevision::new(4, 1, 4, 5),
                KeyRevision::new(4, 2, 6, 6),
                KeyRevision::new(4, 3, 8, 8),
            ],
        );

        match_values(
            &index,
            b"bar",
            &[
                KeyRevision::new(5, 1, 5, 4),
                KeyRevision::new(5, 2, 7, 7),
                KeyRevision::new(5, 3, 9, 9),
            ],
        );

        index
    }

    #[test]
    fn test_get() {
        let index = init_and_test_insert();
        let txn = index.transaction();
        assert_eq!(txn.get(b"key", b"", 0), vec![Revision::new(3, 1)]);
        assert_eq!(txn.get(b"key", b"", 1), vec![Revision::new(1, 3)]);
        txn.commit();
        assert_eq!(
            index.get_from_rev(b"key", b"", 2),
            vec![Revision::new(2, 2), Revision::new(3, 1)]
        );
        assert_eq!(
            index.get_from_rev(b"a", b"g", 3),
            vec![
                Revision::new(4, 5),
                Revision::new(5, 4),
                Revision::new(6, 6),
                Revision::new(7, 7),
                Revision::new(8, 8),
                Revision::new(9, 9)
            ]
        );
        assert_eq!(
            index.get_from_rev(b"\0", b"\0", 3),
            vec![
                Revision::new(3, 1),
                Revision::new(4, 5),
                Revision::new(5, 4),
                Revision::new(6, 6),
                Revision::new(7, 7),
                Revision::new(8, 8),
                Revision::new(9, 9)
            ]
        );
    }

    #[test]
    fn test_delete() {
        let index = init_and_test_insert();
        let mut txn = index.transaction();

        assert_eq!(
            txn.delete(b"key", b"", 10, 0),
            (
                vec![(Revision::new(3, 1), Revision::new(10, 0))],
                vec![b"key".to_vec()]
            )
        );

        assert_eq!(
            txn.delete(b"a", b"g", 11, 0),
            (
                vec![
                    (Revision::new(9, 9), Revision::new(11, 0)),
                    (Revision::new(8, 8), Revision::new(11, 1)),
                ],
                vec![b"bar".to_vec(), b"foo".to_vec()]
            )
        );

        assert_eq!(txn.delete(b"\0", b"\0", 12, 0), (vec![], vec![]));

        txn.commit();

        match_values(
            &index,
            b"key",
            &[
                KeyRevision::new(1, 1, 1, 3),
                KeyRevision::new(1, 2, 2, 2),
                KeyRevision::new(1, 3, 3, 1),
                KeyRevision::new_deletion(10, 0),
            ],
        );

        match_values(
            &index,
            b"foo",
            &[
                KeyRevision::new(4, 1, 4, 5),
                KeyRevision::new(4, 2, 6, 6),
                KeyRevision::new(4, 3, 8, 8),
                KeyRevision::new_deletion(11, 1),
            ],
        );

        match_values(
            &index,
            b"bar",
            &[
                KeyRevision::new(5, 1, 5, 4),
                KeyRevision::new(5, 2, 7, 7),
                KeyRevision::new(5, 3, 9, 9),
                KeyRevision::new_deletion(11, 0),
            ],
        );
    }

    #[test]
    fn test_restore() {
        let index = Index::new();
        index.restore(b"key".to_vec(), 2, 0, 2, 1);
        index.restore(b"key".to_vec(), 3, 0, 2, 2);
        index.restore(b"foo".to_vec(), 4, 0, 4, 1);
        match_values(
            &index,
            b"key",
            &[KeyRevision::new(2, 1, 2, 0), KeyRevision::new(2, 2, 3, 0)],
        );
    }

    #[test]
    fn test_compact() {
        let index = init_and_test_insert();
        let res = index.compact(7);
        match_values(&index, b"key", &[KeyRevision::new(1, 3, 3, 1)]);

        match_values(
            &index,
            b"foo",
            &[KeyRevision::new(4, 2, 6, 6), KeyRevision::new(4, 3, 8, 8)],
        );

        match_values(
            &index,
            b"bar",
            &[KeyRevision::new(5, 2, 7, 7), KeyRevision::new(5, 3, 9, 9)],
        );
        assert_eq!(
            res,
            vec![
                KeyRevision::new(5, 1, 5, 4),
                KeyRevision::new(4, 1, 4, 5),
                KeyRevision::new(1, 1, 1, 3),
                KeyRevision::new(1, 2, 2, 2),
            ]
        );
    }

    #[test]
    fn test_compact_with_deletion() {
        let index = init_and_test_insert();
        let mut txn = index.transaction();

        txn.delete(b"a", b"g", 10, 0);
        txn.insert(vec![(
            b"bar".to_vec(),
            txn.register_revision(b"bar", 11, 0),
        )]);
        txn.commit();

        let res = index.compact(10);

        match_values(&index, b"key", &[KeyRevision::new(1, 3, 3, 1)]);

        match_values(&index, b"bar", &[KeyRevision::new(11, 1, 11, 0)]);

        assert_eq!(
            res,
            vec![
                KeyRevision::new(5, 1, 5, 4),
                KeyRevision::new(5, 2, 7, 7),
                KeyRevision::new(5, 3, 9, 9),
                KeyRevision::new(0, 0, 10, 0),
                KeyRevision::new(4, 1, 4, 5),
                KeyRevision::new(4, 2, 6, 6),
                KeyRevision::new(4, 3, 8, 8),
                KeyRevision::new(0, 0, 10, 1),
                KeyRevision::new(1, 1, 1, 3),
                KeyRevision::new(1, 2, 2, 2),
            ]
        );
    }
}
