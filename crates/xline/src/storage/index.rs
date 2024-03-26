#![allow(clippy::multiple_inherent_impl)]
#![allow(unused)]

use std::{
    collections::{btree_map, BTreeMap, HashSet},
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

/// Operations for `Index`
pub(crate) trait IndexOperate {
    /// Get `Revision` of keys, get the latest `Revision` when revision <= 0
    fn get(&self, key: &[u8], range_end: &[u8], revision: i64) -> Vec<Revision>;

    /// Register a new `KeyRevision` of the given key
    ///
    /// Returns a new `KeyRevision` and previous `KeyRevision` of the key
    fn register_revision(
        &self,
        key: Vec<u8>,
        revision: i64,
        sub_revision: i64,
    ) -> (KeyRevision, Option<KeyRevision>);

    /// Gets the previous revision of the key
    fn prev_rev(&self, key: &[u8]) -> Option<KeyRevision>;

    /// Insert or update `KeyRevision`
    fn insert(&self, key_revisions: Vec<(Vec<u8>, KeyRevision)>);

    /// Mark keys as deleted and return latest revision before deletion and deletion revision
    /// return all revision pairs and all keys in range
    fn delete(
        &self,
        key: &[u8],
        range_end: &[u8],
        revision: i64,
        sub_revision: i64,
    ) -> (Vec<(Revision, Revision)>, Vec<Vec<u8>>);
}

// TODO: Remove `RwLock` from this data structure so that we could avoid some cloning
// in some of the methods.
/// A Key revisions map
type KeyRevMap = RwLock<BTreeMap<Vec<u8>, Vec<KeyRevision>>>;

/// Keys to revisions mapping
#[derive(Debug)]
pub(crate) struct Index {
    /// Inner struct of `Index`
    inner: Arc<RwLock<IndexInner>>,
}

impl Index {
    /// New `Index`
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::default()),
        }
    }

    /// Creates a transaction
    pub(crate) fn state(&self) -> IndexState {
        IndexState {
            index_ref: Arc::clone(&self.inner),
            state: RwLock::default(),
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
        self.inner.read().keep(at_rev)
    }

    /// Insert `KeyRevision` of deleted and generate `Revision` pair of deleted
    fn gen_del_revision(
        revs: &mut Vec<KeyRevision>,
        revision: i64,
        sub_revision: i64,
    ) -> Option<(Revision, Revision)> {
        let last_available_rev = Self::get_revision(revs, 0)?;
        let del_rev = KeyRevision::new_deletion(revision, sub_revision);
        revs.push(del_rev);
        Some((last_available_rev, del_rev.as_revision()))
    }
}

impl Index {
    /// Get `Revision` of keys from one revision
    pub(super) fn get_from_rev(
        &self,
        key: &[u8],
        range_end: &[u8],
        revision: i64,
    ) -> Vec<Revision> {
        self.inner.read().get_from_rev(key, range_end, revision)
    }

    /// Restore `KeyRevision` of a key
    pub(super) fn restore(
        &self,
        key: Vec<u8>,
        revision: i64,
        sub_revision: i64,
        create_revision: i64,
        version: i64,
    ) {
        self.inner
            .write()
            .restore(key, revision, sub_revision, create_revision, version)
    }

    /// Compact a `KeyRevision` by removing the versions with smaller or equal
    /// revision than the given atRev except the largest one (If the largest one is
    /// a tombstone, it will not be kept).
    pub(super) fn compact(&self, at_rev: i64) -> Vec<KeyRevision> {
        self.inner.write().compact(at_rev)
    }
}

impl IndexOperate for Index {
    fn get(&self, key: &[u8], range_end: &[u8], revision: i64) -> Vec<Revision> {
        self.inner.read().get(key, range_end, revision)
    }

    fn register_revision(
        &self,
        key: Vec<u8>,
        revision: i64,
        sub_revision: i64,
    ) -> (KeyRevision, Option<KeyRevision>) {
        self.inner
            .write()
            .register_revision(key, revision, sub_revision)
    }

    fn prev_rev(&self, key: &[u8]) -> Option<KeyRevision> {
        self.inner.read().prev_rev(key)
    }

    fn insert(&self, key_revisions: Vec<(Vec<u8>, KeyRevision)>) {
        self.inner.write().insert(key_revisions)
    }

    fn delete(
        &self,
        key: &[u8],
        range_end: &[u8],
        revision: i64,
        sub_revision: i64,
    ) -> (Vec<(Revision, Revision)>, Vec<Vec<u8>>) {
        self.inner
            .write()
            .delete(key, range_end, revision, sub_revision)
    }
}

/// Inner type of `Index`
#[derive(Debug, Default)]
struct IndexInner {
    rev_map: BTreeMap<Vec<u8>, Vec<KeyRevision>>,
}

impl IndexInner {
    /// Get all revisions that need to be kept after compact at the given revision
    pub(crate) fn keep(&self, at_rev: i64) -> HashSet<Revision> {
        let mut revs = HashSet::new();
        self.rev_map.values().for_each(|revisions| {
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

    /// Get `Revision` of keys from one revision
    pub(super) fn get_from_rev(
        &self,
        key: &[u8],
        range_end: &[u8],
        revision: i64,
    ) -> Vec<Revision> {
        match RangeType::get_range_type(key, range_end) {
            RangeType::OneKey => self
                .rev_map
                .get(key)
                .map(|revs| Index::filter_revision(revs.as_ref(), revision))
                .unwrap_or_default(),
            RangeType::AllKeys => self
                .rev_map
                .values()
                .flat_map(|revs| Index::filter_revision(revs.as_ref(), revision))
                .sorted()
                .collect(),
            RangeType::Range => self
                .rev_map
                .range(KeyRange::new(key, range_end))
                .flat_map(|(_, revs)| Index::filter_revision(revs.as_ref(), revision))
                .sorted()
                .collect(),
        }
    }

    /// Restore `KeyRevision` of a key
    pub(super) fn restore(
        &mut self,
        key: Vec<u8>,
        revision: i64,
        sub_revision: i64,
        create_revision: i64,
        version: i64,
    ) {
        let revisions = self.rev_map.entry(key).or_default();
        revisions.push(KeyRevision::new(
            create_revision,
            version,
            revision,
            sub_revision,
        ));
    }

    /// Compact a `KeyRevision` by removing the versions with smaller or equal
    /// revision than the given atRev except the largest one (If the largest one is
    /// a tombstone, it will not be kept).
    pub(super) fn compact(&mut self, at_rev: i64) -> Vec<KeyRevision> {
        let mut revs = Vec::new();
        let mut del_keys = Vec::new();

        self.rev_map.iter_mut().for_each(|(key, revisions)| {
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
            let _ignore = self.rev_map.remove(&key);
        }
        revs
    }
}

impl IndexInner {
    fn get(&self, key: &[u8], range_end: &[u8], revision: i64) -> Vec<Revision> {
        match RangeType::get_range_type(key, range_end) {
            RangeType::OneKey => self
                .rev_map
                .get(key)
                .and_then(|revs| Index::get_revision(revs.as_ref(), revision))
                .map(|rev| vec![rev])
                .unwrap_or_default(),
            RangeType::AllKeys => self
                .rev_map
                .values()
                .filter_map(|revs| Index::get_revision(revs.as_ref(), revision))
                .collect(),
            RangeType::Range => self
                .rev_map
                .range(KeyRange::new(key, range_end))
                .filter_map(|(_, revs)| Index::get_revision(revs.as_ref(), revision))
                .collect(),
        }
    }

    fn insert(&mut self, key_revisions: Vec<(Vec<u8>, KeyRevision)>) {
        for (key, revision) in key_revisions {
            if let Some(revs) = self.rev_map.get_mut::<[u8]>(key.as_ref()) {
                revs.push(revision);
            } else {
                _ = self.rev_map.insert(key, vec![revision]);
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
            RangeType::OneKey => {
                let pairs: Vec<(Revision, Revision)> = self
                    .rev_map
                    .get_mut(key)
                    .into_iter()
                    .filter_map(|revs| {
                        Index::gen_del_revision(revs.as_mut(), revision, sub_revision)
                    })
                    .collect();
                let keys = if pairs.is_empty() {
                    vec![]
                } else {
                    vec![key.to_vec()]
                };
                (pairs, keys)
            }
            RangeType::AllKeys => self
                .rev_map
                .iter_mut()
                .zip(0..)
                .filter_map(|((key, revs), i)| {
                    Index::gen_del_revision(revs, revision, sub_revision.overflow_add(i))
                        .map(|pair| (pair, key.clone()))
                })
                .unzip(),
            RangeType::Range => self
                .rev_map
                .range_mut(KeyRange::new(key, range_end))
                .zip(0..)
                .filter_map(|((key, revs), i)| {
                    Index::gen_del_revision(revs, revision, sub_revision.overflow_add(i))
                        .map(|pair| (pair, key.clone()))
                })
                .unzip(),
        };
        (pairs, keys)
    }

    fn register_revision(
        &mut self,
        key: Vec<u8>,
        revision: i64,
        sub_revision: i64,
    ) -> (KeyRevision, Option<KeyRevision>) {
        match self.rev_map.entry(key) {
            btree_map::Entry::Vacant(e) => {
                let new_rev = KeyRevision::new(revision, 1, revision, sub_revision);
                let _ignore = e.insert(vec![new_rev]);
                (new_rev, None)
            }
            btree_map::Entry::Occupied(e) => {
                let revisions = e.into_mut();
                let last = revisions
                    .last()
                    .unwrap_or_else(|| unreachable!("empty revision list"))
                    .clone();
                let new_rev = if last.is_deleted() {
                    KeyRevision::new(revision, 1, revision, sub_revision)
                } else {
                    KeyRevision::new(
                        last.create_revision,
                        last.version.overflow_add(1),
                        revision,
                        sub_revision,
                    )
                };
                revisions.push(new_rev);
                (new_rev, Some(last))
            }
        }
    }

    fn prev_rev(&self, key: &[u8]) -> Option<KeyRevision> {
        self.rev_map.get(key).and_then(|revs| revs.last()).copied()
    }
}

/// A index with extra state, it won't mutate the index directly before commit
#[derive(Debug)]
pub(crate) struct IndexState {
    /// Inner struct of `Index`
    index_ref: Arc<RwLock<IndexInner>>,
    /// State current modification
    state: KeyRevMap,
}

impl IndexState {
    /// Commits all changes
    pub(crate) fn commit(self) {
        let mut index_w = self.index_ref.write();
        for (key, state_revs) in self.state.write().iter() {
            let revs = index_w.rev_map.entry(key.clone()).or_default();
            revs.extend_from_slice(state_revs);
        }
    }

    /// Discards all changes
    pub(crate) fn discard(&self) {
        self.state.write().clear();
    }

    /// Gets the revisions for a single key
    fn one_key_revisions(&self, key: &[u8]) -> Vec<KeyRevision> {
        let index_r = self.index_ref.read();
        let state_r = self.state.read();
        let mut result = index_r.rev_map.get(key).cloned().unwrap_or_default();
        if let Some(revs) = state_r.get(key) {
            result.extend_from_slice(&revs);
        }
        result
    }

    /// Gets the revisions for a range of keys
    fn range_key_revisions(&self, range: KeyRange) -> BTreeMap<Vec<u8>, Vec<KeyRevision>> {
        let mut map: BTreeMap<Vec<u8>, Vec<KeyRevision>> = BTreeMap::new();
        let state_r = self.state.read();
        for (key, value) in self
            .index_ref
            .read()
            .rev_map
            .range(range.clone())
            .map(|(k, v)| (k.clone(), v.clone()))
            .chain(state_r.range(range).map(|(k, v)| (k.clone(), v.clone())))
        {
            let entry = map.entry(key.clone()).or_default();
            entry.extend(value);
        }
        map
    }

    /// Gets the revisions for all keys
    fn all_key_revisions(&self) -> BTreeMap<Vec<u8>, Vec<KeyRevision>> {
        let mut map: BTreeMap<Vec<u8>, Vec<KeyRevision>> = BTreeMap::new();
        let state_r = self.state.read();
        for (key, value) in self
            .index_ref
            .read()
            .rev_map
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .chain(state_r.clone().into_iter())
        {
            let entry = map.entry(key.clone()).or_default();
            entry.extend(value.clone());
        }
        map
    }

    /// Deletes on key
    fn delete_one(
        &self,
        key: &[u8],
        revision: i64,
        sub_revision: i64,
    ) -> Option<((Revision, Revision), Vec<u8>)> {
        let mut state_w = self.state.write();
        let revs = self.one_key_revisions(key);
        let last_available_rev = Index::get_revision(&revs, 0)?;
        let entry = state_w.entry(key.to_vec()).or_insert(vec![]);
        let del_rev = KeyRevision::new_deletion(revision, sub_revision);
        entry.push(del_rev);
        let pair = (last_available_rev, del_rev.as_revision());

        Some((pair, key.to_vec()))
    }

    /// Deletes a range of keys
    fn delete_range(
        &self,
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
        &self,
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

impl IndexOperate for IndexState {
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

    fn register_revision(
        &self,
        key: Vec<u8>,
        revision: i64,
        sub_revision: i64,
    ) -> (KeyRevision, Option<KeyRevision>) {
        let index_r = &self.index_ref.read().rev_map;
        let mut state_w = self.state.write();

        let next_rev = |revisions: &[KeyRevision]| {
            let last = revisions
                .last()
                .unwrap_or_else(|| unreachable!("empty revision list"))
                .clone();
            let new_rev = if last.is_deleted() {
                KeyRevision::new(revision, 1, revision, sub_revision)
            } else {
                KeyRevision::new(
                    last.create_revision,
                    last.version.overflow_add(1),
                    revision,
                    sub_revision,
                )
            };
            (new_rev, Some(last))
        };

        match (index_r.get(&key), state_w.entry(key)) {
            (None, btree_map::Entry::Vacant(e)) => {
                let new_rev = KeyRevision::new(revision, 1, revision, sub_revision);
                let _ignore = e.insert(vec![new_rev]);
                (new_rev, None)
            }
            (None, btree_map::Entry::Occupied(mut e)) => {
                let (new_rev, last) = next_rev(e.get_mut());
                e.get_mut().push(new_rev);
                (new_rev, last)
            }
            (Some(e), btree_map::Entry::Vacant(se)) => {
                let (new_rev, last) = next_rev(e);
                let _ignore = se.insert(vec![new_rev]);
                (new_rev, last)
            }
            (Some(_), btree_map::Entry::Occupied(mut e)) => {
                let (new_rev, last) = next_rev(e.get_mut());
                e.get_mut().push(new_rev);
                (new_rev, last)
            }
        }
    }

    fn prev_rev(&self, key: &[u8]) -> Option<KeyRevision> {
        let index_r = &self.index_ref.read().rev_map;
        let mut state_r = self.state.read();

        match (index_r.get(key), state_r.get(key)) {
            (None, None) => None,
            (None, Some(revs)) => revs.last().copied(),
            (Some(revs), None) => revs.last().copied(),
            (Some(_), Some(revs)) => revs.last().copied(),
        }
    }

    fn insert(&self, key_revisions: Vec<(Vec<u8>, KeyRevision)>) {
        let mut state_w = self.state.write();
        for (key, revision) in key_revisions {
            if let Some(revs) = state_w.get_mut::<[u8]>(key.as_ref()) {
                revs.push(revision);
            } else {
                _ = state_w.insert(key, vec![revision]);
            }
        }
    }

    fn delete(
        &self,
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
}

#[cfg(test)]
mod test {
    use super::*;
    #[allow(clippy::expect_used)]
    fn match_values(index: &Index, key: impl AsRef<[u8]>, expected_values: &[KeyRevision]) {
        let inner_r = index.inner.read();
        let revs = inner_r
            .rev_map
            .get(key.as_ref())
            .expect("index entry should not be None");
        assert_eq!(*revs, expected_values);
    }

    fn init_and_test_insert() -> Index {
        let index = Index::new();
        let mut txn = index.state();

        txn.insert(vec![
            (
                b"key".to_vec(),
                txn.register_revision(b"key".to_vec(), 1, 3).0,
            ),
            (
                b"foo".to_vec(),
                txn.register_revision(b"foo".to_vec(), 4, 5).0,
            ),
            (
                b"bar".to_vec(),
                txn.register_revision(b"bar".to_vec(), 5, 4).0,
            ),
        ]);

        txn.insert(vec![
            (
                b"key".to_vec(),
                txn.register_revision(b"key".to_vec(), 2, 2).0,
            ),
            (
                b"foo".to_vec(),
                txn.register_revision(b"foo".to_vec(), 6, 6).0,
            ),
            (
                b"bar".to_vec(),
                txn.register_revision(b"bar".to_vec(), 7, 7).0,
            ),
        ]);
        txn.insert(vec![
            (
                b"key".to_vec(),
                txn.register_revision(b"key".to_vec(), 3, 1).0,
            ),
            (
                b"foo".to_vec(),
                txn.register_revision(b"foo".to_vec(), 8, 8).0,
            ),
            (
                b"bar".to_vec(),
                txn.register_revision(b"bar".to_vec(), 9, 9).0,
            ),
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
        let txn = index.state();
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
        let mut txn = index.state();

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
        let mut txn = index.state();

        txn.delete(b"a", b"g", 10, 0);
        txn.insert(vec![(
            b"bar".to_vec(),
            txn.register_revision(b"bar".to_vec(), 11, 0).0,
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
