use std::{
    cmp::Ordering,
    collections::HashMap,
    sync::{
        atomic::{AtomicI64, Ordering::Relaxed},
        Arc,
    },
};

use clippy_utilities::{Cast, OverflowArithmetic};
use engine::Transaction;
use prost::Message;
use tokio::sync::mpsc;
use tracing::{debug, warn};
use xlineapi::{
    command::{CommandResponse, KeyRange, SyncResponse},
    execute_error::ExecuteError,
};

use super::{
    index::{Index, IndexOperate, IndexTransaction, IndexTransactionOperate},
    lease_store::LeaseCollection,
    revision::{KeyRevision, Revision},
    storage_api::{StorageApi, StorageTxnApi},
};
use crate::{
    header_gen::HeaderGenerator,
    revision_check::RevisionCheck,
    revision_number::RevisionNumberGenerator,
    rpc::{
        CompactionRequest, CompactionResponse, Compare, CompareResult, CompareTarget,
        DeleteRangeRequest, DeleteRangeResponse, Event, EventType, KeyValue, PutRequest,
        PutResponse, RangeRequest, RangeResponse, Request, RequestWithToken, RequestWrapper,
        ResponseWrapper, SortOrder, SortTarget, TargetUnion, TxnRequest, TxnResponse,
    },
    server::command::META_TABLE,
    storage::db::{WriteOp, COMPACT_REVISION},
};

/// KV table name
pub(crate) const KV_TABLE: &str = "kv";

/// KV store
#[derive(Debug)]
pub(crate) struct KvStore<DB>
where
    DB: StorageApi,
{
    /// Kv storage inner
    inner: Arc<KvStoreInner<DB>>,
    /// Revision
    revision: Arc<RevisionNumberGenerator>,
    /// Header generator
    header_gen: Arc<HeaderGenerator>,
    /// KV update sender
    kv_update_tx: mpsc::Sender<(i64, Vec<Event>)>,
    /// Compact task submit sender
    compact_task_tx: mpsc::Sender<(i64, Option<Arc<event_listener::Event>>)>,
    /// Lease collection
    lease_collection: Arc<LeaseCollection>,
}

/// KV store inner, shared by `KvStore` and `KvWatcher`
#[derive(Debug)]
pub(crate) struct KvStoreInner<DB>
where
    DB: StorageApi,
{
    /// Key Index
    index: Arc<Index>,
    /// DB to store key value
    db: Arc<DB>,
    /// Compacted Revision
    compacted_rev: AtomicI64,
}

impl<DB> KvStoreInner<DB>
where
    DB: StorageApi,
{
    /// Create new `KvStoreInner`
    pub(crate) fn new(index: Arc<Index>, db: Arc<DB>) -> Self {
        Self {
            index,
            db,
            compacted_rev: AtomicI64::new(-1),
        }
    }

    /// Get `KeyValue` from the `KvStore`
    fn get_values<T>(txn: &T, revisions: &[Revision]) -> Result<Vec<KeyValue>, ExecuteError>
    where
        T: StorageTxnApi,
    {
        let revisions = revisions
            .iter()
            .map(Revision::encode_to_vec)
            .collect::<Vec<Vec<u8>>>();
        let values = txn.get_values(KV_TABLE, &revisions)?;
        let kvs: Vec<KeyValue> = values
            .into_iter()
            .flatten()
            .map(|v| KeyValue::decode(v.as_slice()))
            .collect::<Result<_, _>>()
            .map_err(|e| {
                ExecuteError::DbError(format!("Failed to decode key-value from DB, error: {e}"))
            })?;
        debug_assert_eq!(kvs.len(), revisions.len(), "index does not match with db");
        Ok(kvs)
    }

    /// Get `KeyValue` of a range
    ///
    /// If `range_end` is `&[]`, this function will return one or zero `KeyValue`.
    fn get_range<T>(
        txn_db: &T,
        txn_index: &IndexTransaction,
        key: &[u8],
        range_end: &[u8],
        revision: i64,
    ) -> Result<Vec<KeyValue>, ExecuteError>
    where
        T: StorageTxnApi,
    {
        let revisions = txn_index.get(key, range_end, revision);
        Self::get_values(txn_db, &revisions)
    }

    /// Get `KeyValue` of a range with limit and count only, return kvs and total count
    fn get_range_with_opts(
        txn_db: &Transaction,
        txn_index: &IndexTransaction,
        key: &[u8],
        range_end: &[u8],
        revision: i64,
        limit: usize,
        count_only: bool,
    ) -> Result<(Vec<KeyValue>, usize), ExecuteError> {
        let mut revisions = txn_index.get(key, range_end, revision);
        let total = revisions.len();
        if count_only || total == 0 {
            return Ok((vec![], total));
        }
        if limit != 0 {
            revisions.truncate(limit);
        }
        let kvs = Self::get_values(txn_db, &revisions)?;
        Ok((kvs, total))
    }

    /// Get previous `KeyValue` of a `KeyValue`
    pub(crate) fn get_prev_kv(&self, kv: &KeyValue) -> Option<KeyValue> {
        let txn_db = self.db.transaction();
        let txn_index = self.index.transaction();
        Self::get_range(
            &txn_db,
            &txn_index,
            &kv.key,
            &[],
            kv.mod_revision.overflow_sub(1),
        )
        .ok()?
        .pop()
    }

    /// Get `KeyValue` start from a revision and convert to `Event`
    pub(crate) fn get_event_from_revision(
        &self,
        key_range: KeyRange,
        revision: i64,
    ) -> Result<Vec<Event>, ExecuteError> {
        let txn = self.db.transaction();
        let revisions =
            self.index
                .get_from_rev(key_range.range_start(), key_range.range_end(), revision);
        let events = Self::get_values(&txn, &revisions)?
            .into_iter()
            .map(|kv| {
                // Delete
                #[allow(clippy::as_conversions)] // This cast is always valid
                let event_type = if kv.version == 0 && kv.create_revision == 0 {
                    EventType::Delete
                } else {
                    EventType::Put
                };
                let mut event = Event {
                    kv: Some(kv),
                    prev_kv: None,
                    ..Default::default()
                };
                event.set_type(event_type);
                event
            })
            .collect();
        Ok(events)
    }

    /// Get compacted revision of  KV store
    pub(crate) fn compacted_revision(&self) -> i64 {
        self.compacted_rev.load(Relaxed)
    }
}

impl<DB> KvStore<DB>
where
    DB: StorageApi,
{
    /// Executes a request
    pub(crate) fn execute(
        &self,
        request: &RequestWithToken,
    ) -> Result<CommandResponse, ExecuteError> {
        self.execute_request(&request.request)
            .map(CommandResponse::new)
    }

    /// After-Syncs a request
    pub(crate) async fn after_sync<T>(
        &self,
        request: &RequestWithToken,
        txn_db: T,
    ) -> Result<SyncResponse, ExecuteError>
    where
        T: StorageTxnApi,
    {
        self.sync_request(&request.request, txn_db).await
    }

    /// Recover data from persistent storage
    pub(crate) async fn recover(&self) -> Result<(), ExecuteError> {
        let mut key_to_lease: HashMap<Vec<u8>, i64> = HashMap::new();
        let kvs = self.inner.db.get_all(KV_TABLE)?;

        let current_rev = kvs
            .last()
            .map_or(1, |pair| Revision::decode(&pair.0).revision());
        self.revision.set(current_rev);

        for (key, value) in kvs {
            let rev = Revision::decode(key.as_slice());
            let kv = KeyValue::decode(value.as_slice())
                .unwrap_or_else(|e| panic!("decode kv error: {e:?}"));

            if kv.lease == 0 {
                let _ignore = key_to_lease.remove(&kv.key);
            } else {
                let _ignore = key_to_lease.insert(kv.key.clone(), kv.lease);
            }

            self.inner.index.restore(
                kv.key,
                rev.revision(),
                rev.sub_revision(),
                kv.create_revision,
                kv.version,
            );
        }

        for (key, lease_id) in key_to_lease {
            self.attach(lease_id, key)?;
        }

        if let Some(revision_bytes) = self.inner.db.get_value(META_TABLE, COMPACT_REVISION)? {
            let compacted_revision =
                i64::from_le_bytes(revision_bytes.try_into().map_err(|e| {
                    ExecuteError::DbError(format!(
                        "cannot decode compacted revision from META_TABLE: {e:?}"
                    ))
                })?);
            assert!(
                compacted_revision >= -1 && compacted_revision <= current_rev,
                "compacted revision corruption, which ({compacted_revision}) must belong to the range [-1, {current_rev}]"
            );
            self.update_compacted_revision(compacted_revision);
            if let Err(e) = self.compact_task_tx.send((compacted_revision, None)).await {
                panic!("the compactor exited unexpectedly: {e:?}");
            }
        }
        Ok(())
    }
}

impl<DB> KvStore<DB>
where
    DB: StorageApi,
{
    /// New `KvStore`
    pub(crate) fn new(
        inner: Arc<KvStoreInner<DB>>,
        header_gen: Arc<HeaderGenerator>,
        kv_update_tx: mpsc::Sender<(i64, Vec<Event>)>,
        compact_task_tx: mpsc::Sender<(i64, Option<Arc<event_listener::Event>>)>,
        lease_collection: Arc<LeaseCollection>,
    ) -> Self {
        Self {
            inner,
            revision: header_gen.general_revision_arc(),
            header_gen,
            kv_update_tx,
            compact_task_tx,
            lease_collection,
        }
    }

    /// Get revision of KV store
    pub(crate) fn revision(&self) -> i64 {
        self.revision.get()
    }

    /// Get compacted revision of  KV store
    pub(crate) fn compacted_revision(&self) -> i64 {
        self.inner.compacted_rev.load(Relaxed)
    }

    /// Update compacted revision of KV store
    pub(crate) fn update_compacted_revision(&self, revision: i64) {
        self.inner.compacted_rev.store(revision, Relaxed);
    }

    /// Notify KV changes to KV watcher
    async fn notify_updates(&self, revision: i64, updates: Vec<Event>) {
        assert!(
            self.kv_update_tx.send((revision, updates)).await.is_ok(),
            "Failed to send updates to KV watcher"
        );
    }

    /// Sort kvs by sort target and order
    fn sort_kvs(kvs: &mut [KeyValue], sort_order: SortOrder, sort_target: SortTarget) {
        match (sort_target, sort_order) {
            (SortTarget::Key, SortOrder::None) => {}
            (SortTarget::Key, SortOrder::Ascend) => {
                kvs.sort_by(|a, b| a.key.cmp(&b.key));
            }
            (SortTarget::Key, SortOrder::Descend) => {
                kvs.sort_by(|a, b| b.key.cmp(&a.key));
            }
            (SortTarget::Version, SortOrder::Ascend | SortOrder::None) => {
                kvs.sort_by(|a, b| a.version.cmp(&b.version));
            }
            (SortTarget::Version, SortOrder::Descend) => {
                kvs.sort_by(|a, b| b.version.cmp(&a.version));
            }
            (SortTarget::Create, SortOrder::Ascend | SortOrder::None) => {
                kvs.sort_by(|a, b| a.create_revision.cmp(&b.create_revision));
            }
            (SortTarget::Create, SortOrder::Descend) => {
                kvs.sort_by(|a, b| b.create_revision.cmp(&a.create_revision));
            }
            (SortTarget::Mod, SortOrder::Ascend | SortOrder::None) => {
                kvs.sort_by(|a, b| a.mod_revision.cmp(&b.mod_revision));
            }
            (SortTarget::Mod, SortOrder::Descend) => {
                kvs.sort_by(|a, b| b.mod_revision.cmp(&a.mod_revision));
            }
            (SortTarget::Value, SortOrder::Ascend | SortOrder::None) => {
                kvs.sort_by(|a, b| a.value.cmp(&b.value));
            }
            (SortTarget::Value, SortOrder::Descend) => {
                kvs.sort_by(|a, b| b.value.cmp(&a.value));
            }
        };
    }

    /// filter kvs by `{max,min}_{mod,create}_revision`
    fn filter_kvs(
        kvs: &mut Vec<KeyValue>,
        max_mod_revision: i64,
        min_mod_revision: i64,
        max_create_revision: i64,
        min_create_revision: i64,
    ) {
        if max_mod_revision > 0 {
            kvs.retain(|kv| kv.mod_revision <= max_mod_revision);
        }
        if min_mod_revision > 0 {
            kvs.retain(|kv| kv.mod_revision >= min_mod_revision);
        }
        if max_create_revision > 0 {
            kvs.retain(|kv| kv.create_revision <= max_create_revision);
        }
        if min_create_revision > 0 {
            kvs.retain(|kv| kv.create_revision >= min_create_revision);
        }
    }

    /// Compare i64
    fn compare_i64(val: i64, target: i64) -> CompareResult {
        match val.cmp(&target) {
            Ordering::Greater => CompareResult::Greater,
            Ordering::Less => CompareResult::Less,
            Ordering::Equal => CompareResult::Equal,
        }
    }

    /// Compare vec<u8>
    fn compare_vec_u8(val: &[u8], target: &[u8]) -> CompareResult {
        match val.cmp(target) {
            Ordering::Greater => CompareResult::Greater,
            Ordering::Less => CompareResult::Less,
            Ordering::Equal => CompareResult::Equal,
        }
    }

    /// Check one `KeyValue` with `Compare`
    fn compare_kv(cmp: &Compare, kv: &KeyValue) -> bool {
        let result = match cmp.target() {
            CompareTarget::Version => {
                let rev = if let Some(TargetUnion::Version(v)) = cmp.target_union {
                    v
                } else {
                    0
                };
                Self::compare_i64(kv.version, rev)
            }
            CompareTarget::Create => {
                let rev = if let Some(TargetUnion::CreateRevision(v)) = cmp.target_union {
                    v
                } else {
                    0
                };
                Self::compare_i64(kv.create_revision, rev)
            }
            CompareTarget::Mod => {
                let rev = if let Some(TargetUnion::ModRevision(v)) = cmp.target_union {
                    v
                } else {
                    0
                };
                Self::compare_i64(kv.mod_revision, rev)
            }
            CompareTarget::Value => {
                let empty = vec![];
                let val = if let Some(TargetUnion::Value(ref v)) = cmp.target_union {
                    v
                } else {
                    &empty
                };
                Self::compare_vec_u8(&kv.value, val)
            }
            CompareTarget::Lease => {
                let les = if let Some(TargetUnion::Lease(v)) = cmp.target_union {
                    v
                } else {
                    0
                };
                Self::compare_i64(kv.mod_revision, les)
            }
        };

        match cmp.result() {
            CompareResult::Equal => result == CompareResult::Equal,
            CompareResult::Greater => result == CompareResult::Greater,
            CompareResult::Less => result == CompareResult::Less,
            CompareResult::NotEqual => result != CompareResult::Equal,
        }
    }

    /// Check result of a `Compare`
    fn check_compare<T>(txn_db: &T, txn_index: &IndexTransaction, cmp: &Compare) -> bool
    where
        T: StorageTxnApi,
    {
        let kvs = KvStoreInner::<DB>::get_range(txn_db, txn_index, &cmp.key, &cmp.range_end, 0)
            .unwrap_or_default();
        if kvs.is_empty() {
            if let Some(TargetUnion::Value(_)) = cmp.target_union {
                false
            } else {
                Self::compare_kv(cmp, &KeyValue::default())
            }
        } else {
            kvs.iter().all(|kv| Self::compare_kv(cmp, kv))
        }
    }

    /// Send get lease to lease store
    fn get_lease(&self, key: &[u8]) -> i64 {
        self.lease_collection.get_lease(key)
    }

    /// Send detach to lease store
    fn detach(&self, lease_id: i64, key: impl AsRef<[u8]>) -> Result<(), ExecuteError> {
        self.lease_collection.detach(lease_id, key.as_ref())
    }

    /// Send attach to lease store
    fn attach(&self, lease_id: i64, key: impl Into<Vec<u8>>) -> Result<(), ExecuteError> {
        self.lease_collection.attach(lease_id, key.into())
    }

    /// Compact kv storage
    pub(crate) fn compact(&self, revisions: &[Vec<u8>]) -> Result<(), ExecuteError> {
        let mut ops = Vec::new();
        revisions
            .iter()
            .for_each(|rev| ops.push(WriteOp::DeleteKeyValue(rev.as_ref())));
        self.inner.db.flush_ops(ops)?;
        Ok(())
    }
}

#[cfg(test)]
/// Test uitls
impl<DB> KvStore<DB>
where
    DB: StorageApi,
{
    pub(crate) fn db(&self) -> &DB {
        self.inner.db.as_ref()
    }
}

// Speculatively execute requests
impl<DB> KvStore<DB>
where
    DB: StorageApi,
{
    /// execute requests
    fn execute_request(&self, wrapper: &RequestWrapper) -> Result<ResponseWrapper, ExecuteError> {
        debug!("Execute {:?}", wrapper);

        let txn_db = self.inner.db.transaction();
        let txn_index = self.inner.index.transaction();
        // As we store use revision as key in the DB storage,
        // a fake revision needs to be used during speculative execution
        let fake_revision = i64::MAX;

        #[allow(clippy::wildcard_enum_match_arm)]
        let res: ResponseWrapper = match *wrapper {
            RequestWrapper::RangeRequest(ref req) => self
                .execute_range(&txn_db, &txn_index, req)
                .map(Into::into)?,
            RequestWrapper::PutRequest(ref req) => self
                .execute_put(&txn_db, &txn_index, req, fake_revision, &mut 0)
                .map(Into::into)?,
            RequestWrapper::DeleteRangeRequest(ref req) => self
                .execute_delete_range(&txn_db, &txn_index, req, fake_revision, &mut 0)
                .map(Into::into)?,
            RequestWrapper::TxnRequest(ref req) => self
                .execute_txn(&txn_db, &txn_index, req, fake_revision, &mut 0)
                .map(Into::into)?,
            RequestWrapper::CompactionRequest(ref req) => {
                debug!("Receive CompactionRequest {:?}", req);
                self.execute_compaction(req).map(Into::into)?
            }
            _ => unreachable!("Other request should not be sent to this store"),
        };

        Ok(res)
    }

    /// Handle `RangeRequest`
    fn execute_range(
        &self,
        tnx_db: &Transaction,
        txn_index: &IndexTransaction,
        req: &RangeRequest,
    ) -> Result<RangeResponse, ExecuteError> {
        req.check_revision(self.compacted_revision(), self.revision())?;

        let storage_fetch_limit = if (req.sort_order() != SortOrder::None)
            || (req.max_mod_revision != 0)
            || (req.min_mod_revision != 0)
            || (req.max_create_revision != 0)
            || (req.min_create_revision != 0)
            || (req.limit == 0)
        {
            0 // get all from storage then sort and filter
        } else {
            req.limit.overflow_add(1) // get one extra for "more" flag
        };
        let (mut kvs, total) = KvStoreInner::<DB>::get_range_with_opts(
            tnx_db,
            txn_index,
            &req.key,
            &req.range_end,
            req.revision,
            storage_fetch_limit.cast(),
            req.count_only,
        )?;
        let mut response = RangeResponse {
            header: Some(self.header_gen.gen_header(false)),
            count: total.cast(),
            ..RangeResponse::default()
        };
        if kvs.is_empty() {
            return Ok(response);
        }

        Self::filter_kvs(
            &mut kvs,
            req.max_mod_revision,
            req.min_mod_revision,
            req.max_create_revision,
            req.min_create_revision,
        );
        Self::sort_kvs(&mut kvs, req.sort_order(), req.sort_target());

        if (req.limit > 0) && (kvs.len() > req.limit.cast()) {
            response.more = true;
            kvs.truncate(req.limit.cast());
        }
        if req.keys_only {
            kvs.iter_mut().for_each(|kv| kv.value.clear());
        }
        response.kvs = kvs;

        Ok(response)
    }

    /// Handle `PutRequest`
    #[allow(unused)]
    fn execute_put(
        &self,
        txn_db: &Transaction,
        txn_index: &IndexTransaction,
        req: &PutRequest,
        revision: i64,
        sub_revision: &mut i64,
    ) -> Result<PutResponse, ExecuteError> {
        let response = PutResponse {
            header: Some(self.header_gen.gen_header(false)),
            ..Default::default()
        };
        Ok(response)
    }

    /// Handle `PutRequest`
    #[allow(unused)]
    fn execute_put_old(
        &self,
        txn_db: &Transaction,
        txn_index: &IndexTransaction,
        req: &PutRequest,
        revision: i64,
        sub_revision: &mut i64,
    ) -> Result<PutResponse, ExecuteError> {
        let mut response = PutResponse {
            header: Some(self.header_gen.gen_header(false)),
            ..Default::default()
        };
        let new_rev = txn_index.register_revision(&req.key, revision, *sub_revision);
        let mut kv = KeyValue {
            key: req.key.clone(),
            value: req.value.clone(),
            create_revision: new_rev.create_revision,
            mod_revision: new_rev.mod_revision,
            version: new_rev.version,
            lease: req.lease,
        };

        if req.prev_kv || req.ignore_lease || req.ignore_value {
            let prev_kv = KvStoreInner::<DB>::get_range(txn_db, txn_index, &req.key, &[], 0)?.pop();
            if req.ignore_lease || req.ignore_value {
                let prev = prev_kv.as_ref().ok_or(ExecuteError::KeyNotFound)?;
                if req.ignore_lease {
                    kv.lease = prev.lease;
                }
                if req.ignore_value {
                    kv.value = prev.value.clone();
                }
            }
            if req.prev_kv {
                response.prev_kv = prev_kv;
            }
        };

        txn_db.write_op(WriteOp::PutKeyValue(new_rev.as_revision(), kv.clone()))?;
        *sub_revision = sub_revision.overflow_add(1);
        let revs = vec![(
            kv.key.clone(),
            KeyRevision::new(
                kv.create_revision,
                kv.version,
                new_rev.as_revision().revision(),
                new_rev.as_revision().sub_revision(),
            ),
        )];
        txn_index.insert(revs);

        Ok(response)
    }

    /// Handle `DeleteRangeRequest`
    fn execute_delete_range<T>(
        &self,
        txn_db: &T,
        txn_index: &IndexTransaction,
        req: &DeleteRangeRequest,
        revision: i64,
        sub_revision: &mut i64,
    ) -> Result<DeleteRangeResponse, ExecuteError>
    where
        T: StorageTxnApi,
    {
        let prev_kvs =
            KvStoreInner::<DB>::get_range(txn_db, txn_index, &req.key, &req.range_end, 0)?;
        let mut response = DeleteRangeResponse {
            header: Some(self.header_gen.gen_header(false)),
            ..DeleteRangeResponse::default()
        };
        response.deleted = prev_kvs.len().cast();
        if req.prev_kv {
            response.prev_kvs = prev_kvs;
        }

        let _keys = Self::delete_keys(
            txn_db,
            txn_index,
            &req.key,
            &req.range_end,
            revision,
            sub_revision,
        )?;

        Ok(response)
    }

    /// Handle `TxnRequest`
    fn execute_txn(
        &self,
        txn_db: &Transaction,
        txn_index: &IndexTransaction,
        request: &TxnRequest,
        revision: i64,
        sub_revision: &mut i64,
    ) -> Result<TxnResponse, ExecuteError> {
        let success = request
            .compare
            .iter()
            .all(|compare| Self::check_compare(txn_db, txn_index, compare));
        let requests = if success {
            request.success.iter()
        } else {
            request.failure.iter()
        };

        let responses = requests
            .filter_map(|op| op.request.as_ref())
            .map(|req| match *req {
                Request::RequestRange(ref r) => {
                    self.execute_range(txn_db, txn_index, r).map(Into::into)
                }
                Request::RequestTxn(ref r) => self
                    .execute_txn(txn_db, txn_index, r, revision, sub_revision)
                    .map(Into::into),
                Request::RequestPut(ref r) => self
                    .execute_put(txn_db, txn_index, r, revision, sub_revision)
                    .map(Into::into),
                Request::RequestDeleteRange(ref r) => self
                    .execute_delete_range(txn_db, txn_index, r, revision, sub_revision)
                    .map(Into::into),
            })
            .collect::<Result<Vec<ResponseWrapper>, _>>()?;

        Ok(TxnResponse {
            header: Some(self.header_gen.gen_header(false)),
            succeeded: success,
            responses: responses.into_iter().map(Into::into).collect(),
        })
    }

    /// Handle `CompactionRequest`
    fn execute_compaction(
        &self,
        req: &CompactionRequest,
    ) -> Result<CompactionResponse, ExecuteError> {
        req.check_revision(self.compacted_revision(), self.revision())?;

        let target_revision = req.revision;
        debug_assert!(
            target_revision > self.compacted_revision(),
            "required revision should not be compacted"
        );
        self.update_compacted_revision(target_revision);
        Ok(CompactionResponse {
            header: Some(self.header_gen.gen_header(false)),
        })
    }
}

/// Sync requests
impl<DB> KvStore<DB>
where
    DB: StorageApi,
{
    /// Handle kv requests
    async fn sync_request<T>(
        &self,
        wrapper: &RequestWrapper,
        txn_db: T,
    ) -> Result<SyncResponse, ExecuteError>
    where
        T: StorageTxnApi,
    {
        debug!("Execute {:?}", wrapper);

        let txn_index = self.inner.index.transaction();
        let next_revision = self.revision.get().overflow_add(1);

        #[allow(clippy::wildcard_enum_match_arm)]
        let events = match *wrapper {
            RequestWrapper::RangeRequest(_) => {
                vec![]
            }
            RequestWrapper::PutRequest(ref req) => {
                self.sync_put(&txn_db, &txn_index, req, next_revision, &mut 0)?
            }
            RequestWrapper::DeleteRangeRequest(ref req) => {
                self.sync_delete_range(&txn_db, &txn_index, req, next_revision, &mut 0)?
            }
            RequestWrapper::TxnRequest(ref req) => {
                self.sync_txn(&txn_db, &txn_index, req, next_revision, &mut 0)?
            }
            RequestWrapper::CompactionRequest(ref req) => self.sync_compaction(req).await?,
            _ => unreachable!("Other request should not be sent to this store"),
        };

        txn_db
            .commit()
            .await
            .map_err(|e| ExecuteError::DbError(e.to_string()))?;
        txn_index.commit();

        let response = if events.is_empty() {
            SyncResponse::new(self.revision.get())
        } else {
            self.notify_updates(next_revision, events).await;
            SyncResponse::new(self.revision.next())
        };

        Ok(response)
    }

    /// Handle `PutRequest`
    fn sync_put<T>(
        &self,
        txn_db: &T,
        txn_index: &IndexTransaction,
        req: &PutRequest,
        revision: i64,
        sub_revision: &mut i64,
    ) -> Result<Vec<Event>, ExecuteError>
    where
        T: StorageTxnApi,
    {
        let new_rev = txn_index.register_revision(&req.key, revision, *sub_revision);
        let mut kv = KeyValue {
            key: req.key.clone(),
            value: req.value.clone(),
            create_revision: new_rev.create_revision,
            mod_revision: new_rev.mod_revision,
            version: new_rev.version,
            lease: req.lease,
        };

        if req.ignore_lease || req.ignore_value {
            let prev_kv = KvStoreInner::<DB>::get_range(txn_db, txn_index, &req.key, &[], 0)?.pop();
            let prev = prev_kv.as_ref().ok_or(ExecuteError::KeyNotFound)?;
            if req.ignore_lease {
                kv.lease = prev.lease;
            }
            if req.ignore_value {
                kv.value = prev.value.clone();
            }
        };

        let old_lease = self.get_lease(&kv.key);
        if old_lease != 0 {
            self.detach(old_lease, kv.key.as_slice())
                .unwrap_or_else(|e| warn!("Failed to detach lease from a key, error: {:?}", e));
        }
        if req.lease != 0 {
            self.attach(req.lease, kv.key.as_slice())
                .unwrap_or_else(|e| panic!("unexpected error from lease Attach: {e}"));
        }

        txn_db.write_op(WriteOp::PutKeyValue(new_rev.as_revision(), kv.clone()))?;
        *sub_revision = sub_revision.overflow_add(1);

        let revs = vec![(
            kv.key.clone(),
            KeyRevision::new(
                kv.create_revision,
                kv.version,
                new_rev.as_revision().revision(),
                new_rev.as_revision().sub_revision(),
            ),
        )];
        txn_index.insert(revs);

        Ok(vec![Event {
            #[allow(clippy::as_conversions)] // This cast is always valid
            r#type: EventType::Put as i32,
            kv: Some(kv),
            prev_kv: None,
        }])
    }

    /// Handle `DeleteRangeRequest`
    fn sync_delete_range<T>(
        &self,
        txn_db: &T,
        txn_index: &IndexTransaction,
        req: &DeleteRangeRequest,
        revision: i64,
        sub_revision: &mut i64,
    ) -> Result<Vec<Event>, ExecuteError>
    where
        T: StorageTxnApi,
    {
        let keys = Self::delete_keys(
            txn_db,
            txn_index,
            &req.key,
            &req.range_end,
            revision,
            sub_revision,
        )?;

        Self::detach_leases(&keys, &self.lease_collection);

        Ok(Self::new_deletion_events(revision, keys))
    }

    /// Handle `TxnRequest`
    fn sync_txn<T>(
        &self,
        txn_db: &T,
        txn_index: &IndexTransaction,
        request: &TxnRequest,
        revision: i64,
        sub_revision: &mut i64,
    ) -> Result<Vec<Event>, ExecuteError>
    where
        T: StorageTxnApi,
    {
        request.check_revision(self.compacted_revision(), self.revision())?;
        let success = request
            .compare
            .iter()
            .all(|compare| Self::check_compare(txn_db, txn_index, compare));
        let requests = if success {
            request.success.iter()
        } else {
            request.failure.iter()
        };

        let events = requests
            .filter_map(|op| op.request.as_ref())
            .map(|req| match *req {
                Request::RequestRange(_) => Ok(vec![]),
                Request::RequestTxn(ref r) => {
                    self.sync_txn(txn_db, txn_index, r, revision, sub_revision)
                }
                Request::RequestPut(ref r) => {
                    self.sync_put(txn_db, txn_index, r, revision, sub_revision)
                }
                Request::RequestDeleteRange(ref r) => {
                    self.sync_delete_range(txn_db, txn_index, r, revision, sub_revision)
                }
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect();

        Ok(events)
    }

    /// Sync `CompactionRequest` and return if kvstore is changed
    async fn sync_compaction(&self, req: &CompactionRequest) -> Result<Vec<Event>, ExecuteError> {
        let revision = req.revision;
        // TODO: Remove the physical process logic here. It's better to move into the KvServer
        #[allow(clippy::collapsible_else_if)]
        if req.physical {
            let event = Arc::new(event_listener::Event::new());
            if let Err(e) = self
                .compact_task_tx
                .send((revision, Some(Arc::clone(&event))))
                .await
            {
                panic!("the compactor exited unexpectedly: {e:?}");
            }
            event.listen().await;
        } else {
            if let Err(e) = self.compact_task_tx.send((revision, None)).await {
                panic!("the compactor exited unexpectedly: {e:?}");
            }
        }
        let ops = vec![WriteOp::PutCompactRevision(revision)];
        self.inner.db.flush_ops(ops)?;

        Ok(vec![])
    }
}

impl<DB> KvStore<DB>
where
    DB: StorageApi,
{
    /// create events for a deletion
    pub(crate) fn new_deletion_events(revision: i64, keys: Vec<Vec<u8>>) -> Vec<Event> {
        keys.into_iter()
            .map(|key| {
                let kv = KeyValue {
                    key,
                    mod_revision: revision,
                    ..Default::default()
                };
                Event {
                    #[allow(clippy::as_conversions)] // This cast is always valid
                    r#type: EventType::Delete as i32,
                    kv: Some(kv),
                    prev_kv: None,
                }
            })
            .collect()
    }

    /// Mark deletion for keys
    fn mark_deletions<'a>(
        revisions: &[(Revision, Revision)],
        keys: &[Vec<u8>],
    ) -> (Vec<WriteOp<'a>>, Vec<(Vec<u8>, KeyRevision)>) {
        assert_eq!(keys.len(), revisions.len(), "Index doesn't match with DB");
        keys.iter()
            .zip(revisions.iter())
            .map(|(key, &(_, new_rev))| {
                let del_kv = KeyValue {
                    key: key.clone(),
                    mod_revision: new_rev.revision(),
                    ..KeyValue::default()
                };

                let key_revision = (
                    del_kv.key.clone(),
                    KeyRevision::new(
                        del_kv.create_revision,
                        del_kv.version,
                        new_rev.revision(),
                        new_rev.sub_revision(),
                    ),
                );
                (WriteOp::PutKeyValue(new_rev, del_kv), key_revision)
            })
            .unzip()
    }

    /// Delete keys from index and detach them in lease collection, return all the write operations and events
    pub(crate) fn delete_keys<T>(
        txn_db: &T,
        txn_index: &IndexTransaction,
        key: &[u8],
        range_end: &[u8],
        revision: i64,
        sub_revision: &mut i64,
    ) -> Result<Vec<Vec<u8>>, ExecuteError>
    where
        T: StorageTxnApi,
    {
        let (revisions, keys) = txn_index.delete(key, range_end, revision, *sub_revision);
        let (del_ops, key_revisions) = Self::mark_deletions(&revisions, &keys);

        txn_index.insert(key_revisions);

        *sub_revision = sub_revision.overflow_add(del_ops.len().cast());
        for op in del_ops {
            txn_db.write_op(op)?;
        }

        Ok(keys)
    }

    /// Detaches the leases
    pub(crate) fn detach_leases(keys: &[Vec<u8>], lease_collection: &LeaseCollection) {
        for k in keys {
            let lease_id = lease_collection.get_lease(k);
            lease_collection
                .detach(lease_id, k)
                .unwrap_or_else(|e| warn!("Failed to detach lease from a key, error: {:?}", e));
        }
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use test_macros::abort_on_panic;
    use tokio::{runtime::Handle, task::block_in_place};
    use utils::{config::EngineConfig, shutdown};

    use super::*;
    use crate::{
        revision_number::RevisionNumberGenerator,
        rpc::{Request as UniRequest, RequestOp},
        storage::{
            compact::{compact_bg_task, COMPACT_CHANNEL_SIZE},
            db::DB,
            kvwatcher::KvWatcher,
        },
    };

    const CHANNEL_SIZE: usize = 1024;

    struct StoreWrapper(Option<Arc<KvStore<DB>>>, shutdown::Trigger);

    impl Drop for StoreWrapper {
        fn drop(&mut self) {
            drop(self.0.take());
            block_in_place(move || {
                Handle::current().block_on(async move {
                    self.1.self_shutdown_and_wait().await;
                });
            });
        }
    }

    impl std::ops::Deref for StoreWrapper {
        type Target = Arc<KvStore<DB>>;

        fn deref(&self) -> &Self::Target {
            self.0.as_ref().unwrap()
        }
    }

    impl std::ops::DerefMut for StoreWrapper {
        fn deref_mut(&mut self) -> &mut Self::Target {
            self.0.as_mut().unwrap()
        }
    }

    fn sort_req(sort_order: SortOrder, sort_target: SortTarget) -> RangeRequest {
        RangeRequest {
            key: vec![0],
            range_end: vec![0],
            sort_order: sort_order as i32,
            sort_target: sort_target as i32,
            ..Default::default()
        }
    }

    async fn init_store(
        db: Arc<DB>,
    ) -> Result<(StoreWrapper, RevisionNumberGenerator), ExecuteError> {
        let store = init_empty_store(db);
        let keys = vec!["a", "b", "c", "d", "e", "z", "z", "z"];
        let vals = vec!["a", "b", "c", "d", "e", "z1", "z2", "z3"];
        let revision = RevisionNumberGenerator::default();
        for (key, val) in keys.into_iter().zip(vals.into_iter()) {
            let req = RequestWithToken::new(
                PutRequest {
                    key: key.into(),
                    value: val.into(),
                    ..Default::default()
                }
                .into(),
            );
            exe_as_and_flush(&store, &req).await?;
        }
        Ok((store, revision))
    }

    fn init_empty_store(db: Arc<DB>) -> StoreWrapper {
        let (shutdown_trigger, shutdown_listener) = shutdown::channel();
        let (compact_tx, compact_rx) = mpsc::channel(COMPACT_CHANNEL_SIZE);
        let (kv_update_tx, kv_update_rx) = mpsc::channel(CHANNEL_SIZE);
        let lease_collection = Arc::new(LeaseCollection::new(0));
        let header_gen = Arc::new(HeaderGenerator::new(0, 0));
        let index = Arc::new(Index::new());
        let kv_store_inner = Arc::new(KvStoreInner::new(Arc::clone(&index), db));
        let storage = Arc::new(KvStore::new(
            Arc::clone(&kv_store_inner),
            header_gen,
            kv_update_tx,
            compact_tx,
            lease_collection,
        ));
        let _watcher = KvWatcher::new_arc(
            kv_store_inner,
            kv_update_rx,
            Duration::from_millis(10),
            shutdown_listener.clone(),
        );
        let _compactor = tokio::spawn(compact_bg_task(
            Arc::clone(&storage),
            index,
            1000,
            Duration::from_millis(10),
            compact_rx,
            shutdown_listener,
        ));
        StoreWrapper(Some(storage), shutdown_trigger)
    }

    async fn exe_as_and_flush(
        store: &Arc<KvStore<DB>>,
        request: &RequestWithToken,
    ) -> Result<(), ExecuteError> {
        let txn = store.db().transaction();
        store.after_sync(request, txn).await.map(|_| ())
    }

    fn index_compact(store: &Arc<KvStore<DB>>, at_rev: i64) -> Vec<Vec<u8>> {
        store
            .inner
            .index
            .compact(at_rev)
            .into_iter()
            .map(|key_rev| key_rev.as_revision().encode_to_vec())
            .collect::<Vec<Vec<_>>>()
    }

    #[tokio::test(flavor = "multi_thread")]
    #[abort_on_panic]
    async fn test_keys_only() -> Result<(), ExecuteError> {
        let db = DB::open(&EngineConfig::Memory)?;
        let (store, _rev) = init_store(db).await?;
        let request = RangeRequest {
            key: vec![0],
            range_end: vec![0],
            keys_only: true,
            ..Default::default()
        };
        let txn_db = store.inner.db.transaction();
        let txn_index = store.inner.index.transaction();
        let response = store.execute_range(&txn_db, &txn_index, &request)?;
        assert_eq!(response.kvs.len(), 6);
        for kv in response.kvs {
            assert!(kv.value.is_empty());
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    #[abort_on_panic]
    async fn test_range_empty() -> Result<(), ExecuteError> {
        let db = DB::open(&EngineConfig::Memory)?;
        let (store, _rev) = init_store(db).await?;

        let request = RangeRequest {
            key: "x".into(),
            range_end: "y".into(),
            keys_only: true,
            ..Default::default()
        };
        let txn_db = store.inner.db.transaction();
        let txn_index = store.inner.index.transaction();
        let response = store.execute_range(&txn_db, &txn_index, &request)?;
        assert_eq!(response.kvs.len(), 0);
        assert_eq!(response.count, 0);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    #[abort_on_panic]
    async fn test_range_filter() -> Result<(), ExecuteError> {
        let db = DB::open(&EngineConfig::Memory)?;
        let (store, _rev) = init_store(db).await?;

        let request = RangeRequest {
            key: vec![0],
            range_end: vec![0],
            max_create_revision: 3,
            min_create_revision: 2,
            max_mod_revision: 3,
            min_mod_revision: 2,
            ..Default::default()
        };
        let txn_db = store.inner.db.transaction();
        let txn_index = store.inner.index.transaction();
        let response = store.execute_range(&txn_db, &txn_index, &request)?;
        assert_eq!(response.count, 6);
        assert_eq!(response.kvs.len(), 2);
        assert_eq!(response.kvs[0].create_revision, 2);
        assert_eq!(response.kvs[1].create_revision, 3);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    #[abort_on_panic]
    async fn test_range_sort() -> Result<(), ExecuteError> {
        let db = DB::open(&EngineConfig::Memory)?;
        let (store, _rev) = init_store(db).await?;
        let keys = ["a", "b", "c", "d", "e", "z"];
        let reversed_keys = ["z", "e", "d", "c", "b", "a"];
        let version_keys = ["z", "a", "b", "c", "d", "e"];

        for order in [SortOrder::Ascend, SortOrder::Descend, SortOrder::None] {
            for target in [
                SortTarget::Key,
                SortTarget::Create,
                SortTarget::Mod,
                SortTarget::Value,
            ] {
                let txn_db = store.inner.db.transaction();
                let txn_index = store.inner.index.transaction();
                let response =
                    store.execute_range(&txn_db, &txn_index, &sort_req(order, target))?;
                assert_eq!(response.count, 6);
                assert_eq!(response.kvs.len(), 6);
                let expected: [&str; 6] = match order {
                    SortOrder::Descend => reversed_keys,
                    SortOrder::Ascend | SortOrder::None => keys,
                };
                for (kv, want) in response.kvs.iter().zip(expected.iter()) {
                    assert_eq!(
                        kv.key,
                        want.as_bytes(),
                        "order: {:?}, target: {:?}, key {:?}, want {:?}",
                        order,
                        target,
                        kv.key,
                        want.as_bytes(),
                    );
                }
            }
        }
        for order in [SortOrder::Ascend, SortOrder::Descend, SortOrder::None] {
            let txn_db = store.inner.db.transaction();
            let txn_index = store.inner.index.transaction();
            let response =
                store.execute_range(&txn_db, &txn_index, &sort_req(order, SortTarget::Version))?;
            assert_eq!(response.count, 6);
            assert_eq!(response.kvs.len(), 6);
            let expected = match order {
                SortOrder::Ascend | SortOrder::None => keys,
                SortOrder::Descend => version_keys,
            };
            for (kv, want) in response.kvs.iter().zip(expected.iter()) {
                assert_eq!(
                    kv.key,
                    want.as_bytes(),
                    "order: {:?}, key {:?}, want {:?}",
                    order,
                    kv.key,
                    want.as_bytes(),
                );
            }
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    #[abort_on_panic]
    async fn test_recover() -> Result<(), ExecuteError> {
        let db = DB::open(&EngineConfig::Memory)?;
        let ops = vec![WriteOp::PutCompactRevision(8)];
        db.flush_ops(ops)?;
        let (store, _rev_gen) = init_store(Arc::clone(&db)).await?;
        assert_eq!(store.inner.index.get_from_rev(b"z", b"", 5).len(), 3);

        let new_store = init_empty_store(db);

        let range_req = RangeRequest {
            key: "a".into(),
            range_end: vec![],
            ..Default::default()
        };

        let txn_db = new_store.inner.db.transaction();
        let txn_index = new_store.inner.index.transaction();
        let res = new_store.execute_range(&txn_db, &txn_index, &range_req)?;
        assert_eq!(res.kvs.len(), 0);
        assert_eq!(new_store.compacted_revision(), -1);

        new_store.recover().await?;

        let txn_db_recovered = new_store.inner.db.transaction();
        let txn_index_recovered = new_store.inner.index.transaction();
        let res = store.execute_range(&txn_db_recovered, &txn_index_recovered, &range_req)?;
        assert_eq!(res.kvs.len(), 1);
        assert_eq!(res.kvs[0].key, b"a");
        assert_eq!(new_store.compacted_revision(), 8);
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(new_store.inner.index.get_from_rev(b"z", b"", 5).len(), 2);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    #[abort_on_panic]
    async fn test_txn() -> Result<(), ExecuteError> {
        let txn_req = RequestWithToken::new(
            TxnRequest {
                compare: vec![Compare {
                    result: CompareResult::Equal as i32,
                    target: CompareTarget::Value as i32,
                    key: "a".into(),
                    range_end: vec![],
                    target_union: Some(TargetUnion::Value("a".into())),
                }],
                success: vec![RequestOp {
                    request: Some(Request::RequestTxn(TxnRequest {
                        compare: vec![Compare {
                            result: CompareResult::Equal as i32,
                            target: CompareTarget::Value as i32,
                            key: "b".into(),
                            range_end: vec![],
                            target_union: Some(TargetUnion::Value("b".into())),
                        }],
                        success: vec![RequestOp {
                            request: Some(Request::RequestPut(PutRequest {
                                key: "success".into(),
                                value: "1".into(),
                                ..Default::default()
                            })),
                        }],
                        failure: vec![],
                    })),
                }],
                failure: vec![RequestOp {
                    request: Some(Request::RequestPut(PutRequest {
                        key: "success".into(),
                        value: "0".into(),
                        ..Default::default()
                    })),
                }],
            }
            .into(),
        );
        let db = DB::open(&EngineConfig::Memory)?;
        let (store, _rev) = init_store(db).await?;
        exe_as_and_flush(&store, &txn_req).await?;
        let request = RangeRequest {
            key: "success".into(),
            range_end: vec![],
            ..Default::default()
        };

        let txn_db = store.inner.db.transaction();
        let txn_index = store.inner.index.transaction();
        let response = store.execute_range(&txn_db, &txn_index, &request)?;
        assert_eq!(response.count, 1);
        assert_eq!(response.kvs.len(), 1);
        assert_eq!(response.kvs[0].value, "1".as_bytes());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    #[abort_on_panic]
    async fn test_kv_store_index_available() {
        let db = DB::open(&EngineConfig::Memory).unwrap();
        let (store, _revision) = init_store(Arc::clone(&db)).await.unwrap();
        let handle = tokio::spawn({
            let store = Arc::clone(&store);
            async move {
                for i in 0..100_u8 {
                    let req = RequestWithToken::new(
                        PutRequest {
                            key: "foo".into(),
                            value: vec![i],
                            ..Default::default()
                        }
                        .into(),
                    );
                    exe_as_and_flush(&store, &req).await.unwrap();
                }
            }
        });
        tokio::time::sleep(std::time::Duration::from_micros(50)).await;
        let revs = store.inner.index.get_from_rev(b"foo", b"", 1);
        let kvs = KvStoreInner::<DB>::get_values(&db.transaction(), &revs).unwrap();
        assert_eq!(
            kvs.len(),
            revs.len(),
            "kvs.len() != revs.len(), maybe some operations already inserted into index, but not flushed to db"
        );
        handle.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    #[allow(clippy::too_many_lines)] // TODO: splits this test
    async fn test_compaction() -> Result<(), ExecuteError> {
        let db = DB::open(&EngineConfig::Memory)?;
        let store = init_empty_store(db);
        // sample requests: (a, 1) (b, 2) (a, 3) (del a)
        // their revisions:     2      3      4       5
        let requests = vec![
            RequestWithToken::new(
                PutRequest {
                    key: "a".into(),
                    value: "1".into(),
                    ..Default::default()
                }
                .into(),
            ),
            RequestWithToken::new(
                PutRequest {
                    key: "b".into(),
                    value: "2".into(),
                    ..Default::default()
                }
                .into(),
            ),
            RequestWithToken::new(
                PutRequest {
                    key: "a".into(),
                    value: "3".into(),
                    ..Default::default()
                }
                .into(),
            ),
            RequestWithToken::new(
                DeleteRangeRequest {
                    key: "a".into(),
                    ..Default::default()
                }
                .into(),
            ),
        ];

        for req in requests {
            exe_as_and_flush(&store, &req).await.unwrap();
        }

        let target_revisions = index_compact(&store, 3);
        store.compact(target_revisions.as_ref())?;

        let txn_db = store.inner.db.transaction();
        let txn_index = store.inner.index.transaction();
        assert_eq!(
            KvStoreInner::<DB>::get_range(&txn_db, &txn_index, b"a", b"", 2)
                .unwrap()
                .len(),
            1,
            "(a, 1) should not be removed"
        );
        assert_eq!(
            KvStoreInner::<DB>::get_range(&txn_db, &txn_index, b"b", b"", 3)
                .unwrap()
                .len(),
            1,
            "(b, 2) should not be removed"
        );

        let target_revisions = index_compact(&store, 4);
        store.compact(target_revisions.as_ref())?;
        assert!(
            KvStoreInner::<DB>::get_range(&txn_db, &txn_index, b"a", b"", 2)
                .unwrap()
                .is_empty(),
            "(a, 1) should be removed"
        );
        assert_eq!(
            KvStoreInner::<DB>::get_range(&txn_db, &txn_index, b"b", b"", 3)
                .unwrap()
                .len(),
            1,
            "(b, 2) should not be removed"
        );
        assert_eq!(
            KvStoreInner::<DB>::get_range(&txn_db, &txn_index, b"a", b"", 4)
                .unwrap()
                .len(),
            1,
            "(a, 3) should not be removed"
        );

        let target_revisions = index_compact(&store, 5);
        store.compact(target_revisions.as_ref())?;
        assert!(
            KvStoreInner::<DB>::get_range(&txn_db, &txn_index, b"a", b"", 2)
                .unwrap()
                .is_empty(),
            "(a, 1) should be removed"
        );
        assert_eq!(
            KvStoreInner::<DB>::get_range(&txn_db, &txn_index, b"b", b"", 3)
                .unwrap()
                .len(),
            1,
            "(b, 2) should not be removed"
        );
        assert!(
            KvStoreInner::<DB>::get_range(&txn_db, &txn_index, b"a", b"", 4)
                .unwrap()
                .is_empty(),
            "(a, 3) should be removed"
        );
        assert!(
            KvStoreInner::<DB>::get_range(&txn_db, &txn_index, b"a", b"", 5)
                .unwrap()
                .is_empty(),
            "(a, 4) should be removed"
        );

        Ok(())
    }

    #[test]
    fn check_revision_will_return_correct_error_type() {
        let request = TxnRequest {
            compare: vec![],
            success: vec![RequestOp {
                request: Some(UniRequest::RequestRange(RangeRequest {
                    key: "k".into(),
                    revision: 3,
                    ..Default::default()
                })),
            }],
            failure: vec![],
        };
        assert!(matches!(
            request.check_revision(1, 2).unwrap_err(),
            ExecuteError::RevisionTooLarge(_, _)
        ));

        let request = RangeRequest {
            key: "k".into(),
            revision: 1,
            ..Default::default()
        };
        assert!(matches!(
            request.check_revision(2, 3).unwrap_err(),
            ExecuteError::RevisionCompacted(_, _)
        ));
    }

    #[test]
    fn check_compaction_will_return_correct_error_type() {
        let request = CompactionRequest {
            revision: 3,
            physical: false,
        };
        assert!(matches!(
            request.check_revision(1, 2).unwrap_err(),
            ExecuteError::RevisionTooLarge(_, _)
        ));

        let request = CompactionRequest {
            revision: 1,
            physical: false,
        };
        assert!(matches!(
            request.check_revision(2, 3).unwrap_err(),
            ExecuteError::RevisionCompacted(_, _)
        ));
    }
}
