use std::{cmp::Ordering, marker::PhantomData, sync::Arc, time::Duration};

use async_trait::async_trait;
use curp_external_api::cmd::Command;
use futures::{stream::FuturesUnordered, Future, Stream, StreamExt};
use tonic::Response;
use tracing::{debug, warn};

use super::{state::State, ClientApi, LeaderStateUpdate, ProposeResponse, RepeatableClientApi};
use crate::{
    members::ServerId,
    quorum, recover_quorum,
    response::ResponseReceiver,
    rpc::{
        connect::ConnectApi, ConfChange, CurpError, FetchClusterRequest, FetchClusterResponse,
        FetchReadStateRequest, Member, MoveLeaderRequest, OpResponse, ProposeConfChangeRequest,
        ProposeId, ProposeRequest, PublishRequest, ReadState, RecordRequest, RecordResponse,
        ShutdownRequest,
    },
    super_quorum,
};

/// The unary client config
#[derive(Debug)]
pub(super) struct UnaryConfig {
    /// The rpc timeout of a propose request
    propose_timeout: Duration,
    /// The rpc timeout of a 2-RTT request, usually takes longer than propose timeout
    /// The recommended the values is within (propose_timeout, 2 * propose_timeout].
    wait_synced_timeout: Duration,
}

impl UnaryConfig {
    /// Create a unary config
    pub(super) fn new(propose_timeout: Duration, wait_synced_timeout: Duration) -> Self {
        Self {
            propose_timeout,
            wait_synced_timeout,
        }
    }
}

/// The unary client
#[derive(Debug)]
pub(super) struct Unary<C: Command> {
    /// Client state
    state: Arc<State>,
    /// Unary config
    config: UnaryConfig,
    /// marker
    phantom: PhantomData<C>,
}

impl<C: Command> Unary<C> {
    /// Create an unary client
    pub(super) fn new(state: Arc<State>, config: UnaryConfig) -> Self {
        Self {
            state,
            config,
            phantom: PhantomData,
        }
    }

    /// Get a handle `f` and apply to the leader
    /// NOTICE:
    /// The leader might be outdate if the local state is stale.
    /// `map_leader` should never be invoked in [`ClientApi::fetch_cluster`]
    /// `map_leader` might call `fetch_leader_id`, `fetch_cluster`, finally
    /// result in stack overflow.
    async fn map_leader<R, F: Future<Output = Result<R, CurpError>>>(
        &self,
        f: impl FnOnce(Arc<dyn ConnectApi>) -> F,
    ) -> Result<R, UnaryError> {
        let cached_leader = self.state.leader_id().await;
        let leader_id = match cached_leader {
            Some(id) => id,
            None => <Unary<C> as ClientApi>::fetch_leader_id(self, false).await?,
        };

        self.state
            .map_server(leader_id, f)
            .await
            .map_err(Into::into)
    }

    async fn leader_id(&self) -> Result<u64, UnaryError> {
        let cached_leader = self.state.leader_id().await;
        match cached_leader {
            Some(id) => Ok(id),
            None => <Unary<C> as ClientApi>::fetch_leader_id(self, false)
                .await
                .map_err(Into::into),
        }
    }

    /// New a seq num and record it
    #[allow(clippy::unused_self)] // TODO: implement request tracker
    fn new_seq_num(&self) -> u64 {
        rand::random()
    }
}

impl<C: Command> Unary<C> {
    /// Propose for read only commands
    ///
    /// For read-only commands, we only need to send propose to leader
    async fn propose_read_only<PF>(propose_fut: PF) -> Result<ProposeResponse<C>, UnaryError>
    where
        PF: Future<
            Output = Result<
                Response<Box<dyn Stream<Item = tonic::Result<OpResponse>> + Send>>,
                CurpError,
            >,
        >,
    {
        let propose_res = propose_fut.await.map_err(UnaryError::new_idempotent);
        let resp_stream = propose_res?.into_inner();
        let mut response_rx = ResponseReceiver::new(resp_stream);
        response_rx
            .recv::<C>(false)
            .await
            .map_err(UnaryError::new_idempotent)
    }

    /// Propose for mutative commands
    async fn propose_mutative<PF, RF>(
        propose_fut: PF,
        mut record_futs: FuturesUnordered<RF>,
        use_fast_path: bool,
        size: usize,
    ) -> Result<ProposeResponse<C>, UnaryError>
    where
        PF: Future<
            Output = Result<
                Response<Box<dyn Stream<Item = tonic::Result<OpResponse>> + Send>>,
                CurpError,
            >,
        >,
        RF: Future<Output = Result<Response<RecordResponse>, CurpError>>,
    {
        let superquorum = super_quorum(size);
        let recover_quorum = recover_quorum(size);

        let propose_result = propose_fut.await;
        // A successful propose to the leader also record the command
        let mut ok_cnt: usize = if propose_result.is_ok() { 1 } else { 0 };

        while let Some(result) = record_futs.next().await {
            if result.map(|r| !r.get_ref().conflict).unwrap_or(false) {
                ok_cnt = ok_cnt.wrapping_add(1);
                if ok_cnt == superquorum {
                    break;
                }
            }
        }
        // Adds one to `ok_cnt` because the when an error occurs in the leader's
        // response, it does not necessarily guarantee a record failure
        let definite = ok_cnt.wrapping_add(1) < recover_quorum;

        match propose_result {
            Ok(resp) => {
                let mut response_rx = ResponseReceiver::new(resp.into_inner());
                let fast_path_failed = ok_cnt < superquorum;
                response_rx
                    .recv::<C>(fast_path_failed || !use_fast_path)
                    .await
                    .map_err(|err| UnaryError::new_propose(err, definite))
            }
            Err(err) => Err(UnaryError::new_propose(err, definite)),
        }
    }
}

#[async_trait]
impl<C: Command> ClientApi for Unary<C> {
    /// The error is generated from server
    type Error = UnaryError;

    /// The command type
    type Cmd = C;

    /// Send propose to the whole cluster, `use_fast_path` set to `false` to fallback into ordered
    /// requests (event the requests are commutative).
    async fn propose(
        &self,
        cmd: &C,
        token: Option<&String>,
        use_fast_path: bool,
    ) -> Result<ProposeResponse<C>, Self::Error> {
        let propose_id = self.gen_propose_id()?;
        RepeatableClientApi::propose(self, propose_id, cmd, token, use_fast_path).await
    }

    /// Send propose configuration changes to the cluster
    async fn propose_conf_change(
        &self,
        changes: Vec<ConfChange>,
    ) -> Result<Vec<Member>, Self::Error> {
        let propose_id = self.gen_propose_id()?;
        RepeatableClientApi::propose_conf_change(self, propose_id, changes).await
    }

    /// Send propose to shutdown cluster
    async fn propose_shutdown(&self) -> Result<(), Self::Error> {
        let propose_id = self.gen_propose_id()?;
        RepeatableClientApi::propose_shutdown(self, propose_id).await
    }

    /// Send propose to publish a node id and name
    async fn propose_publish(
        &self,
        node_id: ServerId,
        node_name: String,
        node_client_urls: Vec<String>,
    ) -> Result<(), Self::Error> {
        let propose_id = self.gen_propose_id()?;
        RepeatableClientApi::propose_publish(self, propose_id, node_id, node_name, node_client_urls)
            .await
    }

    /// Send move leader request
    async fn move_leader(&self, node_id: ServerId) -> Result<(), Self::Error> {
        let req = MoveLeaderRequest::new(node_id, self.state.cluster_version().await);
        let timeout = self.config.wait_synced_timeout;
        let _ig = self
            .map_leader(|conn| async move { conn.move_leader(req, timeout).await })
            .await?;
        Ok(())
    }

    /// Send fetch read state from leader
    async fn fetch_read_state(&self, cmd: &C) -> Result<ReadState, Self::Error> {
        // Same as fast_round, we blame the serializing error to the server even
        // thought it is the local error
        let req = FetchReadStateRequest::new(cmd, self.state.cluster_version().await).map_err(
            |ser_err| {
                warn!("serializing error: {ser_err}");
                CurpError::from(ser_err)
            },
        )?;
        let timeout = self.config.wait_synced_timeout;
        let state = self
            .map_leader(|conn| async move { conn.fetch_read_state(req, timeout).await })
            .await?
            .into_inner()
            .read_state
            .unwrap_or_else(|| unreachable!("read_state must be set in fetch read state response"));
        Ok(state)
    }

    /// Send fetch cluster requests to all servers
    /// Note: The fetched cluster may still be outdated if `linearizable` is false
    async fn fetch_cluster(&self, linearizable: bool) -> Result<FetchClusterResponse, Self::Error> {
        fn check_members(members: &[Member]) -> bool {
            if members.is_empty() {
                return false;
            }
            for member in members {
                if member.client_urls.is_empty() {
                    debug!("new node {} not published yet", member.id());
                    return false;
                    // return Err(CurpError::Internal(format!(
                    //     "New node {} not published yet",
                    //     member.id()
                    // )));
                }
            }
            true
        }

        let timeout = self.config.wait_synced_timeout;
        if !linearizable {
            // firstly, try to fetch the local server
            if let Some(connect) = self.state.local_connect().await {
                /// local timeout, in fact, local connect should only be bypassed, so the timeout maybe unused.
                const FETCH_LOCAL_TIMEOUT: Duration = Duration::from_secs(1);

                let resp = connect
                    .fetch_cluster(FetchClusterRequest::default(), FETCH_LOCAL_TIMEOUT)
                    .await?
                    .into_inner();
                debug!("fetch local cluster {resp:?}");

                return Ok(resp);
            }
        }
        // then fetch the whole cluster
        let mut responses = self
            .state
            .for_each_server(|conn| async move {
                (
                    conn.id(),
                    conn.fetch_cluster(FetchClusterRequest { linearizable }, timeout)
                        .await
                        .map(Response::into_inner),
                )
            })
            .await;
        let quorum = quorum(responses.len());

        let mut max_term = 0;
        let mut res = None;
        let mut ok_cnt = 0;
        let mut err: Option<CurpError> = None;

        while let Some((id, resp)) = responses.next().await {
            tracing::info!("fetching cluster from {id}");
            let inner = match resp {
                Ok(r) => r,
                Err(e) => {
                    warn!("fetch cluster from {} failed, {:?}", id, e);
                    // similar to fast round
                    if e.should_abort_fast_round() {
                        return Err(e.into());
                    }
                    if let Some(old_err) = err.as_ref() {
                        if old_err.priority() <= e.priority() {
                            err = Some(e);
                        }
                    } else {
                        err = Some(e);
                    }
                    continue;
                }
            };
            // Ignore the response of a node that doesn't know who the leader is.
            if inner.leader_id.is_some() {
                #[allow(clippy::arithmetic_side_effects)]
                match max_term.cmp(&inner.term) {
                    Ordering::Less => {
                        max_term = inner.term;
                        if check_members(&inner.members) {
                            res = Some(inner);
                        }
                        // reset ok count to 1
                        ok_cnt = 1;
                    }
                    Ordering::Equal => {
                        if check_members(&inner.members) {
                            res = Some(inner);
                        }
                        ok_cnt += 1;
                    }
                    Ordering::Greater => {}
                }
            }
            // first check quorum
            if ok_cnt >= quorum {
                // then check if we got the response
                if let Some(res) = res {
                    debug!("fetch cluster succeeded, result: {res:?}");
                    if let Err(e) = self.state.check_and_update(&res).await {
                        warn!("update to a new cluster state failed, error {e}");
                    }
                    return Ok(res);
                }
                debug!("fetch cluster quorum ok, but members are empty");
            }
            debug!("fetch cluster from {id} success");
        }

        if let Some(err) = err {
            debug!("fetch cluster failed, err: {err:?}");
            return Err(err.into());
        }
        debug!("fetch cluster failed, cnt: {ok_cnt:?}");

        // It seems that the max term has not reached the majority here. Mock a transport error and return it to the external to retry.
        return Err(CurpError::RpcTransport(()).into());
    }
}

#[async_trait]
impl<C: Command> RepeatableClientApi for Unary<C> {
    /// Generate a unique propose id during the retry process.
    fn gen_propose_id(&self) -> Result<ProposeId, Self::Error> {
        let client_id = self.state.client_id();
        let seq_num = self.new_seq_num();
        Ok(ProposeId(client_id, seq_num))
    }

    /// Send propose to the whole cluster, `use_fast_path` set to `false` to fallback into ordered
    /// requests (event the requests are commutative).
    async fn propose(
        &self,
        propose_id: ProposeId,
        cmd: &Self::Cmd,
        token: Option<&String>,
        use_fast_path: bool,
    ) -> Result<ProposeResponse<Self::Cmd>, Self::Error> {
        let cmd_arc = Arc::new(cmd);
        let propose_req = ProposeRequest::new::<C>(
            propose_id,
            cmd_arc.as_ref(),
            self.state.cluster_version().await,
            self.state.term().await,
            !use_fast_path,
        );
        let record_req = RecordRequest::new::<C>(propose_id, cmd_arc.as_ref());
        let superquorum = super_quorum(self.state.connects_len().await);
        let leader_id = self.leader_id().await?;
        let timeout = self.config.propose_timeout;

        let propose_fut = self.state.map_server(leader_id, |conn| async move {
            conn.propose_stream(propose_req, token.cloned(), timeout)
                .await
        });
        let record_futs = self
            .state
            .for_each_follower(leader_id, |conn| {
                let record_req_c = record_req.clone();
                async move { conn.record(record_req_c, timeout).await }
            })
            .await;

        if cmd.is_read_only() {
            Self::propose_read_only(propose_fut).await
        } else {
            Self::propose_mutative(propose_fut, record_futs, use_fast_path, superquorum).await
        }
    }

    /// Send propose configuration changes to the cluster
    async fn propose_conf_change(
        &self,
        propose_id: ProposeId,
        changes: Vec<ConfChange>,
    ) -> Result<Vec<Member>, Self::Error> {
        let req =
            ProposeConfChangeRequest::new(propose_id, changes, self.state.cluster_version().await);
        let timeout = self.config.wait_synced_timeout;
        let members = self
            .map_leader(|conn| async move { conn.propose_conf_change(req, timeout).await })
            .await?
            .into_inner()
            .members;
        Ok(members)
    }

    /// Send propose to shutdown cluster
    async fn propose_shutdown(&self, propose_id: ProposeId) -> Result<(), Self::Error> {
        let req = ShutdownRequest::new(propose_id, self.state.cluster_version().await);
        let timeout = self.config.wait_synced_timeout;
        let _ig = self
            .map_leader(|conn| async move { conn.shutdown(req, timeout).await })
            .await?;
        Ok(())
    }

    /// Send propose to publish a node id and name
    async fn propose_publish(
        &self,
        propose_id: ProposeId,
        node_id: ServerId,
        node_name: String,
        node_client_urls: Vec<String>,
    ) -> Result<(), Self::Error> {
        let req = PublishRequest::new(propose_id, node_id, node_name, node_client_urls);
        let timeout = self.config.wait_synced_timeout;
        let _ig = self
            .map_leader(|conn| async move { conn.publish(req, timeout).await })
            .await?;
        Ok(())
    }
}

#[async_trait]
impl<C: Command> LeaderStateUpdate for Unary<C> {
    /// Update leader
    async fn update_leader(&self, leader_id: Option<ServerId>, term: u64) -> bool {
        self.state.check_and_update_leader(leader_id, term).await
    }
}

/// Error during propose in unary client
#[derive(Debug)]
pub(super) enum UnaryError {
    /// Idempotent operations
    ///
    /// This consist of two types of errors
    /// * Errors that does not related to propose
    /// * Propose errors but idempotent
    Idempotent(CurpError),
    /// Propose errors
    Propose {
        /// The error returned by the leader
        leader_err: CurpError,
        /// The command is committed through witnesses
        ///
        /// If the client finds that the command is accepted by less than `⌈f/2⌉+1` witnesses
        /// Then the error is definite a failure.
        definite: bool,
    },
}

impl UnaryError {
    /// Creates a new none propose error
    fn new_idempotent(err: CurpError) -> Self {
        Self::Idempotent(err)
    }

    /// Creates a propose error
    fn new_propose(leader_err: CurpError, definite: bool) -> Self {
        Self::Propose {
            leader_err,
            definite,
        }
    }

    /// Gets the `CurpError`
    pub(super) fn curp_err(&self) -> &CurpError {
        match *self {
            UnaryError::Idempotent(ref err) => err,
            UnaryError::Propose { ref leader_err, .. } => leader_err,
        }
    }

    /// Converts self into `CurpError`
    pub(super) fn into_curp_err(self) -> CurpError {
        match self {
            UnaryError::Idempotent(err) => err,
            UnaryError::Propose { leader_err, .. } => leader_err,
        }
    }

    /// Retruns `true` if a retry could proceed
    pub(super) fn retriable(&self) -> bool {
        match *self {
            UnaryError::Idempotent(_) => true,
            UnaryError::Propose { definite, .. } => definite,
        }
    }
}

impl From<CurpError> for UnaryError {
    fn from(err: CurpError) -> Self {
        UnaryError::new_idempotent(err)
    }
}
