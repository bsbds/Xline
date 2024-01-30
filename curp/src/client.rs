use std::{
    cmp::Ordering,
    collections::HashMap,
    fmt::Debug,
    iter,
    marker::PhantomData,
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};

use dashmap::DashMap;
use event_listener::Event;
use futures::{future, stream::FuturesUnordered, StreamExt};
use parking_lot::RwLock;
use tokio::time::timeout;
use tracing::{debug, instrument, warn};
use utils::config::ClientConfig;

use crate::{
    cmd::Command,
    error::{ClientBuildError, ClientError},
    members::ServerId,
    response::ResponseReceiver,
    rpc::{
        self, connect::ConnectApi, protocol_client::ProtocolClient, ConfChange, CurpError,
        FetchClusterRequest, FetchClusterResponse, FetchReadStateRequest, Member,
        ProposeConfChangeRequest, ProposeId, ProposeRequest, PublishRequest,
        ReadState as PbReadState, RecordRequest, Redirect, ShutdownRequest,
    },
    LogIndex,
};

/// Protocol client
#[derive(derive_builder::Builder)]
#[builder(build_fn(skip), name = "Builder")]
pub struct Client<C: Command> {
    /// local server id. Only use in an inner client.
    #[builder(field(type = "Option<ServerId>"), setter(custom))]
    local_server_id: Option<ServerId>,
    /// Current leader and term
    #[builder(setter(skip))]
    state: RwLock<State>,
    /// All servers's `Connect`
    #[builder(setter(skip))]
    connects: DashMap<ServerId, Arc<dyn ConnectApi>>,
    /// Cluster version
    #[builder(setter(skip))]
    cluster_version: AtomicU64,
    /// Curp client config settings
    config: ClientConfig,
    /// To keep Command type
    #[builder(setter(skip))]
    phantom: PhantomData<C>,
}

impl<C: Command> Builder<C> {
    /// Set local server id.
    #[inline]
    pub fn local_server_id(&mut self, value: ServerId) -> &mut Self {
        self.local_server_id = Some(value);
        self
    }

    /// Build client from all members
    /// # Errors
    /// Return error when meet rpc error or missing some arguments
    #[inline]
    pub async fn build_from_all_members(
        &self,
        all_members: HashMap<ServerId, Vec<String>>,
        leader_id: Option<ServerId>,
    ) -> Result<Client<C>, ClientBuildError> {
        let Some(config) = self.config else {
            return Err(ClientBuildError::invalid_arguments("timeout is required"));
        };
        let connects = rpc::connects(all_members).await?.collect();
        let client = Client::<C> {
            local_server_id: self.local_server_id,
            state: RwLock::new(State::new(leader_id, 0)),
            config,
            connects,
            cluster_version: AtomicU64::new(0),
            phantom: PhantomData,
        };
        Ok(client)
    }

    /// Fetch cluster from server, return the first `FetchClusterResponse`
    async fn fast_fetch_cluster(
        &self,
        addrs: Vec<String>,
        propose_timeout: Duration,
    ) -> Result<FetchClusterResponse, ClientBuildError> {
        let mut futs: FuturesUnordered<_> = addrs
            .into_iter()
            .map(|mut addr| {
                if !addr.starts_with("http://") {
                    addr.insert_str(0, "http://");
                }
                async move {
                    let mut protocol_client = ProtocolClient::connect(addr).await?;
                    let mut req = tonic::Request::new(FetchClusterRequest::default());
                    req.set_timeout(propose_timeout);
                    let fetch_cluster_res = protocol_client.fetch_cluster(req).await?.into_inner();
                    Ok::<FetchClusterResponse, ClientBuildError>(fetch_cluster_res)
                }
            })
            .collect();
        let mut err = ClientBuildError::invalid_arguments("addrs is empty");
        while let Some(r) = futs.next().await {
            match r {
                Ok(r) => {
                    return Ok(r);
                }
                Err(e) => err = e,
            }
        }
        Err(err)
    }

    /// Build client from addresses (could be incomplete), this method will fetch all members from servers
    /// # Errors
    /// Return error when meet rpc error or missing some arguments
    #[inline]
    pub async fn build_from_addrs(
        &self,
        addrs: Vec<String>,
    ) -> Result<Client<C>, ClientBuildError> {
        let Some(config) = self.config else {
            return Err(ClientBuildError::invalid_arguments("timeout is required"));
        };
        let res: FetchClusterResponse = self
            .fast_fetch_cluster(addrs.clone(), *config.propose_timeout())
            .await?;
        let client = Client::<C> {
            local_server_id: self.local_server_id,
            state: RwLock::new(State::new(res.leader_id, res.term)),
            config,
            cluster_version: AtomicU64::new(res.cluster_version),
            connects: rpc::connects(res.into_members_addrs()).await?.collect(),
            phantom: PhantomData,
        };
        Ok(client)
    }
}

impl<C: Command> Debug for Client<C> {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("state", &self.state)
            .field("timeout", &self.config)
            .finish()
    }
}

/// State of a client
#[derive(Debug, Default)]
struct State {
    /// Current leader
    leader: Option<ServerId>,
    /// Current term
    term: u64,
    /// When a new leader is set, notify
    leader_notify: Arc<Event>,
}

impl State {
    /// Create the initial client state
    fn new(leader: Option<ServerId>, term: u64) -> Self {
        Self {
            leader,
            term,
            leader_notify: Arc::new(Event::new()),
        }
    }

    /// Set the leader and notify all the waiters
    fn set_leader(&mut self, id: ServerId) {
        debug!("client update its leader to {id}");
        self.leader = Some(id);
        self.leader_notify.notify(usize::MAX);
    }

    /// Update to the newest term and reset local cache
    fn update_to_term(&mut self, term: u64) {
        debug_assert!(self.term <= term, "the client's term {} should not be greater than the given term {} when update the term", self.term, term);
        self.term = term;
        self.leader = None;
    }

    /// Check the term and leader id, update the state if needed
    fn check_and_update(&mut self, leader_id: Option<u64>, term: u64) {
        match self.term.cmp(&term) {
            Ordering::Less => {
                // reset term only when the resp has leader id to prevent:
                // If a server loses contact with its leader, it will update its term for election. Since other servers are all right, the election will not succeed.
                // But if the client learns about the new term and updates its term to it, it will never get the true leader.
                if let Some(new_leader_id) = leader_id {
                    self.update_to_term(term);
                    self.set_leader(new_leader_id);
                }
            }
            Ordering::Equal => {
                if let Some(new_leader_id) = leader_id {
                    if self.leader.is_none() {
                        self.set_leader(new_leader_id);
                    }
                    assert_eq!(
                        self.leader,
                        Some(new_leader_id),
                        "there should never be two leader in one term"
                    );
                }
            }
            Ordering::Greater => {}
        }
    }
}

/// Read state of a command
#[derive(Debug)]
#[non_exhaustive]
pub enum ReadState {
    /// need to wait the inflight commands
    Ids(Vec<u64>),
    /// need to wait the commit index
    CommitIndex(LogIndex),
}

#[allow(clippy::wildcard_enum_match_arm)] // TODO: wait refactoring
impl<C> Client<C>
where
    C: Command,
{
    /// Client builder
    #[inline]
    #[must_use]
    pub fn builder() -> Builder<C> {
        Builder::default()
    }

    /// Get cluster version
    fn cluster_version(&self) -> u64 {
        self.cluster_version
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Set the information of current cluster
    async fn set_cluster(&self, cluster: FetchClusterResponse) -> Result<(), ClientError<C>> {
        debug!("update client by remote cluster: {cluster:?}");
        self.state
            .write()
            .check_and_update(cluster.leader_id, cluster.term);
        let member_addrs = cluster
            .members
            .into_iter()
            .map(|m| (m.id, m.addrs))
            .collect::<HashMap<ServerId, Vec<String>>>();
        self.connects.clear();
        for (id, connect) in rpc::connects(member_addrs)
            .await
            .map_err(|e| ClientError::InternalError(format!("connect to cluster failed: {e}")))?
        {
            let _ig = self.connects.insert(id, connect);
        }
        self.cluster_version.store(
            cluster.cluster_version,
            std::sync::atomic::Ordering::Relaxed,
        );
        Ok(())
    }

    /// The shutdown rpc of curp protocol
    #[instrument(skip_all)]
    pub async fn shutdown(&self) -> Result<(), ClientError<C>> {
        let propose_id = self.gen_propose_id().await?;
        let mut retry_timeout = self.get_backoff();
        let retry_count = *self.config.retry_count();
        for _ in 0..retry_count {
            let leader_id = match self.get_leader_id().await {
                Ok(leader_id) => leader_id,
                Err(e) => {
                    warn!("failed to fetch leader, {e}");
                    continue;
                }
            };
            debug!("shutdown request sent to {}", leader_id);
            if let Err(e) = self
                .get_connect(leader_id)
                .unwrap_or_else(|| unreachable!("leader {leader_id} not found"))
                .shutdown(
                    ShutdownRequest::new(propose_id, self.cluster_version()),
                    *self.config.wait_synced_timeout(),
                )
                .await
            {
                warn!("shutdown rpc error: {e:?}");
                match e {
                    CurpError::ShuttingDown(_) => return Err(ClientError::ShuttingDown),
                    CurpError::WrongClusterVersion(_) => {
                        let cluster = self.fetch_cluster(false).await?;
                        self.set_cluster(cluster).await?;
                        continue;
                    }
                    CurpError::Redirect(Redirect {
                        leader_id: new_leader,
                        term,
                    }) => {
                        self.state.write().check_and_update(new_leader, term);
                        warn!("shutdown: redirect to new leader {new_leader:?}, term is {term}",);
                        continue;
                    }
                    _ => {
                        tokio::time::sleep(retry_timeout.next_retry()).await;
                        continue;
                    }
                }
            };
            return Ok(());
        }
        Err(ClientError::Timeout)
    }

    /// Send fetch cluster requests to all servers
    /// Note: The fetched cluster may still be outdated if `linearizable` is false
    /// # Errors
    ///   `ClientError<C>::Timeout` if timeout
    #[inline]
    async fn fetch_cluster(
        &self,
        linearizable: bool,
    ) -> Result<FetchClusterResponse, ClientError<C>> {
        let mut retry_timeout = self.get_backoff();
        let retry_count = *self.config.retry_count();
        for _ in 0..retry_count {
            let connects = self.all_connects();
            let timeout = retry_timeout.next_retry();
            let mut rpcs: FuturesUnordered<_> = connects
                .iter()
                .map(|connect| async {
                    (
                        connect.id(),
                        connect
                            .fetch_cluster(FetchClusterRequest { linearizable }, timeout)
                            .await,
                    )
                })
                .collect();
            let mut max_term = 0;
            let mut res = None;

            let mut ok_cnt = 0;
            #[allow(clippy::integer_arithmetic)]
            let majority_cnt = connects.len() / 2 + 1;
            while let Some((id, resp)) = rpcs.next().await {
                let inner = match resp {
                    Ok(resp) => resp.into_inner(),
                    Err(e) => {
                        warn!("fetch cluster from {} failed, {:?}", id, e);
                        continue;
                    }
                };

                #[allow(clippy::integer_arithmetic)]
                match max_term.cmp(&inner.term) {
                    Ordering::Less => {
                        max_term = inner.term;
                        if !inner.members.is_empty() {
                            res = Some(inner);
                        }
                        ok_cnt = 1;
                    }
                    Ordering::Equal => {
                        if !inner.members.is_empty() {
                            res = Some(inner);
                        }
                        ok_cnt += 1;
                    }
                    Ordering::Greater => {}
                }
                if ok_cnt >= majority_cnt {
                    break;
                }
            }

            if let Some(res) = res {
                let mut state = self.state.write();
                debug!("Fetch cluster succeeded, result: {res:?}");
                state.check_and_update(res.leader_id, res.term);
                return Ok(res);
            }

            // wait until the election is completed
            // TODO: let user configure it according to average leader election cost
            tokio::time::sleep(timeout).await;
        }
        Err(ClientError::Timeout)
    }

    /// Send fetch leader requests to all servers until there is a leader
    /// Note: The fetched leader may still be outdated
    async fn fetch_leader(&self) -> Result<ServerId, ClientError<C>> {
        let retry_count = *self.config.retry_count();
        for _ in 0..retry_count {
            let res = self.fetch_cluster(false).await?;
            if let Some(leader_id) = res.leader_id {
                return Ok(leader_id);
            }
        }
        // This timeout is a bit different. It refers to the situation where
        // multiple attempts to fetch the cluster are successful, but there
        // is no leader id (very rare).
        Err(ClientError::Timeout)
    }

    /// Get leader id from the state or fetch it from servers
    /// # Errors
    /// `ClientError::Timeout` if timeout
    #[inline]
    pub async fn get_leader_id(&self) -> Result<ServerId, ClientError<C>> {
        let notify = Arc::clone(&self.state.read().leader_notify);
        let mut retry_timeout = self.get_backoff();
        let retry_count = *self.config.retry_count();
        for _ in 0..retry_count {
            if let Some(id) = self.state.read().leader {
                return Ok(id);
            }
            if timeout(retry_timeout.next_retry(), notify.listen())
                .await
                .is_err()
            {
                return self.fetch_leader().await;
            }
        }
        Err(ClientError::Timeout)
    }

    /// Propose
    #[inline]
    #[instrument(skip_all)]
    #[allow(clippy::type_complexity)] // This type is not complex
    pub async fn propose(
        &self,
        cmd: C,
        use_fast_path: bool,
    ) -> Result<(C::ER, Option<C::ASR>), ClientError<C>> {
        let cmd_arc = Arc::new(cmd);
        let propose_id = self.gen_propose_id().await?;
        let propose_req = ProposeRequest::new(
            propose_id,
            cmd_arc.as_ref(),
            self.cluster_version(),
            self.state.read().term,
        );
        let record_req = RecordRequest::new(propose_id, cmd_arc.as_ref());
        let leader_id = self.get_leader_id().await?;
        let leader_connect = self
            .connects
            .get(&leader_id)
            .map(|e| Arc::clone(e.value()))
            .unwrap_or_else(|| unreachable!());
        let follower_connects: Vec<_> = self
            .connects
            .iter()
            .filter(|e| *e.key() != leader_id)
            .map(|e| Arc::clone(e.value()))
            .collect();
        let superquorum = superquorum(self.connects.len());

        let propose_fut =
            leader_connect.propose_stream(propose_req.clone(), *self.config.propose_timeout());
        let record_stream: FuturesUnordered<_> = follower_connects
            .iter()
            .zip(iter::repeat(record_req))
            .map(|(connect, req_cloned)| connect.record(req_cloned, *self.config.propose_timeout()))
            .collect();
        let record_futs = record_stream
            .filter_map(|res| future::ready(res.ok()))
            .filter(|resp| future::ready(!resp.get_ref().conflict))
            .take(superquorum.wrapping_sub(1))
            .collect::<Vec<_>>();
        let (propose_res, record_resps) = tokio::join!(propose_fut, record_futs);

        let resp_stream = propose_res
            .map_err(|e| match e {
                CurpError::ShuttingDown(_) => ClientError::ShuttingDown,
                CurpError::WrongClusterVersion(_) => ClientError::WrongClusterVersion,
                CurpError::Redirect(_) => ClientError::TermOutdated,
                CurpError::RpcTransport(_) => {
                    ClientError::InternalError("rpc transport".to_owned())
                }
                e => unreachable!("err: {e:?}"),
            })?
            .into_inner();
        let mut response_rx = ResponseReceiver::new(resp_stream);

        let (er, conflict) = response_rx.recv_er::<C>().await?;
        let fast_path_failed = conflict || record_resps.len() < superquorum.wrapping_sub(1);
        if !use_fast_path || fast_path_failed {
            let asr = response_rx.recv_asr::<C>().await?;
            return Ok((er, Some(asr)));
        }

        Ok((er, None))
    }

    /// Propose the conf change request to servers
    #[instrument(skip_all)]
    pub async fn propose_conf_change(
        &self,
        changes: Vec<ConfChange>,
    ) -> Result<Result<Vec<Member>, CurpError>, ClientError<C>> {
        let propose_id = self.gen_propose_id().await?;
        debug!(
            "propose_conf_change with propose_id({}) started",
            propose_id
        );
        let mut retry_timeout = self.get_backoff();
        let retry_count = *self.config.retry_count();
        for _ in 0..retry_count {
            let leader_id = match self.get_leader_id().await {
                Ok(leader_id) => leader_id,
                Err(e) => {
                    warn!("failed to fetch leader, {e}");
                    continue;
                }
            };
            debug!("propose_conf_change request sent to {}", leader_id);
            match self
                .get_connect(leader_id)
                .unwrap_or_else(|| unreachable!("leader {leader_id} not found"))
                .propose_conf_change(
                    ProposeConfChangeRequest::new(
                        propose_id,
                        changes.clone(),
                        self.cluster_version(),
                    ),
                    *self.config.wait_synced_timeout(),
                )
                .await
            {
                Ok(resp) => return Ok(Ok(resp.into_inner().members)),
                Err(e) => {
                    warn!("propose_conf_change rpc error: {e:?}");
                    match e {
                        CurpError::ShuttingDown(_) => return Err(ClientError::ShuttingDown),
                        CurpError::WrongClusterVersion(_) => {
                            let cluster = self.fetch_cluster(false).await?;
                            self.set_cluster(cluster).await?;
                            continue;
                        }
                        CurpError::Redirect(Redirect {
                            leader_id: new_leader,
                            term,
                        }) => {
                            self.state.write().check_and_update(new_leader, term);
                            warn!(
                                "propose_conf_change: redirect to new leader {new_leader:?}, term is {term}",
                            );
                            continue;
                        }
                        CurpError::InvalidConfig(_)
                        | CurpError::LearnerNotCatchUp(_)
                        | CurpError::NodeAlreadyExists(_)
                        | CurpError::NodeNotExists(_)
                        | CurpError::Duplicated(_)
                        | CurpError::ExpiredClientId(_) => return Ok(Err(e)),
                        _ => {
                            tokio::time::sleep(retry_timeout.next_retry()).await;
                            continue;
                        }
                    }
                }
            };
        }
        Err(ClientError::Timeout)
    }

    /// publish new node's name
    #[instrument(skip_all)]
    pub async fn publish(
        &self,
        node_id: ServerId,
        node_name: String,
    ) -> Result<(), ClientError<C>> {
        let propose_id = self.gen_propose_id().await?;
        debug!("publish with propose_id({}) started", propose_id);
        let mut retry_timeout = self.get_backoff();
        let retry_count = *self.config.retry_count();
        for _ in 0..retry_count {
            let leader_id = match self.get_leader_id().await {
                Ok(leader_id) => leader_id,
                Err(e) => {
                    warn!("failed to fetch leader, {e}");
                    continue;
                }
            };
            debug!("publish request sent to {}", leader_id);
            if let Err(e) = self
                .get_connect(leader_id)
                .unwrap_or_else(|| unreachable!("leader {leader_id} not found"))
                .publish(
                    PublishRequest::new(propose_id, node_id, node_name.clone()),
                    *self.config.wait_synced_timeout(),
                )
                .await
            {
                warn!("publish rpc error: {e:?}");
                match e {
                    CurpError::ShuttingDown(_) => return Err(ClientError::ShuttingDown),
                    CurpError::WrongClusterVersion(_) => {
                        let cluster = self.fetch_cluster(false).await?;
                        self.set_cluster(cluster).await?;
                        continue;
                    }
                    CurpError::Redirect(Redirect {
                        leader_id: new_leader,
                        term,
                    }) => {
                        self.state.write().check_and_update(new_leader, term);
                        warn!(
                            "propose_conf_change: redirect to new leader {new_leader:?}, term is {term}",
                        );
                        continue;
                    }
                    _ => {
                        tokio::time::sleep(retry_timeout.next_retry()).await;
                        continue;
                    }
                }
            };
            return Ok(());
        }
        Err(ClientError::Timeout)
    }

    /// Fetch Read state from leader
    /// # Errors
    ///   `ClientError::EncodingError` encoding error met while deserializing the propose id
    #[inline]
    pub async fn fetch_read_state(&self, cmd: &C) -> Result<ReadState, ClientError<C>> {
        let mut retry_timeout = self.get_backoff();
        let retry_count = *self.config.retry_count();
        for _ in 0..retry_count {
            let leader_id = match self.get_leader_id().await {
                Ok(id) => id,
                Err(e) => {
                    warn!("failed to fetch leader, {e}");
                    continue;
                }
            };
            debug!("fetch read state request sent to {}", leader_id);
            let resp = match self
                .get_connect(leader_id)
                .unwrap_or_else(|| unreachable!("leader {leader_id} not found"))
                .fetch_read_state(
                    FetchReadStateRequest::new(cmd, self.cluster_version())?,
                    *self.config.wait_synced_timeout(),
                )
                .await
            {
                Ok(resp) => resp.into_inner(),
                Err(e) => match e {
                    CurpError::ShuttingDown(_) => return Err(ClientError::ShuttingDown),
                    CurpError::WrongClusterVersion(_) => {
                        let cluster = self.fetch_cluster(false).await?;
                        self.set_cluster(cluster).await?;
                        continue;
                    }
                    _ => {
                        warn!("fetch read state rpc error: {e:?}");
                        tokio::time::sleep(retry_timeout.next_retry()).await;
                        continue;
                    }
                },
            };
            let pb_state = resp
                .read_state
                .unwrap_or_else(|| unreachable!("read state should be some"));
            let state = match pb_state {
                PbReadState::CommitIndex(i) => ReadState::CommitIndex(i),
                PbReadState::Ids(i) => ReadState::Ids(i.inflight_ids),
            };
            return Ok(state);
        }
        Err(ClientError::Timeout)
    }

    /// Fetch the current cluster from the curp server where is on the same node.
    /// Note that this method should not be invoked by an outside client because
    /// we will fallback to fetch the full cluster for the response if fetching local
    /// failed.
    #[inline]
    async fn fetch_local_cluster(&self) -> Result<FetchClusterResponse, ClientError<C>> {
        if let Some(local_server) = self.local_server_id {
            let resp = self
                .get_connect(local_server)
                .unwrap_or_else(|| unreachable!("self id {} not found", local_server))
                .fetch_cluster(
                    FetchClusterRequest::default(),
                    *self.config.initial_retry_timeout(),
                )
                .await
                .map_err(|e| ClientError::InternalError(format!("{e:?}")))?
                .into_inner();
            Ok(resp)
        } else {
            unreachable!("The outer client shouldn't invoke fetch_local_leader_info");
        }
    }

    /// Fetch the current leader id without cache
    /// # Errors
    /// `ClientError::Timeout` if timeout
    #[inline]
    pub async fn get_leader_id_from_curp(&self) -> Result<ServerId, ClientError<C>> {
        if let Ok(FetchClusterResponse {
            leader_id: Some(leader_id),
            ..
        }) = self.fetch_local_cluster().await
        {
            return Ok(leader_id);
        }
        self.fetch_leader().await
    }

    /// Fetch the current cluster without cache
    /// # Errors
    /// `ClientError::Timeout` if timeout
    #[inline]
    pub async fn get_cluster_from_curp(
        &self,
        linearizable: bool,
    ) -> Result<FetchClusterResponse, ClientError<C>> {
        if linearizable {
            return self.fetch_cluster(true).await;
        }
        if let Ok(resp) = self.fetch_local_cluster().await {
            return Ok(resp);
        }
        self.fetch_cluster(false).await
    }

    /// Get the connect by server id
    fn get_connect(&self, id: ServerId) -> Option<Arc<dyn ConnectApi>> {
        self.connects.get(&id).map(|c| Arc::clone(&c))
    }

    /// Get all connects
    fn all_connects(&self) -> Vec<Arc<dyn ConnectApi>> {
        self.connects.iter().map(|c| Arc::clone(&c)).collect()
    }

    /// Get the client id
    ///
    /// # Errors
    ///
    ///   `ClientError::Timeout` if timeout
    #[allow(clippy::unused_async)] // TODO: grant a client id from server
    async fn get_client_id(&self) -> Result<u64, ClientError<C>> {
        Ok(rand::random())
    }

    /// New a seq num and record it
    #[allow(clippy::unused_self)] // TODO: implement request tracker
    fn new_seq_num(&self) -> u64 {
        0
    }

    /// Generate a propose id
    ///
    /// # Errors
    ///   `ClientError::Timeout` if timeout
    async fn gen_propose_id(&self) -> Result<ProposeId, ClientError<C>> {
        let client_id = self.get_client_id().await?;
        let seq_num = self.new_seq_num();
        Ok(ProposeId(client_id, seq_num))
    }

    /// Get the initial backoff config
    fn get_backoff(&self) -> BackOff {
        BackOff::new(
            *self.config.initial_retry_timeout(),
            *self.config.max_retry_timeout(),
            *self.config.use_backoff(),
        )
    }
}

/// Generate timeout using exponential backoff algorithm
struct BackOff {
    /// Current timeout
    timeout: Duration,
    /// Max timeout
    max_timeout: Duration,
    /// Whether to use backoff
    use_backoff: bool,
}

impl BackOff {
    /// Creates a new `BackOff`
    fn new(initial_timeout: Duration, max_timeout: Duration, use_backoff: bool) -> Self {
        Self {
            timeout: initial_timeout,
            max_timeout,
            use_backoff,
        }
    }

    /// Get current timeout
    fn next_retry(&mut self) -> Duration {
        let current = self.timeout;
        if self.use_backoff {
            self.timeout = self
                .timeout
                .checked_mul(2)
                .unwrap_or(self.timeout)
                .min(self.max_timeout);
        }
        current
    }
}

/// Get the superquorum for curp protocol
/// Although curp can proceed with f + 1 available replicas, it needs f + 1 + (f + 1)/2 replicas
/// (for superquorum of witnesses) to use 1 RTT operations. With less than superquorum replicas,
/// clients must ask masters to commit operations in f + 1 replicas before returning result.(2 RTTs).
#[inline]
fn superquorum(nodes: usize) -> usize {
    let fault_tolerance = nodes.wrapping_div(2);
    fault_tolerance
        .wrapping_add(fault_tolerance.wrapping_add(1).wrapping_div(2))
        .wrapping_add(1)
}

#[cfg(test)]
mod tests {
    use curp_test_utils::test_cmd::TestCommand;

    use super::*;

    #[test]
    fn superquorum_should_work() {
        assert_eq!(superquorum(1), 1);
        assert_eq!(superquorum(11), 9);
        assert_eq!(superquorum(97), 73);
        assert_eq!(superquorum(31), 24);
        assert_eq!(superquorum(59), 45);
    }

    #[tokio::test]
    async fn client_builder_should_return_err_when_arguments_invalid() {
        let res = Client::<TestCommand>::builder()
            .config(ClientConfig::default())
            .build_from_all_members(HashMap::from([(123, vec!["addr".to_owned()])]), None)
            .await;
        assert!(res.is_ok());

        let res = Client::<TestCommand>::builder()
            .local_server_id(123)
            .build_from_addrs(vec!["addr".to_owned()])
            .await;
        assert!(matches!(res, Err(ClientBuildError::InvalidArguments(_))));
    }
}
