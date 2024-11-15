use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use tonic::transport::Channel;

use crate::{
    error::Result,
    types::cluster::{Change, WaitLearner},
    AuthService, CurpClient,
};
use xlineapi::{
    MemberAddResponse, MemberListResponse, MemberPromoteResponse, MemberRemoveResponse,
    MemberUpdateResponse,
};

/// Etcd competible membership operation
#[async_trait]
pub trait EtcdMembership {
    /// Add a new member to the cluster.
    ///
    /// # Errors
    ///
    /// Returns an error if the request could not be sent or if the response is invalid.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use xline_client::{Client, ClientOptions};
    /// use anyhow::Result;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let mut client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .cluster_client();
    ///
    ///     let resp = client.member_add(["127.0.0.1:2380"], true).await?;
    ///
    ///     println!(
    ///         "members: {:?}, added: {:?}",
    ///         resp.members, resp.member
    ///     );
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn member_add<U, I>(
        &mut self,
        peer_urls: I,
        is_learner: bool,
    ) -> Result<MemberAddResponse>
    where
        U: AsRef<str> + Send,
        I: IntoIterator<Item = U> + Send;

    /// Remove an existing member from the cluster.
    ///
    /// # Errors
    ///
    /// Returns an error if the request could not be sent or if the response is invalid.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use xline_client::{Client, ClientOptions};
    /// use anyhow::Result;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let mut client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .cluster_client();
    ///     let resp = client.member_remove(1).await?;
    ///
    ///     println!("members: {:?}", resp.members);
    ///
    ///     Ok(())
    ///  }
    ///
    async fn member_remove(&mut self, id: u64) -> Result<MemberRemoveResponse>;

    /// Promote an existing member to be the leader of the cluster.
    ///
    /// # Errors
    ///
    /// Returns an error if the request could not be sent or if the response is invalid.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use xline_client::{Client, ClientOptions};
    /// use anyhow::Result;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let mut client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .cluster_client();
    ///     let resp = client.member_promote(1).await?;
    ///
    ///     println!("members: {:?}", resp.members);
    ///
    ///     Ok(())
    /// }
    ///
    async fn member_promote(&mut self, id: u64) -> Result<MemberPromoteResponse>;

    /// Update an existing member in the cluster.
    ///
    /// # Errors
    ///
    /// Returns an error if the request could not be sent or if the response is invalid.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use xline_client::{Client, ClientOptions};
    /// use anyhow::Result;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let mut client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .cluster_client();
    ///     let resp = client.member_update(1, ["127.0.0.1:2379"]).await?;
    ///
    ///     println!("members: {:?}", resp.members);
    ///
    ///     Ok(())
    ///  }
    ///
    async fn member_update<U, I>(&mut self, id: u64, peer_urls: I) -> Result<MemberUpdateResponse>
    where
        U: AsRef<str> + Send,
        I: IntoIterator<Item = U> + Send;

    /// List all members in the cluster.
    ///
    /// # Errors
    ///
    /// Returns an error if the request could not be sent or if the response is invalid.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use xline_client::{Client, ClientOptions};
    /// use anyhow::Result;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let mut client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .cluster_client();
    ///     let resp = client.member_list(false).await?;
    ///
    ///     println!("members: {:?}", resp.members);
    ///
    ///     Ok(())
    /// }
    async fn member_list(&mut self, linearizable: bool) -> Result<MemberListResponse>;
}

/// Xline specific membership operation
#[async_trait]
pub trait XlineMembership {
    /// Updates the membership
    ///
    /// # Note
    /// The ids in `changes` should never overlap with each other.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use anyhow::Result;
    /// use futures::StreamExt;
    /// use xline_client::{
    ///     clients::XlineMembership,
    ///     types::cluster::{Change, LearnerStatus, Node},
    ///     Client, ClientOptions,
    /// };
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     // the name and address of all curp members
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .cluster_client();
    ///
    ///     let node1 = Node::new(1, "n1", vec!["10.0.0.4:2380"], vec!["10.0.0.4.2379"]);
    ///     let node2 = Node::new(2, "n2", vec!["10.0.0.5:2380"], vec!["10.0.0.5.2379"]);
    ///     client
    ///         .change(vec![Change::Add(node1), Change::Add(node2)])
    ///         .await?;
    ///     client.change(vec![Change::Promote(1)]).await?;
    ///     client.change(vec![Change::Demote(1)]).await?;
    ///     // Remove the previously added learners
    ///     client
    ///         .change(vec![Change::Remove(1), Change::Remove(2)])
    ///         .await?;
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn change<I>(&self, changes: I) -> Result<()>
    where
        I: IntoIterator<Item = Change> + Send;

    /// Wait for learners to be added to the cluster.
    ///
    /// # Arguments
    ///
    /// * `node_ids` - An iterator of node IDs to wait for.
    ///
    /// # Errors
    ///
    /// Returns an error if the request could not be sent or if the response is invalid.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use xline_client::{Client, ClientOptions, clients::LearnerStatus};
    /// use anyhow::Result;
    /// use futures::StreamExt;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let mut client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .cluster_client();
    ///     let mut stream = client.wait_learner(vec![1, 2, 3]).await?;
    ///
    ///     while let Some(Ok(status)) = stream.next().await {
    ///         match status {
    ///             LearnerStatus::Pending { node_id, index } => {
    ///                 println!("Learner node {} is pending with index {}", node_id, index);
    ///             }
    ///             LearnerStatus::Ready => {
    ///                 println!("Learner node is ready");
    ///             }
    ///         }
    ///     }
    ///
    ///     // all learners are up-to-date
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn wait_learner<Ids>(&self, node_ids: Ids) -> Result<WaitLearner>
    where
        Ids: IntoIterator<Item = u64> + Send;
}

/// Client for Cluster operations.
#[derive(Clone)]
#[non_exhaustive]
pub struct ClusterClient {
    /// Inner client
    #[cfg(not(madsim))]
    inner: xlineapi::ClusterClient<AuthService<Channel>>,
    /// The client running the CURP protocol, communicate with all servers.
    curp_client: Arc<CurpClient>,
    /// Inner client
    #[cfg(madsim)]
    inner: xlineapi::ClusterClient<Channel>,
}

#[async_trait]
impl EtcdMembership for ClusterClient {
    #[inline]
    async fn member_add<U, I>(
        &mut self,
        peer_urls: I,
        is_learner: bool,
    ) -> Result<MemberAddResponse>
    where
        U: AsRef<str> + Send,
        I: IntoIterator<Item = U> + Send,
    {
        let peer_urls: Vec<String> = peer_urls
            .into_iter()
            .map(|s| s.as_ref().to_owned())
            .collect();

        Ok(self
            .inner
            .member_add(xlineapi::MemberAddRequest {
                peer_ur_ls: peer_urls,
                is_learner,
            })
            .await?
            .into_inner())
    }

    #[inline]
    async fn member_remove(&mut self, id: u64) -> Result<MemberRemoveResponse> {
        Ok(self
            .inner
            .member_remove(xlineapi::MemberRemoveRequest { id })
            .await?
            .into_inner())
    }

    #[inline]
    async fn member_promote(&mut self, id: u64) -> Result<MemberPromoteResponse> {
        Ok(self
            .inner
            .member_promote(xlineapi::MemberPromoteRequest { id })
            .await?
            .into_inner())
    }

    #[inline]
    async fn member_update<U, I>(&mut self, id: u64, peer_urls: I) -> Result<MemberUpdateResponse>
    where
        U: AsRef<str> + Send,
        I: IntoIterator<Item = U> + Send,
    {
        let peer_urls: Vec<String> = peer_urls
            .into_iter()
            .map(|s| s.as_ref().to_owned())
            .collect();

        Ok(self
            .inner
            .member_update(xlineapi::MemberUpdateRequest {
                id,
                peer_ur_ls: peer_urls,
            })
            .await?
            .into_inner())
    }

    #[inline]
    async fn member_list(&mut self, linearizable: bool) -> Result<MemberListResponse> {
        Ok(self
            .inner
            .member_list(xlineapi::MemberListRequest { linearizable })
            .await?
            .into_inner())
    }
}

#[async_trait]
impl XlineMembership for ClusterClient {
    #[inline]
    async fn change<I>(&self, changes: I) -> Result<()>
    where
        I: IntoIterator<Item = Change> + Send,
    {
        self.curp_client
            .change_membership(changes.into_iter().map(Into::into).collect())
            .await
            .map_err(Into::into)
    }

    #[inline]
    async fn wait_learner<Ids>(&self, node_ids: Ids) -> Result<WaitLearner>
    where
        Ids: IntoIterator<Item = u64> + Send,
    {
        let stream = self
            .curp_client
            .wait_learner(node_ids.into_iter().collect())
            .await?;
        let stream_mapped = Box::into_pin(stream).map(|r| r.map(Into::into).map_err(Into::into));

        Ok(WaitLearner::new(Box::pin(stream_mapped)))
    }
}

impl ClusterClient {
    /// Create a new cluster client
    #[inline]
    #[must_use]
    pub fn new(curp_client: Arc<CurpClient>, channel: Channel, token: Option<String>) -> Self {
        Self {
            inner: xlineapi::ClusterClient::new(AuthService::new(
                channel,
                token.and_then(|t| t.parse().ok().map(Arc::new)),
            )),
            curp_client,
        }
    }
}

impl std::fmt::Debug for ClusterClient {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterClient")
            .field("inner", &self.inner)
            .finish()
    }
}
