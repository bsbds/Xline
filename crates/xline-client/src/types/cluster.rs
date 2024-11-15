use std::{
    pin::Pin,
    task::{Context, Poll},
};

use curp::rpc::WaitLearnerResponse;
use futures::Stream;

use crate::error::Result;

/// Represents a change in cluster membership.
#[allow(variant_size_differences)]
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Change {
    /// Adds a new learner.
    Add(Node),
    /// Removes a learner by its id.
    Remove(u64),
    /// Promotes a learner to voter
    Promote(u64),
    /// Demotes a voter to learner.
    Demote(u64),
}

impl From<Change> for curp::rpc::Change {
    #[inline]
    fn from(change: Change) -> Self {
        match change {
            Change::Add(node) => curp::rpc::Change::Add(node.into()),
            Change::Remove(id) => curp::rpc::Change::Remove(id),
            Change::Promote(id) => curp::rpc::Change::Promote(id),
            Change::Demote(id) => curp::rpc::Change::Demote(id),
        }
    }
}

/// Represents a node in the cluster with its associated metadata.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Node {
    /// The id of the node
    pub node_id: u64,
    /// Name of the node.
    pub name: String,
    /// List of URLs used for peer-to-peer communication.
    pub peer_urls: Vec<String>,
    /// List of URLs used for client communication.
    pub client_urls: Vec<String>,
}

impl Node {
    /// Creates a new `Node`
    #[inline]
    #[must_use]
    pub fn new<N, A, AS>(id: u64, name: N, peer_urls: AS, client_urls: AS) -> Self
    where
        N: AsRef<str>,
        A: AsRef<str>,
        AS: IntoIterator<Item = A>,
    {
        Self {
            node_id: id,
            name: name.as_ref().to_owned(),
            peer_urls: peer_urls
                .into_iter()
                .map(|s| s.as_ref().to_owned())
                .collect(),
            client_urls: client_urls
                .into_iter()
                .map(|s| s.as_ref().to_owned())
                .collect(),
        }
    }
}

impl From<Node> for curp::rpc::Node {
    #[inline]
    fn from(node: Node) -> Self {
        let meta = curp::rpc::NodeMetadata {
            name: node.name,
            peer_urls: node.peer_urls,
            client_urls: node.client_urls,
        };
        Self {
            node_id: node.node_id,
            meta: Some(meta),
        }
    }
}

#[allow(clippy::exhaustive_enums)] // only two states
#[derive(Debug, Clone, Copy)]
/// Represents the state of a learner
pub enum LearnerStatus {
    /// The learner node is pending and not yet ready.
    Pending {
        /// The id of the node
        node_id: u64,
        /// The current replicated log index of the node
        index: u64,
    },
    /// The learner node is up-to-date.
    Ready,
}

impl From<WaitLearnerResponse> for LearnerStatus {
    #[inline]
    fn from(resp: WaitLearnerResponse) -> Self {
        if resp.current_idx == resp.latest_idx {
            return LearnerStatus::Ready;
        }
        LearnerStatus::Pending {
            node_id: resp.node_id,
            index: resp.current_idx,
        }
    }
}

#[allow(missing_debug_implementations)]
/// A stream that waits for learner status updates
pub struct WaitLearner {
    /// Inner stream
    pub(super) inner: Pin<Box<dyn Stream<Item = Result<LearnerStatus>> + Send>>,
}

impl WaitLearner {
    /// Creates a new `WaitLearner`
    pub(crate) fn new(inner: Pin<Box<dyn Stream<Item = Result<LearnerStatus>> + Send>>) -> Self {
        Self { inner }
    }
}

impl Stream for WaitLearner {
    type Item = Result<LearnerStatus>;

    #[inline]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}
