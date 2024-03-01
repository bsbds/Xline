use std::sync::atomic::{AtomicBool, Ordering};

use curp_external_api::cmd::Command;
use tokio::sync::mpsc::Sender;
use tokio_stream::StreamExt;
use tonic::Status;

use crate::{
    error::ClientError,
    rpc::{OpResponse, ProposeResponse, ResponseOp, SyncedResponse},
};

/// The response sender
#[derive(Debug)]
pub(super) struct ResponseSender {
    /// The stream sender
    tx: Sender<Result<OpResponse, Status>>,
    /// Whether the command will be speculatively executed
    conflict: AtomicBool,
}

impl ResponseSender {
    /// Creates a new `ResponseSender`
    pub(super) fn new(tx: Sender<Result<OpResponse, Status>>) -> ResponseSender {
        ResponseSender {
            tx,
            conflict: AtomicBool::new(false),
        }
    }

    /// Gets whether the command associated with this sender will be
    /// speculatively executed
    pub(super) fn is_conflict(&self) -> bool {
        self.conflict.load(Ordering::Acquire)
    }

    /// Sets the the command associated with this sender will be
    /// speculatively executed    
    pub(super) fn set_conflict(&self, conflict: bool) {
        self.conflict.store(conflict, Ordering::Release);
    }

    /// Sends propose result
    pub(super) fn send_propose(&self, resp: ProposeResponse) {
        let resp = OpResponse {
            op: Some(ResponseOp::Propose(resp)),
        };
        // Ignore the result because the client might close the receiving stream
        let _ignore = self.tx.try_send(Ok(resp));
    }

    /// Sends after sync result
    pub(super) fn send_synced(&self, resp: SyncedResponse) {
        let resp = OpResponse {
            op: Some(ResponseOp::Synced(resp)),
        };
        // Ignore the result because the client might close the receiving stream
        let _ignore = self.tx.try_send(Ok(resp));
    }
}

pub(crate) struct ResponseReceiver {
    resp_stream: tonic::Streaming<OpResponse>,
}

impl ResponseReceiver {
    pub(crate) fn new(resp_stream: tonic::Streaming<OpResponse>) -> Self {
        Self { resp_stream }
    }

    pub(crate) async fn recv_er<C: Command>(&mut self) -> Result<(C::ER, bool), ClientError<C>> {
        let op = self.recv_resp().await?;
        // TODO: replace unreachable with error
        let ResponseOp::Propose(resp)  = op else { unreachable!("op: {op:?}") };

        let conflict = resp.conflict;
        let er = resp.map_result::<C, _, Result<<C as Command>::ER, ClientError<C>>>(|res| {
            res.unwrap_or_else(|| unreachable!())
                .map_err(|e| ClientError::CommandError::<C>(e))
        })??;

        Ok((er, conflict))
    }

    pub(crate) async fn recv_asr<C: Command>(&mut self) -> Result<C::ASR, ClientError<C>> {
        let op = self.recv_resp().await?;
        // TODO: replace unreachable with error
        let ResponseOp::Synced(resp)  = op else { unreachable!() };

        resp.map_result::<C, _, Result<<C as Command>::ASR, ClientError<C>>>(|res| {
            res.unwrap_or_else(|| unreachable!())
                .map_err(|e| ClientError::CommandError::<C>(e))
        })?
        .map_err(Into::into)
    }

    async fn recv_resp<C: Command>(&mut self) -> Result<ResponseOp, ClientError<C>> {
        let resp = self
            .resp_stream
            .next()
            .await
            .ok_or(ClientError::InternalError(
                "stream reaches on an end".to_owned(),
            ))??;
        Ok(resp
            .op
            .unwrap_or_else(|| unreachable!("op should always exist")))
    }
}
