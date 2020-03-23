use anyhow;
use futures::channel::oneshot;
use std::io;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RpcError {
    #[error("IO error: {0}")]
    IoError(#[from] io::Error),
    #[error("Rpc timed out")]
    TimedOut,
    #[error("Received unexpected rpc response message; expected remote to half-close.")]
    UnexpectedRpcResponse,
}

#[derive(Debug, Error)]
pub enum RpcOutboundUpgradeError {
    #[error("Application layer unexpectedly dropped response channel")]
    UnexpectedResponseChannelCancel,
}

#[derive(Debug, Error)]
pub enum InboundUpgradeError {
    #[error("IO error: {0}")]
    IoError(#[from] io::Error),
    #[error("Too many inbound request.")]
    TooManyInboundRpcRequest,
    #[error("Received unexpected rpc request message; expected remote to half-close.")]
    UnexpectedRpcRequest,
    #[error("behaviour have behave badly.")]
    BehaviourError,
    #[error("Error in application layer handling rpc request: {0:?}")]
    ApplicationError(#[from] anyhow::Error),
    #[error("application layer timeout handle rpc request")]
    ApplicationTimedOut,
    #[error("Application layer unexpectedly dropped response channel")]
    UnexpectedResponseChannelCancel,
}
impl From<oneshot::Canceled> for InboundUpgradeError {
    fn from(_: oneshot::Canceled) -> Self {
        InboundUpgradeError::UnexpectedResponseChannelCancel
    }
}
