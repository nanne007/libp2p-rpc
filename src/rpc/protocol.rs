use crate::rpc::error::{InboundUpgradeError, RpcError, RpcOutboundUpgradeError};
use anyhow;
use atomic_refcell::AtomicRefCell;
use futures::{
    channel::{mpsc, oneshot},
    AsyncRead, AsyncReadExt, AsyncWrite, FutureExt, SinkExt,
};
use futures_timer::Delay;
use libp2p::{
    bytes::Bytes,
    core::upgrade::{read_varint, write_one, UpgradeInfo},
    InboundUpgrade, OutboundUpgrade,
};
use log::{debug, error, info, warn};
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

pub type ProtocolId = &'static str;

/// A wrapper struct for an inbound rpc request and its associated context.
#[derive(Debug)]
pub struct InboundRpcRequest {
    /// Rpc method identifier, e.g., `/rpc/0.1.0/get_block/0.1.0`. This is used
    /// to dispatch the request to the corresponding client handler.
    pub protocol: ProtocolId,
    /// The serialized request data received from the sender.
    pub data: Bytes,
    /// Channel over which the rpc response is sent from the upper client layer
    /// to the rpc layer.
    ///
    /// The rpc module holds onto the receiving end of this channel, awaiting the
    /// response from the upper layer. If there is an error in, e.g.,
    /// deserializing the request, the upper layer should send an [`anyhow::Error`]
    /// down the channel to signify that there was an error while handling this
    /// rpc request. Currently, we just log these errors and drop the substream;
    /// in the future, we will send an error response to the peer and/or log any
    /// malicious behaviour.
    ///
    /// The upper client layer should be prepared for `res_tx` to be potentially
    /// disconnected when trying to send their response, as the rpc call might
    /// have timed out while handling the request.
    pub res_tx: oneshot::Sender<Result<Bytes, anyhow::Error>>,
}

/// A wrapper struct for an outbound rpc request and its associated context.
#[derive(Debug, Clone)]
pub struct OutboundRpcRequest {
    /// Rpc method identifier, e.g., `/rpc/0.1.0/get_block/0.1.0`. This is the
    /// protocol we will negotiate our outbound substream to.
    pub protocol: ProtocolId,
    /// The serialized request data to be sent to the receiver.
    pub data: Bytes,
    /// Channel over which the rpc response is sent from the rpc layer to the
    /// upper client layer.
    ///
    /// If there is an error while performing the rpc protocol, e.g., the remote
    /// peer drops the connection, we will send an [`RpcError`] over the channel.
    pub res_tx: Arc<AtomicRefCell<Option<oneshot::Sender<Result<Bytes, RpcError>>>>>,
    /// The timeout duration for the entire rpc call. If the timeout elapses, the
    /// rpc layer will send an [`RpcError::TimedOut`] error over the
    /// `res_tx` channel to the upper client layer.
    pub timeout: Duration,
}

pub struct RpcOutBoundUpgrade {
    req: OutboundRpcRequest,
}
impl RpcOutBoundUpgrade {
    pub fn new(rpc_request: OutboundRpcRequest) -> Self {
        Self { req: rpc_request }
    }
}

impl UpgradeInfo for RpcOutBoundUpgrade {
    type Info = ProtocolId;
    type InfoIter = Vec<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        vec![self.req.protocol]
    }
}

impl<C> OutboundUpgrade<C> for RpcOutBoundUpgrade
where
    C: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Output = ();
    type Error = RpcOutboundUpgradeError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Output, Self::Error>> + Send>>;

    fn upgrade_outbound(self, mut socket: C, _info: Self::Info) -> Self::Future {
        let OutboundRpcRequest {
            res_tx,
            data,
            timeout,
            ..
        } = self.req;
        // NOTICE: we know one rpc request is only handled by one connection.
        // So, no one borrow_mut it and take the sender.
        let res_tx = res_tx.borrow_mut().take();
        assert!(res_tx.is_some());
        let mut res_tx = res_tx.unwrap();

        let mut rpc_future = async move {
            // Write request data and close.
            // We won't send anything else on this substream, so we can half-close our
            // output side.
            write_one(&mut socket, data.as_ref()).await?;

            // then read response
            let len = read_varint(&mut socket).await?;
            let mut buf = vec![0; len];
            socket.read_exact(&mut buf).await?;
            let resp = Bytes::from(buf);
            // Wait for listener to half-close their side.
            let mut buf = vec![0; 1];
            match socket.read(&mut buf).await {
                Ok(left) => {
                    if left > 0 {
                        return Err(RpcError::UnexpectedRpcResponse);
                    }
                }
                Err(err) => {
                    info!("after read request, try read next should err: {:?}", &err);
                }
            }
            Ok(resp)
        }
        .boxed()
        .fuse();
        let mut timeout = Delay::new(timeout).fuse();

        Box::pin(async move {
            // If the rpc client drops their oneshot receiver, this future should
            // cancel the request.
            let mut f_rpc_cancel = futures::future::poll_fn(|ctx| res_tx.poll_canceled(ctx)).fuse();

            let result: Option<Result<Bytes, RpcError>> = futures::select! {
                rpc_result = rpc_future => {
                    Some(rpc_result)
                }
                _timeout = timeout => {
                    Some(Err(RpcError::TimedOut))
                }
                _cancel = f_rpc_cancel => {
                    debug!("Rpc client canceled outbound rpc call");
                    None
                }
            };
            match result {
                Some(resp) => match res_tx.send(resp) {
                    Ok(_) => Ok(()),
                    Err(_) => Err(RpcOutboundUpgradeError::UnexpectedResponseChannelCancel),
                },
                None => Err(RpcOutboundUpgradeError::UnexpectedResponseChannelCancel),
            }
        })
    }
}

pub struct RpcInBoundUpgrade {
    supported_protocols: Vec<ProtocolId>,
    inbound_timeout: Duration,
    protocol_event_sender: mpsc::Sender<InboundRpcRequest>,
}
impl RpcInBoundUpgrade {
    pub fn new(
        inbound_timeout: Duration,
        supported_protocols: Vec<ProtocolId>,
        protocol_event_sender: mpsc::Sender<InboundRpcRequest>,
    ) -> Self {
        Self {
            supported_protocols,
            inbound_timeout,
            protocol_event_sender,
        }
    }
}

impl UpgradeInfo for RpcInBoundUpgrade {
    type Info = ProtocolId;
    type InfoIter = Vec<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        self.supported_protocols.clone()
    }
}

impl<C> InboundUpgrade<C> for RpcInBoundUpgrade
where
    C: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    /// Output after the upgrade has been successfully negotiated and the handshake performed.
    type Output = ();
    /// Possible error during the handshake.
    type Error = InboundUpgradeError;
    /// Future that performs the handshake with the remote.
    type Future = Pin<Box<dyn Future<Output = Result<Self::Output, Self::Error>> + Send>>;

    /// After we have determined that the remote supports one of the protocols we support, this
    /// method is called to start the handshake.
    ///
    /// The `info` is the identifier of the protocol, as produced by `protocol_info`.
    fn upgrade_inbound(mut self, mut socket: C, info: Self::Info) -> Self::Future {
        Box::pin(async move {
            let len = read_varint(&mut socket).await?;
            let mut buf = vec![0; len];
            socket.read_exact(&mut buf).await?;

            // Build the event and context we push up to upper layers for handling.
            let (res_tx, res_rx) = oneshot::channel();
            let notification = InboundRpcRequest {
                protocol: info,
                data: Bytes::from(buf),
                res_tx,
            };
            if let Err(send_err) = self.protocol_event_sender.send(notification).await {
                if send_err.is_disconnected() {
                    error!(target:"libp2p-rpc", "rpc behavior is dropped! This should not happen");
                    return Err(InboundUpgradeError::BehaviourError);
                } else if send_err.is_full() {
                    warn!(target:"libp2p-rpc", "too many concurrent inbound rpc request, maybe you should raise the rpc inbound concurrency limit");
                    return Err(InboundUpgradeError::TooManyInboundRpcRequest);
                }
            }
            let mut buf = vec![0; 1];
            match socket.read(&mut buf).await {
                Ok(left) => {
                    if left > 0 {
                        return Err(InboundUpgradeError::UnexpectedRpcRequest);
                    }
                }
                Err(err) => {
                    info!("after read request, try read next should err: {:?}", &err);
                }
            }
            let resp_data = match futures::future::select(res_rx, Delay::new(self.inbound_timeout))
                .await
            {
                futures::future::Either::Left((data, _)) => data??,
                futures::future::Either::Right((_, _)) => {
                    warn!(target: "libp2p-rpc", "application handle inbound rpc request timeouted");
                    return Err(InboundUpgradeError::ApplicationTimedOut);
                }
            };

            // We won't send anything else on this substream, so we can half-close
            // our output. The initiator will have also half-closed their side before
            // this, so this should gracefully shutdown the socket.
            write_one(&mut socket, resp_data).await?;
            Ok(())
        })
    }
}
