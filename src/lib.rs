use anyhow;
use atomic_refcell::AtomicRefCell;
use futures::{
    channel::{mpsc, oneshot},
    AsyncRead, AsyncReadExt, AsyncWrite, FutureExt, SinkExt, StreamExt,
};
use futures_timer::Delay;
use libp2p::{
    bytes::Bytes,
    core::{
        connection::ConnectionId,
        upgrade::{read_varint, write_one, UpgradeInfo},
        ConnectedPoint, Multiaddr,
    },
    swarm::{
        protocols_handler::{
            InboundUpgradeSend, IntoProtocolsHandler, OutboundUpgradeSend, ProtocolsHandlerUpgrErr,
        },
        KeepAlive, NetworkBehaviour, NetworkBehaviourAction, NotifyHandler, PollParameters,
        ProtocolsHandler, ProtocolsHandlerEvent, SubstreamProtocol,
    },
    InboundUpgrade, OutboundUpgrade, PeerId,
};
use log::{debug, error, info, warn};
use std::{
    collections::VecDeque,
    future::Future,
    io,
    ops::Add,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use thiserror::Error;

#[cfg(test)]
mod test;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct RpcConfig {
    pub inbound_rpc_timeout: Duration,
    pub max_concurrent_outbound_rpcs: u32,
    pub max_concurrent_inbound_rpcs: u32,
}

impl Default for RpcConfig {
    fn default() -> Self {
        Self {
            inbound_rpc_timeout: Duration::from_secs(10),
            max_concurrent_inbound_rpcs: 100,
            max_concurrent_outbound_rpcs: 100,
        }
    }
}

#[derive(Default)]
pub struct Rpc {
    config: RpcConfig,
    supported_protocols: Vec<ProtocolId>,
    pending_rpc_requests: VecDeque<(PeerId, RpcRequest)>,
    pending_rpc_notification: VecDeque<(PeerId, RpcNotification)>,
}

#[derive(Debug)]
pub enum RpcEvent {
    RpcNotification(PeerId, RpcNotification),
}

impl Rpc {
    pub fn new(config: RpcConfig) -> Self {
        Self {
            config,
            supported_protocols: Vec::new(),
            pending_rpc_requests: VecDeque::new(),
            pending_rpc_notification: VecDeque::new(),
        }
    }

    /// Add a rpc protocol to support.
    pub fn add_protocol(&mut self, protocol: ProtocolId) {
        self.supported_protocols.push(protocol);
    }

    /// Send rpc request to the `peer` with given `timeout`.
    pub fn send_rpc(
        &mut self,
        peer: PeerId,
        protocol_id: ProtocolId,
        data: Bytes,
        timeout: Duration,
    ) -> oneshot::Receiver<Result<Bytes, RpcError>> {
        let (tx, rx) = oneshot::channel();
        self.pending_rpc_requests.push_back((
            peer,
            RpcRequest::SendRpc(OutboundRpcRequest {
                protocol: protocol_id,
                data,
                timeout,
                res_tx: Arc::new(AtomicRefCell::new(Some(tx))),
            }),
        ));
        rx
    }
}

impl NetworkBehaviour for Rpc {
    type ProtocolsHandler = RpcProtocolHandler;
    type OutEvent = RpcEvent;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        let max_inflight_rpcs = self
            .config
            .max_concurrent_inbound_rpcs
            .checked_next_power_of_two()
            .expect("config error");
        RpcProtocolHandler::new(
            self.supported_protocols.clone(),
            max_inflight_rpcs as usize,
            self.config.inbound_rpc_timeout,
        )
    }

    fn addresses_of_peer(&mut self, _peer_id: &PeerId) -> Vec<Multiaddr> {
        Vec::new()
    }

    fn inject_connected(&mut self, _peer_id: PeerId, _endpoint: ConnectedPoint) {}

    fn inject_disconnected(&mut self, _peer_id: &PeerId, _endpoint: ConnectedPoint) {}

    fn inject_event(
        &mut self,
        peer_id: PeerId,
        _connection: ConnectionId,
        event: <<Self::ProtocolsHandler as IntoProtocolsHandler>::Handler as ProtocolsHandler>::OutEvent,
    ) {
        self.pending_rpc_notification.push_back((peer_id, event));
    }

    fn poll(&mut self, _cx: &mut Context, _params: &mut impl PollParameters)
-> Poll<NetworkBehaviourAction<<<Self::ProtocolsHandler as IntoProtocolsHandler>::Handler as ProtocolsHandler>::InEvent, Self::OutEvent>>{
        match self.pending_rpc_requests.pop_front() {
            Some((peer, pending_req)) => {
                return Poll::Ready(NetworkBehaviourAction::NotifyHandler {
                    peer_id: peer,
                    handler: NotifyHandler::Any,
                    event: pending_req,
                });
            }
            None => {}
        };

        match self.pending_rpc_notification.pop_front() {
            None => Poll::Pending,
            Some((peer, pending_rpc)) => {
                return Poll::Ready(NetworkBehaviourAction::GenerateEvent(
                    RpcEvent::RpcNotification(peer, pending_rpc),
                ));
            }
        }
    }
}

type ProtocolId = &'static str;

#[derive(Debug, Error)]
pub enum RpcError {
    #[error("IO error: {0}")]
    IoError(#[from] io::Error),
    #[error("Rpc timed out")]
    TimedOut,
    #[error("Received unexpected rpc response message; expected remote to half-close.")]
    UnexpectedRpcResponse,
}

/// A wrapper struct for an inbound rpc request and its associated context.
#[derive(Debug)]
pub struct InboundRpcRequest {
    /// Rpc method identifier, e.g., `/libra/rpc/0.1.0/consensus/0.1.0`. This is used
    /// to dispatch the request to the corresponding client handler.
    pub protocol: ProtocolId,
    /// The serialized request data received from the sender.
    pub data: Bytes,
    /// Channel over which the rpc response is sent from the upper client layer
    /// to the rpc layer.
    ///
    /// The rpc actor holds onto the receiving end of this channel, awaiting the
    /// response from the upper layer. If there is an error in, e.g.,
    /// deserializing the request, the upper layer should send an [`RpcError`]
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
    /// Rpc method identifier, e.g., `/libra/rpc/0.1.0/consensus/0.1.0`. This is the
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

/// Events sent from the [`NetworkProvider`](crate::interface::NetworkProvider)
/// actor to the [`Rpc`] actor.
#[derive(Debug, Clone)]
pub enum RpcRequest {
    /// Send an outbound rpc request to a remote peer.
    SendRpc(OutboundRpcRequest),
}

/// Events sent from the [`Rpc`] actor to the
/// [`NetworkProvider`](crate::interface::NetworkProvider) actor.
#[derive(Debug)]
pub enum RpcNotification {
    /// A new inbound rpc request has been received from a remote peer.
    RecvRpc(InboundRpcRequest),
}

pub struct RpcProtocolHandler {
    inbound_rpc_timeout: Duration,
    protocol_ids: Vec<ProtocolId>,
    pending_rpc_requests: VecDeque<RpcRequest>,
    pending_rpc_notifications_sender: mpsc::Sender<RpcNotification>,
    pending_rpc_notifications_receiver: mpsc::Receiver<RpcNotification>,
}

impl RpcProtocolHandler {
    pub fn new(
        protocol_ids: Vec<ProtocolId>,
        max_inflight_inbound_rpc_requests: usize,
        inbound_rpc_timeout: Duration,
    ) -> Self {
        let (tx, rx) = mpsc::channel(max_inflight_inbound_rpc_requests);
        Self {
            inbound_rpc_timeout,
            protocol_ids,
            pending_rpc_requests: VecDeque::new(),
            pending_rpc_notifications_sender: tx,
            pending_rpc_notifications_receiver: rx,
        }
    }
}

impl ProtocolsHandler for RpcProtocolHandler {
    type InEvent = RpcRequest;
    type OutEvent = RpcNotification;
    type Error = io::Error;
    type InboundProtocol = RpcInBoundUpgrade;
    type OutboundProtocol = RpcOutBoundUpgrade;
    type OutboundOpenInfo = ();

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol> {
        SubstreamProtocol::new(RpcInBoundUpgrade::new(
            self.inbound_rpc_timeout,
            self.protocol_ids.clone(),
            self.pending_rpc_notifications_sender.clone(),
        ))
    }

    fn inject_fully_negotiated_inbound(
        &mut self,
        _protocol: <Self::InboundProtocol as InboundUpgradeSend>::Output,
    ) {
    }

    fn inject_fully_negotiated_outbound(
        &mut self,
        _protocol: <Self::OutboundProtocol as OutboundUpgradeSend>::Output,
        _info: Self::OutboundOpenInfo,
    ) {
    }

    fn inject_event(&mut self, event: Self::InEvent) {
        self.pending_rpc_requests.push_back(event);
    }

    fn inject_dial_upgrade_error(
        &mut self,
        _info: Self::OutboundOpenInfo,
        _error: ProtocolsHandlerUpgrErr<<Self::OutboundProtocol as OutboundUpgradeSend>::Error>,
    ) {
    }

    fn connection_keep_alive(&self) -> KeepAlive {
        KeepAlive::Yes
    }

    fn poll(
        &mut self,
        cx: &mut Context,
    ) -> Poll<
        ProtocolsHandlerEvent<
            Self::OutboundProtocol,
            Self::OutboundOpenInfo,
            Self::OutEvent,
            Self::Error,
        >,
    > {
        // Poll out event first, to let upper do it work.
        match self.pending_rpc_notifications_receiver.poll_next_unpin(cx) {
            Poll::Pending => {}
            Poll::Ready(Some(evt)) => return Poll::Ready(ProtocolsHandlerEvent::Custom(evt)),
            Poll::Ready(None) => unreachable!(), // This should not happen, because this always has a sender.
        };

        match self.pending_rpc_requests.pop_front() {
            None => Poll::Pending,
            Some(req) => match req {
                RpcRequest::SendRpc(rpc_request) => {
                    let rpc_timeout = rpc_request.timeout;
                    // protocol timeout should be larger to let rpc do it work.
                    let protocol_timeout = rpc_timeout.add(Duration::from_secs(10));
                    Poll::Ready(ProtocolsHandlerEvent::OutboundSubstreamRequest {
                        protocol: SubstreamProtocol::new(RpcOutBoundUpgrade::new(rpc_request))
                            .with_timeout(protocol_timeout),
                        info: (),
                    })
                }
            },
        }
    }
}
#[derive(Debug, Error)]
pub enum RpcOutboundUpgradeError {
    #[error("Application layer unexpectedly dropped response channel")]
    UnexpectedResponseChannelCancel,
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
    protocol_event_sender: mpsc::Sender<RpcNotification>,
}
impl RpcInBoundUpgrade {
    pub fn new(
        inbound_timeout: Duration,
        supported_protocols: Vec<ProtocolId>,
        protocol_event_sender: mpsc::Sender<RpcNotification>,
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
            let notification = RpcNotification::RecvRpc(InboundRpcRequest {
                protocol: info,
                data: Bytes::from(buf),
                res_tx,
            });
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
