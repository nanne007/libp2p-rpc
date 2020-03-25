use crate::rpc::{
    error::RpcError,
    handler::{RpcNotification, RpcProtocolHandler, RpcRequest},
    protocol::{OutboundRpcRequest, ProtocolId},
};
use atomic_refcell::AtomicRefCell;
use futures::channel::oneshot;
use libp2p::{
    bytes::Bytes,
    core::{connection::ConnectionId, ConnectedPoint, Multiaddr},
    swarm::{
        protocols_handler::IntoProtocolsHandler, NetworkBehaviour, NetworkBehaviourAction,
        NotifyHandler, PollParameters, ProtocolsHandler,
    },
    PeerId,
};
use std::{
    collections::VecDeque,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

pub mod error;
pub mod handler;
pub mod protocol;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct RpcConfig {
    /// The timeout duration for application layer to handle inbound rpc calls.
    pub inbound_rpc_timeout: Duration,
    /// The maximum number of concurrent outbound rpc requests per peer that we will
    /// service before back-pressure kicks in.
    pub max_concurrent_outbound_rpcs: u32,
    /// The maximum number of concurrent inbound rpc requests per peer that we will
    /// service before back-pressure kicks in.    
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
    #[allow(dead_code)]
    pub fn new(config: RpcConfig) -> Self {
        Self {
            config,
            supported_protocols: Vec::new(),
            pending_rpc_requests: VecDeque::new(),
            pending_rpc_notification: VecDeque::new(),
        }
    }

    /// Add a rpc protocol to support.
    #[allow(dead_code)]
    pub fn add_protocol(&mut self, protocol: ProtocolId) {
        self.supported_protocols.push(protocol);
    }

    /// Send rpc request to the `peer` with given `timeout`.
    #[allow(dead_code)]
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
        RpcProtocolHandler::new(
            self.supported_protocols.clone(),
            self.config.inbound_rpc_timeout,
            self.config.max_concurrent_inbound_rpcs,
            self.config.max_concurrent_outbound_rpcs,
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
