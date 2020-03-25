use crate::rpc::protocol::{
    InboundRpcRequest, OutboundRpcRequest, ProtocolId, RpcInBoundUpgrade, RpcOutBoundUpgrade,
};
use futures::{channel::mpsc, StreamExt};
use libp2p::swarm::{
    protocols_handler::{InboundUpgradeSend, OutboundUpgradeSend, ProtocolsHandlerUpgrErr},
    KeepAlive, ProtocolsHandler, ProtocolsHandlerEvent, SubstreamProtocol,
};
use std::{
    collections::VecDeque,
    io,
    ops::Add,
    task::{Context, Poll},
    time::Duration,
};
/// Events sent from application layer.
#[derive(Debug, Clone)]
pub enum RpcRequest {
    /// Send an outbound rpc request to a remote peer.
    SendRpc(OutboundRpcRequest),
}

/// Events received from peer.
#[derive(Debug)]
pub enum RpcNotification {
    /// A new inbound rpc request has been received from a remote peer.
    RecvRpc(InboundRpcRequest),
}

pub struct RpcProtocolHandler {
    inbound_rpc_timeout: Duration,
    // how many outbound rpc request can send.
    available_outbound_rpc_nums: u32,

    protocol_ids: Vec<ProtocolId>,
    pending_rpc_requests: VecDeque<RpcRequest>,
    pending_rpc_notifications_sender: mpsc::Sender<InboundRpcRequest>,
    pending_rpc_notifications_receiver: mpsc::Receiver<InboundRpcRequest>,
}

impl RpcProtocolHandler {
    pub fn new(
        protocol_ids: Vec<ProtocolId>,
        inbound_rpc_timeout: Duration,
        max_concurrent_inbound_rpcs: u32,
        max_concurrent_outbound_rpcs: u32,
    ) -> Self {
        let (tx, rx) = mpsc::channel(max_concurrent_inbound_rpcs as usize);
        Self {
            inbound_rpc_timeout,
            available_outbound_rpc_nums: max_concurrent_outbound_rpcs,
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
        self.available_outbound_rpc_nums += 1;
    }

    fn inject_event(&mut self, event: Self::InEvent) {
        self.pending_rpc_requests.push_back(event);
    }

    fn inject_dial_upgrade_error(
        &mut self,
        _info: Self::OutboundOpenInfo,
        _error: ProtocolsHandlerUpgrErr<<Self::OutboundProtocol as OutboundUpgradeSend>::Error>,
    ) {
        self.available_outbound_rpc_nums += 1;
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
            Poll::Ready(Some(evt)) => {
                return Poll::Ready(ProtocolsHandlerEvent::Custom(RpcNotification::RecvRpc(evt)))
            }
            Poll::Ready(None) => unreachable!(), // This should not happen, because this always has a sender.
        };

        if self.available_outbound_rpc_nums == 0 {
            return Poll::Pending;
        }
        match self.pending_rpc_requests.pop_front() {
            None => Poll::Pending,
            Some(req) => match req {
                RpcRequest::SendRpc(rpc_request) => {
                    self.available_outbound_rpc_nums -= 1;
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
