// use crate::rpc::{
//     error::RpcError,
//     handler::{RpcNotification, RpcProtocolHandler, RpcRequest},
//     protocol::{OutboundRpcRequest, ProtocolId},
// };
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
use std::fmt::Debug;
use std::{
    collections::VecDeque,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

pub mod error;
pub mod handler;
pub mod protocol;

pub type ProtocolId = String;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DirectSendRequest {
    /// A request to send out a message.
    SendMessage(Message),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DirectSendNotification {
    /// A notification that a DirectSend message is received.
    RecvMessage(Message),
}

#[derive(Clone, Eq, PartialEq)]
pub struct Message {
    /// Message type.
    pub protocol: ProtocolId,
    /// Serialized message data.
    pub mdata: Bytes,
}

impl Debug for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mdata_str = if self.mdata.len() <= 10 {
            format!("{:?}", self.mdata)
        } else {
            format!("{:?}...", self.mdata.slice(..10))
        };
        write!(
            f,
            "Message {{ protocol: {:?}, mdata: {} }}",
            self.protocol, mdata_str
        )
    }
}

/// cast is
pub struct Cast {}

impl NetworkBehaviour for Cast {
    type ProtocolsHandler = handler::CastProtocolHandler;
    type OutEvent = DirectSendNotification;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        unimplemented!()
    }

    fn addresses_of_peer(&mut self, peer_id: &PeerId) -> Vec<Multiaddr> {
        unimplemented!()
    }

    fn inject_connected(&mut self, peer_id: PeerId, endpoint: ConnectedPoint) {
        unimplemented!()
    }

    fn inject_disconnected(&mut self, peer_id: &PeerId, endpoint: ConnectedPoint) {
        unimplemented!()
    }

    fn inject_event(
        &mut self,
        peer_id: PeerId,
        connection: ConnectionId,
        event: <<Self::ProtocolsHandler as IntoProtocolsHandler>::Handler as ProtocolsHandler>::OutEvent,
    ) {
        unimplemented!()
    }

    fn poll(&mut self, cx: &mut Context, params: &mut impl PollParameters)
-> Poll<NetworkBehaviourAction<<<Self::ProtocolsHandler as IntoProtocolsHandler>::Handler as ProtocolsHandler>::InEvent, Self::OutEvent>>{
        todo!()
    }
}
