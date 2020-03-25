use crate::cast::protocol::{BytesFramed, CastInboundUpgrade, CastOutboundUpgrade};
use crate::cast::{Message, ProtocolId};
use futures::task::{Context, Poll};
use futures::{channel::mpsc, SinkExt, StreamExt, TryStreamExt};
use libp2p::bytes::Bytes;
use libp2p::swarm::{
    protocols_handler::{InboundUpgradeSend, OutboundUpgradeSend, ProtocolsHandlerUpgrErr},
    KeepAlive, ProtocolsHandler, ProtocolsHandlerEvent, SubstreamProtocol,
};
use log::warn;
use std::collections::{HashMap, VecDeque};
use std::future::Future;

pub struct CastProtocolHandler {
    pending_messages: VecDeque<Message>,
    outbound_message_frame: HashMap<ProtocolId, BytesFramed>,
    inbound_message_streams: HashMap<ProtocolId, BytesFramed>,
}

impl ProtocolsHandler for CastProtocolHandler {
    type InEvent = super::Message;
    type OutEvent = super::Message;
    type Error = ();
    type InboundProtocol = CastInboundUpgrade;
    type OutboundProtocol = CastOutboundUpgrade;
    type OutboundOpenInfo = Bytes;

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol> {
        let upgrade = CastInboundUpgrade::new(vec![]);
        SubstreamProtocol::new(upgrade)
    }

    fn inject_fully_negotiated_inbound(
        &mut self,
        protocol: <Self::InboundProtocol as InboundUpgradeSend>::Output,
    ) {
        let (protocol_id, framed) = protocol;
        if let Some(old) = self.inbound_message_streams.insert(protocol_id, framed) {
            warn!("an old inbound stream exists, drop it now");
        }
    }

    fn inject_fully_negotiated_outbound(
        &mut self,
        protocol: (ProtocolId, BytesFramed),
        info: Self::OutboundOpenInfo,
    ) {
        let (protocol_id, framed) = protocol;
        if let Some(old) = self
            .outbound_message_frame
            .insert(protocol_id.clone(), framed)
        {
            warn!("an old outbound stream exists, drop it now");
        }
        self.pending_messages.push_front(Message {
            protocol: protocol_id,
            mdata: info,
        });
    }

    fn inject_event(&mut self, event: Self::InEvent) {
        self.pending_messages.push_back(event);
    }

    fn inject_dial_upgrade_error(
        &mut self,
        info: Self::OutboundOpenInfo,
        error: ProtocolsHandlerUpgrErr<<Self::OutboundProtocol as OutboundUpgradeSend>::Error>,
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
        if let Some(message) = self.pending_messages.pop_front() {
            let Message { protocol, mdata } = message;
            if !self.outbound_message_frame.contains_key(&protocol) {
                return Poll::Ready(ProtocolsHandlerEvent::OutboundSubstreamRequest {
                    protocol: SubstreamProtocol::new(CastOutboundUpgrade::new(protocol)),
                    info: mdata,
                });
            } else {
                let protocol_sink = self.outbound_message_frame.get_mut(&protocol).unwrap();

                let send_future = protocol_sink.send(mdata);
                futures::pin_mut!(send_future);
                Future::poll(send_future, tx)
            }
        }

        let mut stream_to_remove = vec![];
        let mut inbound_msg = None;
        for (protocol_id, stream) in &mut self.inbound_message_streams {
            match futures::ready!(stream.poll_next_unpin(cx)) {
                None => {
                    debug!("substream of {} is closed", protocol_id);
                    stream_to_remove.push(protocol_id);
                }
                Some(Err(e)) => {
                    warn!("substream of {} error: {}", protocol_id, e);
                    stream_to_remove.push(protocol_id);
                }
                Some(Ok(data)) => {
                    inbound_msg = Some(Message {
                        protocol: protocol_id.clone(),
                        mdata: data,
                    });
                    break;
                }
            }
        }
        for protocol in stream_to_remove {
            self.inbound_message_streams.remove(protocol);
        }
        if let Some(msg) = inbound_msg {
            return Poll::Ready(ProtocolsHandlerEvent::Custom(msg));
        }
        unimplemented!()
    }
}
