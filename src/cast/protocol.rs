use crate::cast::ProtocolId;
use futures::stream::Forward;
use futures::{
    channel::{mpsc, oneshot},
    AsyncRead, AsyncReadExt, AsyncWrite, FutureExt, SinkExt, StreamExt,
};
use futures_codec::{Framed, LengthCodec};
use futures_timer::Delay;
use libp2p::{
    bytes::Bytes,
    core::upgrade::{read_varint, write_one, UpgradeInfo},
    swarm::{
        protocols_handler::{InboundUpgradeSend, OutboundUpgradeSend, UpgradeInfoSend},
        NegotiatedSubstream,
    },
    InboundUpgrade, OutboundUpgrade,
};
use log::{debug, error, info, warn};
use std::process::Output;
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

pub struct CastOutboundUpgrade {
    protocol: ProtocolId,
}

impl CastOutboundUpgrade {
    pub fn new(protocol: ProtocolId) -> Self {
        Self { protocol }
    }
}

impl UpgradeInfoSend for CastOutboundUpgrade {
    type Info = ProtocolId;
    type InfoIter = Vec<ProtocolId>;

    fn protocol_info(&self) -> Self::InfoIter {
        vec![self.protocol.clone()]
    }
}

impl OutboundUpgradeSend for CastOutboundUpgrade {
    type Output = (ProtocolId, BytesFramed);
    type Error = ();
    type Future = futures::future::Ready<Self::Output>;

    fn upgrade_outbound(self, socket: NegotiatedSubstream, info: Self::Info) -> Self::Future {
        // let framed_write = Framed::new(socket, LengthCodec);
        // Box::pin(self.mdata_receiver.forward(framed_write))
        let framed = Framed::new(socket, LengthCodec);
        futures::future::ok::<Self::Output, Self::Error>((info, framed))
    }
}

pub struct CastInboundUpgrade {
    supported_protocol_ids: Vec<ProtocolId>,
}
impl CastInboundUpgrade {
    pub fn new(supported_protocol_ids: Vec<ProtocolId>) -> Self {
        Self {
            supported_protocol_ids,
        }
    }
}

impl UpgradeInfoSend for CastInboundUpgrade {
    type Info = ProtocolId;
    type InfoIter = Vec<ProtocolId>;

    fn protocol_info(&self) -> Self::InfoIter {
        self.supported_protocol_ids.clone()
    }
}

impl InboundUpgradeSend for CastInboundUpgrade {
    type Output = (ProtocolId, BytesFramed);
    type Error = ();
    type Future = futures::future::Ready<Self::Output>;

    fn upgrade_inbound(self, socket: NegotiatedSubstream, info: Self::Info) -> Self::Future {
        let framed = Framed::new(socket, LengthCodec);
        futures::future::ok::<Self::Output, Self::Error>((info, framed))
    }
}
pub type BytesFramed = Framed<NegotiatedSubstream, LengthCodec>;
