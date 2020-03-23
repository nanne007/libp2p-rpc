use crate::rpc::{handler::RpcNotification, Rpc, RpcConfig, RpcEvent};
use futures::{pin_mut, prelude::*};
use libp2p::{
    bytes::Bytes,
    core::{identity, muxing::StreamMuxer, upgrade, PeerId, Transport},
    swarm::{Swarm, SwarmEvent},
};
use libp2p_mplex::MplexConfig;
use libp2p_secio::SecioConfig;
use libp2p_tcp::TcpConfig;
use log::info;
use std::{fmt, io, time::Duration};

fn transport() -> (
    identity::PublicKey,
    impl Transport<
            Output = (
                PeerId,
                impl StreamMuxer<
                    Substream = impl Send,
                    OutboundSubstream = impl Send,
                    Error = impl Into<io::Error>,
                >,
            ),
            Listener = impl Send,
            ListenerUpgrade = impl Send,
            Dial = impl Send,
            Error = impl fmt::Debug,
        > + Clone,
) {
    let id_keys = identity::Keypair::generate_ed25519();
    let pubkey = id_keys.public();
    let transport = TcpConfig::new()
        .nodelay(true)
        .upgrade(upgrade::Version::V1)
        .authenticate(SecioConfig::new(id_keys))
        .multiplex(MplexConfig::new());
    (pubkey, transport)
}

const TEST_PROTOCOL: &'static str = "/rpc/1.0.0/ping/1.0.0";
#[test]
fn test_rpc_send_and_receive() {
    env_logger::init();
    let (mut swarm1, _pubkey1) = {
        let (pubkey, transport) = transport();
        let mut rpc = Rpc::new(RpcConfig::default());
        rpc.add_protocol(TEST_PROTOCOL);
        let swarm = Swarm::new(transport, rpc, pubkey.clone().into_peer_id());
        (swarm, pubkey)
    };
    let peer1 = Swarm::local_peer_id(&swarm1).clone();
    let (mut swarm2, _pubkey2) = {
        let (pubkey, transport) = transport();
        let mut rpc = Rpc::new(RpcConfig::default());
        rpc.add_protocol(TEST_PROTOCOL);
        let swarm = Swarm::new(transport, rpc, pubkey.clone().into_peer_id());
        (swarm, pubkey)
    };
    let peer2 = Swarm::local_peer_id(&swarm2).clone();

    Swarm::listen_on(&mut swarm1, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();
    let listen_addr = async_std::task::block_on(async {
        loop {
            let swarm1_fut = swarm1.next_event();
            pin_mut!(swarm1_fut);
            match swarm1_fut.await {
                SwarmEvent::NewListenAddr(addr) => return addr,
                evt => {
                    info!("peer1 swarm event: {:?}", evt);
                }
            }
        }
    });
    info!("peer1 listen on addr: {}", &listen_addr);

    let rpc_requests = vec![(TEST_PROTOCOL, vec![1u8; 10])];
    let mut rpc_requests_clone = rpc_requests.clone();
    let rpc_responses = vec![(vec![0u8; 10])];
    let mut rpc_responses_clone = rpc_responses.clone();
    let swarm1_loop = async move {
        rpc_requests_clone.reverse();
        rpc_responses_clone.reverse();
        loop {
            match swarm1.next_event().await {
                SwarmEvent::Behaviour(evt) => {
                    info!("peer1 receive the rpc request");
                    match evt {
                        RpcEvent::RpcNotification(peer_id, rpc_notification) => {
                            assert_eq!(peer_id, peer2);
                            match rpc_notification {
                                RpcNotification::RecvRpc(rpc_request) => {
                                    let (protocol, data) = match rpc_requests_clone.pop() {
                                        None => return,
                                        Some(data) => data,
                                    };
                                    assert_eq!(rpc_request.protocol, protocol);
                                    assert_eq!(rpc_request.data.as_ref(), data.as_slice());
                                    let response_to_send = rpc_responses_clone.pop().unwrap();
                                    rpc_request
                                        .res_tx
                                        .send(Ok(Bytes::from(response_to_send)))
                                        .unwrap();
                                    info!("peer2 respond the rpc request");
                                }
                            }
                        }
                    }
                }
                evt => {
                    info!("peer1 swarm event: {:?}", evt);
                }
            }
        }
    };

    Swarm::dial_addr(&mut swarm2, listen_addr).unwrap();
    let swarm2_loop = async move {
        let remote_peer_id = loop {
            let swarm2_fut = swarm2.next_event();
            pin_mut!(swarm2_fut);
            match swarm2_fut.await {
                SwarmEvent::Connected(peer_id) => break peer_id,
                swarm_event => {
                    info!("peer2 swarm event: {:?}", swarm_event);
                }
            }
        };
        assert_eq!(remote_peer_id, peer1.clone());
        info!("peer connected");
        let mut pending_rpcs = vec![];
        for (protocol, data) in rpc_requests {
            let rpc_receiver = swarm2.send_rpc(
                peer1.clone(),
                protocol,
                Bytes::from(data),
                Duration::from_secs(10),
            );
            pending_rpcs.push(rpc_receiver);
        }

        let mut loop_swarm2 = Box::pin(async move {
            loop {
                let evt = swarm2.next_event().await;
                info!("peer2 swarm event: {:?}", evt);
            }
        });
        for (idx, rpc_receiver) in pending_rpcs.into_iter().enumerate() {
            match future::select(rpc_receiver, loop_swarm2).await {
                future::Either::Right((_a, _result)) => unreachable!(),
                future::Either::Left((result, b)) => {
                    loop_swarm2 = b;
                    match result {
                        Err(_e) => {
                            panic!("sender cancelled");
                        }
                        Ok(Err(e)) => {
                            panic!("rpc error: {:?}", e);
                        }
                        Ok(Ok(resp)) => {
                            assert_eq!(resp.as_ref(), rpc_responses[idx].as_slice());
                        }
                    }
                }
            }
        }
    };
    let run_future = future::select(Box::pin(swarm1_loop), Box::pin(swarm2_loop));
    async_std::task::block_on(run_future);
}
