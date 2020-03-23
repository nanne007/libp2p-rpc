# libp2p-rpc

Implementation of RPC protocol for libp2p.
The core idea is adopted from libra network implementation.
I implement it to learn the design and concept of libp2p, so use it at your risk.

## Usage

``` rust
        let mut rpc = Rpc::new(RpcConfig::default());
        rpc.add_protocol(TEST_PROTOCOL);
        let swarm = Swarm::new(transport, rpc, pubkey.clone().into_peer_id());
        swarm.send_rpc(
            peer_id,
            TEST_PROTOCOL,
            Bytes::from(data),
            Duration::from_secs(10),
        );
```