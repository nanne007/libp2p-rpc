# libp2p-rpc

Implementation of RPC protocol for libp2p.

Libra network implements it using full async model, and I thought it's possible to adopt it for libp2p.

The implementation is a little bit complicating comparing with the origin libra implementation. 


## Usage

See test for more details. A simple showcase is:

```rust
        let (pubkey, transport) = transport();
        let mut rpc = Rpc::new(RpcConfig::default());
        rpc.add_protocol(TEST_PROTOCOL);
        let swarm = Swarm::new(transport, rpc, pubkey.clone().into_peer_id());
```