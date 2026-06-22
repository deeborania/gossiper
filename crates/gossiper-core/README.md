# gossiper-core

Transport-independent gossip protocol building blocks for Rust.

`gossiper-core` contains the deterministic protocol state machine and core data
types used by the `gossiper` workspace. It does not open sockets, spawn tasks,
sleep, or read the system clock. Instead, a `GossipNode` stores protocol state
and returns effects for a runtime, transport adapter, or simulator to execute.

Most users should depend on the facade crate:

```toml
gossiper = "0.1"
```

Use this crate directly when you specifically want the lower-level core API.

## What It Provides

- `GossipNode`
- `GossipConfig`
- `NodeId`
- `MessageId`
- `MessageIdGenerator`
- `Rumor`
- `GossipMessage`
- `GossipEvent`
- duplicate suppression
- bounded rumor storage
- logical-round retention
- deterministic peer selection hooks

## Example

```rust
use gossiper_core::{
    DeterministicRng, GossipConfig, GossipNode, MessageIdGenerator, NodeId, Round,
};

let config = GossipConfig::new(1, 64).expect("valid config");
let mut node = GossipNode::new(NodeId::from("node-a"), config);
let mut ids = MessageIdGenerator::default();

node.set_peers(vec![NodeId::from("node-b")]);

let rumor_id = ids.next_id().expect("generator should have IDs");
node.publish(rumor_id, Round::ZERO, "hello");

let mut rng = DeterministicRng::new(1);
let effects = node.tick(&mut rng, Round::ZERO);

assert_eq!(effects.len(), 1);
```

## Features

- `serde`: derives `Serialize` and `Deserialize` for protocol value types.

Serialization support is intentionally format-neutral. Applications choose JSON,
bincode, postcard, MessagePack, or another serde-compatible format.

## Status

This crate is early and experimental. APIs may change before the first stable
release.
