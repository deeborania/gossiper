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
- anti-entropy digest/delta traits
- `IdSetDigest`
- `AntiEntropyMessage`
- `MergeReport`
- digest/delta/merge helper functions

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

## Anti-Entropy Example

Anti-entropy lets one node send a compact summary of what it already knows, so
another node can send back only the missing items.

```rust
use gossiper_core::{
    delta_message, merge_delta, AntiEntropyMessage, IdSetDigest, MessageId, NodeId, Round, Rumor,
    RumorStore,
};

let mut node_a = RumorStore::new(8);
let mut node_b = RumorStore::new(8);

let first = Rumor::new(MessageId::new(1), NodeId::from("node-a"), Round::ZERO, "one");
let second = Rumor::new(MessageId::new(2), NodeId::from("node-a"), Round::ZERO, "two");

node_a.insert(first.clone());
node_a.insert(second);
node_b.insert(first);

let node_b_digest = IdSetDigest::from_ids([MessageId::new(1)]);
let message = delta_message(&node_a, &node_b_digest);

if let AntiEntropyMessage::Delta(items) = message {
    let report = merge_delta(&mut node_b, items);

    assert_eq!(report.changed(), 1);
    assert!(node_b.contains(MessageId::new(2)));
}
```

Run the full example with:

```bash
cargo run -p gossiper-core --example anti_entropy
cargo run -p gossiper-core --example grow_only_counter
cargo run -p gossiper-core --example grow_only_set
cargo run -p gossiper-core --example versioned_kv
```

## Features

- `serde`: derives `Serialize` and `Deserialize` for protocol value types.

Serialization support is intentionally format-neutral. Applications choose JSON,
bincode, postcard, MessagePack, or another serde-compatible format.

## Status

This crate is early and experimental. APIs may change before the first stable
release.
