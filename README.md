# gossiper

Reusable gossip protocol building blocks for Rust.

This repository contains a small workspace for learning, designing, and testing gossip-based distributed systems components. The main user-facing crate is `gossiper`.

## Status

Early and experimental. The current implementation focuses on:

- transport-independent protocol state machines
- deterministic testing
- simulation
- API design
- learning-friendly structure

Real network transports, membership protocols, and consensus components are planned future layers.

## Which Crate Should I Use?

Most users should start with:

```toml
gossiper = "0.1"
```

For simulation helpers:

```toml
gossiper = { version = "0.1", features = ["sim"] }
```

The lower-level crates exist to keep the design modular.

## Workspace Layout

- `crates/gossiper`: user-facing facade crate
- `crates/gossip-core`: transport-independent gossip protocol core
- `crates/gossip-transport`: transport traits and in-memory transport helpers
- `crates/gossip-sim`: deterministic simulation utilities
- `docs`: design notes and learning material
- `index.html`: local HTML gossip protocol course

## Quick Example

```rust
use gossiper::{
    apply_effects, DeterministicRng, GossipConfig, GossipMessage, GossipNode, MessageId, NodeId,
    Round,
};

let config = GossipConfig::new(1, 10).expect("valid config");

let node_a_id = NodeId::from("node-a");
let node_b_id = NodeId::from("node-b");

let mut node_a = GossipNode::new(node_a_id.clone(), config.clone());
let mut node_b = GossipNode::new(node_b_id.clone(), config);

node_a.set_peers(vec![node_b_id.clone()]);

let rumor_id = MessageId::new(1);
node_a.publish(rumor_id, Round::ZERO, "hello");

let mut rng = DeterministicRng::new(1);
let effects = node_a.tick(&mut rng, Round::ZERO);

let mut transport = gossiper::InMemoryTransport::<GossipMessage<&str>>::new();
let report = apply_effects(&mut transport, effects);

assert_eq!(report.sent(), 1);

for message in transport.drain(&node_b_id) {
    node_b.receive(message);
}

assert!(node_b.contains_rumor(rumor_id));
```

## Run Examples

```bash
cargo run -p gossiper --example basic_gossip
cargo run -p gossip-sim --example convergence
```

## Run Tests

```bash
cargo test --workspace
cargo test -p gossiper --no-default-features
cargo test -p gossiper --features sim
```

## Design Principle

The core protocol does not open sockets, spawn tasks, sleep, or read the system clock. Instead, it returns effects such as “send this message to that peer.” A runtime, simulator, or transport layer decides how to execute those effects.

This keeps the protocol deterministic, testable, and reusable across different networking stacks.
