# gossiper

Reusable gossip protocol building blocks for Rust.

`gossiper` is the user-facing facade crate for this workspace. It re-exports a small, transport-independent gossip protocol core and optional helper layers for transport and simulation.

## Status

This crate is early and experimental. The current focus is learning, API design, deterministic testing, and simulation before adding production network transports.

## What It Provides

- Transport-independent gossip node state machine
- Rumor storage with duplicate suppression
- Configurable fanout, retention, and per-message rumor limits
- Effect-based protocol output instead of direct socket I/O
- Transport traits and in-memory transport helpers
- Optional simulation utilities for convergence experiments

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

## Features

Default features:

- `transport`: re-exports transport traits, `apply_effects`, and `InMemoryTransport`

Optional features:

- `sim`: enables simulation utilities such as `Cluster` and `ConvergenceExperiment`

Example:

```toml
gossiper = { version = "0.1", features = ["sim"] }
```

## Workspace Crates

- `gossip-core`: protocol state machine and core data types
- `gossip-transport`: transport traits and in-memory transport
- `gossip-sim`: deterministic simulation utilities
- `gossiper`: user-facing facade crate

## Design Principle

The core protocol does not open sockets, spawn tasks, sleep, or read the system clock. It returns effects that a runtime, simulator, or transport layer can execute.
