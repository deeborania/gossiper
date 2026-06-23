# gossiper

Reusable gossip protocol building blocks for Rust.

This repository contains a small workspace for learning, designing, and testing gossip-based distributed systems components. The main user-facing crate is `gossiper`.

## Status

Early and experimental. The current implementation focuses on:

- transport-independent protocol state machines
- rumor dissemination
- anti-entropy digest/delta exchange
- transport helper traits
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

## What Is Implemented

### Gossip Core

Where to look:

- crate: `crates/gossiper-core`
- README: `crates/gossiper-core/README.md`
- source entry point: `crates/gossiper-core/src/lib.rs`
- main protocol state: `crates/gossiper-core/src/node.rs`
- rumor storage: `crates/gossiper-core/src/rumor_store.rs`

Implemented:

- `GossipNode`: deterministic node state machine
- `GossipConfig`: fanout, storage limit, per-message limit, and retention settings
- `NodeId`, `MessageId`, `MessageIdGenerator`
- `Rumor`, `RumorStore`, `GossipMessage`, `GossipEvent`
- duplicate suppression
- bounded rumor storage
- logical-round retention
- rotating limited rumor batches
- deterministic peer selection hooks
- effect-based output instead of direct socket I/O

### Anti-Entropy

Where to look:

- source: `crates/gossiper-core/src/anti_entropy.rs`
- example: `crates/gossiper-core/examples/anti_entropy.rs`
- README section: `crates/gossiper-core/README.md`

Implemented:

- `Digest`: compact summary of known item IDs
- `IdSetDigest`: exact digest backed by a sorted set
- `DeltaStore`: store trait for building digests and missing-item deltas
- `Merge`: trait for merging incoming items
- `MergeOutcome` and `MergeReport`
- `AntiEntropyMessage`
- `digest_message`, `delta_message`, and `merge_delta`

This is useful when nodes should exchange summaries first and then send only missing data.

### Transport Helpers

Where to look:

- crate: `crates/gossiper-transport`
- README: `crates/gossiper-transport/README.md`
- source: `crates/gossiper-transport/src/lib.rs`

Implemented:

- `Transport`
- `TransportError`
- `EffectReport`
- `apply_effects`
- `InMemoryTransport`

This layer is intentionally small. Real network adapters such as UDP, QUIC, Tokio, libp2p, or Iroh are future layers.

### Simulator

Where to look:

- crate: `crates/gossiper-sim`
- README: `crates/gossiper-sim/README.md`
- source: `crates/gossiper-sim/src/lib.rs`
- network model guide: `docs/simulator-network-model.md`

Implemented:

- `Cluster` and `ClusterBuilder`
- fully connected and line topologies
- deterministic seeded runs
- packet loss
- duplicate delivery
- delayed delivery and reordering
- network partitions and healing
- fixed-round runs
- reach/convergence reports
- named convergence comparisons
- simulator metrics

### Facade Crate

Where to look:

- crate: `crates/gossiper`
- README: `crates/gossiper/README.md`
- source: `crates/gossiper/src/lib.rs`

Implemented:

- default re-export of core API
- default `transport` feature
- optional `sim` feature
- optional `serde` forwarding to core protocol value types

## Workspace Layout

- `crates/gossiper`: user-facing facade crate
- `crates/gossiper-core`: transport-independent gossip protocol core
- `crates/gossiper-transport`: transport traits and in-memory transport helpers
- `crates/gossiper-sim`: deterministic simulation utilities
- `docs`: design notes and learning material
- `index.html`: local HTML gossip protocol course

## Quick Example

```rust
use gossiper::{
    apply_effects, DeterministicRng, GossipConfig, GossipMessage, GossipNode, MessageIdGenerator,
    NodeId, Round,
};

let config = GossipConfig::new(1, 10).expect("valid config");

let node_a_id = NodeId::from("node-a");
let node_b_id = NodeId::from("node-b");

let mut node_a = GossipNode::new(node_a_id.clone(), config.clone());
let mut node_b = GossipNode::new(node_b_id.clone(), config);

node_a.set_peers(vec![node_b_id.clone()]);

let mut message_ids = MessageIdGenerator::default();
let rumor_id = message_ids.next_id().expect("generator should have IDs");
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
cargo run -p gossiper-core --example anti_entropy
cargo run -p gossiper --features sim --example sim_network
cargo run -p gossiper-sim --example convergence
```

More simulator examples:

```bash
cargo run -p gossiper-sim --example cluster
cargo run -p gossiper-sim --example line_topology
cargo run -p gossiper-sim --example fixed_run
cargo run -p gossiper-sim --example network_conditions
cargo run -p gossiper-sim --example scenario_builder
```

## Run Tests

```bash
cargo test --workspace
cargo test -p gossiper --no-default-features
cargo test -p gossiper --features sim
cargo test -p gossiper-core --features serde
```

## Documentation Map

- `docs/project-status.md`: current implementation status and roadmap
- `docs/library-architecture.md`: crate design and long-term layering
- `docs/implementation-plan.md`: guided implementation plan
- `docs/guided-build-mode.md`: learning-oriented build steps
- `docs/simulator-network-model.md`: simulator network behavior guide
- `docs/consensus-roadmap.md`: future consensus learning path
- `index.html`: local HTML gossip protocol course

## Design Principle

The core protocol does not open sockets, spawn tasks, sleep, or read the system clock. Instead, it returns effects such as “send this message to that peer.” A runtime, simulator, or transport layer decides how to execute those effects.

This keeps the protocol deterministic, testable, and reusable across different networking stacks.
