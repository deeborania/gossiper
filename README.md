# gossiper

**Reusable, deterministic gossip protocol building blocks for Rust.**

[![CI](https://github.com/deeborania/gossiper/actions/workflows/ci.yml/badge.svg)](https://github.com/deeborania/gossiper/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
[![Rust 2021](https://img.shields.io/badge/rust-2021-orange.svg)](https://www.rust-lang.org)

`gossiper` is a Rust workspace for building, testing, and learning gossip-based
distributed systems. The protocol core is **transport-independent** and
**deterministic**: it never opens sockets, spawns tasks, sleeps, or reads the
clock. It just decides what should happen and returns *effects* like
"send this message to that peer." You decide how to execute them, over any
network stack or a simulator.

## Contents

- [Why gossiper](#why-gossiper)
- [Project status](#project-status)
- [Install](#install)
- [Quick example](#quick-example)
- [How it works](#how-it-works)
- [Crates](#crates)
- [Examples](#examples)
- [Testing](#testing)
- [Contributing](#contributing)
- [License](#license)

## Why gossiper

- **Deterministic core** - same inputs always produce the same effects, so tests
  are reproducible and bugs are easy to pin down.
- **Transport-agnostic** - the protocol emits effects instead of doing I/O, so it
  drops into UDP, QUIC, Tokio, libp2p, an in-memory bus, or a simulator without
  changing the core.
- **Rumor dissemination + anti-entropy** - push-style rumor spreading plus
  digest/delta exchange so nodes reconcile only the data they are actually
  missing.
- **Batteries-included simulator** - exercise convergence under packet loss,
  duplication, delay, reordering, and partitions, all from a seeded, repeatable run.
- **Learning-friendly** - small, layered crates with their own READMEs and
  runnable examples.

## Project status

Early and experimental. The implemented layers (core protocol, anti-entropy,
in-memory transport, simulator) are tested and usable, but APIs may change.

Real network transports, membership/failure detection, and consensus components
are planned future layers.

> **Not yet published to crates.io.** Use the git dependency shown below.

## Install

Add the facade crate as a git dependency:

```toml
[dependencies]
gossiper = { git = "https://github.com/deeborania/gossiper" }
```

With the simulation helpers:

```toml
[dependencies]
gossiper = { git = "https://github.com/deeborania/gossiper", features = ["sim"] }
```

Feature flags on `gossiper`:

| Feature     | Default | Enables                                              |
|-------------|---------|------------------------------------------------------|
| `transport` | yes     | transport traits + in-memory transport helpers       |
| `sim`       | no      | the deterministic simulator (implies `transport`)    |
| `serde`     | no      | `serde` derive on core protocol value types          |

Most users only need the facade crate `gossiper`; the lower-level crates exist to
keep the design modular.

## Quick example

Two nodes, one rumor, delivered over the in-memory transport:

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

// Publish a rumor on node A.
let mut message_ids = MessageIdGenerator::default();
let rumor_id = message_ids.next_id().expect("generator should have IDs");
node_a.publish(rumor_id, Round::ZERO, "hello");

// Tick the protocol: it returns effects, it does not send anything itself.
let mut rng = DeterministicRng::new(1);
let effects = node_a.tick(&mut rng, Round::ZERO);

// You decide how to execute the effects - here, an in-memory transport.
let mut transport = gossiper::InMemoryTransport::<GossipMessage<&str>>::new();
let report = apply_effects(&mut transport, effects);
assert_eq!(report.sent(), 1);

// Deliver to node B.
for message in transport.drain(&node_b_id) {
    node_b.receive(message);
}

assert!(node_b.contains_rumor(rumor_id));
```

## How it works

The core protocol does not open sockets, spawn tasks, sleep, or read the system
clock. Instead, a `tick` returns **effects** such as "send this message to that
peer." A runtime, transport layer, or simulator decides how to execute those
effects.

```
        +-------------------+        effects        +-----------------------+
inputs  |    GossipNode     |  ------------------->  | transport / simulator |
------> | (deterministic    |                        | (does the real I/O)   |
        |  state machine)   |  <-------------------  |                       |
        +-------------------+      messages in       +-----------------------+
```

This separation keeps the protocol deterministic, unit-testable without a
network, and reusable across different networking stacks.

## Crates

| Crate                                                       | What it is                                                                 |
|------------------------------------------------------------|---------------------------------------------------------------------------|
| [`gossiper`](crates/gossiper/README.md)                     | User-facing facade. Re-exports the core API and gates transport/sim/serde. |
| [`gossiper-core`](crates/gossiper-core/README.md)           | Transport-independent protocol core: node state machine, rumor store, and anti-entropy (digest/delta) exchange. |
| [`gossiper-transport`](crates/gossiper-transport/README.md) | Transport trait, effect application, and an in-memory transport for tests. |
| [`gossiper-sim`](crates/gossiper-sim/README.md)             | Deterministic cluster simulator with configurable network conditions and convergence reporting. |

Each crate's README documents its public types and intended use. The full type
reference lives in the rustdoc (`cargo doc --workspace --open`).

## Examples

```bash
cargo run -p gossiper --example basic_gossip
cargo run -p gossiper-core --example anti_entropy
cargo run -p gossiper --features sim --example sim_network
```

Simulator examples (require the `gossiper-sim` crate):

```bash
cargo run -p gossiper-sim --example cluster
cargo run -p gossiper-sim --example convergence
cargo run -p gossiper-sim --example line_topology
cargo run -p gossiper-sim --example fixed_run
cargo run -p gossiper-sim --example network_conditions
cargo run -p gossiper-sim --example scenario_builder
```

## Testing

```bash
cargo test --workspace                       # everything
cargo test -p gossiper --no-default-features  # core only, no transport
cargo test -p gossiper --features sim         # with the simulator
cargo test -p gossiper-core --features serde  # with serde derives
```

## Contributing

Issues and pull requests are welcome. Before opening a PR, please run:

```bash
cargo fmt --all
cargo test --workspace
cargo clippy --workspace --all-targets
```

This is a learning-oriented project, so clear explanations and small, focused
changes are especially appreciated.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
