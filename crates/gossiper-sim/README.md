# gossiper-sim

Deterministic simulation utilities for gossip protocol implementations.

`gossiper-sim` helps test and understand gossip behavior without opening real
network sockets. It builds on `gossiper-core` and `gossiper-transport` to simulate
clusters, topology, unreliable network behavior, convergence, and metrics.

Most users should depend on the facade crate with the `sim` feature:

```toml
gossiper = { version = "0.1", features = ["sim"] }
```

Use this crate directly when you specifically want the lower-level simulator
API.

## What It Provides

- `Cluster`
- `ClusterBuilder`
- generated fully-connected and line topologies
- `NetworkModel`
- packet loss
- duplicate delivery
- delayed delivery and reordering
- partitions and healing
- fixed-round runs
- reach/convergence reports
- named convergence comparisons

## Example

```rust
use gossiper_core::{GossipConfig, MessageId, NodeId, Round};
use gossiper_sim::{ClusterBuilder, NetworkModel};

let network = NetworkModel::new()
    .with_loss_rate(0.10)
    .expect("valid loss rate");

let mut cluster = ClusterBuilder::new(GossipConfig::new(2, 64).expect("valid config"))
    .with_node_count(5)
    .with_seed(42)
    .with_network_model(network)
    .fully_connected();

let rumor_id = MessageId::new(1);

cluster
    .publish(&NodeId::from("node-0"), rumor_id, Round::ZERO, "hello")
    .expect("origin node should exist");

let report = cluster.run_for_rounds(Round::ZERO, 5);

assert_eq!(report.rounds_run(), 5);
```

## Status

This crate is early and experimental. It is intended for deterministic learning,
testing, and protocol evaluation before adding real network runtime adapters.
