# gossiper-transport

Transport traits and in-memory helpers for gossip protocol implementations.

`gossiper-transport` connects the effect-based protocol core to a concrete
message sink. It intentionally stays small: the crate defines a `Transport`
trait, an `apply_effects` helper, and an `InMemoryTransport` useful for tests and
examples.

Most users should depend on the facade crate:

```toml
gossiper = "0.1"
```

Use this crate directly when you specifically want the lower-level transport
API.

## What It Provides

- `Transport`
- `TransportError`
- `EffectReport`
- `apply_effects`
- `InMemoryTransport`

## Example

```rust
use gossiper_core::{Effect, NodeId};
use gossiper_transport::{apply_effects, InMemoryTransport};

let mut transport = InMemoryTransport::<String>::new();
let effects = vec![Effect::Send {
    target: NodeId::from("node-b"),
    message: "hello".to_string(),
}];

let report = apply_effects(&mut transport, effects);

assert_eq!(report.sent(), 1);
assert_eq!(transport.queued_len(&NodeId::from("node-b")), 1);
```

## Status

This crate is early and experimental. Runtime-specific adapters such as UDP,
QUIC, Tokio, libp2p, or Iroh are planned as future layers.
