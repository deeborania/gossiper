#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! Transport-independent gossip protocol building blocks.
//!
//! This crate intentionally does not open sockets, spawn tasks, sleep, or depend
//! on an async runtime. It models protocol state and returns effects for a
//! runtime or simulator to execute.

mod config;
mod effect;
mod event;
mod identity;
mod message;
mod node;
mod peer_selection;
mod rng;
mod rumor;
mod rumor_store;
mod time;

pub use config::{ConfigError, GossipConfig};
pub use effect::Effect;
pub use event::GossipEvent;
pub use identity::{MessageId, NodeId};
pub use message::GossipMessage;
pub use node::GossipNode;
pub use peer_selection::choose_distinct_peers;
pub use rng::{DeterministicRng, RandomSource};
pub use rumor::Rumor;
pub use rumor_store::{InsertOutcome, RumorStore};
pub use time::{Round, Timestamp};

#[cfg(test)]
mod tests {
    use super::{
        choose_distinct_peers, DeterministicRng, Effect, GossipConfig, GossipMessage, GossipNode,
        InsertOutcome, MessageId, NodeId, RandomSource, Round, Rumor, RumorStore, Timestamp,
    };

    #[test]
    fn public_types_are_available() {
        let config = GossipConfig::default();
        let node = NodeId::from("node-a");
        let message = MessageId::from(7);
        let round = Round::new(3);
        let now = Timestamp::from_millis(1_000);
        let rumor = Rumor::new(message, node.clone(), round, "hello");
        let gossip_message = GossipMessage::rumors(vec![rumor.clone()]);
        let mut store = RumorStore::new(config.max_rumors());
        let mut gossip_node: GossipNode<&str> = GossipNode::new(node.clone(), config.clone());
        let effect: Effect<GossipMessage<&str>, ()> = Effect::Send {
            target: node.clone(),
            message: gossip_message,
        };
        let mut rng = DeterministicRng::new(1);
        let peers = vec![node.clone(), NodeId::from("node-b")];

        gossip_node.set_peers(peers.clone());

        assert_eq!(config.fanout(), 3);
        assert_eq!(gossip_node.self_id(), &node);
        assert_eq!(node.as_str(), "node-a");
        assert_eq!(message.get(), 7);
        assert_eq!(round.get(), 3);
        assert_eq!(now.as_millis(), 1_000);
        assert_eq!(rumor.payload(), &"hello");
        assert_eq!(store.insert(rumor), InsertOutcome::Inserted);
        assert!(rng.index(10).expect("non-empty range") < 10);
        assert_eq!(
            choose_distinct_peers(&mut rng, &node, &peers, 1),
            vec![NodeId::from("node-b")]
        );

        match effect {
            Effect::Send { target, message } => {
                assert_eq!(target, NodeId::from("node-a"));
                assert_eq!(message.rumor_count(), 1);
            }
            Effect::Emit(()) => panic!("expected send effect"),
        }
    }
}
