#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! Transport-independent gossip protocol building blocks.
//!
//! This crate intentionally does not open sockets, spawn tasks, sleep, or depend
//! on an async runtime. It models protocol state and returns effects for a
//! runtime or simulator to execute.

mod anti_entropy;
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

pub use anti_entropy::{
    delta_message, digest_message, merge_delta, AntiEntropyMessage, DeltaStore, Digest,
    IdSetDigest, Merge, MergeOutcome, MergeReport,
};
pub use config::{ConfigError, GossipConfig};
pub use effect::Effect;
pub use event::GossipEvent;
pub use identity::{MessageId, MessageIdGenerator, NodeId};
pub use message::GossipMessage;
pub use node::{GossipNode, PublishManyError};
pub use peer_selection::choose_distinct_peers;
pub use rng::{DeterministicRng, RandomSource};
pub use rumor::Rumor;
pub use rumor_store::{InsertOutcome, RumorStore};
pub use time::{Round, Timestamp};

#[cfg(test)]
mod tests {
    use super::{
        choose_distinct_peers, delta_message, digest_message, merge_delta, AntiEntropyMessage,
        DeltaStore, DeterministicRng, Digest, Effect, GossipConfig, GossipMessage, GossipNode,
        IdSetDigest, InsertOutcome, MergeOutcome, MessageId, MessageIdGenerator, NodeId,
        PublishManyError, RandomSource, Round, Rumor, RumorStore, Timestamp,
    };

    #[test]
    fn public_types_are_available() {
        let config = GossipConfig::default();
        let node = NodeId::from("node-a");
        let message = MessageId::from(7);
        let mut message_ids = MessageIdGenerator::new(8);
        let round = Round::new(3);
        let now = Timestamp::from_millis(1_000);
        let rumor = Rumor::new(message, node.clone(), round, "hello");
        let gossip_message = GossipMessage::rumors(vec![rumor.clone()]);
        let mut store = RumorStore::new(config.max_rumors());
        let mut gossip_node: GossipNode<&str> = GossipNode::new(node.clone(), config.clone());
        let publish_error = PublishManyError::MessageIdGeneratorExhausted;
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
        assert_eq!(message_ids.next_id(), Some(MessageId::new(8)));
        assert_eq!(publish_error.to_string(), "message ID generator exhausted");
        assert_eq!(round.get(), 3);
        assert_eq!(now.as_millis(), 1_000);
        assert_eq!(rumor.payload(), &"hello");
        assert_eq!(store.insert(rumor), InsertOutcome::Inserted);
        let digest = IdSetDigest::from_ids([MessageId::new(7)]);
        let anti_entropy_message: AntiEntropyMessage<_, Rumor<&str>> =
            AntiEntropyMessage::digest(digest.clone());
        assert!(digest.contains(&MessageId::new(7)));
        assert_eq!(anti_entropy_message.delta_len(), 0);
        assert_eq!(store.digest().len(), 1);
        assert_eq!(digest_message(&store).delta_len(), 0);
        assert_eq!(delta_message(&store, &digest).delta_len(), 0);
        assert_eq!(
            merge_delta(&mut store, Vec::<Rumor<&str>>::new()).total(),
            0
        );
        assert_eq!(MergeOutcome::Changed, MergeOutcome::Changed);
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
