//! Gossip node state machine.

use crate::{
    choose_distinct_peers, Effect, GossipConfig, GossipEvent, GossipMessage, InsertOutcome,
    MessageId, MessageIdGenerator, NodeId, RandomSource, Round, Rumor, RumorStore,
};
use core::fmt;

/// Error returned when publishing a batch of local rumors fails.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublishManyError {
    /// The message ID generator did not have enough IDs for the requested batch.
    MessageIdGeneratorExhausted,
}

impl fmt::Display for PublishManyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MessageIdGeneratorExhausted => {
                formatter.write_str("message ID generator exhausted")
            }
        }
    }
}

impl std::error::Error for PublishManyError {}

/// A transport-independent gossip node.
///
/// This type contains protocol state only. It does not open sockets, spawn
/// tasks, sleep, or call the system clock.
#[derive(Clone, Debug)]
pub struct GossipNode<T> {
    self_id: NodeId,
    config: GossipConfig,
    peers: Vec<NodeId>,
    rumors: RumorStore<T>,
}

impl<T> GossipNode<T> {
    /// Creates a new gossip node.
    pub fn new(self_id: NodeId, config: GossipConfig) -> Self {
        let rumors = RumorStore::new(config.max_rumors());

        Self {
            self_id,
            config,
            peers: Vec::new(),
            rumors,
        }
    }

    /// Returns this node's identity.
    pub fn self_id(&self) -> &NodeId {
        &self.self_id
    }

    /// Returns this node's configuration.
    pub fn config(&self) -> &GossipConfig {
        &self.config
    }

    /// Replaces the known peer list.
    pub fn set_peers(&mut self, peers: Vec<NodeId>) {
        self.peers = peers;
    }

    /// Returns the known peers.
    pub fn peers(&self) -> &[NodeId] {
        &self.peers
    }

    /// Inserts a locally known rumor.
    pub fn insert_rumor(&mut self, rumor: Rumor<T>) -> InsertOutcome {
        self.rumors.insert(rumor)
    }

    /// Publishes a local rumor from this node.
    pub fn publish(&mut self, id: MessageId, round: Round, payload: T) -> InsertOutcome {
        self.insert_rumor(Rumor::new(id, self.self_id.clone(), round, payload))
    }

    /// Publishes a batch of local rumors from this node.
    ///
    /// This method first allocates every required message ID. If the generator
    /// is exhausted, no rumors are inserted.
    pub fn publish_many(
        &mut self,
        ids: &mut MessageIdGenerator,
        round: Round,
        payloads: impl IntoIterator<Item = T>,
    ) -> Result<Vec<InsertOutcome>, PublishManyError> {
        let payloads: Vec<_> = payloads.into_iter().collect();
        let mut allocated_ids = Vec::with_capacity(payloads.len());
        let mut next_ids = ids.clone();

        for _ in 0..payloads.len() {
            let Some(id) = next_ids.next_id() else {
                return Err(PublishManyError::MessageIdGeneratorExhausted);
            };

            allocated_ids.push(id);
        }

        *ids = next_ids;

        let outcomes = allocated_ids
            .into_iter()
            .zip(payloads)
            .map(|(id, payload)| self.publish(id, round, payload))
            .collect();

        Ok(outcomes)
    }

    /// Returns the number of known rumors.
    pub fn rumor_count(&self) -> usize {
        self.rumors.len()
    }

    /// Returns `true` if this node already knows a rumor ID.
    pub fn contains_rumor(&self, id: MessageId) -> bool {
        self.rumors.contains(id)
    }

    /// Returns a known rumor by ID.
    pub fn get_rumor(&self, id: MessageId) -> Option<&Rumor<T>> {
        self.rumors.get(id)
    }
}

impl<T: Clone> GossipNode<T> {
    /// Processes an incoming gossip message.
    pub fn receive(
        &mut self,
        message: GossipMessage<T>,
    ) -> Vec<Effect<GossipMessage<T>, GossipEvent<T>>> {
        match message {
            GossipMessage::Rumors(rumors) => {
                let mut effects = Vec::new();

                for rumor in rumors {
                    let outcome = self.rumors.insert(rumor.clone());

                    if matches!(
                        outcome,
                        InsertOutcome::Inserted | InsertOutcome::InsertedWithEviction { .. }
                    ) {
                        effects.push(Effect::Emit(GossipEvent::NewRumor(rumor)));
                    }
                }

                effects
            }
        }
    }

    /// Runs one gossip round and returns effects for the caller to execute.
    pub fn tick<R: RandomSource>(
        &mut self,
        rng: &mut R,
        round: Round,
    ) -> Vec<Effect<GossipMessage<T>, ()>> {
        let oldest_kept_round = Round::new(
            round
                .get()
                .saturating_sub(self.config.rumor_retention_rounds()),
        );
        self.rumors.prune_older_than(oldest_kept_round);

        if self.rumors.is_empty() {
            return Vec::new();
        }

        let selected = choose_distinct_peers(rng, &self.self_id, &self.peers, self.config.fanout());

        let available_rumors: Vec<_> = self.rumors.iter_in_insertion_order().collect();
        let rumor_count = available_rumors.len();
        let rumor_limit = self.config.max_rumors_per_message().min(rumor_count);
        let start = round.get() as usize % rumor_count;

        let rumors: Vec<_> = available_rumors
            .iter()
            .cycle()
            .skip(start)
            .take(rumor_limit)
            .map(|rumor| (*rumor).clone())
            .collect();

        selected
            .into_iter()
            .map(|target| Effect::Send {
                target,
                message: GossipMessage::rumors(rumors.clone()),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::GossipNode;
    use crate::{
        DeterministicRng, Effect, GossipConfig, GossipEvent, GossipMessage, InsertOutcome,
        MessageId, MessageIdGenerator, NodeId, PublishManyError, Round, Rumor,
    };

    fn rumor(id: u128, payload: &'static str) -> Rumor<&'static str> {
        Rumor::new(
            MessageId::new(id),
            NodeId::from("node-a"),
            Round::new(0),
            payload,
        )
    }

    #[test]
    fn starts_without_peers_or_rumors() {
        let node: GossipNode<&str> =
            GossipNode::new(NodeId::from("node-a"), GossipConfig::default());

        assert_eq!(node.self_id(), &NodeId::from("node-a"));
        assert!(node.peers().is_empty());
        assert_eq!(node.rumor_count(), 0);
    }

    #[test]
    fn inserts_rumor() {
        let mut node = GossipNode::new(NodeId::from("node-a"), GossipConfig::default());

        let outcome = node.insert_rumor(rumor(1, "hello"));

        assert_eq!(outcome, InsertOutcome::Inserted);
        assert_eq!(node.rumor_count(), 1);
    }

    #[test]
    fn receive_stores_new_rumor_and_emits_event() {
        let mut node = GossipNode::new(NodeId::from("node-b"), GossipConfig::default());
        let message = GossipMessage::rumors(vec![rumor(1, "hello")]);

        let effects = node.receive(message);

        assert_eq!(node.rumor_count(), 1);
        assert_eq!(effects.len(), 1);

        match &effects[0] {
            Effect::Emit(GossipEvent::NewRumor(rumor)) => {
                assert_eq!(rumor.id(), MessageId::new(1));
                assert_eq!(rumor.payload(), &"hello");
            }
            Effect::Send { .. } => panic!("expected event effect"),
        }
    }

    #[test]
    fn receive_suppresses_duplicate_rumor() {
        let mut node = GossipNode::new(NodeId::from("node-b"), GossipConfig::default());

        let first = node.receive(GossipMessage::rumors(vec![rumor(1, "hello")]));
        let second = node.receive(GossipMessage::rumors(vec![rumor(1, "hello again")]));

        assert_eq!(first.len(), 1);
        assert!(second.is_empty());
        assert_eq!(node.rumor_count(), 1);
    }

    #[test]
    fn tick_returns_no_effects_without_rumors() {
        let mut node: GossipNode<&str> =
            GossipNode::new(NodeId::from("node-a"), GossipConfig::default());
        node.set_peers(vec![NodeId::from("node-b")]);

        let mut rng = DeterministicRng::new(1);
        let effects = node.tick(&mut rng, Round::new(0));

        assert!(effects.is_empty());
    }

    #[test]
    fn tick_sends_known_rumors_to_selected_peers() {
        let config = GossipConfig::new(2, 10).expect("valid config");
        let mut node = GossipNode::new(NodeId::from("node-a"), config);
        node.set_peers(vec![
            NodeId::from("node-a"),
            NodeId::from("node-b"),
            NodeId::from("node-c"),
            NodeId::from("node-d"),
        ]);
        node.insert_rumor(rumor(1, "hello"));

        let mut rng = DeterministicRng::new(1);
        let effects = node.tick(&mut rng, Round::new(0));

        assert_eq!(effects.len(), 2);

        for effect in effects {
            match effect {
                Effect::Send { target, message } => {
                    assert_ne!(target, NodeId::from("node-a"));

                    match message {
                        GossipMessage::Rumors(rumors) => {
                            assert_eq!(rumors.len(), 1);
                            assert_eq!(rumors[0].payload(), &"hello");
                        }
                    }
                }
                Effect::Emit(()) => panic!("expected send effect"),
            }
        }
    }

    #[test]
    fn tick_keeps_rumors_within_retention_window() {
        let config = GossipConfig::new(1, 10)
            .expect("valid config")
            .with_rumor_retention_rounds(3)
            .expect("valid retention");
        let mut node = GossipNode::new(NodeId::from("node-a"), config);

        node.set_peers(vec![NodeId::from("node-b")]);
        node.insert_rumor(rumor(1, "hello"));

        let mut rng = DeterministicRng::new(1);
        let effects = node.tick(&mut rng, Round::new(3));

        assert_eq!(effects.len(), 1);
        assert_eq!(node.rumor_count(), 1);
    }

    #[test]
    fn tick_prunes_rumors_after_retention_window() {
        let config = GossipConfig::new(1, 10)
            .expect("valid config")
            .with_rumor_retention_rounds(3)
            .expect("valid retention");
        let mut node = GossipNode::new(NodeId::from("node-a"), config);

        node.set_peers(vec![NodeId::from("node-b")]);
        node.insert_rumor(rumor(1, "hello"));

        let mut rng = DeterministicRng::new(1);
        let effects = node.tick(&mut rng, Round::new(4));

        assert!(effects.is_empty());
        assert_eq!(node.rumor_count(), 0);
    }

    #[test]
    fn exposes_read_only_rumor_lookup() {
        let mut node = GossipNode::new(NodeId::from("node-a"), GossipConfig::default());

        node.insert_rumor(rumor(1, "hello"));

        assert!(node.contains_rumor(MessageId::new(1)));
        assert!(!node.contains_rumor(MessageId::new(2)));

        let stored = node
            .get_rumor(MessageId::new(1))
            .expect("rumor should be stored");

        assert_eq!(stored.payload(), &"hello");
        assert_eq!(node.get_rumor(MessageId::new(2)), None);
    }

    #[test]
    fn tick_limits_rumors_per_message() {
        let config = GossipConfig::new(1, 10)
            .expect("valid config")
            .with_max_rumors_per_message(2)
            .expect("valid per-message limit");
        let mut node = GossipNode::new(NodeId::from("node-a"), config);

        node.set_peers(vec![NodeId::from("node-b")]);
        node.insert_rumor(rumor(1, "first"));
        node.insert_rumor(rumor(2, "second"));
        node.insert_rumor(rumor(3, "third"));

        let mut rng = DeterministicRng::new(1);
        let effects = node.tick(&mut rng, Round::new(0));

        assert_eq!(effects.len(), 1);

        match &effects[0] {
            Effect::Send { message, .. } => {
                assert_eq!(message.rumor_count(), 2);
            }
            Effect::Emit(()) => panic!("expected send effect"),
        }
    }

    #[test]
    fn tick_rotates_limited_rumor_batch_by_round() {
        let config = GossipConfig::new(1, 10)
            .expect("valid config")
            .with_max_rumors_per_message(2)
            .expect("valid per-message limit");
        let mut node = GossipNode::new(NodeId::from("node-a"), config);

        node.set_peers(vec![NodeId::from("node-b")]);
        node.insert_rumor(rumor(1, "first"));
        node.insert_rumor(rumor(2, "second"));
        node.insert_rumor(rumor(3, "third"));

        let mut rng = DeterministicRng::new(1);
        let effects = node.tick(&mut rng, Round::new(1));

        assert_eq!(effects.len(), 1);

        match &effects[0] {
            Effect::Send {
                message: GossipMessage::Rumors(rumors),
                ..
            } => {
                let payloads: Vec<_> = rumors.iter().map(|rumor| *rumor.payload()).collect();

                assert_eq!(payloads, vec!["second", "third"]);
            }
            Effect::Emit(()) => panic!("expected send effect"),
        }
    }

    #[test]
    fn publishes_local_rumor_with_self_as_origin() {
        let mut node = GossipNode::new(NodeId::from("node-a"), GossipConfig::default());

        let outcome = node.publish(MessageId::new(1), Round::new(7), "hello");

        assert_eq!(outcome, InsertOutcome::Inserted);

        let stored = node
            .get_rumor(MessageId::new(1))
            .expect("rumor should be stored");

        assert_eq!(stored.origin(), &NodeId::from("node-a"));
        assert_eq!(stored.created_at(), Round::new(7));
        assert_eq!(stored.payload(), &"hello");
    }

    #[test]
    fn publishes_many_local_rumors_with_generated_ids() {
        let mut node = GossipNode::new(NodeId::from("node-a"), GossipConfig::default());
        let mut ids = MessageIdGenerator::new(10);

        let outcomes = node
            .publish_many(&mut ids, Round::new(7), ["first", "second", "third"])
            .expect("generator has enough IDs");

        assert_eq!(
            outcomes,
            vec![
                InsertOutcome::Inserted,
                InsertOutcome::Inserted,
                InsertOutcome::Inserted,
            ]
        );
        assert_eq!(ids.peek(), Some(MessageId::new(13)));
        assert_eq!(node.rumor_count(), 3);
        assert_eq!(
            node.get_rumor(MessageId::new(10))
                .expect("rumor should exist")
                .payload(),
            &"first"
        );
        assert_eq!(
            node.get_rumor(MessageId::new(11))
                .expect("rumor should exist")
                .payload(),
            &"second"
        );
        assert_eq!(
            node.get_rumor(MessageId::new(12))
                .expect("rumor should exist")
                .payload(),
            &"third"
        );
    }

    #[test]
    fn publish_many_does_not_partially_insert_when_generator_is_exhausted() {
        let mut node = GossipNode::new(NodeId::from("node-a"), GossipConfig::default());
        let mut ids = MessageIdGenerator::new(u128::MAX);

        let error = node
            .publish_many(&mut ids, Round::new(7), ["first", "second"])
            .expect_err("generator has only one ID left");

        assert_eq!(error, PublishManyError::MessageIdGeneratorExhausted);
        assert_eq!(ids.peek(), Some(MessageId::new(u128::MAX)));
        assert_eq!(node.rumor_count(), 0);
    }
}
