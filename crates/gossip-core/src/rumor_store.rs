//! Bounded storage for gossip rumors.

use std::collections::{BTreeMap, VecDeque};

use crate::{MessageId, Round, Rumor};

/// Result of inserting a rumor into a store.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InsertOutcome {
    /// The rumor was newly inserted.
    Inserted,

    /// The rumor was already known.
    Duplicate,

    /// The rumor was inserted and the oldest rumor was evicted.
    InsertedWithEviction {
        /// The ID of the evicted rumor.
        evicted: MessageId,
    },
}

/// Bounded rumor storage with duplicate suppression.
///
/// The store keeps insertion order so it can evict the oldest rumor when it
/// reaches capacity.
#[derive(Clone, Debug)]
pub struct RumorStore<T> {
    max_rumors: usize,
    order: VecDeque<MessageId>,
    rumors: BTreeMap<MessageId, Rumor<T>>,
}

impl<T> RumorStore<T> {
    /// Creates a rumor store with a fixed capacity.
    ///
    /// Panics if `max_rumors` is zero. Use `GossipConfig` when accepting user
    /// configuration so invalid values are rejected before this point.
    pub fn new(max_rumors: usize) -> Self {
        assert!(max_rumors > 0, "max_rumors must be greater than zero");

        Self {
            max_rumors,
            order: VecDeque::new(),
            rumors: BTreeMap::new(),
        }
    }

    /// Returns the number of rumors currently stored.
    pub fn len(&self) -> usize {
        self.rumors.len()
    }

    /// Returns `true` if the store has no rumors.
    pub fn is_empty(&self) -> bool {
        self.rumors.is_empty()
    }

    /// Returns `true` if the store already knows this message ID.
    pub fn contains(&self, id: MessageId) -> bool {
        self.rumors.contains_key(&id)
    }

    /// Returns a rumor by ID.
    pub fn get(&self, id: MessageId) -> Option<&Rumor<T>> {
        self.rumors.get(&id)
    }

    /// Inserts a rumor, suppressing duplicates and evicting the oldest item when
    /// the store is full.
    pub fn insert(&mut self, rumor: Rumor<T>) -> InsertOutcome {
        let id = rumor.id();

        if self.rumors.contains_key(&id) {
            return InsertOutcome::Duplicate;
        }

        if self.rumors.len() == self.max_rumors {
            let evicted = self
                .order
                .pop_front()
                .expect("order should contain an id when store is full");
            self.rumors.remove(&evicted);
            self.order.push_back(id);
            self.rumors.insert(id, rumor);

            return InsertOutcome::InsertedWithEviction { evicted };
        }

        self.order.push_back(id);
        self.rumors.insert(id, rumor);

        InsertOutcome::Inserted
    }

    /// Removes rumors created before `minimum_round`.
    ///
    /// Returns the number of removed rumors.
    pub fn prune_older_than(&mut self, minimum_round: Round) -> usize {
        let before = self.rumors.len();

        self.rumors
            .retain(|_, rumor| rumor.created_at() >= minimum_round);

        self.order.retain(|id| self.rumors.contains_key(id));

        before - self.rumors.len()
    }

    /// Returns rumors in insertion order.
    pub fn iter_in_insertion_order(&self) -> impl Iterator<Item = &Rumor<T>> {
        self.order.iter().filter_map(|id| self.rumors.get(id))
    }
}

#[cfg(test)]
mod tests {
    use super::{InsertOutcome, RumorStore};
    use crate::{MessageId, NodeId, Round, Rumor};

    fn rumor(id: u128, payload: &'static str) -> Rumor<&'static str> {
        Rumor::new(
            MessageId::new(id),
            NodeId::from("node-a"),
            Round::new(0),
            payload,
        )
    }

    #[test]
    fn starts_empty() {
        let store: RumorStore<&str> = RumorStore::new(3);

        assert!(store.is_empty());
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn inserts_new_rumor() {
        let mut store = RumorStore::new(3);

        let outcome = store.insert(rumor(1, "hello"));

        assert_eq!(outcome, InsertOutcome::Inserted);
        assert!(store.contains(MessageId::new(1)));
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn suppresses_duplicate_rumor() {
        let mut store = RumorStore::new(3);

        assert_eq!(store.insert(rumor(1, "hello")), InsertOutcome::Inserted);
        assert_eq!(
            store.insert(rumor(1, "hello again")),
            InsertOutcome::Duplicate
        );

        assert_eq!(store.len(), 1);
        assert_eq!(
            store
                .get(MessageId::new(1))
                .expect("rumor should exist")
                .payload(),
            &"hello"
        );
    }

    #[test]
    fn evicts_oldest_rumor_when_full() {
        let mut store = RumorStore::new(2);

        assert_eq!(store.insert(rumor(1, "first")), InsertOutcome::Inserted);
        assert_eq!(store.insert(rumor(2, "second")), InsertOutcome::Inserted);
        assert_eq!(
            store.insert(rumor(3, "third")),
            InsertOutcome::InsertedWithEviction {
                evicted: MessageId::new(1)
            }
        );

        assert!(!store.contains(MessageId::new(1)));
        assert!(store.contains(MessageId::new(2)));
        assert!(store.contains(MessageId::new(3)));
        assert_eq!(store.len(), 2);
    }

    #[test]
    fn iterates_in_insertion_order() {
        let mut store = RumorStore::new(3);

        store.insert(rumor(1, "first"));
        store.insert(rumor(2, "second"));
        store.insert(rumor(3, "third"));

        let payloads: Vec<_> = store
            .iter_in_insertion_order()
            .map(|rumor| *rumor.payload())
            .collect();

        assert_eq!(payloads, vec!["first", "second", "third"]);
    }

    #[test]
    fn prunes_rumors_older_than_minimum_round() {
        let mut store = RumorStore::new(5);

        store.insert(Rumor::new(
            MessageId::new(1),
            NodeId::from("node-a"),
            Round::new(1),
            "old",
        ));
        store.insert(Rumor::new(
            MessageId::new(2),
            NodeId::from("node-a"),
            Round::new(3),
            "new",
        ));

        let removed = store.prune_older_than(Round::new(3));

        assert_eq!(removed, 1);
        assert!(!store.contains(MessageId::new(1)));
        assert!(store.contains(MessageId::new(2)));
    }

    #[test]
    fn prune_keeps_order_consistent() {
        let mut store = RumorStore::new(5);

        store.insert(Rumor::new(
            MessageId::new(1),
            NodeId::from("node-a"),
            Round::new(1),
            "old",
        ));
        store.insert(Rumor::new(
            MessageId::new(2),
            NodeId::from("node-a"),
            Round::new(3),
            "middle",
        ));
        store.insert(Rumor::new(
            MessageId::new(3),
            NodeId::from("node-a"),
            Round::new(4),
            "new",
        ));

        store.prune_older_than(Round::new(3));

        let payloads: Vec<_> = store
            .iter_in_insertion_order()
            .map(|rumor| *rumor.payload())
            .collect();

        assert_eq!(payloads, vec!["middle", "new"]);
    }
}
