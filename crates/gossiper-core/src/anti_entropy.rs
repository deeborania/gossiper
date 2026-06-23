//! Anti-entropy traits for digest/delta synchronization.

use std::collections::BTreeSet;

use crate::{InsertOutcome, MessageId, Rumor, RumorStore};

/// A compact summary of known item IDs.
///
/// A digest does not need to contain the full items. It only needs enough
/// information to answer whether an item is probably or definitely known.
pub trait Digest {
    /// The identifier type summarized by this digest.
    type ItemId;

    /// Returns `true` if the digest says this item is known.
    fn contains(&self, id: &Self::ItemId) -> bool;
}

/// A simple exact digest backed by a sorted set of item IDs.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
    feature = "serde",
    serde(bound(
        serialize = "ItemId: serde::Serialize",
        deserialize = "ItemId: Ord + serde::Deserialize<'de>"
    ))
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdSetDigest<ItemId> {
    ids: BTreeSet<ItemId>,
}

impl<ItemId> IdSetDigest<ItemId>
where
    ItemId: Ord,
{
    /// Creates an empty digest.
    pub fn new() -> Self {
        Self {
            ids: BTreeSet::new(),
        }
    }

    /// Creates a digest from item IDs.
    pub fn from_ids(ids: impl IntoIterator<Item = ItemId>) -> Self {
        Self {
            ids: ids.into_iter().collect(),
        }
    }

    /// Returns the number of IDs in the digest.
    pub fn len(&self) -> usize {
        self.ids.len()
    }

    /// Returns `true` if the digest contains no IDs.
    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }
}

impl<ItemId> Default for IdSetDigest<ItemId>
where
    ItemId: Ord,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<ItemId> Digest for IdSetDigest<ItemId>
where
    ItemId: Ord,
{
    type ItemId = ItemId;

    fn contains(&self, id: &Self::ItemId) -> bool {
        self.ids.contains(id)
    }
}

impl<ItemId> FromIterator<ItemId> for IdSetDigest<ItemId>
where
    ItemId: Ord,
{
    fn from_iter<T: IntoIterator<Item = ItemId>>(iter: T) -> Self {
        Self::from_ids(iter)
    }
}

/// A store that can summarize itself and produce missing items for a peer.
pub trait DeltaStore {
    /// The item identifier type.
    type ItemId;

    /// The full item type sent as a delta.
    type Item;

    /// The digest type produced by this store.
    type Digest: Digest<ItemId = Self::ItemId>;

    /// Returns a compact summary of local state.
    fn digest(&self) -> Self::Digest;

    /// Returns items known locally but missing from `remote_digest`.
    fn delta<D>(&self, remote_digest: &D) -> Vec<Self::Item>
    where
        D: Digest<ItemId = Self::ItemId>;
}

/// Result of merging one incoming item into local state.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MergeOutcome {
    /// The item changed local state.
    Changed,

    /// The item was already known or otherwise did not change local state.
    Unchanged,
}

/// Summary of applying a delta batch.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MergeReport {
    changed: usize,
    unchanged: usize,
}

impl MergeReport {
    /// Returns how many items changed local state.
    pub fn changed(&self) -> usize {
        self.changed
    }

    /// Returns how many items did not change local state.
    pub fn unchanged(&self) -> usize {
        self.unchanged
    }

    /// Returns the total number of merged items.
    pub fn total(&self) -> usize {
        self.changed + self.unchanged
    }

    /// Returns `true` if at least one item changed local state.
    pub fn has_changes(&self) -> bool {
        self.changed > 0
    }
}

/// A protocol message for digest/delta anti-entropy exchange.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AntiEntropyMessage<D, I> {
    /// A compact summary of local state.
    Digest(D),

    /// Full items missing from the receiver's state.
    Delta(Vec<I>),
}

impl<D, I> AntiEntropyMessage<D, I> {
    /// Creates a digest message.
    pub fn digest(digest: D) -> Self {
        Self::Digest(digest)
    }

    /// Creates a delta message.
    pub fn delta(items: Vec<I>) -> Self {
        Self::Delta(items)
    }

    /// Returns `true` if this message carries no useful delta items.
    pub fn is_empty_delta(&self) -> bool {
        match self {
            Self::Digest(_) => false,
            Self::Delta(items) => items.is_empty(),
        }
    }

    /// Returns the number of delta items in this message.
    pub fn delta_len(&self) -> usize {
        match self {
            Self::Digest(_) => 0,
            Self::Delta(items) => items.len(),
        }
    }
}

/// A state container that can merge incoming anti-entropy items.
pub trait Merge {
    /// The incoming item type.
    type Item;

    /// Merges one item into local state.
    fn merge(&mut self, item: Self::Item) -> MergeOutcome;
}

/// Builds a digest message from a store.
pub fn digest_message<S>(store: &S) -> AntiEntropyMessage<S::Digest, S::Item>
where
    S: DeltaStore,
{
    AntiEntropyMessage::digest(store.digest())
}

/// Builds a delta message containing items missing from a remote digest.
pub fn delta_message<S, D>(store: &S, remote_digest: &D) -> AntiEntropyMessage<S::Digest, S::Item>
where
    S: DeltaStore,
    D: Digest<ItemId = S::ItemId>,
{
    AntiEntropyMessage::delta(store.delta(remote_digest))
}

/// Merges a batch of incoming delta items.
pub fn merge_delta<S>(store: &mut S, items: impl IntoIterator<Item = S::Item>) -> MergeReport
where
    S: Merge,
{
    let mut report = MergeReport::default();

    for item in items {
        match store.merge(item) {
            MergeOutcome::Changed => report.changed += 1,
            MergeOutcome::Unchanged => report.unchanged += 1,
        }
    }

    report
}

impl<T> DeltaStore for RumorStore<T>
where
    T: Clone,
{
    type ItemId = MessageId;
    type Item = Rumor<T>;
    type Digest = IdSetDigest<MessageId>;

    fn digest(&self) -> Self::Digest {
        self.iter_in_insertion_order().map(Rumor::id).collect()
    }

    fn delta<D>(&self, remote_digest: &D) -> Vec<Self::Item>
    where
        D: Digest<ItemId = Self::ItemId>,
    {
        self.iter_in_insertion_order()
            .filter(|rumor| !remote_digest.contains(&rumor.id()))
            .cloned()
            .collect()
    }
}

impl<T> Merge for RumorStore<T> {
    type Item = Rumor<T>;

    fn merge(&mut self, item: Self::Item) -> MergeOutcome {
        match self.insert(item) {
            InsertOutcome::Inserted | InsertOutcome::InsertedWithEviction { .. } => {
                MergeOutcome::Changed
            }
            InsertOutcome::Duplicate => MergeOutcome::Unchanged,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        delta_message, digest_message, merge_delta, AntiEntropyMessage, DeltaStore, Digest,
        IdSetDigest, Merge, MergeOutcome,
    };
    use crate::{MessageId, NodeId, Round, Rumor, RumorStore};

    fn rumor(id: u128, payload: &'static str) -> Rumor<&'static str> {
        Rumor::new(
            MessageId::new(id),
            NodeId::from("node-a"),
            Round::new(0),
            payload,
        )
    }

    #[test]
    fn id_set_digest_reports_known_ids() {
        let digest = IdSetDigest::from_ids([MessageId::new(1), MessageId::new(2)]);

        assert_eq!(digest.len(), 2);
        assert!(digest.contains(&MessageId::new(1)));
        assert!(!digest.contains(&MessageId::new(3)));
    }

    #[test]
    fn rumor_store_builds_digest_from_known_rumors() {
        let mut store = RumorStore::new(3);
        store.insert(rumor(1, "one"));
        store.insert(rumor(2, "two"));

        let digest = store.digest();

        assert!(digest.contains(&MessageId::new(1)));
        assert!(digest.contains(&MessageId::new(2)));
        assert!(!digest.contains(&MessageId::new(3)));
    }

    #[test]
    fn rumor_store_delta_returns_only_missing_rumors() {
        let mut store = RumorStore::new(3);
        store.insert(rumor(1, "one"));
        store.insert(rumor(2, "two"));
        store.insert(rumor(3, "three"));

        let remote_digest = IdSetDigest::from_ids([MessageId::new(1), MessageId::new(3)]);
        let delta = store.delta(&remote_digest);

        assert_eq!(delta.len(), 1);
        assert_eq!(delta[0].id(), MessageId::new(2));
        assert_eq!(delta[0].payload(), &"two");
    }

    #[test]
    fn rumor_store_merge_reports_whether_state_changed() {
        let mut store = RumorStore::new(3);
        let first = rumor(1, "one");

        assert_eq!(store.merge(first.clone()), MergeOutcome::Changed);
        assert_eq!(store.merge(first), MergeOutcome::Unchanged);
    }

    #[test]
    fn anti_entropy_message_reports_delta_size() {
        let digest = IdSetDigest::from_ids([MessageId::new(1)]);
        let digest_message: AntiEntropyMessage<_, Rumor<&str>> = AntiEntropyMessage::digest(digest);
        let delta_message: AntiEntropyMessage<IdSetDigest<MessageId>, _> =
            AntiEntropyMessage::delta(vec![rumor(2, "two"), rumor(3, "three")]);
        let empty_delta: AntiEntropyMessage<IdSetDigest<MessageId>, Rumor<&str>> =
            AntiEntropyMessage::delta(Vec::new());

        assert_eq!(digest_message.delta_len(), 0);
        assert!(!digest_message.is_empty_delta());
        assert_eq!(delta_message.delta_len(), 2);
        assert!(!delta_message.is_empty_delta());
        assert_eq!(empty_delta.delta_len(), 0);
        assert!(empty_delta.is_empty_delta());
    }

    #[test]
    fn digest_message_builds_digest_from_store() {
        let mut store = RumorStore::new(3);
        store.insert(rumor(1, "one"));

        let message = digest_message(&store);

        match message {
            AntiEntropyMessage::Digest(digest) => {
                assert!(digest.contains(&MessageId::new(1)));
            }
            AntiEntropyMessage::Delta(_) => panic!("expected digest message"),
        }
    }

    #[test]
    fn delta_message_builds_items_missing_from_remote_digest() {
        let mut store = RumorStore::new(3);
        store.insert(rumor(1, "one"));
        store.insert(rumor(2, "two"));

        let remote_digest = IdSetDigest::from_ids([MessageId::new(1)]);
        let message = delta_message(&store, &remote_digest);

        match message {
            AntiEntropyMessage::Digest(_) => panic!("expected delta message"),
            AntiEntropyMessage::Delta(items) => {
                assert_eq!(items.len(), 1);
                assert_eq!(items[0].id(), MessageId::new(2));
                assert_eq!(items[0].payload(), &"two");
            }
        }
    }

    #[test]
    fn merge_delta_reports_changed_and_unchanged_items() {
        let mut store = RumorStore::new(3);
        let known = rumor(1, "one");
        let missing = rumor(2, "two");

        store.insert(known.clone());

        let report = merge_delta(&mut store, [known, missing]);

        assert_eq!(report.changed(), 1);
        assert_eq!(report.unchanged(), 1);
        assert_eq!(report.total(), 2);
        assert!(report.has_changes());
        assert!(store.contains(MessageId::new(2)));
    }
}
