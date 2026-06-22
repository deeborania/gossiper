//! Peer selection utilities.

use crate::{NodeId, RandomSource};

/// Chooses up to `fanout` distinct peers.
///
/// The returned peers:
///
/// - never include `self_id`
/// - never contain duplicates
/// - are selected from `peers` using `rng`
pub fn choose_distinct_peers<R: RandomSource>(
    rng: &mut R,
    self_id: &NodeId,
    peers: &[NodeId],
    fanout: usize,
) -> Vec<NodeId> {
    if fanout == 0 || peers.is_empty() {
        return Vec::new();
    }

    let mut candidates: Vec<NodeId> = peers
        .iter()
        .filter(|peer| *peer != self_id)
        .cloned()
        .collect();

    let limit = fanout.min(candidates.len());
    let mut selected = Vec::with_capacity(limit);

    for index in 0..limit {
        let swap_with = index
            + rng
                .index(candidates.len() - index)
                .expect("non-empty range");
        candidates.swap(index, swap_with);
        selected.push(candidates[index].clone());
    }

    selected
}

#[cfg(test)]
mod tests {
    use super::choose_distinct_peers;
    use crate::{DeterministicRng, NodeId};
    use std::collections::BTreeSet;

    fn node(value: &str) -> NodeId {
        NodeId::from(value)
    }

    #[test]
    fn returns_empty_when_fanout_is_zero() {
        let peers = vec![node("a"), node("b")];
        let mut rng = DeterministicRng::new(1);

        let selected = choose_distinct_peers(&mut rng, &node("a"), &peers, 0);

        assert!(selected.is_empty());
    }

    #[test]
    fn never_selects_self() {
        let self_id = node("a");
        let peers = vec![node("a"), node("b"), node("c")];
        let mut rng = DeterministicRng::new(1);

        let selected = choose_distinct_peers(&mut rng, &self_id, &peers, 2);

        assert!(!selected.contains(&self_id));
    }

    #[test]
    fn does_not_return_duplicates() {
        let self_id = node("a");
        let peers = vec![node("a"), node("b"), node("c"), node("d"), node("e")];
        let mut rng = DeterministicRng::new(2);

        let selected = choose_distinct_peers(&mut rng, &self_id, &peers, 3);
        let unique: BTreeSet<_> = selected.iter().collect();

        assert_eq!(selected.len(), unique.len());
    }

    #[test]
    fn caps_selection_at_available_peers() {
        let self_id = node("a");
        let peers = vec![node("a"), node("b"), node("c")];
        let mut rng = DeterministicRng::new(3);

        let selected = choose_distinct_peers(&mut rng, &self_id, &peers, 10);

        assert_eq!(selected.len(), 2);
    }

    #[test]
    fn same_seed_produces_same_selection() {
        let self_id = node("a");
        let peers = vec![node("a"), node("b"), node("c"), node("d"), node("e")];

        let mut rng_a = DeterministicRng::new(99);
        let mut rng_b = DeterministicRng::new(99);

        let selected_a = choose_distinct_peers(&mut rng_a, &self_id, &peers, 3);
        let selected_b = choose_distinct_peers(&mut rng_b, &self_id, &peers, 3);

        assert_eq!(selected_a, selected_b);
    }
}
