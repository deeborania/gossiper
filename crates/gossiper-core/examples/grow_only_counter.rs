use std::collections::BTreeMap;

use gossiper_core::{
    delta_message, merge_delta, AntiEntropyMessage, DeltaStore, Digest, Merge, MergeOutcome,
};

#[derive(Clone, Debug, PartialEq, Eq)]
struct CounterCell {
    replica: String,
    count: u64,
}

impl CounterCell {
    fn new(replica: impl Into<String>, count: u64) -> Self {
        Self {
            replica: replica.into(),
            count,
        }
    }

    fn id(&self) -> CounterCellId {
        CounterCellId {
            replica: self.replica.clone(),
            count: self.count,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CounterCellId {
    replica: String,
    count: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct CounterDigest {
    counts: BTreeMap<String, u64>,
}

impl Digest for CounterDigest {
    type ItemId = CounterCellId;

    fn contains(&self, id: &Self::ItemId) -> bool {
        self.counts
            .get(&id.replica)
            .is_some_and(|known_count| *known_count >= id.count)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct GrowOnlyCounter {
    counts: BTreeMap<String, u64>,
}

impl GrowOnlyCounter {
    fn increment(&mut self, replica: impl Into<String>, amount: u64) {
        *self.counts.entry(replica.into()).or_default() += amount;
    }

    fn value(&self) -> u64 {
        self.counts.values().sum()
    }

    fn print_state(&self, node_name: &str) {
        println!("{node_name}: total = {}", self.value());

        for (replica, count) in &self.counts {
            println!("  {replica}: {count}");
        }
    }
}

impl DeltaStore for GrowOnlyCounter {
    type ItemId = CounterCellId;
    type Item = CounterCell;
    type Digest = CounterDigest;

    fn digest(&self) -> Self::Digest {
        CounterDigest {
            counts: self.counts.clone(),
        }
    }

    fn delta<D>(&self, remote_digest: &D) -> Vec<Self::Item>
    where
        D: Digest<ItemId = Self::ItemId>,
    {
        self.counts
            .iter()
            .map(|(replica, count)| CounterCell::new(replica.clone(), *count))
            .filter(|cell| !remote_digest.contains(&cell.id()))
            .collect()
    }
}

impl Merge for GrowOnlyCounter {
    type Item = CounterCell;

    fn merge(&mut self, item: Self::Item) -> MergeOutcome {
        let current = self.counts.entry(item.replica).or_default();

        if item.count > *current {
            *current = item.count;
            MergeOutcome::Changed
        } else {
            MergeOutcome::Unchanged
        }
    }
}

fn exchange_both_ways(node_a: &mut GrowOnlyCounter, node_b: &mut GrowOnlyCounter) {
    let node_a_digest = node_a.digest();
    let node_b_digest = node_b.digest();

    let to_node_b = delta_message(node_a, &node_b_digest);
    let to_node_a = delta_message(node_b, &node_a_digest);

    if let AntiEntropyMessage::Delta(items) = to_node_b {
        merge_delta(node_b, items);
    }

    if let AntiEntropyMessage::Delta(items) = to_node_a {
        merge_delta(node_a, items);
    }
}

fn main() {
    let mut node_a = GrowOnlyCounter::default();
    let mut node_b = GrowOnlyCounter::default();

    node_a.increment("node-a", 3);
    node_b.increment("node-b", 2);

    println!("before anti-entropy");
    node_a.print_state("node_a");
    node_b.print_state("node_b");

    exchange_both_ways(&mut node_a, &mut node_b);

    println!();
    println!("after anti-entropy");
    node_a.print_state("node_a");
    node_b.print_state("node_b");

    assert_eq!(node_a, node_b);
    assert_eq!(node_a.value(), 5);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replicas_converge_to_the_sum_of_per_replica_counts() {
        let mut node_a = GrowOnlyCounter::default();
        let mut node_b = GrowOnlyCounter::default();

        node_a.increment("node-a", 3);
        node_b.increment("node-b", 2);

        exchange_both_ways(&mut node_a, &mut node_b);

        assert_eq!(node_a, node_b);
        assert_eq!(node_a.value(), 5);
    }

    #[test]
    fn stale_counts_do_not_replace_newer_counts() {
        let mut counter = GrowOnlyCounter::default();

        assert_eq!(
            counter.merge(CounterCell::new("node-a", 3)),
            MergeOutcome::Changed
        );
        assert_eq!(
            counter.merge(CounterCell::new("node-a", 2)),
            MergeOutcome::Unchanged
        );
        assert_eq!(counter.value(), 3);
    }
}
