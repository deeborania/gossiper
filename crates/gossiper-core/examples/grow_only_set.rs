use std::collections::BTreeSet;

use gossiper_core::{
    delta_message, merge_delta, AntiEntropyMessage, DeltaStore, Digest, IdSetDigest, Merge,
    MergeOutcome,
};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct GrowOnlySet {
    items: BTreeSet<String>,
}

impl GrowOnlySet {
    fn add(&mut self, item: impl Into<String>) {
        self.items.insert(item.into());
    }

    fn contains(&self, item: &str) -> bool {
        self.items.contains(item)
    }

    fn print_state(&self, node_name: &str) {
        println!("{node_name}:");

        for item in &self.items {
            println!("  {item}");
        }
    }
}

impl DeltaStore for GrowOnlySet {
    type ItemId = String;
    type Item = String;
    type Digest = IdSetDigest<String>;

    fn digest(&self) -> Self::Digest {
        self.items.iter().cloned().collect()
    }

    fn delta<D>(&self, remote_digest: &D) -> Vec<Self::Item>
    where
        D: Digest<ItemId = Self::ItemId>,
    {
        self.items
            .iter()
            .filter(|item| !remote_digest.contains(item))
            .cloned()
            .collect()
    }
}

impl Merge for GrowOnlySet {
    type Item = String;

    fn merge(&mut self, item: Self::Item) -> MergeOutcome {
        if self.items.insert(item) {
            MergeOutcome::Changed
        } else {
            MergeOutcome::Unchanged
        }
    }
}

fn exchange_both_ways(node_a: &mut GrowOnlySet, node_b: &mut GrowOnlySet) {
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
    let mut node_a = GrowOnlySet::default();
    let mut node_b = GrowOnlySet::default();

    node_a.add("service/api");
    node_a.add("service/cache");

    node_b.add("service/api");
    node_b.add("service/db");

    println!("before anti-entropy");
    node_a.print_state("node_a");
    node_b.print_state("node_b");

    exchange_both_ways(&mut node_a, &mut node_b);

    println!();
    println!("after anti-entropy");
    node_a.print_state("node_a");
    node_b.print_state("node_b");

    assert_eq!(node_a, node_b);
    assert!(node_a.contains("service/api"));
    assert!(node_a.contains("service/cache"));
    assert!(node_a.contains("service/db"));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replicas_converge_to_the_union_of_all_items() {
        let mut node_a = GrowOnlySet::default();
        let mut node_b = GrowOnlySet::default();

        node_a.add("service/api");
        node_a.add("service/cache");

        node_b.add("service/api");
        node_b.add("service/db");

        exchange_both_ways(&mut node_a, &mut node_b);

        assert_eq!(node_a, node_b);
        assert!(node_a.contains("service/api"));
        assert!(node_a.contains("service/cache"));
        assert!(node_a.contains("service/db"));
    }

    #[test]
    fn merging_known_items_is_unchanged() {
        let mut set = GrowOnlySet::default();

        assert_eq!(set.merge("service/api".to_owned()), MergeOutcome::Changed);
        assert_eq!(set.merge("service/api".to_owned()), MergeOutcome::Unchanged);
    }
}
