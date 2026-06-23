use std::collections::BTreeMap;

use gossiper_core::{
    delta_message, merge_delta, AntiEntropyMessage, DeltaStore, Digest, Merge, MergeOutcome,
    MergeReport,
};

#[derive(Clone, Debug, PartialEq, Eq)]
struct VersionedRecord {
    key: String,
    version: u64,
    writer: String,
    value: String,
}

impl VersionedRecord {
    fn new(
        key: impl Into<String>,
        version: u64,
        writer: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        Self {
            key: key.into(),
            version,
            writer: writer.into(),
            value: value.into(),
        }
    }

    fn id(&self) -> VersionedRecordId {
        VersionedRecordId {
            key: self.key.clone(),
            version: self.version,
            writer: self.writer.clone(),
        }
    }

    fn is_newer_than(&self, other: &Self) -> bool {
        (self.version, self.writer.as_str()) > (other.version, other.writer.as_str())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct VersionedRecordId {
    key: String,
    version: u64,
    writer: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct VersionStamp {
    version: u64,
    writer: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct VersionDigest {
    versions: BTreeMap<String, VersionStamp>,
}

impl Digest for VersionDigest {
    type ItemId = VersionedRecordId;

    fn contains(&self, id: &Self::ItemId) -> bool {
        self.versions.get(&id.key).is_some_and(|known| {
            (known.version, known.writer.as_str()) >= (id.version, id.writer.as_str())
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct VersionedKvStore {
    records: BTreeMap<String, VersionedRecord>,
}

impl VersionedKvStore {
    fn insert_local(
        &mut self,
        key: impl Into<String>,
        version: u64,
        writer: impl Into<String>,
        value: impl Into<String>,
    ) {
        let record = VersionedRecord::new(key, version, writer, value);
        self.records.insert(record.key.clone(), record);
    }

    fn get(&self, key: &str) -> Option<&VersionedRecord> {
        self.records.get(key)
    }

    fn print_state(&self, node_name: &str) {
        println!("{node_name}:");

        for record in self.records.values() {
            println!(
                "  {} = {} (version {}, writer {})",
                record.key, record.value, record.version, record.writer
            );
        }
    }
}

impl DeltaStore for VersionedKvStore {
    type ItemId = VersionedRecordId;
    type Item = VersionedRecord;
    type Digest = VersionDigest;

    fn digest(&self) -> Self::Digest {
        let versions = self
            .records
            .values()
            .map(|record| {
                (
                    record.key.clone(),
                    VersionStamp {
                        version: record.version,
                        writer: record.writer.clone(),
                    },
                )
            })
            .collect();

        VersionDigest { versions }
    }

    fn delta<D>(&self, remote_digest: &D) -> Vec<Self::Item>
    where
        D: Digest<ItemId = Self::ItemId>,
    {
        self.records
            .values()
            .filter(|record| !remote_digest.contains(&record.id()))
            .cloned()
            .collect()
    }
}

impl Merge for VersionedKvStore {
    type Item = VersionedRecord;

    fn merge(&mut self, item: Self::Item) -> MergeOutcome {
        match self.records.get(&item.key) {
            Some(current) if !item.is_newer_than(current) => MergeOutcome::Unchanged,
            _ => {
                self.records.insert(item.key.clone(), item);
                MergeOutcome::Changed
            }
        }
    }
}

fn exchange_both_ways(
    node_a: &mut VersionedKvStore,
    node_b: &mut VersionedKvStore,
) -> (MergeReport, MergeReport) {
    let node_a_digest = node_a.digest();
    let node_b_digest = node_b.digest();

    let to_node_b = delta_message(node_a, &node_b_digest);
    let to_node_a = delta_message(node_b, &node_a_digest);

    let report_b = match to_node_b {
        AntiEntropyMessage::Digest(_) => unreachable!("delta_message returns a delta"),
        AntiEntropyMessage::Delta(items) => merge_delta(node_b, items),
    };

    let report_a = match to_node_a {
        AntiEntropyMessage::Digest(_) => unreachable!("delta_message returns a delta"),
        AntiEntropyMessage::Delta(items) => merge_delta(node_a, items),
    };

    (report_a, report_b)
}

fn main() {
    let mut node_a = VersionedKvStore::default();
    let mut node_b = VersionedKvStore::default();

    node_a.insert_local("service/api", 3, "node-a", "10.0.0.9:8080");
    node_a.insert_local("service/cache", 1, "node-a", "10.0.0.11:6379");

    node_b.insert_local("service/api", 3, "node-b", "10.0.0.8:8080");
    node_b.insert_local("service/db", 1, "node-b", "10.0.0.12:5432");

    println!("before anti-entropy");
    node_a.print_state("node_a");
    node_b.print_state("node_b");

    let (report_a, report_b) = exchange_both_ways(&mut node_a, &mut node_b);

    println!();
    println!("merge reports");
    println!(
        "  node_a changed {}, unchanged {}",
        report_a.changed(),
        report_a.unchanged()
    );
    println!(
        "  node_b changed {}, unchanged {}",
        report_b.changed(),
        report_b.unchanged()
    );

    println!();
    println!("after anti-entropy");
    node_a.print_state("node_a");
    node_b.print_state("node_b");

    assert_eq!(node_a.get("service/api"), node_b.get("service/api"));
    assert_eq!(node_a.get("service/cache"), node_b.get("service/cache"));
    assert_eq!(node_a.get("service/db"), node_b.get("service/db"));
    assert_eq!(
        node_a
            .get("service/api")
            .map(|record| record.writer.as_str()),
        Some("node-b")
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equal_version_conflicts_converge_with_writer_tie_breaker() {
        let mut node_a = VersionedKvStore::default();
        let mut node_b = VersionedKvStore::default();

        node_a.insert_local("service/api", 3, "node-a", "10.0.0.9:8080");
        node_b.insert_local("service/api", 3, "node-b", "10.0.0.8:8080");

        exchange_both_ways(&mut node_a, &mut node_b);

        assert_eq!(node_a.get("service/api"), node_b.get("service/api"));
        assert_eq!(
            node_a
                .get("service/api")
                .map(|record| record.writer.as_str()),
            Some("node-b")
        );
    }

    #[test]
    fn older_versions_do_not_replace_newer_versions() {
        let mut node_a = VersionedKvStore::default();
        let mut node_b = VersionedKvStore::default();

        node_a.insert_local("service/api", 4, "node-a", "10.0.0.9:8080");
        node_b.insert_local("service/api", 3, "node-b", "10.0.0.8:8080");

        exchange_both_ways(&mut node_a, &mut node_b);

        assert_eq!(node_a.get("service/api"), node_b.get("service/api"));
        assert_eq!(
            node_a.get("service/api").map(|record| record.version),
            Some(4)
        );
        assert_eq!(
            node_a
                .get("service/api")
                .map(|record| record.writer.as_str()),
            Some("node-a")
        );
    }
}
