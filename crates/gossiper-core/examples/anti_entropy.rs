use gossiper_core::{
    delta_message, merge_delta, AntiEntropyMessage, IdSetDigest, MessageId, NodeId, Round, Rumor,
    RumorStore,
};

fn main() {
    let mut node_a = RumorStore::new(8);
    let mut node_b = RumorStore::new(8);

    let first = Rumor::new(
        MessageId::new(1),
        NodeId::from("node-a"),
        Round::ZERO,
        "service-a is healthy",
    );
    let second = Rumor::new(
        MessageId::new(2),
        NodeId::from("node-a"),
        Round::ZERO,
        "service-b moved to 10.0.0.7",
    );

    node_a.insert(first.clone());
    node_a.insert(second);
    node_b.insert(first);

    let node_b_digest = IdSetDigest::from_ids([MessageId::new(1)]);
    let message = delta_message(&node_a, &node_b_digest);

    match message {
        AntiEntropyMessage::Digest(_) => unreachable!("delta_message returns a delta"),
        AntiEntropyMessage::Delta(items) => {
            let report = merge_delta(&mut node_b, items);

            println!("changed: {}", report.changed());
            println!("unchanged: {}", report.unchanged());
            println!(
                "node_b knows rumor 2: {}",
                node_b.contains(MessageId::new(2))
            );
        }
    }
}
