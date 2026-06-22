use gossip_core::{GossipConfig, MessageId, NodeId, Round};
use gossip_sim::{Cluster, NetworkModel, NetworkPartition};

fn percent(rate: f64) -> f64 {
    rate * 100.0
}

fn main() {
    delayed_duplicate_delivery();
    partition_and_heal();
}

fn delayed_duplicate_delivery() {
    let config = GossipConfig::new(1, 10).expect("valid config");

    let network = NetworkModel::new()
        .with_duplicate_rate(1.0)
        .expect("valid duplicate rate")
        .with_delay_rate(1.0, 1)
        .expect("valid delay rate");

    let mut cluster = Cluster::fully_connected(config, 2).with_network_model(network);
    let rumor_id = MessageId::new(1);

    cluster
        .publish(
            &NodeId::from("node-0"),
            rumor_id,
            Round::ZERO,
            "hello through an unreliable network",
        )
        .expect("node should exist");

    let first = cluster.tick(Round::ZERO);

    println!("delayed duplicate delivery");
    println!(
        "round 0: attempted={}, sent={}, duplicated={}, delayed={}, pending_delayed={}, delay_rate={:.2}%, received={}, new_rumors={}, reach={}/{}",
        first.attempted(),
        first.sent(),
        first.duplicated(),
        first.delayed(),
        cluster.pending_delayed_count(),
        percent(first.observed_delay_rate()),
        first.received(),
        first.new_rumors(),
        cluster.rumor_reach(rumor_id),
        cluster.node_count(),
    );

    let second = cluster.tick(Round::new(1));

    println!(
        "round 1: attempted={}, sent={}, duplicated={}, delayed={}, pending_delayed={}, delivery_rate={:.2}%, new_rumor_rate={:.2}%, received={}, new_rumors={}, reach={}/{}",
        second.attempted(),
        second.sent(),
        second.duplicated(),
        second.delayed(),
        cluster.pending_delayed_count(),
        percent(second.observed_delivery_rate()),
        percent(second.new_rumor_rate()),
        second.received(),
        second.new_rumors(),
        cluster.rumor_reach(rumor_id),
        cluster.node_count(),
    );
    println!();
}

fn partition_and_heal() {
    let config = GossipConfig::new(1, 10).expect("valid config");

    let partition =
        NetworkPartition::new(vec![NodeId::from("node-0")], vec![NodeId::from("node-1")]);

    let mut cluster = Cluster::fully_connected(config, 2).with_partition(partition);
    let rumor_id = MessageId::new(2);

    cluster
        .publish(
            &NodeId::from("node-0"),
            rumor_id,
            Round::ZERO,
            "hello after healing",
        )
        .expect("node should exist");

    let blocked = cluster.tick(Round::ZERO);

    println!("partition and heal");
    println!(
        "round 0 partitioned: attempted={}, sent={}, dropped={}, drop_rate={:.2}%, received={}, reach={}/{}",
        blocked.attempted(),
        blocked.sent(),
        blocked.dropped(),
        percent(blocked.observed_drop_rate()),
        blocked.received(),
        cluster.rumor_reach(rumor_id),
        cluster.node_count(),
    );

    cluster = cluster.without_partitions();

    let healed = cluster.tick(Round::new(1));

    println!(
        "round 1 healed: attempted={}, sent={}, dropped={}, delivery_rate={:.2}%, received={}, new_rumors={}, reach={}/{}",
        healed.attempted(),
        healed.sent(),
        healed.dropped(),
        percent(healed.observed_delivery_rate()),
        healed.received(),
        healed.new_rumors(),
        cluster.rumor_reach(rumor_id),
        cluster.node_count(),
    );
}
