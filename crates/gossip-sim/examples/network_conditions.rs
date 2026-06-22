use gossip_core::{GossipConfig, MessageId, NodeId, Round};
use gossip_sim::{Cluster, NetworkModel, NetworkPartition};

fn main() {
    delayed_duplicate_delivery();
    partition_and_heal();
}

fn delayed_duplicate_delivery() {
    let config = GossipConfig::new(1, 10).expect("valid config");
    let node_ids = vec![NodeId::from("node-a"), NodeId::from("node-b")];

    let network = NetworkModel::new()
        .with_duplicate_rate(1.0)
        .expect("valid duplicate rate")
        .with_delay_rate(1.0, 1)
        .expect("valid delay rate");

    let mut cluster = Cluster::new(config, node_ids).with_network_model(network);
    let rumor_id = MessageId::new(1);

    cluster
        .publish(
            &NodeId::from("node-a"),
            rumor_id,
            Round::ZERO,
            "hello through an unreliable network",
        )
        .expect("node should exist");

    let first = cluster.tick(Round::ZERO);

    println!("delayed duplicate delivery");
    println!(
        "round 0: attempted={}, sent={}, duplicated={}, delayed={}, received={}, new_rumors={}, reach={}/{}",
        first.attempted(),
        first.sent(),
        first.duplicated(),
        first.delayed(),
        first.received(),
        first.new_rumors(),
        cluster.rumor_reach(rumor_id),
        cluster.node_count(),
    );

    let second = cluster.tick(Round::new(1));

    println!(
        "round 1: attempted={}, sent={}, duplicated={}, delayed={}, received={}, new_rumors={}, reach={}/{}",
        second.attempted(),
        second.sent(),
        second.duplicated(),
        second.delayed(),
        second.received(),
        second.new_rumors(),
        cluster.rumor_reach(rumor_id),
        cluster.node_count(),
    );
    println!();
}

fn partition_and_heal() {
    let config = GossipConfig::new(1, 10).expect("valid config");
    let node_ids = vec![NodeId::from("node-a"), NodeId::from("node-b")];

    let partition =
        NetworkPartition::new(vec![NodeId::from("node-a")], vec![NodeId::from("node-b")]);

    let mut cluster = Cluster::new(config, node_ids).with_partition(partition);
    let rumor_id = MessageId::new(2);

    cluster
        .publish(
            &NodeId::from("node-a"),
            rumor_id,
            Round::ZERO,
            "hello after healing",
        )
        .expect("node should exist");

    let blocked = cluster.tick(Round::ZERO);

    println!("partition and heal");
    println!(
        "round 0 partitioned: attempted={}, sent={}, dropped={}, received={}, reach={}/{}",
        blocked.attempted(),
        blocked.sent(),
        blocked.dropped(),
        blocked.received(),
        cluster.rumor_reach(rumor_id),
        cluster.node_count(),
    );

    cluster = cluster.without_partitions();

    let healed = cluster.tick(Round::new(1));

    println!(
        "round 1 healed: attempted={}, sent={}, dropped={}, received={}, new_rumors={}, reach={}/{}",
        healed.attempted(),
        healed.sent(),
        healed.dropped(),
        healed.received(),
        healed.new_rumors(),
        cluster.rumor_reach(rumor_id),
        cluster.node_count(),
    );
}
