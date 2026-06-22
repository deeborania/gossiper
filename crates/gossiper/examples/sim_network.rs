use gossiper::{Cluster, GossipConfig, MessageId, NetworkModel, NetworkPartition, NodeId, Round};

fn main() {
    let network = NetworkModel::new()
        .with_duplicate_rate(1.0)
        .expect("valid duplicate rate")
        .with_delay_rate(1.0, 1)
        .expect("valid delay rate")
        .with_partition(NetworkPartition::new(
            vec![NodeId::from("node-0")],
            vec![NodeId::from("node-1")],
        ));

    let mut cluster = Cluster::fully_connected(GossipConfig::new(1, 10).expect("valid config"), 2)
        .with_network_model(network);

    let rumor_id = MessageId::new(1);

    cluster
        .publish(
            &NodeId::from("node-0"),
            rumor_id,
            Round::ZERO,
            "facade simulator example",
        )
        .expect("node should exist");

    let blocked = cluster.tick(Round::ZERO);

    println!(
        "partitioned: attempted={}, dropped={}, sent={}, reach={}/{}",
        blocked.attempted(),
        blocked.dropped(),
        blocked.sent(),
        cluster.rumor_reach(rumor_id),
        cluster.node_count(),
    );

    cluster = cluster.without_partitions();

    let healed = cluster.tick(Round::new(1));

    println!(
        "healed: attempted={}, sent={}, delayed={}, received={}, new_rumors={}, reach={}/{}",
        healed.attempted(),
        healed.sent(),
        healed.delayed(),
        healed.received(),
        healed.new_rumors(),
        cluster.rumor_reach(rumor_id),
        cluster.node_count(),
    );

    let delivered = cluster.tick(Round::new(2));

    println!(
        "delivered: sent={}, received={}, new_rumors={}, reach={}/{}",
        delivered.sent(),
        delivered.received(),
        delivered.new_rumors(),
        cluster.rumor_reach(rumor_id),
        cluster.node_count(),
    );
}
