use gossip_core::{GossipConfig, MessageId, NodeId, Round};
use gossip_sim::Cluster;

fn main() {
    let node_ids = vec![
        NodeId::from("node-a"),
        NodeId::from("node-b"),
        NodeId::from("node-c"),
        NodeId::from("node-d"),
        NodeId::from("node-e"),
    ];

    let mut cluster = Cluster::new(
        GossipConfig::new(2, 32).expect("valid config"),
        node_ids.clone(),
    );

    let rumor_id = MessageId::new(1);

    cluster
        .publish(
            &NodeId::from("node-a"),
            rumor_id,
            Round::ZERO,
            "cluster config changed",
        )
        .expect("node-a should exist");

    println!(
        "round 0 reach before tick: {}/{}",
        cluster.rumor_reach(rumor_id),
        cluster.node_count()
    );

    for round in 0..5 {
        let report = cluster.tick(Round::new(round));

        println!(
            "round {} sent {} messages, reach: {}/{}",
            round,
            report.sent(),
            cluster.rumor_reach(rumor_id),
            cluster.node_count()
        );

        if !cluster.all_know(rumor_id) {
            println!("  missing: {:?}", cluster.unknown_by(rumor_id));
        }
    }
}
