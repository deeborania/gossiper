use gossiper_core::{GossipConfig, MessageId, NodeId, Round};
use gossiper_sim::Cluster;

fn main() {
    let mut cluster = Cluster::fully_connected(GossipConfig::new(2, 32).expect("valid config"), 5);

    let rumor_id = MessageId::new(1);

    cluster
        .publish(
            &NodeId::from("node-0"),
            rumor_id,
            Round::ZERO,
            "cluster config changed",
        )
        .expect("origin node should exist");

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
