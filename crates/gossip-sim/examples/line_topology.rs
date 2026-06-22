use gossip_core::{GossipConfig, MessageId, NodeId, Round};
use gossip_sim::Cluster;

fn main() {
    let mut cluster = Cluster::line(GossipConfig::new(1, 32).expect("valid config"), 5);
    let rumor_id = MessageId::new(1);

    cluster
        .publish(
            &NodeId::from("node-0"),
            rumor_id,
            Round::ZERO,
            "line topology rumor",
        )
        .expect("origin node should exist");

    println!(
        "line topology reach before ticks: {}/{}",
        cluster.rumor_reach(rumor_id),
        cluster.node_count()
    );

    for round in 0..8 {
        let report = cluster.tick(Round::new(round));

        println!(
            "round {}: attempted={}, sent={}, received={}, new_rumors={}, reach={}/{}",
            round,
            report.attempted(),
            report.sent(),
            report.received(),
            report.new_rumors(),
            cluster.rumor_reach(rumor_id),
            cluster.node_count()
        );

        if !cluster.all_know(rumor_id) {
            println!("  missing: {:?}", cluster.unknown_by(rumor_id));
        }
    }
}
