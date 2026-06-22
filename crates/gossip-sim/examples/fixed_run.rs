use gossip_core::{GossipConfig, MessageId, NodeId, Round};
use gossip_sim::ClusterBuilder;

fn percent(rate: f64) -> f64 {
    rate * 100.0
}

fn main() {
    let mut cluster = ClusterBuilder::new(GossipConfig::new(2, 32).expect("valid config"))
        .with_node_count(6)
        .with_seed(7)
        .with_loss_rate(0.25)
        .expect("valid loss rate")
        .with_duplicate_rate(0.25)
        .expect("valid duplicate rate")
        .fully_connected();
    let rumor_id = MessageId::new(1);

    cluster
        .publish(
            &NodeId::from("node-0"),
            rumor_id,
            Round::ZERO,
            "fixed run rumor",
        )
        .expect("origin node should exist");

    let report = cluster.run_for_rounds(Round::ZERO, 5);

    println!("fixed run summary");
    println!("  rounds: {}", report.rounds_run());
    println!("  attempted: {}", report.attempted());
    println!("  sent: {}", report.sent());
    println!("  dropped: {}", report.dropped());
    println!("  duplicated: {}", report.duplicated());
    println!("  delayed: {}", report.delayed());
    println!("  received: {}", report.received());
    println!("  new rumors: {}", report.new_rumors());
    println!(
        "  observed drop rate: {:.2}%",
        percent(report.observed_drop_rate())
    );
    println!(
        "  observed duplicate rate: {:.2}%",
        percent(report.observed_duplicate_rate())
    );
    println!("  new rumor rate: {:.2}%", percent(report.new_rumor_rate()));
    println!(
        "  reach: {}/{}",
        cluster.rumor_reach(rumor_id),
        cluster.node_count()
    );
    println!("  missing: {:?}", cluster.unknown_by(rumor_id));
}
