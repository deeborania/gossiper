use gossip_core::{GossipConfig, MessageId, NodeId, Round};
use gossip_sim::{Cluster, ClusterBuilder};

fn percent(rate: f64) -> f64 {
    rate * 100.0
}

fn main() {
    let config = GossipConfig::new(2, 64).expect("valid config");

    let mut reliable: Cluster<&'static str> = ClusterBuilder::new(config.clone())
        .with_node_count(8)
        .with_seed(42)
        .fully_connected();

    let mut lossy_line: Cluster<&'static str> = ClusterBuilder::new(config)
        .with_node_ids(vec![
            NodeId::from("edge-a"),
            NodeId::from("relay-a"),
            NodeId::from("relay-b"),
            NodeId::from("edge-b"),
        ])
        .with_seed(7)
        .with_loss_rate(0.20)
        .expect("valid loss rate")
        .with_delay_rate(0.25, 2)
        .expect("valid delay rate")
        .line();

    run_scenario(
        "reliable fully connected",
        &mut reliable,
        &NodeId::from("node-0"),
        MessageId::new(1),
        "builder rumor",
    );

    println!();

    run_scenario(
        "lossy delayed line",
        &mut lossy_line,
        &NodeId::from("edge-a"),
        MessageId::new(2),
        "builder rumor",
    );
}

fn run_scenario(
    label: &str,
    cluster: &mut Cluster<&'static str>,
    origin: &NodeId,
    rumor_id: MessageId,
    payload: &'static str,
) {
    cluster
        .publish(origin, rumor_id, Round::ZERO, payload)
        .expect("origin node should exist");

    let report = cluster.run_for_rounds(Round::ZERO, 8);

    println!("{label}");
    println!("  nodes: {}", cluster.node_count());
    println!("  rounds: {}", report.rounds_run());
    println!(
        "  reach: {}/{}",
        cluster.rumor_reach(rumor_id),
        cluster.node_count()
    );
    println!("  attempted: {}", report.attempted());
    println!("  sent: {}", report.sent());
    println!("  dropped: {}", report.dropped());
    println!("  delayed: {}", report.delayed());
    println!("  received: {}", report.received());
    println!(
        "  observed drop rate: {:.2}%",
        percent(report.observed_drop_rate())
    );
    println!(
        "  observed delay rate: {:.2}%",
        percent(report.observed_delay_rate())
    );
}
