use gossiper::{
    apply_effects, DeterministicRng, GossipConfig, GossipMessage, GossipNode, MessageId, NodeId,
    Round,
};

fn main() {
    let config = GossipConfig::new(1, 10).expect("valid config");

    let node_a_id = NodeId::from("node-a");
    let node_b_id = NodeId::from("node-b");

    let mut node_a = GossipNode::new(node_a_id.clone(), config.clone());
    let mut node_b = GossipNode::new(node_b_id.clone(), config);

    node_a.set_peers(vec![node_b_id.clone()]);

    let rumor_id = MessageId::new(1);
    node_a.publish(rumor_id, Round::ZERO, "cluster config changed");

    let mut rng = DeterministicRng::new(1);
    let effects = node_a.tick(&mut rng, Round::ZERO);

    let mut transport = gossiper::InMemoryTransport::<GossipMessage<&str>>::new();
    let report = apply_effects(&mut transport, effects);

    println!("sent messages: {}", report.sent());

    for message in transport.drain(&node_b_id) {
        node_b.receive(message);
    }

    println!(
        "node-b learned rumor {}: {}",
        rumor_id,
        node_b.contains_rumor(rumor_id)
    );
}
