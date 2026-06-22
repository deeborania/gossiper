use gossip_core::{DeterministicRng, GossipConfig, GossipNode, MessageId, NodeId, Round, Rumor};
use gossip_transport::{apply_effects, InMemoryTransport};

fn rumor(id: u128, payload: &'static str) -> Rumor<&'static str> {
    Rumor::new(
        MessageId::new(id),
        NodeId::from("node-a"),
        Round::new(0),
        payload,
    )
}

#[test]
fn in_memory_transport_can_drive_gossip_between_two_nodes() {
    let config = GossipConfig::new(1, 10).expect("valid config");

    let node_a_id = NodeId::from("node-a");
    let node_b_id = NodeId::from("node-b");

    let mut node_a = GossipNode::new(node_a_id.clone(), config.clone());
    let mut node_b = GossipNode::new(node_b_id.clone(), config);

    node_a.set_peers(vec![node_b_id.clone()]);
    node_a.insert_rumor(rumor(1, "hello"));

    let mut rng = DeterministicRng::new(1);
    let effects = node_a.tick(&mut rng, Round::new(0));

    let mut transport = InMemoryTransport::new();
    let report = apply_effects(&mut transport, effects);

    assert_eq!(report.sent(), 1);
    assert!(!report.has_errors());
    assert_eq!(transport.queued_len(&node_b_id), 1);

    for message in transport.drain(&node_b_id) {
        let events = node_b.receive(message);

        assert_eq!(events.len(), 1);
    }

    assert!(node_b.contains_rumor(MessageId::new(1)));
    assert_eq!(
        node_b
            .get_rumor(MessageId::new(1))
            .expect("node-b should know the rumor")
            .payload(),
        &"hello"
    );
}
