use gossip_core::{
    DeterministicRng, Effect, GossipConfig, GossipMessage, GossipNode, MessageId, NodeId, Round,
    Rumor,
};

fn rumor(id: u128, origin: &str, round: u64, payload: &'static str) -> Rumor<&'static str> {
    Rumor::new(
        MessageId::new(id),
        NodeId::from(origin),
        Round::new(round),
        payload,
    )
}

fn deliver<T: Clone>(
    receiver: &mut GossipNode<T>,
    effects: Vec<Effect<GossipMessage<T>, ()>>,
) -> usize {
    let mut accepted = 0;

    for effect in effects {
        match effect {
            Effect::Send { message, .. } => {
                accepted += receiver.receive(message).len();
            }
            Effect::Emit(()) => {}
        }
    }

    accepted
}

#[test]
fn rumor_can_propagate_across_multiple_nodes() {
    let config = GossipConfig::new(1, 10).expect("valid config");

    let mut node_a = GossipNode::new(NodeId::from("node-a"), config.clone());
    let mut node_b = GossipNode::new(NodeId::from("node-b"), config.clone());
    let mut node_c = GossipNode::new(NodeId::from("node-c"), config);

    node_a.set_peers(vec![NodeId::from("node-b")]);
    node_b.set_peers(vec![NodeId::from("node-c")]);
    node_c.set_peers(Vec::new());

    node_a.insert_rumor(rumor(1, "node-a", 0, "cluster config changed"));

    let mut rng_a = DeterministicRng::new(1);
    let effects_from_a = node_a.tick(&mut rng_a, Round::new(0));
    let accepted_by_b = deliver(&mut node_b, effects_from_a);

    assert_eq!(accepted_by_b, 1);
    assert_eq!(node_b.rumor_count(), 1);

    let mut rng_b = DeterministicRng::new(2);
    let effects_from_b = node_b.tick(&mut rng_b, Round::new(1));
    let accepted_by_c = deliver(&mut node_c, effects_from_b);

    assert_eq!(accepted_by_c, 1);
    assert_eq!(node_c.rumor_count(), 1);
    assert!(node_c.contains_rumor(MessageId::new(1)));
    assert_eq!(
        node_c
            .get_rumor(MessageId::new(1))
            .expect("node-c should know the rumor")
            .payload(),
        &"cluster config changed"
    );
}
