use gossiper::{GossipConfig, GossipNode, MessageId, MessageIdGenerator, NodeId, PublishManyError};

#[test]
fn facade_reexports_core_types() {
    let node: GossipNode<&str> = GossipNode::new(NodeId::from("node-a"), GossipConfig::default());
    let mut message_ids = MessageIdGenerator::default();

    assert_eq!(node.self_id(), &NodeId::from("node-a"));
    assert_eq!(message_ids.next_id(), Some(MessageId::new(1)));
    assert_eq!(
        PublishManyError::MessageIdGeneratorExhausted.to_string(),
        "message ID generator exhausted"
    );
}

#[cfg(feature = "transport")]
#[test]
fn facade_reexports_transport_types_by_default() {
    let transport = gossiper::InMemoryTransport::<String>::new();

    assert_eq!(transport.queued_len(&NodeId::from("node-a")), 0);
}

#[cfg(feature = "sim")]
#[test]
fn facade_reexports_sim_types_when_feature_enabled() {
    let experiment = gossiper::ConvergenceExperiment::new(3, 1, 3, 2).expect("valid experiment");

    let report = experiment.run();

    assert_eq!(report.trials(), 2);
}
