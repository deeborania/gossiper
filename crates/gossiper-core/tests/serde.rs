#![cfg(feature = "serde")]

use gossiper_core::{
    ConfigError, GossipConfig, GossipEvent, GossipMessage, InsertOutcome, MessageId,
    MessageIdGenerator, NodeId, Round, Rumor, Timestamp,
};

#[test]
fn serializes_core_value_types() {
    let node = NodeId::from("node-a");
    let message = MessageId::new(7);
    let mut generator = MessageIdGenerator::new(10);
    let round = Round::new(3);
    let timestamp = Timestamp::from_millis(123);

    assert_eq!(
        serde_json::to_string(&node).expect("serialize"),
        r#""node-a""#
    );
    assert_eq!(serde_json::to_string(&message).expect("serialize"), "7");
    assert_eq!(
        serde_json::to_string(&generator).expect("serialize"),
        r#"{"next":10}"#
    );
    assert_eq!(generator.next_id(), Some(MessageId::new(10)));
    assert_eq!(serde_json::to_string(&round).expect("serialize"), "3");
    assert_eq!(serde_json::to_string(&timestamp).expect("serialize"), "123");
}

#[test]
fn round_trips_protocol_messages() {
    let rumor = Rumor::new(
        MessageId::new(1),
        NodeId::from("node-a"),
        Round::ZERO,
        "hello".to_string(),
    );
    let message = GossipMessage::rumors(vec![rumor]);

    let encoded = serde_json::to_string(&message).expect("serialize");
    let decoded: GossipMessage<String> = serde_json::from_str(&encoded).expect("deserialize");

    assert_eq!(decoded.rumor_count(), 1);
}

#[test]
fn round_trips_events_and_insert_outcomes() {
    let rumor = Rumor::new(
        MessageId::new(2),
        NodeId::from("node-b"),
        Round::new(9),
        "payload".to_string(),
    );
    let event = GossipEvent::NewRumor(rumor);

    let encoded = serde_json::to_string(&event).expect("serialize");
    let decoded: GossipEvent<String> = serde_json::from_str(&encoded).expect("deserialize");

    match decoded {
        GossipEvent::NewRumor(rumor) => {
            assert_eq!(rumor.id(), MessageId::new(2));
            assert_eq!(rumor.origin(), &NodeId::from("node-b"));
            assert_eq!(rumor.created_at(), Round::new(9));
            assert_eq!(rumor.payload(), "payload");
        }
    }

    let outcome = InsertOutcome::InsertedWithEviction {
        evicted: MessageId::new(1),
    };

    let encoded = serde_json::to_string(&outcome).expect("serialize");
    let decoded: InsertOutcome = serde_json::from_str(&encoded).expect("deserialize");

    assert_eq!(decoded, outcome);
}

#[test]
fn serializes_config_and_config_errors() {
    let config = GossipConfig::new(2, 64)
        .expect("valid config")
        .with_max_rumors_per_message(4)
        .expect("valid per-message limit")
        .with_rumor_retention_rounds(10)
        .expect("valid retention");

    let encoded = serde_json::to_string(&config).expect("serialize");
    let decoded: GossipConfig = serde_json::from_str(&encoded).expect("deserialize");

    assert_eq!(decoded.fanout(), 2);
    assert_eq!(decoded.max_rumors(), 64);
    assert_eq!(decoded.max_rumors_per_message(), 4);
    assert_eq!(decoded.rumor_retention_rounds(), 10);

    let error = ConfigError::ZeroFanout;
    let encoded = serde_json::to_string(&error).expect("serialize");
    let decoded: ConfigError = serde_json::from_str(&encoded).expect("deserialize");

    assert_eq!(decoded, ConfigError::ZeroFanout);
}
