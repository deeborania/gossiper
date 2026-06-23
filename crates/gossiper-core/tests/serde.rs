#![cfg(feature = "serde")]

use gossiper_core::{
    AntiEntropyMessage, ConfigError, Digest, GossipConfig, GossipEvent, GossipMessage, IdSetDigest,
    InsertOutcome, MergeReport, MessageId, MessageIdGenerator, NodeId, Round, Rumor, Timestamp,
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
fn round_trips_anti_entropy_digest() {
    let digest = IdSetDigest::from_ids([MessageId::new(1), MessageId::new(2)]);

    let encoded = serde_json::to_string(&digest).expect("serialize");
    let decoded: IdSetDigest<MessageId> = serde_json::from_str(&encoded).expect("deserialize");

    assert!(decoded.contains(&MessageId::new(1)));
    assert!(decoded.contains(&MessageId::new(2)));
    assert!(!decoded.contains(&MessageId::new(3)));
}

#[test]
fn round_trips_anti_entropy_messages() {
    let digest = IdSetDigest::from_ids([MessageId::new(1), MessageId::new(2)]);
    let digest_message: AntiEntropyMessage<_, Rumor<String>> = AntiEntropyMessage::digest(digest);

    let encoded = serde_json::to_string(&digest_message).expect("serialize");
    let decoded: AntiEntropyMessage<IdSetDigest<MessageId>, Rumor<String>> =
        serde_json::from_str(&encoded).expect("deserialize");

    match decoded {
        AntiEntropyMessage::Digest(digest) => {
            assert!(digest.contains(&MessageId::new(1)));
            assert!(digest.contains(&MessageId::new(2)));
        }
        AntiEntropyMessage::Delta(_) => panic!("expected digest message"),
    }

    let delta_message: AntiEntropyMessage<IdSetDigest<MessageId>, _> =
        AntiEntropyMessage::delta(vec![Rumor::new(
            MessageId::new(3),
            NodeId::from("node-a"),
            Round::ZERO,
            "payload".to_string(),
        )]);

    let encoded = serde_json::to_string(&delta_message).expect("serialize");
    let decoded: AntiEntropyMessage<IdSetDigest<MessageId>, Rumor<String>> =
        serde_json::from_str(&encoded).expect("deserialize");

    match decoded {
        AntiEntropyMessage::Digest(_) => panic!("expected delta message"),
        AntiEntropyMessage::Delta(items) => {
            assert_eq!(items.len(), 1);
            assert_eq!(items[0].id(), MessageId::new(3));
            assert_eq!(items[0].payload(), "payload");
        }
    }
}

#[test]
fn round_trips_merge_report() {
    let report = MergeReport::default();

    let encoded = serde_json::to_string(&report).expect("serialize");
    let decoded: MergeReport = serde_json::from_str(&encoded).expect("deserialize");

    assert_eq!(decoded, report);
    assert_eq!(decoded.total(), 0);
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
