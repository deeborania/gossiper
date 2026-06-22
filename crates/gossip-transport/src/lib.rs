#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! Transport abstractions for gossip protocol implementations.
//!
//! This crate defines traits, not concrete networking. Real transports can use
//! TCP, UDP, QUIC, in-memory channels, or test simulators.

use core::fmt;
use std::collections::{BTreeMap, VecDeque};

use gossip_core::{Effect, NodeId};

/// Error returned by a gossip transport.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransportError {
    target: NodeId,
    message: String,
}

impl TransportError {
    /// Creates a transport error for a target node.
    pub fn new(target: NodeId, message: impl Into<String>) -> Self {
        Self {
            target,
            message: message.into(),
        }
    }

    /// Returns the target node related to this error.
    pub fn target(&self) -> &NodeId {
        &self.target
    }

    /// Returns the error message.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for TransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "transport error sending to {}: {}",
            self.target, self.message
        )
    }
}

impl std::error::Error for TransportError {}

/// A synchronous transport capable of sending one message to one target.
///
/// This trait is intentionally generic over the message type. A caller may send
/// raw bytes, encoded protocol messages, or in-memory test messages.
pub trait Transport<Message> {
    /// Sends one message to one target.
    fn send(&mut self, target: &NodeId, message: Message) -> Result<(), TransportError>;
}

/// In-memory transport useful for tests and simulations.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct InMemoryTransport<Message> {
    inboxes: BTreeMap<NodeId, VecDeque<Message>>,
}

impl<Message> InMemoryTransport<Message> {
    /// Creates an empty in-memory transport.
    pub fn new() -> Self {
        Self {
            inboxes: BTreeMap::new(),
        }
    }

    /// Returns the number of queued messages for a target node.
    pub fn queued_len(&self, target: &NodeId) -> usize {
        self.inboxes.get(target).map_or(0, VecDeque::len)
    }

    /// Returns `true` if the target has at least one queued message.
    pub fn has_queued(&self, target: &NodeId) -> bool {
        self.queued_len(target) > 0
    }

    /// Removes and returns all queued messages for a target node.
    pub fn drain(&mut self, target: &NodeId) -> Vec<Message> {
        self.inboxes
            .remove(target)
            .map(VecDeque::into)
            .unwrap_or_default()
    }
}

impl<Message> Transport<Message> for InMemoryTransport<Message> {
    fn send(&mut self, target: &NodeId, message: Message) -> Result<(), TransportError> {
        self.inboxes
            .entry(target.clone())
            .or_default()
            .push_back(message);

        Ok(())
    }
}

/// Summary produced after applying protocol effects to a transport.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EffectReport<Event> {
    sent: usize,
    events: Vec<Event>,
    errors: Vec<TransportError>,
}

impl<Event> EffectReport<Event> {
    /// Creates an empty effect report.
    pub fn new() -> Self {
        Self {
            sent: 0,
            events: Vec::new(),
            errors: Vec::new(),
        }
    }

    /// Returns the number of messages sent successfully.
    pub fn sent(&self) -> usize {
        self.sent
    }

    /// Returns emitted protocol events.
    pub fn events(&self) -> &[Event] {
        &self.events
    }

    /// Returns transport errors.
    pub fn errors(&self) -> &[TransportError] {
        &self.errors
    }

    /// Returns `true` if at least one transport send failed.
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }
}

impl<Event> Default for EffectReport<Event> {
    fn default() -> Self {
        Self::new()
    }
}

/// Applies protocol effects to a transport.
///
/// Send effects are passed to the transport. Emit effects are collected into the
/// returned report.
pub fn apply_effects<Message, Event, T>(
    transport: &mut T,
    effects: impl IntoIterator<Item = Effect<Message, Event>>,
) -> EffectReport<Event>
where
    T: Transport<Message>,
{
    let mut report = EffectReport::new();

    for effect in effects {
        match effect {
            Effect::Send { target, message } => match transport.send(&target, message) {
                Ok(()) => {
                    report.sent += 1;
                }
                Err(e) => report.errors.push(e),
            },
            Effect::Emit(event) => {
                report.events.push(event);
            }
        }
    }

    report
}

#[cfg(test)]
mod tests {
    use super::InMemoryTransport;
    use super::{Transport, TransportError};
    use gossip_core::{Effect, NodeId};

    #[derive(Default)]
    struct RecordingTransport {
        sent: Vec<(NodeId, String)>,
    }

    impl Transport<String> for RecordingTransport {
        fn send(&mut self, target: &NodeId, message: String) -> Result<(), TransportError> {
            self.sent.push((target.clone(), message));
            Ok(())
        }
    }

    #[test]
    fn recording_transport_can_implement_trait() {
        let mut transport = RecordingTransport::default();

        transport
            .send(&NodeId::from("node-b"), "hello".to_string())
            .expect("send should succeed");

        assert_eq!(
            transport.sent,
            vec![(NodeId::from("node-b"), "hello".to_string())]
        );
    }

    #[test]
    fn transport_error_displays_target_and_message() {
        let error = TransportError::new(NodeId::from("node-b"), "connection refused");

        assert_eq!(error.target(), &NodeId::from("node-b"));
        assert_eq!(error.message(), "connection refused");
        assert_eq!(
            error.to_string(),
            "transport error sending to node-b: connection refused"
        );
    }

    #[test]
    fn apply_effects_sends_messages_and_collects_events() {
        let mut transport = RecordingTransport::default();
        let effects = vec![
            Effect::Send {
                target: NodeId::from("node-b"),
                message: "hello".to_string(),
            },
            Effect::Emit("learned-rumor"),
        ];

        let report = super::apply_effects(&mut transport, effects);

        assert_eq!(report.sent(), 1);
        assert_eq!(report.events(), &["learned-rumor"]);
        assert!(report.errors().is_empty());
        assert_eq!(
            transport.sent,
            vec![(NodeId::from("node-b"), "hello".to_string())]
        );
    }

    struct FailingTransport;

    impl Transport<String> for FailingTransport {
        fn send(&mut self, target: &NodeId, _message: String) -> Result<(), TransportError> {
            Err(TransportError::new(target.clone(), "offline"))
        }
    }

    #[test]
    fn apply_effects_records_transport_errors() {
        let mut transport = FailingTransport;
        let effects: Vec<Effect<String, ()>> = vec![Effect::Send {
            target: NodeId::from("node-b"),
            message: "hello".to_string(),
        }];

        let report = super::apply_effects(&mut transport, effects);

        assert_eq!(report.sent(), 0);
        assert!(report.events().is_empty());
        assert!(report.has_errors());
        assert_eq!(report.errors()[0].target(), &NodeId::from("node-b"));
        assert_eq!(report.errors()[0].message(), "offline");
    }

    #[test]
    fn in_memory_transport_queues_messages_by_target() {
        let mut transport = InMemoryTransport::new();
        let node_b = NodeId::from("node-b");
        let node_c = NodeId::from("node-c");

        transport
            .send(&node_b, "first".to_string())
            .expect("send should succeed");
        transport
            .send(&node_b, "second".to_string())
            .expect("send should succeed");
        transport
            .send(&node_c, "third".to_string())
            .expect("send should succeed");

        assert_eq!(transport.queued_len(&node_b), 2);
        assert_eq!(transport.queued_len(&node_c), 1);
        assert!(transport.has_queued(&node_b));

        assert_eq!(
            transport.drain(&node_b),
            vec!["first".to_string(), "second".to_string()]
        );

        assert_eq!(transport.queued_len(&node_b), 0);
        assert_eq!(transport.drain(&node_b), Vec::<String>::new());
    }

    #[test]
    fn apply_effects_can_drive_in_memory_transport() {
        let mut transport = InMemoryTransport::new();
        let node_b = NodeId::from("node-b");

        let effects = vec![Effect::Send {
            target: node_b.clone(),
            message: "hello".to_string(),
        }];

        let report: super::EffectReport<()> = super::apply_effects(&mut transport, effects);

        assert_eq!(report.sent(), 1);
        assert!(!report.has_errors());
        assert_eq!(transport.drain(&node_b), vec!["hello".to_string()]);
    }
}
