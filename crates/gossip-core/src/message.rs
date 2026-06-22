//! Protocol messages exchanged by gossip nodes.

use crate::Rumor;

/// A protocol message exchanged between gossip nodes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GossipMessage<T> {
    /// Carries rumors to another node.
    Rumors(Vec<Rumor<T>>),
}

impl<T> GossipMessage<T> {
    /// Creates a message containing rumors.
    pub fn rumors(rumors: Vec<Rumor<T>>) -> Self {
        Self::Rumors(rumors)
    }

    /// Returns the number of rumors carried by this message.
    pub fn rumor_count(&self) -> usize {
        match self {
            Self::Rumors(rumors) => rumors.len(),
        }
    }

    /// Returns `true` if this message carries no useful payload.
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Rumors(rumors) => rumors.is_empty(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::GossipMessage;
    use crate::{MessageId, NodeId, Round, Rumor};

    fn rumor(id: u128, payload: &'static str) -> Rumor<&'static str> {
        Rumor::new(
            MessageId::new(id),
            NodeId::from("node-a"),
            Round::new(0),
            payload,
        )
    }

    #[test]
    fn rumors_message_counts_payloads() {
        let message = GossipMessage::rumors(vec![rumor(1, "a"), rumor(2, "b")]);

        assert_eq!(message.rumor_count(), 2);
        assert!(!message.is_empty());
    }

    #[test]
    fn empty_rumors_message_is_empty() {
        let message: GossipMessage<&str> = GossipMessage::rumors(Vec::new());

        assert_eq!(message.rumor_count(), 0);
        assert!(message.is_empty());
    }
}
