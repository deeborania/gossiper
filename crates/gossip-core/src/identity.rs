//! Identity types used by the gossip protocol.

use core::fmt;

/// Stable logical identity for a gossip participant.
///
/// A `NodeId` identifies a protocol participant, not a network address.
/// Addresses can change; protocol identity should remain stable for as long as
/// the participant is considered the same logical node.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(String);

impl NodeId {
    /// Create a new node identifier.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the node identifier as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the node identifier and returns the inner string.
    pub fn into_string(self) -> String {
        self.0
    }
}

impl From<String> for NodeId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&str> for NodeId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Stable identifier for a gossip message or rumor.
///
/// Message IDs are used for duplicate suppression. If a node sees the same
/// `MessageId` again, it can avoid processing the same rumor repeatedly.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MessageId(u128);

impl MessageId {
    /// Creates a new message identifier.
    pub fn new(value: u128) -> Self {
        Self(value)
    }

    /// Returns the numeric value of the message identifier.
    pub fn get(self) -> u128 {
        self.0
    }
}

impl From<u128> for MessageId {
    fn from(value: u128) -> Self {
        Self::new(value)
    }
}

impl fmt::Display for MessageId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[cfg(test)]
mod tests {
    use super::{MessageId, NodeId};

    #[test]
    fn node_id_can_be_created_from_str() {
        let node = NodeId::from("node-a");

        assert_eq!(node.as_str(), "node-a");
        assert_eq!(node.to_string(), "node-a");
    }

    #[test]
    fn node_id_can_be_created_from_string() {
        let node = NodeId::from(String::from("node-b"));

        assert_eq!(node.into_string(), "node-b");
    }

    #[test]
    fn message_id_wraps_u128() {
        let id = MessageId::new(42);

        assert_eq!(id.get(), 42);
        assert_eq!(id.to_string(), "42");
    }
}
