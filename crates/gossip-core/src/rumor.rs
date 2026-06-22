//! Rumor types used for epidemic dissemination.

use crate::{MessageId, NodeId, Round};

/// A piece of information spread by gossip.
///
/// The payload is generic because the core protocol should not know what the
/// application-level rumor means.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rumor<T> {
    id: MessageId,
    origin: NodeId,
    created_at: Round,
    payload: T,
}

impl<T> Rumor<T> {
    /// Creates a new rumor.
    pub fn new(id: MessageId, origin: NodeId, created_at: Round, payload: T) -> Self {
        Self {
            id,
            origin,
            created_at,
            payload,
        }
    }

    /// Returns the unique message identifier.
    pub fn id(&self) -> MessageId {
        self.id
    }

    /// Returns the node that originally created this rumor.
    pub fn origin(&self) -> &NodeId {
        &self.origin
    }

    /// Returns the round when this rumor was created.
    pub fn created_at(&self) -> Round {
        self.created_at
    }

    /// Returns the rumor payload.
    pub fn payload(&self) -> &T {
        &self.payload
    }

    /// Consumes the rumor and returns its payload.
    pub fn into_payload(self) -> T {
        self.payload
    }

    /// Maps the payload while preserving rumor metadata.
    pub fn map_payload<U>(self, map: impl FnOnce(T) -> U) -> Rumor<U> {
        Rumor {
            id: self.id,
            origin: self.origin,
            created_at: self.created_at,
            payload: map(self.payload),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Rumor;
    use crate::{MessageId, NodeId, Round};

    #[test]
    fn rumor_exposes_metadata_and_payload() {
        let rumor = Rumor::new(
            MessageId::new(10),
            NodeId::from("node-a"),
            Round::new(3),
            "service moved",
        );

        assert_eq!(rumor.id(), MessageId::new(10));
        assert_eq!(rumor.origin(), &NodeId::from("node-a"));
        assert_eq!(rumor.created_at(), Round::new(3));
        assert_eq!(rumor.payload(), &"service moved");
    }

    #[test]
    fn rumor_can_map_payload() {
        let rumor = Rumor::new(
            MessageId::new(10),
            NodeId::from("node-a"),
            Round::new(3),
            "service moved",
        );

        let mapped = rumor.map_payload(|payload| payload.len());

        assert_eq!(mapped.id(), MessageId::new(10));
        assert_eq!(mapped.origin(), &NodeId::from("node-a"));
        assert_eq!(mapped.created_at(), Round::new(3));
        assert_eq!(mapped.payload(), &13);
    }
}
