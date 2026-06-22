//! Events emitted by gossip nodes.

use crate::Rumor;

/// An event produced by a gossip node when processing protocol input.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GossipEvent<T> {
    /// The node accepted a rumor it did not already know.
    NewRumor(Rumor<T>),
}

impl<T> GossipEvent<T> {
    /// Creates a `NewRumor` event.
    pub fn new_rumor(rumor: Rumor<T>) -> Self {
        Self::NewRumor(rumor)
    }
}
