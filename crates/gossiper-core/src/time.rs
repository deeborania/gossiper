//! Time-related protocol types.

use core::fmt;

/// A logical gossip round.
///
/// Rounds are controlled by the caller. The core protocol never sleeps and never
/// reads the system clock.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Round(u64);

impl Round {
    /// The first gossip round
    pub const ZERO: Self = Round(0);

    /// Creates a round from a numeric value.
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the numeric round value.
    pub fn get(self) -> u64 {
        self.0
    }

    /// Returns the next round, saturating at `u64::MAX`
    pub fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

impl From<u64> for Round {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

impl fmt::Display for Round {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// A caller-provided monotonic timestamp in milliseconds.
///
/// The protocol core treats this as an opaque monotonic value. It does not know
/// whether the value came from a real clock, a simulator, or a test.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(u64);

impl Timestamp {
    /// Creates a timestamp from milliseconds.
    pub fn from_millis(value: u64) -> Self {
        Self(value)
    }

    /// Returns the timestamp as milliseconds.
    pub fn as_millis(self) -> u64 {
        self.0
    }

    /// Returns the elapsed milliseconds since an earlier timestamp.
    ///
    /// Returns `None` if `earlier` is greater than `self`, which would mean time
    /// moved backwards from the protocol's point of view.
    pub fn duration_since(self, earlier: Self) -> Option<u64> {
        self.0.checked_sub(earlier.0)
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}ms", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::{Round, Timestamp};

    #[test]
    fn round_starts_at_zero() {
        assert_eq!(Round::ZERO.get(), 0);
    }

    #[test]
    fn round_can_advance() {
        assert_eq!(Round::new(7).next().get(), 8);
    }

    #[test]
    fn timestamp_reports_elapsed_duration() {
        let start = Timestamp::from_millis(100);
        let end = Timestamp::from_millis(250);

        assert_eq!(end.duration_since(start), Some(150));
    }

    #[test]
    fn timestamp_detects_backwards_time() {
        let start = Timestamp::from_millis(250);
        let end = Timestamp::from_millis(100);

        assert_eq!(end.duration_since(start), None);
    }
}
