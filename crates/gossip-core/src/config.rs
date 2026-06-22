//! Configuration for gossip protocol components.

use core::fmt;

/// Configuration for gossip behavior.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GossipConfig {
    fanout: usize,
    max_rumors: usize,
    rumor_retention_rounds: u64,
    max_rumors_per_message: usize,
}

impl GossipConfig {
    /// Creates a validated gossip configuration.
    pub fn new(fanout: usize, max_rumors: usize) -> Result<Self, ConfigError> {
        if fanout == 0 {
            return Err(ConfigError::ZeroFanout);
        }

        if max_rumors == 0 {
            return Err(ConfigError::ZeroMaxRumors);
        }

        Ok(Self {
            fanout,
            max_rumors,
            rumor_retention_rounds: 8,
            max_rumors_per_message: 32,
        })
    }

    /// Returns the number of peers contacted per gossip round.
    pub fn fanout(&self) -> usize {
        self.fanout
    }

    /// Returns the maximum number of rumors retained by the node.
    pub fn max_rumors(&self) -> usize {
        self.max_rumors
    }

    /// Returns how many rounds a rumor is kept after its creation round.
    pub fn rumor_retention_rounds(&self) -> u64 {
        self.rumor_retention_rounds
    }

    /// Returns the maximum number of rumors included in one gossip message.
    pub fn max_rumors_per_message(&self) -> usize {
        self.max_rumors_per_message
    }

    /// Returns a copy of this configuration with a different rumor retention window.
    pub fn with_rumor_retention_rounds(
        mut self,
        rumor_retention_rounds: u64,
    ) -> Result<Self, ConfigError> {
        if rumor_retention_rounds == 0 {
            return Err(ConfigError::ZeroRumorRetentionRounds);
        }

        self.rumor_retention_rounds = rumor_retention_rounds;
        Ok(self)
    }

    /// Returns a copy of this configuration with a different per-message rumor limit.
    pub fn with_max_rumors_per_message(
        mut self,
        max_rumors_per_message: usize,
    ) -> Result<Self, ConfigError> {
        if max_rumors_per_message == 0 {
            return Err(ConfigError::ZeroMaxRumorsPerMessage);
        }

        self.max_rumors_per_message = max_rumors_per_message;
        Ok(self)
    }
}

impl Default for GossipConfig {
    fn default() -> Self {
        Self {
            fanout: 3,
            max_rumors: 1_024,
            rumor_retention_rounds: 8,
            max_rumors_per_message: 32,
        }
    }
}

/// Error returned when creating an invalid gossip configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfigError {
    /// Fanout must be greater than zero.
    ZeroFanout,

    /// Maximum retained rumors must be greater than zero.
    ZeroMaxRumors,

    /// Rumor retention must be greater than zero.
    ZeroRumorRetentionRounds,

    /// Per-message rumor limit must be greater than zero.
    ZeroMaxRumorsPerMessage,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroFanout => formatter.write_str("fanout must be greater than zero"),
            Self::ZeroMaxRumors => formatter.write_str("max_rumors must be greater than zero"),
            Self::ZeroRumorRetentionRounds => {
                formatter.write_str("rumor_retention_rounds must be greater than zero")
            }
            Self::ZeroMaxRumorsPerMessage => {
                formatter.write_str("max_rumors_per_message must be greater than zero")
            }
        }
    }
}

impl std::error::Error for ConfigError {}

#[cfg(test)]
mod tests {
    use super::{ConfigError, GossipConfig};

    #[test]
    fn default_config_is_valid() {
        let config = GossipConfig::default();

        assert_eq!(config.fanout(), 3);
        assert_eq!(config.max_rumors(), 1_024);
    }

    #[test]
    fn accepts_positive_values() {
        let config = GossipConfig::new(5, 100).expect("valid config");

        assert_eq!(config.fanout(), 5);
        assert_eq!(config.max_rumors(), 100);
    }

    #[test]
    fn rejects_zero_fanout() {
        let error = GossipConfig::new(0, 100).expect_err("zero fanout should fail");

        assert_eq!(error, ConfigError::ZeroFanout);
        assert_eq!(error.to_string(), "fanout must be greater than zero");
    }

    #[test]
    fn rejects_zero_max_rumors() {
        let error = GossipConfig::new(3, 0).expect_err("zero max rumors should fail");

        assert_eq!(error, ConfigError::ZeroMaxRumors);
        assert_eq!(error.to_string(), "max_rumors must be greater than zero");
    }

    #[test]
    fn default_config_has_rumor_retention() {
        let config = GossipConfig::default();

        assert_eq!(config.rumor_retention_rounds(), 8);
    }

    #[test]
    fn can_override_rumor_retention_rounds() {
        let config = GossipConfig::default()
            .with_rumor_retention_rounds(12)
            .expect("valid retention");

        assert_eq!(config.rumor_retention_rounds(), 12);
    }

    #[test]
    fn rejects_zero_rumor_retention_rounds() {
        let error = GossipConfig::default()
            .with_rumor_retention_rounds(0)
            .expect_err("zero retention should fail");

        assert_eq!(error, ConfigError::ZeroRumorRetentionRounds);
        assert_eq!(
            error.to_string(),
            "rumor_retention_rounds must be greater than zero"
        );
    }

    #[test]
    fn default_config_has_max_rumors_per_message() {
        let config = GossipConfig::default();

        assert_eq!(config.max_rumors_per_message(), 32);
    }

    #[test]
    fn can_override_max_rumors_per_message() {
        let config = GossipConfig::default()
            .with_max_rumors_per_message(7)
            .expect("valid per-message limit");

        assert_eq!(config.max_rumors_per_message(), 7);
    }

    #[test]
    fn rejects_zero_max_rumors_per_message() {
        let error = GossipConfig::default()
            .with_max_rumors_per_message(0)
            .expect_err("zero per-message limit should fail");

        assert_eq!(error, ConfigError::ZeroMaxRumorsPerMessage);
        assert_eq!(
            error.to_string(),
            "max_rumors_per_message must be greater than zero"
        );
    }
}
