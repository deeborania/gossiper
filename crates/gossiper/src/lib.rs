#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! Reusable gossip protocol building blocks.
//!
//! `gossiper` is the user-facing facade crate. It re-exports the core protocol
//! API and, depending on enabled features, transport and simulation utilities.

/// Core protocol state machines and data types.
pub mod core {
    pub use gossiper_core::*;
}

/// Transport traits and helpers.
#[cfg(feature = "transport")]
pub mod transport {
    pub use gossiper_transport::*;
}

/// Simulation utilities.
#[cfg(feature = "sim")]
pub mod sim {
    pub use gossiper_sim::*;
}

pub use crate::core::*;

#[cfg(feature = "transport")]
pub use crate::transport::*;

#[cfg(feature = "sim")]
pub use crate::sim::*;
