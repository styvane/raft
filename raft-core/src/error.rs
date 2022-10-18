//! Error type.
//!
//! This module defines the error types.

use thiserror;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Raft log error
    #[error("{0}")]
    LogError(String),

    /// Configuration error
    #[error(transparent)]
    ConfigError(#[from] config::ConfigError),

    /// IO error
    #[error(transparent)]
    IOError(#[from] std::io::Error),
}
