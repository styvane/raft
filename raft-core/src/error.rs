//! Error type.
//!
//! This module defines the error types.

use thiserror;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    LogError(String),
}
