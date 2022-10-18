//! Result type.
//!
//! This module defines the `Result` type which is an alias to the `std::result::Result`
//! with [`Error`](crate::Error) as error.

use crate::error::Error;

pub type Result<T> = std::result::Result<T, Error>;
