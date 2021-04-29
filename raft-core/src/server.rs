//! Raft server
//!
//! This module contains the Raft server implementation.

use crate::net::Node;

/// The type `Server` is the raft server.
pub struct Server<T> {
    node: T,
}

impl<T> Server<T>
where
    T: Node,
{
    /// Create new Raft server.
    pub fn new(node: T) -> Self {
        Server { node }
    }
}

#[cfg(test)]
mod tests {}
