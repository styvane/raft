//! Raft types
//!
//! This module contains various data types for Raft

/// The `Term` type represents the term for an election.
pub type Term = Option<usize>;

/// The `Index` type represents the index of a log entry.
pub type Index = Option<usize>;
