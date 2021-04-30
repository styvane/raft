//! This module contains the definitions of various events in the Raft
//! network.

use crate::log::Entries;
use crate::types::{Index, Term};
use std::fmt;

/// Event in the Raft system.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub enum Event<V> {
    AppendEntries {
        // Leader's current term
        term: Term,

        // Index of log entry immediately preceding new ones.
        previous_index: Index,

        // Term of previous log index.
        previous_term: Term,

        // Log entries to store (empty for heartbeat).
        entries: Entries<V>,

        // Leader's commit index.
        commit_index: Index,

        // Source Id
        source: String,

        // Destination Id
        dest: String,
    },
    AppendEntriesResponse {
        // Current term for leader to update itself.
        term: Term,

        // `true` if follower contained entry matching previous log
        // index and previous term.
        success: bool,

        // Index of the log that match with the leader.
        match_index: Index,
        // Source Id
        source: String,

        // Destination Id
        dest: String,
    },

    RequestVote {
        // Candidate's term
        term: Term,

        // Candidate requesting vote id.
        candidate_id: String,

        // Index of candidate last log entry.
        last_index: Index,

        // Term of candidate last log entry
        last_term: Term,
        source: String,
    },

    RequestVoteResponse {
        // Current term for candidate to update itself.
        term: Term,

        // `true` means the candidate received vote.
        vote_granted: bool,

        // Source Id
        source: String,

        // Destination Id
        dest: String,
    },
}

impl<V> Event<V>
where
    V: fmt::Debug + Clone,
{
    /// Create new `AppendEntries` event
    pub fn new_append_entries(
        term: Term,
        previous_index: Index,
        previous_term: Term,
        entries: Entries<V>,
        commit_index: Index,
        source: &str,
        dest: &str,
    ) -> Self {
        Self::AppendEntries {
            term,
            previous_index,
            previous_term,
            entries,
            commit_index,
            source: source.to_string(),
            dest: dest.to_string(),
        }
    }

    /// Create new `Append_EntriesResponse` event.
    pub fn new_append_entries_response(
        term: Term,
        success: bool,
        match_index: Index,
        source: &str,
        dest: &str,
    ) -> Self {
        Self::AppendEntriesResponse {
            term,
            success,
            match_index,
            source: source.to_string(),
            dest: dest.to_string(),
        }
    }

    /// Create new `RequestVote` event.
    pub fn new_request_vote(
        term: Term,
        candidate_id: String,
        last_index: Index,
        last_term: Term,
        source: &str,
    ) -> Self {
        Self::RequestVote {
            term,
            candidate_id,
            last_index,
            last_term,
            source: source.to_string(),
        }
    }

    /// Create new `RequestVoteResponse` event.
    pub fn new_request_vote_response(
        term: Term,
        vote_granted: bool,
        source: &str,
        dest: &str,
    ) -> Self {
        Self::RequestVoteResponse {
            term,
            vote_granted,
            source: source.to_string(),
            dest: dest.to_string(),
        }
    }
}
