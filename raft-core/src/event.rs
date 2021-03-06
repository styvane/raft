//! This module contains the definitions of various events in the Raft
//! network.

use crate::log::Entries;
use crate::types::{Index, Term};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
pub struct AppendEntries<V> {
    /// Leader's current term
    pub term: Term,

    /// Index of log entry immediately preceding new ones.
    pub previous_index: Index,

    /// Term of previous log index.
    pub previous_term: Term,

    /// Log entries to store (empty for heartbeat).
    pub entries: Entries<V>,

    /// Leader's commit index.
    pub commit_index: Index,

    /// Source of the entries
    pub source: String,

    /// Destination of the entries
    pub dest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
pub struct AppendEntriesResponse {
    /// Current term for leader to update itself.
    pub term: Term,

    /// `true` if follower contained entry matching previous log index and previous term.
    pub success: bool,

    /// Index of the log that match with the leader.
    pub match_index: Index,

    /// Source of the response.
    pub source: String,

    /// Destination of the entries,
    pub dest: String,
}

/// The `Vote` type owns a vote casted by a peer.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
pub struct Vote {
    pub peer: String,

    /// `true` means the candidate received vote.
    pub granted: bool,
}

impl Vote {
    /// Cast new vote
    pub fn cast(peer: &str) -> Self {
        Vote {
            peer: peer.to_string(),
            granted: true,
        }
    }

    /// Un-grant vote to peer
    pub fn remove(&mut self) {
        self.granted = false;
    }
}

/// The `RequestVote` owns the data to send to request vote from peers.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
pub struct RequestVote {
    /// Candidate's term
    pub term: Term,

    /// Index of candidate last log entry.
    pub last_index: Index,

    /// Term of candidate last log entry
    pub last_term: Term,

    /// Source of the entries
    pub source: String,

    /// Destination of the entries
    pub dest: String,
}

/// The `RequestVoteResponse` owns the reponse data for a [`RequestVote`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
pub struct RequestVoteResponse {
    /// Current term for candidate to update itself.
    pub term: Term,

    /// Vote casted.
    pub vote: Vote,

    /// Source Id
    pub source: String,

    /// Destination Id
    pub dest: String,
}

/// Event in the Raft system.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
pub enum Event<V> {
    AppendEntries(AppendEntries<V>),
    AppendEntriesResponse(AppendEntriesResponse),
    RequestVote(RequestVote),
    RequestVoteResponse(RequestVoteResponse),
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
        Self::AppendEntries(AppendEntries {
            term,
            previous_index,
            previous_term,
            entries,
            commit_index,
            source: source.to_string(),
            dest: dest.to_string(),
        })
    }

    /// Create new `Append_EntriesResponse` event.
    pub fn new_append_entries_response(
        term: Term,
        success: bool,
        match_index: Index,
        source: &str,
        dest: &str,
    ) -> Self {
        Self::AppendEntriesResponse(AppendEntriesResponse {
            term,
            success,
            match_index,
            source: source.to_string(),
            dest: dest.to_string(),
        })
    }

    /// Create new `RequestVote` event.
    pub fn new_request_vote(
        term: Term,
        last_index: Index,
        last_term: Term,
        source: &str,
        dest: &str,
    ) -> Self {
        Self::RequestVote(RequestVote {
            term,
            last_index,
            last_term,
            source: source.to_string(),
            dest: dest.to_string(),
        })
    }

    /// Create new `RequestVoteResponse` event.
    pub fn new_request_vote_response(term: Term, vote: Vote, source: &str, dest: &str) -> Self {
        Self::RequestVoteResponse(RequestVoteResponse {
            term,
            vote,
            source: source.to_string(),
            dest: dest.to_string(),
        })
    }
}

/// The `Message` type wraps the [`Event`] and the source.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
pub struct Message<V> {
    pub event: Event<V>,
    pub dest: String,
}

impl<V> Message<V>
where
    V: fmt::Debug + Clone,
{
    pub fn new(dest: &str, event: Event<V>) -> Self {
        Message {
            dest: dest.to_string(),
            event,
        }
    }
}
