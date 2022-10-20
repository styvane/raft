//! Raft server
//!
//! This module contains the Raft server implementation.

use std::cmp;
use std::collections::{HashMap, HashSet};
use std::fmt;

use async_std::channel::Sender;
use async_std::task;
use futures::channel::oneshot;
use log::{error, info};

use crate::event::{AppendEntries, AppendEntriesResponse};
use crate::event::{Event, Message, RequestVote, RequestVoteResponse, Vote};
use crate::log::{Entry, Log};
use crate::result::Result;
use crate::types::{Index, Term};
use crate::Error;
use crate::{config::Cluster, log::InMemory};

/// Optional channel for sending messages to client when there is consensus.
pub type ConsensusSender = Option<oneshot::Sender<bool>>;

/// Optional channel for receiving messages from the Raft leader.
/// Then client will apply the log to the state machine based on
/// the message received on this channel.
pub type ConsensusReceiver = Option<oneshot::Receiver<bool>>;

/// The `ClientRequest` trait defines the behavior of the Raft client request.
pub trait ClientRequest: Send {
    type EntryKind;

    fn entry_kind(&self) -> Self::EntryKind;
    fn responder(&mut self) -> &mut ConsensusSender;
}

/// The `Commit` type is an optional channel of log that followers must apply
/// to their state machines.
pub type Commit<V> = Option<Sender<V>>;

/// The type `Server` is the raft server.
pub struct Server<T> {
    // Server Id
    id: usize,

    // The server configuration
    config: Cluster,

    // The log for this server.
    log: Box<dyn Log<Item = Entry<T>> + Send>,

    // The current term for this server.
    current_term: Term,

    // Candidate Id that received vote in the current term.
    vote_for: Option<String>,

    // Index of highest log entry known to be committed
    commit_index: Index,

    // Table of the log index to send to each peer.
    // It is initialize to leader's last log index + 1
    next_index: HashMap<String, usize>,

    // Index of the highest log entry known to be replicated.
    match_index: HashMap<String, Index>,

    // Role of the server.
    role: Role,

    // Index of the last log applied to the state machine.
    last_applied: Index,

    // List of votes recorded,
    votes: HashMap<String, Vote>,

    // Heartbeat from followers
    followers_heartbeat: HashSet<String>,

    // Indicate whether is heartbeat was received from leader.
    has_heard_from_leader: bool,

    // Emitted server events and destination.
    messages: Sender<Message<T>>,

    // Waiting entries to be committed.
    waiting: HashMap<usize, oneshot::Sender<bool>>,

    // Entries to apply to the state machine by followers.
    commits: Commit<T>,
}

impl<T> fmt::Display for Server<T>
where
    T: Clone + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let commit_index = self
            .commit_index
            .map_or_else(|| String::from("None"), |x| x.to_string());

        let vote_for = self
            .vote_for
            .clone()
            .unwrap_or_else(|| String::from("None"));

        write!(
            f,
            "Server<id={}, term={:?}, role={},  commit_index={}, vote_for={}, log={}>",
            self.config.get(&self.id).hostname(),
            self.current_term,
            self.role,
            commit_index,
            vote_for,
            self.log
        )
    }
}

impl<T> Server<T>
where
    T: Clone + fmt::Debug + Send + 'static,
{
    /// Create new Raft server.
    pub fn new(
        id: usize,
        config: Cluster,
        messages: Sender<Message<T>>,
        log: Box<dyn Log<Item = Entry<T>> + Send>,
        commits: Commit<T>,
    ) -> Self {
        let size = config.size();
        Server {
            id,
            config,
            log,
            current_term: None,
            vote_for: None,
            commit_index: None,
            next_index: HashMap::with_capacity(size),
            match_index: HashMap::with_capacity(size),
            role: Role::Follower,
            last_applied: None,
            votes: HashMap::with_capacity(size),
            followers_heartbeat: HashSet::with_capacity(size),
            has_heard_from_leader: false,
            messages,
            waiting: HashMap::new(),
            commits,
        }
    }

    /// Create new Raft server.
    pub fn new_in_memory(
        id: usize,
        config: Cluster,
        messages: Sender<Message<T>>,
        commits: Commit<T>,
    ) -> Self {
        let size = config.size();
        Server {
            id,
            config,
            log: Box::new(InMemory::<T>::default()),
            current_term: None,
            vote_for: None,
            commit_index: None,
            next_index: HashMap::with_capacity(size),
            match_index: HashMap::with_capacity(size),
            role: Role::Follower,
            last_applied: None,
            votes: HashMap::with_capacity(size),
            followers_heartbeat: HashSet::with_capacity(size),
            has_heard_from_leader: false,
            messages,
            waiting: HashMap::new(),
            commits,
        }
    }

    /// Create new Raft server with a existing log.
    pub fn with_log(
        id: usize,
        config: Cluster,
        messages: Sender<Message<T>>,
        log: Box<dyn Log<Item = Entry<T>> + Send>,
        commits: Commit<T>,
    ) -> Self {
        let next_index: HashMap<String, usize> = config
            .members_iter()
            .map(|x| (x.hostname().to_string(), 0))
            .collect();

        let size = config.size();
        Server {
            id,
            config,
            log,
            current_term: None,
            vote_for: None,
            commit_index: None,
            next_index,
            match_index: HashMap::with_capacity(size),
            role: Role::Follower,
            last_applied: None,
            votes: HashMap::with_capacity(size),
            followers_heartbeat: HashSet::with_capacity(size),
            has_heard_from_leader: false,
            messages,
            waiting: HashMap::new(),
            commits,
        }
    }

    /// Return servers's hostname.
    pub fn hostname(&self) -> String {
        self.config.get(&self.id).hostname().to_string()
    }

    /// This method checks whether self is truly the leader.
    ///
    /// In presence of network partition, we might have two leaders in the system.
    /// To assert that self is the effective leader, self needs a quorum in of heartbeat.
    pub fn is_leader(&mut self) -> bool {
        if self.followers_heartbeat.len() > self.config.size() / 2 {
            true
        } else {
            self.followers_heartbeat.clear();
            self.become_follower()
        }
    }

    /// Reset some internal state after winning an election.
    pub fn become_leader(&mut self) -> Result<()> {
        if self.role == Role::Leader {
            return Ok(());
        }

        // See TLA⁺ spec L229.
        assert!(
            self.role == Role::Candidate,
            "Only candidate can become leader after winning an election"
        );

        self.role = Role::Leader;
        // Leader initializes all the next index for each follower.
        // See TLA⁺ spec L232
        self.next_index = self
            .config
            .members_iter()
            .map(|x| (x.hostname().to_string(), self.log.len()))
            .collect();

        // Once elected, the self must commit a new entry to it log.
        // It also needs to send send appendEntries to followers.
        self.log
            .append_entries(self.log.previous_index(), self.log.previous_term(), &[])
            .map_err(|e| Error::LogError(format!("failed to commit new log entry: {}", e)))?;
        self.broadcast_append_entries();
        info!("{}", self);
        Ok(())
    }

    /// This method change the server's role to [`Role::Candidate`].
    ///
    /// When the server has not received any heartbeat during a certain period,
    /// it starts an election by sending a vote request to it peers.
    pub fn become_candidate(&mut self) {
        // Only followers and candidate are allowed to start new election.
        assert!(
            self.role == Role::Follower || self.role == Role::Candidate,
            "leader should never became candidate or follower"
        );

        info!("{}", self);

        // After becoming a candidate, increase self current term.
        // See TLA⁺ spec.
        self.current_term
            .replace(self.current_term.map_or_else(|| 1, |t| t + 1));

        self.role = Role::Candidate;
        // Vote for self
        self.votes.insert(
            self.config.get(&self.id).hostname().to_string(),
            Vote::cast(self.config.get(&self.id).hostname()),
        );

        // Send request vote to all.
        self.broadcast_request_vote();
    }

    /// Send heartbeat to followers if self is a leader.
    pub fn send_leader_heartbeat(&mut self) {
        if self.role == Role::Leader {
            self.followers_heartbeat.clear();
            self.broadcast_append_entries();
        }
    }

    /// Start a new election
    pub fn election_timeout(&mut self) {
        if self.role != Role::Leader && !self.has_heard_from_leader {
            self.become_candidate();
            self.has_heard_from_leader = false;
        }
    }

    /// This method changes the role to `Role::Follower.
    fn become_follower(&mut self) -> bool {
        self.role = Role::Follower;
        false
    }

    /// Handle client requests.
    pub fn client_append_entry(&mut self, data: T, response: ConsensusSender) {
        if self.role != Role::Leader {
            return;
        }
        let mut entry = Vec::with_capacity(1);
        let current_term = if let Some(ref term) = self.current_term {
            *term
        } else {
            1
        };

        entry.push(Entry::new(current_term, data));
        if self
            .log
            .append_entries(self.log.previous_index(), self.log.previous_term(), &entry)
            .is_ok()
        {
            if let Some(ch) = self.waiting.remove(&self.log.previous_index().unwrap()) {
                if let Err(err) = ch.send(false) {
                    error!("unable to send consensus: {:?}", err);
                }
            } else if let Some(ch) = response {
                self.waiting.insert(self.log.previous_index().unwrap(), ch);
            }
            self.broadcast_append_entries();
        }
        info!("{}", self);
    }

    /// Send AppendEntries RPC to all peers.
    fn broadcast_append_entries(&mut self) {
        for peer in self.config.clone().members_iter().map(|x| x.hostname()) {
            if let Err(error) = self.send_append_entries(peer) {
                error!("{}", error);
            }
        }
    }

    /// Send AppendEntries RPC to a follower.
    fn send_append_entries(&mut self, peer: &str) -> Result<()> {
        // Get the previous log entry for this peer.

        if !self.next_index.contains_key(peer) {
            return Err(Error::PeerError(format!("unknown peer: {}", peer)));
        }
        let index = self.next_index[peer];

        // See TLA⁺ spec. L206-210
        let (previous_index, previous_term) = if index == 0 {
            (None, None)
        } else {
            (
                Some(index - 1),
                Some(self.log.get_entries()[index - 1].term),
            )
        };

        let entries = &self.log.get_entries()[index..self.log.len()];

        // Set the commit index the minimum value between self commit index
        // and the index of the last log entry.
        // See TLA⁺ spec L222
        let commit_index = cmp::min(self.commit_index, self.log.previous_index());

        let event = Event::new_append_entries(
            self.current_term,
            previous_index,
            previous_term,
            entries.to_vec(),
            commit_index,
            self.config.get(&self.id).hostname(),
            peer,
        );

        self.send(peer, event)
    }

    /// Send the message out.
    fn send(&mut self, peer: &str, event: Event<T>) -> Result<()> {
        task::block_on(async {
            self.messages
                .send(Message::new(peer, event))
                .await
                .map_err(|err| Error::SendError(format!("unable to send message: {:?}", err)))
        })
    }

    /// Send messages for followers to apply pending committed logs.
    fn apply_followers_commits_logs(&mut self) {
        let mut index = self.commit_index.unwrap();
        loop {
            if self.last_applied.is_none() || index > self.last_applied.unwrap_or_default() {
                if let Some(ref commits) = self.commits {
                    task::block_on(async {
                        if let Err(err) = commits
                            .clone()
                            .send(self.log.get_entries()[index].clone().data)
                            .await
                        {
                            error!("request for applying follower's log failed: {:?}", err);
                        }
                    })
                }

                if index == 0 {
                    break;
                }
                index -= 1;
            } else {
                break;
            }
        }
    }
    /// Handle AppendEntries request from the leader.
    fn handle_append_entries_request(&mut self, request: AppendEntries<T>) -> Result<()> {
        // The leader should ignore any received AppendEntries RPC call.
        if self.role == Role::Leader {
            return Ok(());
        }

        let AppendEntries {
            term,
            previous_index,
            previous_term,
            commit_index,
            entries,
            source,
            dest,
        } = request;

        self.update_term(term);
        self.has_heard_from_leader = true;

        let success = self
            .log
            .append_entries(previous_index, previous_term, &entries)
            .is_ok();

        if success {
            // Update self commit index to the minimum value between the leader commit index
            // and the index of the last log entry.
            // See TLA⁺ spec L222
            if let Some(index) = commit_index {
                if self.log.previous_term() == self.current_term {
                    self.commit_index = cmp::min(self.log.previous_index(), Some(index));
                }
            }
            if self.commit_index > self.last_applied {
                // Apply log to the state machine
                self.apply_followers_commits_logs();
                // Update index of last log applied.
                self.last_applied = self.commit_index;
            }
        }

        let match_index = self.log.previous_index();
        let resp = Event::new_append_entries_response(
            self.current_term,
            success,
            match_index,
            &dest,
            &source,
        );
        info!("{}", self);
        self.send(&source, resp)
    }

    /// Send client response.
    fn reply_client(&mut self) {
        // Apply log to the state machine
        let mut index = self.commit_index.unwrap();
        loop {
            if self.last_applied.is_none() || index > self.last_applied.unwrap_or_default() {
                // Update index of last log applied.
                if let Some(ch) = self.waiting.remove(&index) {
                    if let Err(err) = ch.send(true) {
                        error!("unable to send client reply: {:?}", err);
                    }
                };
                if index == 0 {
                    break;
                }
                index -= 1;
            } else {
                break;
            }
        }
    }

    /// Handle AppendEntries RPC response from a server.
    fn handle_append_entries_response(&mut self, response: AppendEntriesResponse) -> Result<()> {
        let AppendEntriesResponse {
            source,
            success,
            match_index,
            ..
        } = response;

        // Record heartbeat for this follower.
        self.followers_heartbeat.insert(source.clone());

        if success {
            // The next log to send is the log at the index immediately
            // after match index.
            // See TLA⁺ spec L395
            if let Some(index) = match_index {
                self.next_index.insert(source.clone(), index + 1);
            }
            // Update match index.
            // See TLA⁺ spec L396.
            self.match_index.insert(source, match_index);

            // Check whether a consensus has been reached.
            // In which case, the new commit index is the median high of the
            // match index values.
            let mut sorted_match = self.match_index.values().collect::<Vec<_>>();
            sorted_match.sort();
            if let Some(index) = sorted_match.into_iter().nth(self.match_index.len() / 2) {
                self.commit_index = *index;
            }

            if self.commit_index > self.last_applied {
                // Reply to all client waiting for consensus.
                self.reply_client();

                // Update last applied index.
                self.last_applied = self.commit_index;
            }
            Ok(())
        } else {
            // Decrement the next log index for this peer when the AppendEntries
            // consistency check failed.
            // See TLA⁺ spec. 398
            self.next_index.entry(source.clone()).and_modify(|x| {
                if *x > 0 {
                    *x -= 1;
                }
            });
            // Retry the RPC for this peer.
            self.send_append_entries(&source)
        }
    }

    /// Send a vote request to all peers.
    pub fn broadcast_request_vote(&mut self) {
        for peer in self.config.clone().members_iter().map(|x| x.hostname()) {
            if !self.votes.contains_key(peer) {
                let event: Event<T> = Event::new_request_vote(
                    self.current_term,
                    self.log.previous_index(),
                    self.log.previous_term(),
                    self.config.get(&self.id).hostname(),
                    peer,
                );

                if let Err(error) = self.send(peer, event) {
                    error!("{}", error)
                }
            }
        }
    }

    /// Handle a request vote from a candidate.
    pub fn handle_request_vote(&mut self, request: RequestVote) -> Result<()> {
        let RequestVote {
            term,
            last_index,
            last_term,
            source,
            dest,
        } = request;

        self.update_term(term);

        let mut vote = Vote::cast(&source);

        // See TLA⁺ spec L284
        if self.current_term > term {
            vote.remove();
        }

        // See TLA⁺ spec L285-L286
        if self.log.previous_term() > last_term
            || self.log.previous_term() == last_term && self.log.previous_index() > last_index
        {
            vote.remove();
        }

        // Set the peer self has voted for.
        // See TLA+ spec 291.
        if vote.granted {
            self.vote_for = Some(source.clone());
        }

        let resp: Event<T> =
            Event::new_request_vote_response(self.current_term, vote, &dest, &source);

        self.send(&source, resp)
    }

    /// Handle a request vote response from a peer.
    pub fn handle_request_vote_response(&mut self, response: RequestVoteResponse) -> Result<()> {
        let RequestVoteResponse {
            term, vote, source, ..
        } = response;

        self.update_term(term);

        // Ignore the response if its from a leader.
        if term > self.current_term {
            return Ok(());
        }
        self.votes.insert(source, vote);
        let granted = self
            .votes
            .values()
            .map(|x| x.granted)
            .filter(|&x| x)
            .count();

        // If self has a majory of votes then it becomes a leader.
        if granted > self.votes.len() / 2 {
            return self.become_leader();
        }

        Ok(())
    }

    /// Handle all possible normal events in the server.
    pub fn handle_message(&mut self, event: Event<T>) -> Result<()> {
        match event {
            Event::AppendEntries(event) => self.handle_append_entries_request(event),
            Event::AppendEntriesResponse(event) => self.handle_append_entries_response(event),
            Event::RequestVote(event) => self.handle_request_vote(event),
            Event::RequestVoteResponse(event) => self.handle_request_vote_response(event),
        }
    }

    fn update_term(&mut self, new_term: Term) {
        // Any RPC with newer term causes the recipient to advance its term first.
        // See TLA⁺ spec L405-L411
        if new_term > self.current_term {
            self.current_term = new_term;
            self.become_follower();
        }
    }
}

/// The `Role` type enumerates the possible roles of a server.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Role {
    Leader,
    Candidate,
    Follower,
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let role = match self {
            Role::Candidate => "Candidate",
            Role::Follower => "Follower",
            Role::Leader => "Leader",
        };

        write!(f, "{}", role)
    }
}

#[cfg(test)]
mod tests {
    use crate::log::InMemory;

    use super::*;
    use async_std::channel::{bounded, Receiver};
    use config::FileFormat;

    fn process_events(servers: &mut [Server<char>], messages: &Vec<Receiver<Message<char>>>) {
        loop {
            for receiver in messages {
                while let Ok(Message { dest, event }) = receiver.try_recv() {
                    if let Err(error) = servers
                        .iter_mut()
                        .find(|srv| srv.config.get(&srv.id).hostname() == dest)
                        .unwrap()
                        .handle_message(event)
                    {
                        error!("{}", error);
                    }
                }
            }

            if servers.iter().all(|x| x.messages.is_empty()) {
                break;
            }
        }
    }

    fn fig7_paper_servers() -> (Vec<Server<char>>, Vec<Receiver<Message<char>>>) {
        let config = Cluster::from_str(
            r#"
            systemLog:
              destination: "console"
              path: "raft.log"
              debug: true

            replication:
              id: "raft"
              members:
              - id: 0
                host: "0"
                me: true
              - id: 1
                host: "1"
              - id: 2
                host: "2"
              - id: 3
                host: "3"
              - id: 4
                host: "4"
              - id: 5
                host: "5"
              - id: 6
                host: "6"
              verbosity:
                election: true
                heartbeats: true
            "#,
            FileFormat::Yaml,
        )
        .unwrap();

        let size = 7;
        let mut servers = Vec::with_capacity(size);

        let mut receivers = Vec::with_capacity(size);
        for (i, log) in setup_logs_scenario_paper_fig7().into_iter().enumerate() {
            let term = log.previous_term();
            let (tx, rx) = bounded(100);
            let mut srv = Server::with_log(i, config.clone(), tx, Box::new(log), None);
            srv.current_term = term;
            servers.push(srv);
            receivers.push(rx);
        }

        (servers, receivers)
    }

    fn new_servers() -> (Vec<Server<char>>, Vec<Receiver<Message<char>>>) {
        let size = 5;
        let mut servers = Vec::with_capacity(size);

        let config = Cluster::from_str(
            r#"
            systemLog:
              destination: "console"
              path: "raft.log"
              debug: true

            replication:
              id: "raft"
              members:
              - id: 0
                host: "0"
                me: true
              - id: 1
                host: "1"
              - id: 2
                host: "2"
              - id: 3
                host: "3"
              - id: 4
                host: "4"
              verbosity:
                election: true
                heartbeats: true
            "#,
            FileFormat::Yaml,
        )
        .unwrap();
        let mut messages = Vec::with_capacity(size);
        for i in 0..size {
            let (tx, rx) = bounded(10);
            let srv = Server::new(i, config.clone(), tx, Box::new(InMemory::default()), None);
            servers.push(srv);
            messages.push(rx);
        }

        (servers, messages)
    }

    #[test]
    fn test_log_replication_scenario_paper_fig7() {
        let (mut servers, receivers) = fig7_paper_servers();
        {
            let srv0 = &mut servers[0];
            srv0.become_candidate();
            srv0.client_append_entry('m', None);
        }

        process_events(&mut servers, &receivers);

        for srv in servers[0..7].iter() {
            assert_eq!(
                servers[0].log.get_entries(),
                srv.log.get_entries(),
                "srv{} log does not match leader",
                srv.config.get(&srv.id).hostname()
            );
        }
    }

    #[test]
    #[should_panic]
    fn test_not_transition_leader_without_being_candidate() {
        let cfg = Cluster::from_str(
            r#"
            systemLog:
              destination: "console"
              path: "raft.log"
              debug: true

            replication:
              id: "raft"
              members:
              - id: 0
                host: "0"
                me: true
            verbosity:
              election: true
            heartbeats: true
            "#,
            FileFormat::Yaml,
        )
        .unwrap();
        let (tx, _) = bounded(0);
        let mut srv: Server<()> = Server::new(0, cfg, tx, Box::new(InMemory::default()), None);
        assert_eq!(srv.role, Role::Follower);
        srv.become_leader().expect("failed to become leader");
    }

    #[test]
    fn test_become_candidate() {
        let cfg = Cluster::from_str(
            r#"
            systemLog:
              destination: "console"
              path: "raft.log"
              debug: true

            replication:
              id: "raft"
              members:
              - id: 0
                host: "0"
                me: true
              verbosity:
                election: true
                heartbeats: true
            "#,
            FileFormat::Yaml,
        )
        .unwrap();
        let (tx, _) = bounded(1);
        let mut srv: Server<()> = Server::new(0, cfg, tx, Box::new(InMemory::default()), None);

        assert_eq!(srv.role, Role::Follower);
        srv.become_candidate();
        assert_eq!(srv.role, Role::Candidate);
        assert_eq!(srv.current_term, Some(1));
        srv.become_candidate();
        assert_eq!(srv.role, Role::Candidate);
        assert_eq!(srv.current_term, Some(2));
    }

    #[test]
    fn test_become_leader() {
        let cfg = Cluster::from_str(
            r#"
            systemLog:
              destination: "console"
              path: "raft.log"
              debug: true

            replication:
              id: "raft"
              members:
              - id: 0
                host: "0"
                me: true
              verbosity:
                election: true
                heartbeats: true
            "#,
            FileFormat::Yaml,
        )
        .unwrap();
        let (tx, _) = bounded(1);
        let mut srv: Server<()> = Server::new(0, cfg, tx, Box::new(InMemory::default()), None);
        assert_eq!(srv.role, Role::Follower);
        srv.become_candidate();
        assert_eq!(srv.role, Role::Candidate);
        assert_eq!(srv.current_term, Some(1));
        srv.become_leader().expect("failed to become leader");
        assert_eq!(srv.role, Role::Leader);
        assert_eq!(srv.current_term, Some(1));
    }

    #[test]
    #[should_panic]
    fn test_leader_cannot_become_candidate() {
        let cfg = Cluster::from_str(
            r#"
            systemLog:
              destination: "console"
              path: "raft.log"
              debug: true

            replication:
              id: "raft"
              members:
              - id: 0
                host: "0"
                me: true
              verbosity:
                election: true
                heartbeats: true
            "#,
            FileFormat::Yaml,
        )
        .unwrap();
        let (tx, _) = bounded(1);
        let mut srv: Server<()> = Server::new(0, cfg, tx, Box::new(InMemory::default()), None);
        srv.role = Role::Leader;
        srv.become_candidate();
    }

    #[test]
    fn test_consensus_log_replication_paper_fig7() {
        let (mut servers, receivers) = fig7_paper_servers();
        {
            let srv0 = &mut servers[0];
            srv0.become_candidate();
            srv0.client_append_entry('m', None);
        }

        process_events(&mut servers, &receivers);

        {
            let srv0 = &mut servers[0];
            srv0.client_append_entry('n', None);
        }

        process_events(&mut servers, &receivers);

        assert_eq!(servers[0].last_applied, Some(11));

        {
            let srv0 = &mut servers[0];
            srv0.client_append_entry('o', None);
        }

        process_events(&mut servers, &receivers);
        assert_eq!(servers[0].last_applied, Some(12));
        for srv in servers[1..7].iter() {
            assert_eq!(
                srv.last_applied,
                Some(11),
                "expected last applied Some(11) found {:?} for srv{}",
                srv.last_applied,
                srv.config.get(&srv.id).hostname(),
            );
        }
    }

    #[test]
    fn test_election_paper_fig7() {
        let config = Cluster::from_str(
            r#"
            systemLog:
              destination: "console"
              path: "raft.log"
              debug: true

            replication:
              id: "raft"
              members:
              - id: 0
                host: "0"
                me: true
              - id: 1
                host: "1"
              - id: 2
                host: "2"
              - id: 3
                host: "3"
              - id: 4
                host: "4"
              - id: 5
                host: "5"
              - id: 6
                host: "6"
              verbosity:
                election: true
                heartbeats: true
            "#,
            FileFormat::Yaml,
        )
        .unwrap();

        let size = 7;
        let mut servers = Vec::with_capacity(size);

        let mut receivers = Vec::with_capacity(size);
        for (i, mut log) in setup_logs_scenario_paper_fig7().into_iter().enumerate() {
            let term = log.previous_term();
            let (tx, rx) = bounded(100);
            if i == 0 {
                log.entries.remove(log.len() - 1);
            }
            let mut srv = Server::with_log(i, config.clone(), tx, Box::new(log), None);
            srv.current_term = term;
            servers.push(srv);
            receivers.push(rx);
        }

        let srv0 = &mut servers[0];
        srv0.become_candidate();
        process_events(&mut servers, &receivers);

        assert!(servers[0].votes.get("0").unwrap().granted);
        assert!(servers[0].votes.get("1").unwrap().granted);
        assert!(servers[0].votes.get("2").unwrap().granted);
        assert!(!servers[0].votes.get("3").unwrap().granted);
        assert!(!servers[0].votes.get("4").unwrap().granted);
        assert!(servers[0].votes.get("5").unwrap().granted);
        assert!(servers[0].votes.get("6").unwrap().granted);
    }

    #[test]
    fn test_server2_cannot_become_leader_paper_fig7() {
        let (mut servers, receivers) = fig7_paper_servers();
        {
            let srv2 = &mut servers[2];
            srv2.become_candidate();
        }
        process_events(&mut servers, &receivers);

        assert!(!servers[2].votes.get("0").unwrap().granted);
        assert!(!servers[2].votes.get("1").unwrap().granted);
        assert!(servers[2].votes.get("2").unwrap().granted);
        assert!(!servers[2].votes.get("3").unwrap().granted);
        assert!(!servers[2].votes.get("4").unwrap().granted);
        assert!(!servers[2].votes.get("5").unwrap().granted);
        assert!(servers[2].votes.get("6").unwrap().granted,);

        assert_eq!(
            servers[2].role,
            Role::Follower,
            "server2 should have gone back to follower"
        );
    }

    #[test]
    fn test_heartbeat_paper_fig7() {
        let (mut servers, receivers) = fig7_paper_servers();
        for srv in servers[..].iter() {
            assert!(!srv.has_heard_from_leader);
        }

        assert!(servers[0].followers_heartbeat.is_empty());

        {
            let srv0 = &mut servers[0];
            srv0.election_timeout();
        }

        process_events(&mut servers, &receivers);

        for srv in servers[1..].iter() {
            assert!(srv.has_heard_from_leader);
            assert!(servers[0]
                .followers_heartbeat
                .contains(srv.config.get(&srv.id).hostname()));
        }
    }

    #[test]
    fn test_election_timeout_paper_fig7() {
        let (mut servers, receivers) = fig7_paper_servers();
        for srv in servers[..].iter() {
            assert!(!srv.has_heard_from_leader);
        }

        assert!(servers[0].followers_heartbeat.is_empty());

        {
            let srv0 = &mut servers[0];
            srv0.election_timeout();
        }

        {
            let srv2 = &mut servers[2];
            srv2.election_timeout();
        }

        assert_eq!(servers[0].role, Role::Candidate);
        assert_eq!(servers[0].current_term, Some(9));
        assert_eq!(servers[2].role, Role::Candidate);
        assert_eq!(servers[2].current_term, Some(5));

        {
            let srv0 = &mut servers[0];
            srv0.election_timeout();
        }

        {
            let srv2 = &mut servers[2];
            srv2.election_timeout();
        }

        assert_eq!(servers[0].role, Role::Candidate);
        assert_eq!(servers[0].current_term, Some(10));
        assert_eq!(servers[2].role, Role::Candidate);
        assert_eq!(servers[2].current_term, Some(6));

        process_events(&mut servers, &receivers);
        assert_eq!(servers[0].role, Role::Leader);
        assert_eq!(servers[2].role, Role::Follower);
    }

    #[test]
    fn test_received_heartbeat_during_election_paper_fig7() {
        let (mut servers, receivers) = fig7_paper_servers();
        for srv in servers[..].iter() {
            assert!(!srv.has_heard_from_leader);
        }

        assert!(servers[0].followers_heartbeat.is_empty());

        {
            let srv0 = &mut servers[0];
            srv0.election_timeout();
        }

        {
            let srv2 = &mut servers[2];
            srv2.election_timeout();
        }

        assert_eq!(servers[0].role, Role::Candidate);
        assert_eq!(servers[0].current_term, Some(9));
        assert_eq!(servers[2].role, Role::Candidate);
        assert_eq!(servers[2].current_term, Some(5));
        {
            let srv0 = &mut servers[0];
            srv0.client_append_entry('m', None);
        }

        process_events(&mut servers, &receivers);
        assert_eq!(servers[0].role, Role::Leader);
        assert_eq!(servers[2].role, Role::Follower);
    }

    #[test]
    fn test_new_servers() {
        let (mut servers, receivers) = new_servers();
        {
            let srv0 = &mut servers[0];
            srv0.become_candidate();
        }

        process_events(&mut servers, &receivers);

        assert_eq!(servers[0].role, Role::Leader);
        for srv in servers[1..5].iter() {
            assert_eq!(
                srv.last_applied,
                None,
                "expected last applied None found {:?} for srv{}",
                srv.last_applied,
                srv.config.get(&srv.id).hostname(),
            );

            assert_eq!(
                srv.current_term,
                Some(1),
                "expected current term Some(1) found {:?} for srv{}",
                srv.current_term,
                srv.config.get(&srv.id).hostname(),
            );
        }

        assert_eq!(servers[0].current_term, Some(1));
        {
            let srv0 = &mut servers[0];
            srv0.client_append_entry('a', None);
        }
        process_events(&mut servers, &receivers);
        assert_eq!(servers[0].last_applied, Some(0));
        for srv in servers[1..5].iter() {
            assert_eq!(
                srv.last_applied,
                None,
                "expected last applied None found {:?} for srv{}",
                srv.last_applied,
                srv.config.get(&srv.id).hostname(),
            );
        }

        {
            let srv0 = &mut servers[0];
            srv0.client_append_entry('b', None);
        }
        process_events(&mut servers, &receivers);
        assert_eq!(servers[0].last_applied, Some(1));
        for srv in servers[1..5].iter() {
            assert_eq!(
                srv.last_applied,
                Some(0),
                "expected last applied 0 found {:?} for srv{}",
                srv.last_applied,
                srv.config.get(&srv.id).hostname(),
            );
        }
    }

    fn setup_logs_scenario_paper_fig7() -> Vec<InMemory<char>> {
        vec![
            InMemory::from(vec![
                Entry::new(1, 'a'),
                Entry::new(1, 'b'),
                Entry::new(1, 'c'),
                Entry::new(4, 'd'),
                Entry::new(4, 'f'),
                Entry::new(5, 'g'),
                Entry::new(5, 'h'),
                Entry::new(6, 'i'),
                Entry::new(6, 'j'),
                Entry::new(6, 'k'),
                Entry::new(8, 'l'),
            ]),
            InMemory::from(vec![
                Entry::new(1, 'a'),
                Entry::new(1, 'b'),
                Entry::new(1, 'c'),
                Entry::new(4, 'd'),
                Entry::new(4, 'f'),
                Entry::new(5, 'g'),
                Entry::new(5, 'h'),
                Entry::new(6, 'i'),
                Entry::new(6, 'j'),
            ]),
            InMemory::from(vec![
                Entry::new(1, 'a'),
                Entry::new(1, 'b'),
                Entry::new(1, 'c'),
                Entry::new(4, 'd'),
            ]),
            InMemory::from(vec![
                Entry::new(1, 'a'),
                Entry::new(1, 'b'),
                Entry::new(1, 'c'),
                Entry::new(4, 'd'),
                Entry::new(4, 'f'),
                Entry::new(5, 'g'),
                Entry::new(5, 'h'),
                Entry::new(6, 'i'),
                Entry::new(6, 'j'),
                Entry::new(6, 'k'),
                Entry::new(6, 'l'),
            ]),
            InMemory::from(vec![
                Entry::new(1, 'a'),
                Entry::new(1, 'b'),
                Entry::new(1, 'c'),
                Entry::new(4, 'd'),
                Entry::new(4, 'f'),
                Entry::new(5, 'g'),
                Entry::new(5, 'h'),
                Entry::new(6, 'i'),
                Entry::new(6, 'j'),
                Entry::new(6, 'k'),
                Entry::new(7, 'l'),
                Entry::new(7, 'm'),
            ]),
            InMemory::from(vec![
                Entry::new(1, 'a'),
                Entry::new(1, 'b'),
                Entry::new(1, 'c'),
                Entry::new(4, 'd'),
                Entry::new(4, 'f'),
                Entry::new(4, 'g'),
                Entry::new(4, 'h'),
            ]),
            InMemory::from(vec![
                Entry::new(1, 'a'),
                Entry::new(1, 'b'),
                Entry::new(1, 'c'),
                Entry::new(2, 'd'),
                Entry::new(2, 'f'),
                Entry::new(2, 'g'),
                Entry::new(3, 'h'),
                Entry::new(3, 'i'),
                Entry::new(3, 'j'),
                Entry::new(3, 'k'),
                Entry::new(3, 'l'),
            ]),
        ]
    }
}
