//! Raft server
//!
//! This module contains the Raft server implementation.

use crate::config::Cluster;

use crate::event::{
    AppendEntries, AppendEntriesResponse, Event, Message, RequestVote, RequestVoteResponse, Vote,
};
use crate::log::{Entry, Log};
use crate::types::{Index, Term};
use std::cmp;
use std::collections::vec_deque::Drain;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;

/// The type `Server` is the raft server.
#[derive(Debug)]
pub struct Server<V> {
    // Server Id
    id: usize,

    // The server configuration
    config: Cluster,

    // The log for this server.
    log: Log<V>,

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

    // Hartbeat from followers
    followers_hartbeat: HashSet<String>,

    // Indicate whether is hartbeat was received from leader.
    leader_hartbeat_is_received: bool,

    // Emitted server events and destination.
    messages: VecDeque<Message<V>>,
}

impl<V> fmt::Display for Server<V>
where
    V: Clone + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let commit_index = self
            .commit_index
            .clone()
            .map_or_else(|| String::from("None"), |x| x.to_string());

        let vote_for = self
            .vote_for
            .clone()
            .unwrap_or_else(|| String::from("None"));

        write!(
            f,
            "Server<id={}, term={:?}, role={},  commit_index={}, vote_for={}, log={:?}>",
            self.config.get(&self.id).hostname(),
            self.current_term,
            self.role,
            commit_index,
            vote_for,
            self.log
        )
    }
}

impl<V> Server<V>
where
    V: Clone + fmt::Debug,
{
    /// Create new Raft server.
    pub fn new(id: usize, config: Cluster) -> Self {
        let size = config.size();
        Server {
            id,
            config,
            log: Log::<V>::default(),
            current_term: None,
            vote_for: None,
            commit_index: None,
            next_index: HashMap::with_capacity(size),
            match_index: HashMap::with_capacity(size),
            role: Role::Follower,
            last_applied: None,
            votes: HashMap::with_capacity(size),
            followers_hartbeat: HashSet::with_capacity(size),
            leader_hartbeat_is_received: false,
            messages: VecDeque::new(),
        }
    }

    /// Create new Raft server with a existing log.
    pub fn with_log(id: usize, config: Cluster, log: Log<V>) -> Self {
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
            followers_hartbeat: HashSet::with_capacity(size),
            leader_hartbeat_is_received: false,
            messages: VecDeque::new(),
        }
    }

    /// Reset some internal state after winning an election.
    pub fn become_leader(&mut self) {
        if self.role == Role::Leader {
            return;
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
        if self
            .log
            .append_entries(self.log.previous_index(), self.log.previous_term(), &[])
            .is_ok()
        {
            self.broadcast_append_entries();
        }
    }

    /// This method change the server's role to [`Role::Candidate`].
    ///
    /// When the server has not received any hartbeat during a certain period,
    /// it starts an election by sending a vote request to it peers.
    pub fn become_candidate(&mut self) {
        // Only followers and candidate are allowed to start new election.
        assert!(
            self.role == Role::Follower || self.role == Role::Candidate,
            "leader should never became candidate or follower"
        );

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

    /// Send hartbeat to followers if self is a leader.
    pub fn send_leader_hartbeat(&mut self) {
        if self.role == Role::Leader {
            self.followers_hartbeat.clear();
            self.broadcast_append_entries();
        }
    }

    /// Start a new election
    pub fn election_timeout(&mut self) {
        if self.role != Role::Leader && !self.leader_hartbeat_is_received {
            self.become_candidate();
            self.leader_hartbeat_is_received = false;
        }
    }

    /// This method changes the role to `Role::Follower.
    fn become_follower(&mut self) {
        self.role = Role::Follower;
    }

    /// Handle client requests.
    pub fn handle_client(&mut self, data: V) {
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
            self.broadcast_append_entries();
        }
    }

    /// Send AppendEntries RPC to all peers.
    fn broadcast_append_entries(&mut self) {
        for peer in self.config.clone().members_iter().map(|x| x.hostname()) {
            self.send_append_entries(peer);
        }
    }

    /// Send AppendEntries RPC a follower.
    fn send_append_entries(&mut self, peer: &str) {
        // Get the previous log entry for this peer.

        let index = self.next_index[peer];

        // See TLA⁺ spec. L206-210
        let (previous_index, previous_term) = if index == 0 {
            (None, None)
        } else {
            (Some(index - 1), Some(self.log.entries[index - 1].term))
        };

        let entries = &self.log.entries[index..self.log.len()];

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
            &peer,
        );

        self.messages.push_back(Message::new(&peer, event));
    }

    /// Handle AppendEntries request from the leader.
    fn handle_append_entries_request(&mut self, request: AppendEntries<V>) {
        // The leader should ignore any received AppendEntries RPC call.
        if self.role == Role::Leader {
            return;
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
        self.leader_hartbeat_is_received = true;

        let success = if {
            self.log
                .append_entries(previous_index, previous_term, &entries)
                .is_ok()
        } {
            true
        } else {
            false
        };

        if success {
            // Update self commit index to the minimum value between the leader commit index
            // and the index of the last log entry.
            // See TLA⁺ spec L222
            if let Some(index) = commit_index {
                self.commit_index = cmp::min(self.log.previous_index(), Some(index));
            }
            if self.commit_index > self.last_applied {
                // Apply log to the state machine
                // ....

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
        self.messages.push_back(Message::new(&source, resp));
    }

    /// Handle AppendEntries RPC response from a server.
    fn handle_append_entries_response(&mut self, response: AppendEntriesResponse) {
        let AppendEntriesResponse {
            source,
            success,
            match_index,
            ..
        } = response;

        // Record hartbeat for this follower.
        self.followers_hartbeat.insert(source.clone());

        if success {
            // The next log to send is the log at the index immediately
            // after match index.
            // See TLA⁺ spec L395
            if match_index.is_some() {
                self.next_index
                    .insert(source.clone(), match_index.unwrap() + 1);
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
                // Do application request

                // Update last applied index.
                self.last_applied = self.commit_index;
            }
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
            self.send_append_entries(&source);
        }
    }

    /// Send a vote request to all peers.
    pub fn broadcast_request_vote(&mut self) {
        for peer in self.config.members_iter().map(|x| x.hostname()) {
            if !self.votes.contains_key(peer) {
                let event: Event<V> = Event::new_request_vote(
                    self.current_term,
                    self.log.previous_index(),
                    self.log.previous_term(),
                    self.config.get(&self.id).hostname(),
                    peer,
                );

                self.messages.push_back(Message::new(peer, event));
            }
        }
    }

    /// Handle a request vote from a candidate.
    pub fn handle_request_vote(&mut self, request: RequestVote) {
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

        let resp: Event<V> =
            Event::new_request_vote_response(self.current_term, vote, &dest, &source);

        self.messages.push_back(Message::new(&source, resp));
    }

    /// Consume all outgoing messages.
    pub fn consume_messages(&mut self) -> Drain<'_, Message<V>> {
        self.messages.drain(..)
    }

    /// Handle a request vote response from a peer.
    pub fn handle_request_vote_response(&mut self, response: RequestVoteResponse) {
        let RequestVoteResponse {
            term, vote, source, ..
        } = response;

        self.update_term(term);

        // Ignore the response if its from a leader.
        if term > self.current_term {
            return;
        }
        self.votes.insert(source, vote);
        let granted: Vec<_> = self
            .votes
            .values()
            .map(|x| x.granted)
            .filter(|&x| x)
            .collect();

        // If self has a majory of votes then it becomes a leader.
        if granted.len() > self.votes.len() / 2 {
            self.become_leader();
        }
    }

    /// Handle all possible normal events in the server.
    pub fn handle(&mut self, event: Event<V>) {
        match event {
            Event::AppendEntries(event) => self.handle_append_entries_request(event),
            Event::AppendEntriesResponse(event) => self.handle_append_entries_response(event),
            Event::RequestVote(event) => self.handle_request_vote(event),
            Event::RequestVoteResponse(event) => self.handle_request_vote_response(event),
        };
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
    use super::*;
    use config::FileFormat;

    fn process_events(servers: &mut [Server<char>]) {
        loop {
            let mut messages = VecDeque::new();
            for srv in servers.iter_mut() {
                messages.extend(srv.consume_messages());
            }

            if messages.is_empty() {
                break;
            }

            while let Some(Message { dest, event }) = messages.pop_front() {
                servers
                    .iter_mut()
                    .find(|srv| srv.config.get(&srv.id).hostname() == dest)
                    .unwrap()
                    .handle(event);
            }
        }
    }

    fn fig7_paper_servers() -> Vec<Server<char>> {
        let size = 7;
        let mut servers = Vec::with_capacity(size);

        let config = Cluster::init_from_str(
            r#"
	        id = "raft"

                [[members]]
	        id = 0
	        host = "0"

                [[members]]
		id = 1
		host = "1"


                [[members]]
		id = 2
		host = "2"


                [[members]]
		id = 3
		host = "3"


                [[members]]
		id = 4
		host = "4"


                [[members]]
		id = 5
		host = "5"


                [[members]]
		id = 6
		host = "6"
            "#,
            FileFormat::Toml,
        )
        .unwrap();
        for (i, log) in setup_logs_scenario_paper_fig7().into_iter().enumerate() {
            let term = log.previous_term();
            let mut srv = Server::with_log(i, config.clone(), log);
            srv.current_term = term;
            servers.push(srv);
        }

        return servers;
    }

    fn new_servers() -> Vec<Server<char>> {
        let size = 5;
        let mut servers = Vec::with_capacity(size);

        let config = Cluster::init_from_str(
            r#"
	        id = "raft"

                [[members]]
	        id = 0
	        host = "0"

                [[members]]
		id = 1
		host = "1"


                [[members]]
		id = 2
		host = "2"


                [[members]]
		id = 3
		host = "3"


                [[members]]
		id = 4
		host = "4"
            "#,
            FileFormat::Toml,
        )
        .unwrap();
        for i in 0..size {
            let srv = Server::new(i, config.clone());
            servers.push(srv);
        }

        return servers;
    }

    #[test]
    fn test_log_replication_scenario_paper_fig7() {
        let mut servers = fig7_paper_servers();
        {
            let srv0 = &mut servers[0];
            srv0.become_candidate();
            srv0.handle_client('m');
        }

        process_events(&mut servers);

        for srv in servers[0..7].iter() {
            assert_eq!(
                servers[0].log.entries,
                srv.log.entries,
                "srv{} log does not match leader",
                srv.config.get(&srv.id).hostname()
            );
        }
    }

    #[test]
    #[should_panic]
    fn test_not_transition_leader_without_being_candidate() {
        let cfg = Cluster::init_from_str(
            r#"
	        id = "raft"

                [[members]]
	        id = 0
	        host = "0"
            "#,
            FileFormat::Toml,
        )
        .unwrap();
        let mut srv: Server<()> = Server::new(0, cfg);
        assert_eq!(srv.role, Role::Follower);
        srv.become_leader();
    }

    #[test]
    fn test_become_candidate() {
        let cfg = Cluster::init_from_str(
            r#"
	        id = "raft"

                [[members]]
	        id = 0
	        host = "0"
            "#,
            FileFormat::Toml,
        )
        .unwrap();
        let mut srv: Server<()> = Server::new(0, cfg);

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
        let cfg = Cluster::init_from_str(
            r#"
	        id = "raft"

                [[members]]
	        id = 0
	        host = "0"
            "#,
            FileFormat::Toml,
        )
        .unwrap();
        let mut srv: Server<()> = Server::new(0, cfg);
        assert_eq!(srv.role, Role::Follower);
        srv.become_candidate();
        assert_eq!(srv.role, Role::Candidate);
        assert_eq!(srv.current_term, Some(1));
        srv.become_leader();
        assert_eq!(srv.role, Role::Leader);
        assert_eq!(srv.current_term, Some(1));
    }

    #[test]
    #[should_panic]
    fn test_leader_cannot_become_candidate() {
        let cfg = Cluster::init_from_str(
            r#"
	        id = "raft"

                [[members]]
	        id = 0
	        host = "0"
            "#,
            FileFormat::Toml,
        )
        .unwrap();
        let mut srv: Server<()> = Server::new(0, cfg);
        srv.role = Role::Leader;
        srv.become_candidate();
    }

    #[test]
    fn test_consensus_log_replication_paper_fig7() {
        let mut servers = fig7_paper_servers();
        {
            let srv0 = &mut servers[0];
            srv0.become_candidate();
            srv0.handle_client('m');
        }

        process_events(&mut servers);

        assert_eq!(servers[0].last_applied, Some(11));

        {
            let srv0 = &mut servers[0];
            srv0.handle_client('m');
        }

        process_events(&mut servers);
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
        let mut servers = fig7_paper_servers();
        {
            let srv0 = &mut servers[0];
            srv0.log.entries.pop();
            srv0.become_candidate();
        }
        process_events(&mut servers);

        assert_eq!(servers[0].votes.get("0").unwrap().granted, true);
        assert_eq!(servers[0].votes.get("1").unwrap().granted, true);
        assert_eq!(servers[0].votes.get("2").unwrap().granted, true);
        assert_eq!(servers[0].votes.get("3").unwrap().granted, false);
        assert_eq!(servers[0].votes.get("4").unwrap().granted, false);
        assert_eq!(servers[0].votes.get("5").unwrap().granted, true);
        assert_eq!(servers[0].votes.get("6").unwrap().granted, true);
    }

    #[test]
    fn test_server2_cannot_become_leader_paper_fig7() {
        let mut servers = fig7_paper_servers();
        {
            let srv2 = &mut servers[2];
            srv2.become_candidate();
        }
        process_events(&mut servers);

        assert_eq!(servers[2].votes.get("0").unwrap().granted, false);
        assert_eq!(servers[2].votes.get("1").unwrap().granted, false);
        assert_eq!(servers[2].votes.get("2").unwrap().granted, true);
        assert_eq!(servers[2].votes.get("3").unwrap().granted, false);
        assert_eq!(servers[2].votes.get("4").unwrap().granted, false);
        assert_eq!(servers[2].votes.get("5").unwrap().granted, false);
        assert_eq!(servers[2].votes.get("6").unwrap().granted, true);

        assert_eq!(
            servers[2].role,
            Role::Follower,
            "server2 should have gone back to follower"
        );
    }

    #[test]
    fn test_hartbeat_paper_fig7() {
        let mut servers = fig7_paper_servers();
        for srv in servers[..].iter() {
            assert!(!srv.leader_hartbeat_is_received);
        }

        assert!(servers[0].followers_hartbeat.is_empty());

        {
            let srv0 = &mut servers[0];
            srv0.election_timeout();
        }

        process_events(&mut servers);

        for srv in servers[1..].iter() {
            assert!(srv.leader_hartbeat_is_received);
            assert!(servers[0]
                .followers_hartbeat
                .contains(srv.config.get(&srv.id).hostname()));
        }
    }

    #[test]
    fn test_election_timeout_paper_fig7() {
        let mut servers = fig7_paper_servers();
        for srv in servers[..].iter() {
            assert!(!srv.leader_hartbeat_is_received);
        }

        assert!(servers[0].followers_hartbeat.is_empty());

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

        process_events(&mut servers);
        assert_eq!(servers[0].role, Role::Leader);
        assert_eq!(servers[2].role, Role::Follower);
    }

    #[test]
    fn test_received_hartbeat_during_election_paper_fig7() {
        let mut servers = fig7_paper_servers();
        for srv in servers[..].iter() {
            assert!(!srv.leader_hartbeat_is_received);
        }

        assert!(servers[0].followers_hartbeat.is_empty());

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
            srv0.handle_client('m');
        }

        process_events(&mut servers);
        assert_eq!(servers[0].role, Role::Leader);
        assert_eq!(servers[2].role, Role::Follower);
    }

    #[test]
    fn test_new_servers() {
        let mut servers = new_servers();
        {
            let srv0 = &mut servers[0];
            srv0.become_candidate();
        }

        process_events(&mut servers);

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
            srv0.handle_client('a');
        }
        process_events(&mut servers);
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
            srv0.handle_client('b');
        }
        process_events(&mut servers);
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

    fn setup_logs_scenario_paper_fig7() -> Vec<Log<char>> {
        vec![
            Log::from(vec![
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
            Log::from(vec![
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
            Log::from(vec![
                Entry::new(1, 'a'),
                Entry::new(1, 'b'),
                Entry::new(1, 'c'),
                Entry::new(4, 'd'),
            ]),
            Log::from(vec![
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
            Log::from(vec![
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
            Log::from(vec![
                Entry::new(1, 'a'),
                Entry::new(1, 'b'),
                Entry::new(1, 'c'),
                Entry::new(4, 'd'),
                Entry::new(4, 'f'),
                Entry::new(4, 'g'),
                Entry::new(4, 'h'),
            ]),
            Log::from(vec![
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
