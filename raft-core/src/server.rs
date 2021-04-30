//! Raft server
//!
//! This module contains the Raft server implementation.

use crate::event::Event;
use crate::log::{Entry, Log};
use crate::net::Node;
use crate::types::{Index, Term};
use std::cmp;
use std::collections::HashMap;
use std::fmt;

/// The type `Server` is the raft server.
#[derive(Debug)]
pub struct Server<N, V> {
    // The node lives on the server.
    node: N,

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
}

impl<N, V> fmt::Display for Server<N, V>
where
    V: Clone + fmt::Debug,
    N: Node,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Server<id={}, term={:?}, log={:?}>",
            self.node.get_id(),
            self.current_term,
            self.log
        )
    }
}

impl<N, V> Server<N, V>
where
    N: Node<EntryKind = V>,
    V: fmt::Debug + Clone,
{
    /// Create new Raft server.
    pub fn new(node: N) -> Self {
        Server {
            node,
            log: Log::<V>::new(),
            current_term: None,
            vote_for: None,
            commit_index: None,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
        }
    }

    /// Reset some internal state after winning an election.
    pub fn become_leader(&mut self) {
        // Leader initializes all the next index for each follower.
        // See TLA⁺ spec L232
        self.next_index = self
            .node
            .peers()
            .iter()
            .map(|x| (x.clone(), self.log.len()))
            .collect();
    }

    /// Create new Raft server with a existing log.
    pub fn with_log(node: N, log: Log<V>) -> Self {
        let next_index: HashMap<String, usize> =
            node.peers().iter().map(|x| (x.clone(), 0)).collect();

        Server {
            node,
            log,
            current_term: None,
            vote_for: None,
            commit_index: None,
            next_index,
            match_index: HashMap::new(),
        }
    }

    /// Accept client request.
    pub fn handle_client(&mut self, data: V) {
        let mut entry = Vec::with_capacity(1);
        let current_term = if let Some(ref term) = self.current_term {
            term.clone()
        } else {
            1
        };

        entry.push(Entry::new(current_term, data));
        let success =
            self.log
                .append_entries(self.log.previous_index(), self.log.previous_term(), &entry);
        if success {
            self.send_all_peers_append_entries();
        }
    }

    fn send_all_peers_append_entries(&mut self) {
        for peer in self.node.peers() {
            self.send_append_entries(&peer);
        }
    }

    /// Send AppendEntries RPC to followers
    fn send_append_entries(&mut self, peer: &str) {
        // Get the previous log entry for this peer.

        let index = self.next_index[peer];

        // See TLA⁺ spec. L206
        let previous_index = if index == 0 { None } else { Some(index - 1) };

        // See TLA⁺ spec. L207-L210
        let previous_term = if index == 0 {
            None
        } else {
            Some(self.log.entries[index].term.clone())
        };

        let entries = &self.log.entries[index..self.log.len()];

        // Set the commit index the minimum value between the leader commit index
        // and the index of the last log entry.
        // See TLA⁺ spec L222
        let commit_index = cmp::min(self.commit_index, self.log.previous_index());

        let msg = Event::new_append_entries(
            self.current_term,
            previous_index,
            previous_term,
            entries.to_vec(),
            commit_index,
            self.node.get_id(),
            &peer,
        );

        self.node.send(&peer, msg);
    }

    /// Handle AppendEntries response.
    fn handle_append_entries_request(&mut self, event: Event<V>) {
        if let Event::AppendEntries {
            previous_index,
            previous_term,
            commit_index,
            entries,
            source,
            dest,
            ..
        } = event
        {
            let success = self
                .log
                .append_entries(previous_index, previous_term, &entries);

            if success {
                self.next_index.entry(source.clone()).and_modify(|x| {
                    *x += 1;
                });

                // Update self commit index to the minimum value between the leader commit index
                // and the index of the last log entry.
                // See TLA⁺ spec L222
                if let Some(index) = commit_index {
                    self.commit_index = cmp::min(self.log.previous_index(), Some(index));
                }
            }

            let match_index = self.log.previous_index();
            let resp = Event::new_append_entries_response(
                self.current_term.clone(),
                success,
                match_index,
                &dest,
                &source,
            );
            self.node.send(&source, resp);
        }
    }

    fn handle_append_entries_response(&mut self, event: Event<V>) {
        if let Event::AppendEntriesResponse {
            source,
            success,
            match_index,
            ..
        } = event
        {
            if success {
                // The next log to send is the log at the index immediately
                // after match index.
                // See TLA⁺ spec L395
                self.next_index
                    .insert(source.clone(), match_index.unwrap() + 1);

                // Update match index.
                // See TLA⁺ spec L396.
                self.match_index.insert(source, match_index);
            } else {
                // Decrement the next log index for this peer
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
    }

    /// Handle events in the server.
    pub fn handle(&mut self, event: Event<V>) {
        match event {
            Event::AppendEntries { .. } => self.handle_append_entries_request(event),
            Event::AppendEntriesResponse { .. } => self.handle_append_entries_response(event),
            _ => (),
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::{FakeNetwork, FakeNode};

    fn process_events(servers: &mut [Server<FakeNode<char>, char>], net: &mut FakeNetwork<char>) {
        loop {
            net.forward();
            for srv in servers.iter_mut() {
                while let Some(event) = srv.node.receive() {
                    srv.handle(event);
                }
            }
            if net.buf.borrow().is_empty() {
                break;
            }
        }
    }

    fn create_servers() -> (FakeNetwork<char>, Vec<Server<FakeNode<char>, char>>) {
        let size = 7;
        let mut net = FakeNetwork::new(size);
        let mut servers = Vec::with_capacity(size);

        for (i, log) in setup_logs_scenario_paper_fig7().into_iter().enumerate() {
            let node = net.new_node(i);
            let term = log.previous_term();
            let mut srv = Server::with_log(node, log);
            srv.current_term = term;
            servers.push(srv);
        }

        return (net, servers);
    }

    #[test]
    fn test_log_replication_scenario_paper_fig7() {
        let (mut net, mut servers) = create_servers();
        {
            let srv0 = &mut servers[0];
            srv0.become_leader();
            srv0.handle_client('m');
        }

        process_events(&mut servers, &mut net);

        for srv in servers[0..7].iter() {
            assert_eq!(
                servers[0].log.entries,
                srv.log.entries,
                "srv{} log does not match leader",
                srv.node.get_id()
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
                Entry::new(6, 'k'),
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
