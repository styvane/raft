//! Raft network
//!
//! This module contains the Raft network node behavior.

use crate::event::Event;
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::rc::Rc;

/// The `Node` trait defines a node behavior on the network.
///
/// The basic operations of a node in the Raft cluster is to send and receive messages
/// from it peers.
///
/// The `send` operation allows the node to send a message to it peers.
/// The `receive` operation allows a node to receive a message from it peers.
pub trait Node {
    type EntryKind;

    fn send(&mut self, dest: &str, event: Event<Self::EntryKind>);
    fn receive(&mut self) -> Option<Event<Self::EntryKind>>;
    fn peers(&self) -> Vec<String>;
    fn get_id(&self) -> &str;
}

/// The type `FakeNetwork` is a fake network for testing and simulation.
pub struct FakeNetwork<V> {
    size: usize,
    members: HashMap<String, Rc<RefCell<VecDeque<Event<V>>>>>,
    buf: Rc<RefCell<VecDeque<(String, Event<V>)>>>,
}

impl<V> FakeNetwork<V>
where
    V: fmt::Debug + Clone,
{
    /// Create new fake network.
    pub fn new(size: usize) -> Self {
        Self {
            size,
            members: HashMap::new(),
            buf: Rc::new(RefCell::new(VecDeque::new())),
        }
    }

    /// Create new node for the fake network.
    pub fn new_node(&mut self, id: usize) -> FakeNode<V> {
        assert!(id < self.size);

        let peers = (0..self.size)
            .filter(|&x| x != id)
            .map(|x| x.to_string())
            .collect::<Vec<_>>();

        let buf = Rc::new(RefCell::new(VecDeque::new()));
        self.members.insert(id.to_string(), buf.clone());

        FakeNode {
            id: id.to_string(),
            peers,
            messages: buf,
            network: self.buf.clone(),
        }
    }

    pub fn forward(&mut self) {
        if let Some((dest, msg)) = self.buf.borrow_mut().pop_front() {
            if let Some(s) = self.members.get(&dest) {
                s.borrow_mut().push_back(msg);
            }
        }
    }
}

#[allow(dead_code)]
pub struct FakeNode<V> {
    id: String,
    peers: Vec<String>,
    pub messages: Rc<RefCell<VecDeque<Event<V>>>>,
    network: Rc<RefCell<VecDeque<(String, Event<V>)>>>,
}

impl<V> Node for FakeNode<V>
where
    V: fmt::Debug + Clone,
{
    type EntryKind = V;
    fn send(&mut self, dest: &str, message: Event<V>) {
        self.network
            .borrow_mut()
            .push_back((dest.to_string(), message));
    }

    fn receive(&mut self) -> Option<Event<V>> {
        self.messages.borrow_mut().pop_front()
    }

    fn peers(&self) -> Vec<String> {
        self.peers.clone()
    }

    fn get_id(&self) -> &str {
        &self.id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_send_receive() {
        let mut net: FakeNetwork<String> = FakeNetwork::new(5);
        let mut node0 = net.new_node(0);
        let mut node1 = net.new_node(1);

        node0.send(
            "1",
            Event::new_append_entries(None, None, None, vec![], None, "0", "1"),
        );

        net.forward();

        assert_eq!(
            node1.receive().unwrap(),
            Event::new_append_entries(None, None, None, vec![], None, "0", "1"),
        );
    }
}
