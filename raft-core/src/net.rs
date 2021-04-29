//! Raft network
//!
//! This module contains the Raft network node behavior.

use crate::event::{Event, Message};
use anyhow;
use anyhow::Context as _;
use std::collections::HashMap;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};

/// The `Node` trait defines a node behavior on the network.
///
/// The basic operations of a node in the Raft cluster is to send and receive messages
/// from it peers.
///
/// The `send` operation allows the node to send a message to it peers.
/// The `receive` operation allows a node to receive a message from it peers.
pub trait Node {
    fn send(&mut self, message: Message) -> anyhow::Result<()>;
    fn receive(&mut self) -> anyhow::Result<Event>;
    fn peers(&self) -> &[String];
}

/// The type `FakeNetwork` is a fake network for testing and simulation.
pub struct FakeNetwork {
    size: usize,
    members: HashMap<String, SyncSender<Event>>,
    buf: Receiver<Message>,
    node_sender: SyncSender<Message>,
}

impl FakeNetwork {
    /// Create new fake network.
    pub fn new(size: usize) -> Self {
        let (node_sender, buf) = sync_channel(100);
        Self {
            size,
            members: HashMap::new(),
            buf,
            node_sender,
        }
    }

    /// Create new node for the fake network.
    pub fn new_node(&mut self, id: usize) -> FakeNode {
        assert!(id < self.size);

        let peers = (0..self.size)
            .filter(|&x| x != id)
            .map(|x| x.to_string())
            .collect::<Vec<_>>();

        let (tx, rx) = sync_channel(10);
        self.members.insert(id.to_string(), tx);

        FakeNode {
            id: id.to_string(),
            peers,
            messages: rx,
            network: self.node_sender.clone(),
        }
    }

    /// Forward event to appropriate destination.
    pub fn forward(&mut self) {
        if let Ok(msg) = self.buf.try_recv() {
            if let Some(s) = self.members.get(&msg.destination()) {
                s.try_send(msg.inner_event()).unwrap()
            }
        }
    }
}

#[allow(dead_code)]
pub struct FakeNode {
    id: String,
    peers: Vec<String>,
    messages: Receiver<Event>,
    network: SyncSender<Message>,
}

impl Node for FakeNode {
    fn send(&mut self, message: Message) -> anyhow::Result<()> {
        self.network
            .try_send(message)
            .context("sending message failed")
    }
    fn receive(&mut self) -> anyhow::Result<Event> {
        self.messages
            .try_recv()
            .context("no message or unable to read")
    }

    fn peers(&self) -> &[String] {
        &self.peers
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_send_receive() {
        let mut net = FakeNetwork::new(5);
        let mut node0 = net.new_node(0);
        let mut node1 = net.new_node(1);

        node0
            .send(Message::new(
                "1",
                Event::AppendEntries {
                    current_term: Some(1),
                },
            ))
            .unwrap();

        net.forward();

        assert_eq!(
            node1.receive().unwrap(),
            Event::AppendEntries {
                current_term: Some(1)
            }
        );
    }
}
