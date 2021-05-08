//! Async Runtime of the Raft server

use crate::config::Cluster;
use crate::event::Message;
use crate::server::Server;
use async_std::channel::{self, Receiver, Sender};
use async_std::net::{TcpListener, TcpStream};
use async_std::task;
use futures::{select, FutureExt, StreamExt};
use raft_utils::Transport;
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

const ELECTION_TIMEOUT_MIN: u64 = 5;
const ELECTION_TIMEOUT_MAX: u64 = 10;
const HEARTBEAT: u64 = 3;

#[derive(Debug)]
pub enum Event<T> {
    HeartBeat,
    Election,
    Message(Message<T>),
    NewPeer { stream: Arc<TcpStream> },
}

#[derive(Debug)]
pub struct Runtime<V> {
    raft_server: Server<V>,
    outgoing: Option<Receiver<Message<V>>>,
}

impl<V> Runtime<V>
where
    V: Clone + fmt::Debug + Send + 'static + DeserializeOwned + Serialize,
{
    pub fn client_append_entry(&mut self, data: V) {
        self.raft_server.client_append_entry(data)
    }

    pub fn new(node_id: usize, config: Cluster) -> Self {
        let (messages, outgoing) = channel::unbounded();
        let server: Server<V> = Server::new(node_id, config, messages);
        Runtime {
            raft_server: server,
            outgoing: Some(outgoing),
        }
    }

    /// Start the runtime server
    pub async fn start(outgoing: Receiver<Message<V>>, raft_server: Server<V>) {
        let netaddr = raft_server.hostname();
        let (broker_tx, broker_rx) = channel::unbounded();

        // Spawn task to accepting peers connections.
        let _handle = task::spawn(Self::accept(netaddr, broker_tx.clone()));

        // Spawn message broker tasks.
        let _handle = task::spawn(Self::message_broker(broker_rx, outgoing, raft_server));

        // Spawn task that emits event for Raft leader elections.
        let _handle = task::spawn(Self::election_timeout(broker_tx.clone()));

        // Spawn task that emits event for sender leader heartbeat.
        let _handle = task::spawn(Self::emit_heartbeat(broker_tx));
    }

    /// Accept incoming connection from peers in the Raft network
    ///
    /// It also spawns tasks for exchanging messages with peers.
    async fn accept(netaddr: String, broker_sender: Sender<Event<V>>) -> anyhow::Result<()> {
        let listener = TcpListener::bind(netaddr).await?;
        let mut incoming = listener.incoming();
        while let Some(stream) = incoming.next().await {
            let stream = stream?;
            let stream = Arc::new(stream);
            let event = Event::NewPeer {
                stream: stream.clone(),
            };
            if let Err(error) = broker_sender.clone().send(event).await {
                eprintln!("{:?}", error);
                continue;
            }

            let _handle = task::spawn(Self::recv_message(broker_sender.clone(), stream.clone()));
        }
        Ok(())
    }

    /// Message broker between Raft peers.
    async fn message_broker(
        incoming: Receiver<Event<V>>,
        outgoing: Receiver<Message<V>>,
        mut server: Server<V>,
    ) {
        let mut connections = HashMap::new();
        let mut incoming = incoming.fuse();
        let mut outgoing = outgoing.fuse();
        loop {
            select! {
                    in_msg = incoming.next().fuse() => match in_msg {
                        Some(event) => {
                            match event  {
                                Event::Election => server.election_timeout(),
                                Event::HeartBeat => server.send_leader_heartbeat(),
                                Event::Message(msg) => server.handle_message(msg.event),
                                Event::NewPeer { stream } => {
                                    if let Ok(addr) = stream.as_ref().peer_addr() {
                    let addr = format!("{}:{}", addr.ip(), addr.port());
                                        if let Entry::Vacant(entry) = connections.entry(addr) {
                                            let (tx, rx) = channel::unbounded();
                                            entry.insert(tx);
                                            let _handle = task::spawn(Self::send_message(rx, stream.clone()));
                                        }
                                    }
                                }
                            }
                },
                _ => continue,
            },
                    out_msg = outgoing.next().fuse() => {
                        if let Some(msg) = out_msg {
                if let Some(ref ch) = connections.get(&msg.dest){
                    if let Err(error) = ch.send(msg).await {
                    eprintln!("{:?}", error);
                    }
                }
                        }
            }
                };
        }
    }

    /// Receive messages from  a peer and send it to broker.
    async fn recv_message(broker_sender: Sender<Event<V>>, stream: Arc<TcpStream>) {
        let mut trp = Transport::new(&*stream);
        loop {
            while let Ok(msg) = trp.recv_message().await {
                if let Ok(event) = serde_json::from_str(&msg) {
                    let msg = Event::Message(event);
                    if let Err(err) = broker_sender.send(msg).await {
                        eprintln!("{:?}", err);
                    }
                }
            }
        }
    }

    /// Start an watch election timer.
    async fn election_timeout(broker_sender: Sender<Event<V>>) {
        loop {
            let mut rng: StdRng = SeedableRng::from_entropy();
            let dur = rng.gen_range(ELECTION_TIMEOUT_MIN..ELECTION_TIMEOUT_MAX);
            let period = Duration::from_secs(dur);
            task::sleep(period).await;
            if let Err(error) = broker_sender.send(Event::Election).await {
                eprintln!("{:?}", error);
            }
        }
    }

    /// This function periodically emit heatbeat events.
    async fn emit_heartbeat(broker_sender: Sender<Event<V>>) {
        loop {
            let period = Duration::from_secs(HEARTBEAT);
            task::sleep(period).await;
            if let Err(error) = broker_sender.send(Event::HeartBeat).await {
                eprintln!("{:?}", error);
            }
        }
    }

    ///  Send the Raft server messages to the appropriate peer.
    async fn send_message(mut outgoing_messages: Receiver<Message<V>>, stream: Arc<TcpStream>) {
        let mut trp = Transport::new(&*stream);
        loop {
            while let Some(msg) = outgoing_messages.next().await {
                let msg = serde_json::to_string(&msg).unwrap();
                let msg = msg.as_bytes();
                if let Err(error) = trp.send_message(msg).await {
                    eprintln!("{:?}", error);
                }
            }
        }
    }
}
