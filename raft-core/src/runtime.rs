//! Runtime of the Raft server

use crate::config::Cluster;
use crate::event::Message;
use crate::server::Server;
use anyhow;
use async_std::channel::{self, Receiver, Sender};
use async_std::net::{TcpListener, TcpStream};
use async_std::stream::StreamExt;
use async_std::task;
use raft_utils::Transport;
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

const ELECTION_TIMEOUT_MIN: u64 = 5;
const ELECTION_TIMEOUT_MAX: u64 = 10;

/// The `Runtime` type is the Raft runtime.
#[derive(Debug)]
pub struct Runtime<V> {
    pub server: Server<V>,
    pub netaddr: String,
}

#[derive(Debug)]
pub enum Event<T> {
    HeartBeat,
    Election,
    Message(Message<T>),
    Connection {
        peer: String,
        chan: Sender<Message<T>>,
    },
}

impl<T> Event<T> {
    pub fn new_connection(peer: &str, chan: Sender<Message<T>>) -> Self {
        Self::Connection {
            peer: peer.to_string(),
            chan,
        }
    }
}

impl<V> Runtime<V>
where
    V: Clone + fmt::Debug + Send + 'static + DeserializeOwned + Serialize,
{
    /// Start the runtime server
    pub async fn start(runtime: Runtime<V>) {
        let netaddr = runtime.netaddr.clone();
        let (broker_tx, broker_rx) = channel::unbounded();
        let _handle = task::spawn(Self::launch_broker(broker_rx, runtime));
        let _handle = task::spawn(Self::election_timeout(broker_tx.clone()));
        let _handle = task::spawn(Self::emit_heartbeat(broker_tx.clone()));

        let _handle = task::spawn(Self::connect(netaddr, broker_tx));
    }

    pub fn new(node_id: usize, config: Cluster) -> anyhow::Result<Self> {
        let netaddr = config.get(&node_id).hostname().to_string();
        let server = Server::new(node_id, config);
        Ok(Self { server, netaddr })
    }

    /// Accept incoming connection from peers.
    ///
    /// It also spawns tasks to exchange messages with peers.
    pub async fn connect(netaddr: String, broker_sender: Sender<Event<V>>) -> anyhow::Result<()> {
        let listener = TcpListener::bind(netaddr).await?;
        let mut incoming = listener.incoming();
        while let Some(stream) = incoming.next().await {
            // do something with the stream.
            let stream = stream?;
            let (tx, rx) = channel::unbounded();
            match stream.peer_addr() {
                Err(error) => {
                    eprintln!("{:?}", error);
                    continue;
                }
                Ok(peer) => {
                    let peer = format!("{}:{}", peer.ip(), peer.port());
                    let event = Event::new_connection(&peer, tx);
                    if let Err(error) = broker_sender.clone().send(event).await {
                        eprintln!("{:?}", error);
                        continue;
                    }
                    let stream = Arc::new(stream);
                    let _handle = task::spawn(Self::send_message(rx, stream.clone()));
                    let _handle =
                        task::spawn(Self::recv_message(broker_sender.clone(), stream.clone()));
                }
            }
        }

        Ok(())
    }

    pub async fn launch_broker(mut messages: Receiver<Event<V>>, mut runtime: Runtime<V>) {
        let mut connections = HashMap::new();
        loop {
            println!("{}", runtime.server);
            while let Some(event) = messages.next().await {
                match event {
                    Event::Connection { peer, chan } => {
                        connections.insert(peer, chan);
                    }

                    Event::Election => runtime.server.election_timeout(),
                    Event::HeartBeat => runtime.server.send_leader_heartbeat(),
                    Event::Message(msg) => runtime.server.handle_message(msg.event),
                }
            }
        }
    }

    /// Receive message from peers and and send it to broker.
    pub async fn recv_message(broker_sender: Sender<Event<V>>, stream: Arc<TcpStream>) {
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

    /// Start an watch election timer
    pub async fn election_timeout(broker_sender: Sender<Event<V>>) {
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

    pub async fn emit_heartbeat(broker_sender: Sender<Event<V>>) {
        loop {
            let period = Duration::from_secs(0);
            task::sleep(period).await;
            if let Err(error) = broker_sender.send(Event::HeartBeat).await {
                eprintln!("{:?}", error);
            }
        }
    }

    ///  Send the Raft server messages to the appropriate peer.
    pub async fn send_message(mut outgoing_messages: Receiver<Message<V>>, stream: Arc<TcpStream>) {
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
