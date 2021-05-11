//! Async Runtime of the Raft server.

use crate::event::Message;
use crate::server::Server;
use async_std::channel;
use async_std::net::{TcpListener, TcpStream};
use async_std::stream::StreamExt;
use async_std::task;
use futures::channel::oneshot;
use futures::{select, FutureExt};
use raft_utils::{recv_frame, send_frame};
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::hash_map;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

const ELECTION_TIMEOUT_MIN: u64 = 5;
const ELECTION_TIMEOUT_MAX: u64 = 10;
const HEARTBEAT: u64 = 3;
const MAX_MESSAGES: usize = 1000;

#[derive(Debug)]
pub enum Event<T> {
    HeartBeat,
    Election,
    Message(Message<T>),
    NewPeer { stream: Arc<TcpStream> },
}

pub type ConsensusSender = Option<oneshot::Sender<bool>>;
pub type ConsensusReceiver = Option<oneshot::Receiver<bool>>;
type Request<V> = channel::Receiver<Box<dyn ClientRequest<EntryKind = V>>>;

/// The `ClientRequest` trait defines the behavior of the Raft client request.
pub trait ClientRequest: Send {
    type EntryKind;

    fn entry_kind(&self) -> Self::EntryKind;
    fn responder(self) -> ConsensusSender;
}

// pub fn start<V: Entry>(node_id: usize, config: Cluster) {
//     let (messages, outgoing) = channel::bounded(MAX_MESSAGES);
//     let server: Server<V> = Server::new(node_id, config, messages);
// }

/// Setup the runtime system.
pub async fn setup<V>(
    outgoing: channel::Receiver<Message<V>>,
    requests: Request<V>,
    raft_server: Server<V>,
) where
    V: Clone + fmt::Debug + Send + 'static + DeserializeOwned + Serialize,
{
    let netaddr = raft_server.hostname();
    let (broker_tx, broker_rx) = channel::bounded(MAX_MESSAGES);

    // Spawn task to accepting peers connections.
    let _handle = task::spawn(accept(netaddr, broker_tx.clone()));

    // Spawn message broker tasks.
    let _handle = task::spawn(message_broker(broker_rx, outgoing, requests, raft_server));

    // Spawn task that emits event for Raft leader elections.
    let _handle = task::spawn(election_timeout(broker_tx.clone()));

    // Spawn task that emits event for sender leader heartbeat.
    let _handle = task::spawn(emit_heartbeat(broker_tx));
}

/// Accept incoming connection from peers in the Raft network
///
/// It also spawns tasks for exchanging messages with peers.
async fn accept<V>(netaddr: String, broker_sender: channel::Sender<Event<V>>) -> anyhow::Result<()>
where
    V: Clone + fmt::Debug + Send + 'static + DeserializeOwned + Serialize,
{
    let listener = TcpListener::bind(netaddr).await?;
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let stream = Arc::new(stream);
                let event = Event::NewPeer {
                    stream: stream.clone(),
                };
                if let Err(error) = broker_sender.clone().send(event).await {
                    eprintln!("{:?}", error);
                    continue;
                }
                let _handle = task::spawn(recv_message(broker_sender.clone(), stream));
            }
            Err(error) => eprintln!("{:?}", error),
        }
    }
}

/// Message broker between Raft peers.
async fn message_broker<V>(
    incoming: channel::Receiver<Event<V>>,
    outgoing: channel::Receiver<Message<V>>,
    client_requests: Request<V>,
    mut server: Server<V>,
) where
    V: Clone + fmt::Debug + Send + 'static + DeserializeOwned + Serialize,
{
    let mut connections = HashMap::new();
    let mut incoming = incoming.fuse();
    let mut outgoing = outgoing.fuse();
    let mut client_requests = client_requests.fuse();
    loop {
        select! {
            msg = incoming.next().fuse() => if let Some(event) = msg {
                match event  {
                    Event::Election => server.election_timeout(),
                    Event::HeartBeat => server.send_leader_heartbeat(),
                    Event::Message(msg) => server.handle_message(msg.event),
                    Event::NewPeer {stream } => {
                        if let Ok(addr) = stream.as_ref().peer_addr() {
                            if let hash_map::Entry::Vacant(entry) = connections.entry(addr.to_string()) {
                                let (tx, rx) = channel::bounded(MAX_MESSAGES);
                                entry.insert(tx);
                                let _handle = task::spawn(send_message(rx,  stream));
                            }
                        }
                    }
                }
            },
            msg = outgoing.next().fuse() => if let Some(event) = msg {
                if let Some(tx) = connections.get(&event.dest) {
                    if let Err(error) = tx.send(event).await {
                        eprintln!("{:?}", error);
                    }
                }
            },

            req = client_requests.next().fuse() => if let Some(msg) = req {
                server.client_append_entry(msg.entry_kind());
            }
        }
    }
}

/// Receive messages from  a peer and send it to broker.
async fn recv_message<V>(broker_sender: channel::Sender<Event<V>>, reader: Arc<TcpStream>)
where
    V: Clone + fmt::Debug + Send + 'static + DeserializeOwned + Serialize,
{
    let mut stream = &*reader;
    loop {
        while let Ok(msg) = recv_frame(&mut stream).await {
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
async fn election_timeout<V>(broker_sender: channel::Sender<Event<V>>)
where
    V: Clone + fmt::Debug + Send + 'static + DeserializeOwned + Serialize,
{
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
async fn emit_heartbeat<V>(broker_sender: channel::Sender<Event<V>>)
where
    V: Clone + fmt::Debug + Send + 'static + DeserializeOwned + Serialize,
{
    let period = Duration::from_secs(HEARTBEAT);
    loop {
        task::sleep(period).await;
        if let Err(error) = broker_sender.send(Event::HeartBeat).await {
            eprintln!("{:?}", error);
        }
    }
}

///  Send the Raft server messages to the appropriate peer.
async fn send_message<V>(
    mut outgoing_messages: channel::Receiver<Message<V>>,
    writer: Arc<TcpStream>,
) where
    V: Clone + fmt::Debug + Send + 'static + DeserializeOwned + Serialize,
{
    let mut stream = &*writer;
    loop {
        while let Some(msg) = outgoing_messages.next().await {
            let msg = serde_json::to_string(&msg).unwrap();
            let msg = msg.as_bytes();
            if let Err(error) = send_frame(&mut stream, msg).await {
                eprintln!("{:?}", error);
            }
        }
    }
}
