//! Runtime of the Raft server.

use std::collections::hash_map::{Entry, HashMap};
use std::fmt;
use std::time::Duration;

use async_std::channel;
use async_std::net::{TcpListener, TcpStream};
use async_std::stream::StreamExt;
use async_std::task;
use futures::{select, FutureExt};
use log::{error, info};
use raft_utils::{recv_frame, send_frame};
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::event::Message;
use crate::result::Result;
use crate::server::ClientRequest;
use crate::server::Server;

const ELECTION_TIMEOUT_MIN: u64 = 9;
const ELECTION_TIMEOUT_MAX: u64 = 15;
const HEARTBEAT: u64 = 5;
const MAX_MESSAGES: usize = 100;

#[derive(Debug)]
pub enum Event<T> {
    HeartBeat,
    Election,
    Message(Message<T>),
}

/// The `Request` is a channel for receiving messages send to the Raft leader.
type Request<T> = channel::Receiver<Box<dyn ClientRequest<EntryKind = T>>>;

/// Setup the runtime system.
pub async fn setup<T>(
    outgoing: channel::Receiver<Message<T>>,
    requests: Request<T>,
    raft_server: Server<T>,
) where
    T: Clone + fmt::Debug + Send + 'static + DeserializeOwned + Serialize,
{
    let netaddr = raft_server.hostname();
    let (broker_tx, broker_rx) = channel::bounded(MAX_MESSAGES);

    // Spawn task to accepting peers connections.
    let _handle = task::spawn(accept(netaddr, broker_tx.clone()));

    // Spawn message broker tasks.
    let _handle = task::spawn(message_broker(broker_rx, requests, raft_server));
    let _handle = task::spawn(send_message(outgoing));

    // Spawn task that emits event for Raft leader elections.
    let _handle = task::spawn(election_timeout(broker_tx.clone()));

    // Spawn task that emits event for sender leader heartbeat.
    let _handle = task::spawn(emit_heartbeat(broker_tx));
}

/// Accept incoming connection from peers in the Raft network
///
/// It also spawns tasks for exchanging messages with peers.
async fn accept<T>(netaddr: String, broker_sender: channel::Sender<Event<T>>) -> Result<()>
where
    T: Clone + fmt::Debug + Send + 'static + DeserializeOwned + Serialize,
{
    let listener = TcpListener::bind(netaddr).await?;
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                info!("new connection received: {}", addr.to_string());
                let _handle = task::spawn(recv_message(broker_sender.clone(), stream));
            }
            Err(err) => error!("{:?}", err),
        }
    }
}

/// Message broker between Raft peers.
async fn message_broker<T>(
    incoming: channel::Receiver<Event<T>>,
    client_requests: Request<T>,
    mut server: Server<T>,
) where
    T: Clone + fmt::Debug + Send + 'static + DeserializeOwned + Serialize,
{
    let mut incoming = incoming.fuse();
    let mut client_requests = client_requests.fuse();
    loop {
        select! {
            msg = incoming.next().fuse() => if let Some(event) = msg {
                match event  {
                    Event::Election => server.election_timeout(),
                    Event::HeartBeat => server.send_leader_heartbeat(),
                    Event::Message(msg) => {
                        if let Err(error) = server.handle_message(msg.event) {
                            error!("{}", error)
                        }
                    }
                };
            },
            req = client_requests.next().fuse() => if let Some(mut msg) = req {
                 let response = msg.responder().take();
                 server.client_append_entry(msg.entry_kind(), response);
            }
        }
    }
}

/// Receive messages from  a peer and send it to broker.
async fn recv_message<T>(broker_sender: channel::Sender<Event<T>>, mut reader: TcpStream)
where
    T: Clone + fmt::Debug + Send + 'static + DeserializeOwned + Serialize,
{
    let mut stream = &mut reader;
    loop {
        while let Ok(msg) = recv_frame(&mut stream).await {
            if let Ok(event) = serde_json::from_str(&msg) {
                let msg = Event::Message(event);
                if let Err(err) = broker_sender.send(msg).await {
                    error!("{:?}", err);
                }
            }
        }
    }
}

/// Start an watch election timer.
async fn election_timeout<T>(broker_sender: channel::Sender<Event<T>>)
where
    T: Clone + fmt::Debug + Send + 'static + DeserializeOwned + Serialize,
{
    loop {
        let mut rng: StdRng = SeedableRng::from_entropy();
        let dur = rng.gen_range(ELECTION_TIMEOUT_MIN..ELECTION_TIMEOUT_MAX);
        let period = Duration::from_secs(dur);
        task::sleep(period).await;
        match broker_sender.send(Event::Election).await {
            Ok(_) => info!("emitting election timeout event"),
            Err(err) => error!("unable to send message for election timeout {:?}", err),
        }
    }
}

/// This function periodically emit heatbeat events.
async fn emit_heartbeat<T>(broker_sender: channel::Sender<Event<T>>)
where
    T: Clone + fmt::Debug + Send + 'static + DeserializeOwned + Serialize,
{
    let period = Duration::from_secs(HEARTBEAT);
    loop {
        task::sleep(period).await;
        match broker_sender.send(Event::HeartBeat).await {
            Ok(_) => info!("emitting heartbeat event"),
            Err(err) => error!("{:?}", err),
        }
    }
}

///  Send the Raft server messages to the appropriate peer.
async fn send_message<T>(mut outgoing_messages: channel::Receiver<Message<T>>)
where
    T: Clone + fmt::Debug + Send + 'static + DeserializeOwned + Serialize,
{
    let mut peers = HashMap::new();
    while let Some(msg) = outgoing_messages.next().await {
        if let Entry::Vacant(entry) = peers.entry(msg.dest.clone()) {
            if let Ok(stream) = TcpStream::connect(&msg.dest).await {
                entry.insert(stream);
            }
        };

        match peers.get(&msg.dest).cloned() {
            Some(mut stream) => {
                let msg = serde_json::to_string(&msg).unwrap();
                let msg = msg.as_bytes();
                if let Err(error) = send_frame(&mut stream, msg).await {
                    error!("{:?}", error);
                }
            }

            None => info!("unable to reach destination: {}", &msg.dest),
        }
    }
}
