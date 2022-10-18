//! Server events
use crate::command::{Command, CommandMessage, Value};
use crate::storage::Storage;
use async_std::channel;
use async_std::net::TcpStream;
use async_std::task;
use futures::{select, FutureExt, StreamExt};
use log::{error, info};
use raft_core::{ClientRequest, ConsensusReceiver};
use raft_utils::send_frame;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use std::{collections::HashMap, str::FromStr};

/// The `Event` type represents the server events.
#[derive(Debug)]
pub enum Event {
    Connection {
        stream: Arc<TcpStream>,
    },
    Request {
        sender: String,
        cmd: CommandMessage,
        consensus: ConsensusReceiver,
    },
}

pub(crate) type Request = channel::Sender<Box<dyn ClientRequest<EntryKind = Command>>>;

impl Event {
    /// Create new request event.
    pub fn new_connection(stream: Arc<TcpStream>) -> Self {
        Self::Connection { stream }
    }

    /// Create request event.
    pub fn new_request(cmd: CommandMessage, sender: String, consensus: ConsensusReceiver) -> Self {
        Self::Request {
            cmd,
            sender,
            consensus,
        }
    }

    /// Read incoming query, run the query and send the result.
    pub async fn response_broker(
        mut storage: Storage,
        events: channel::Receiver<Event>,
        requests: Request,
        commits: channel::Receiver<Command>,
    ) -> anyhow::Result<()> {
        let mut connections = HashMap::new();
        let mut events = events.fuse();
        let mut commits = commits.fuse();
        loop {
            select! {
                msg = commits.next().fuse() => if let Some(cmd) = msg {
                    match storage.query(cmd).await {
                        Some(_) => info!("commit successfully applied"),
                        None => error!("unable to applied committed log to state machine"),
                    }
                },
                msg = events.next().fuse()  => if let Some(event) = msg {
                    match event {
                        Event::Connection { stream } => {
                            if let Ok(addr) = stream.peer_addr() {
                                if let Entry::Vacant(entry) = connections.entry(addr.to_string()) {
                                    let (sender, resp) = channel::bounded(100);
                                    entry.insert(sender);
                                    let _handle = task::spawn(write_stream(resp, stream));
                                }
                            }
                        }
                        Event::Request {
                            sender,
                            cmd,
                            consensus,
                        } => {
                            if let Some(ch) = connections.get(&sender) {
                                if let Command::Invalid(value) = cmd.kind {
                                    let value = Value::from_str(&value).unwrap();
                                    match ch.send(value).await {
                                        Ok(_) => info!("message send to {}", &sender),
                                        Err(err) =>
                                        error!("{:?} while sending query result: {}", err, &sender),
                                    }
                                    continue;
                                }
                                let mut value: Value = "unproccessable entity".parse().unwrap();
                                if let Some(consensus) = consensus {
                                    let query = cmd.kind.clone();

                                    match requests.send(Box::new(cmd)).await {
                                        Ok(_) => info!("client entry successfully sent to raft broker"),
                                        Err(err) =>  error!("{:?} while sending client entry to raft broker", err),
                                    }
                                    match consensus.await {
                                        Ok(b) if b => {
                                            info!("got consensus: applying command");
                                            value = storage.query(query).await.unwrap();
                                        }
                                        Err(err) => info!("consensus not reached: {}", err),
                                        _ => info!("consensus not reached"),
                                    }
                                }
                                match ch.send(value).await {
                                    Ok(_) => info!("message successfully send to {}", &sender),
                                    Err(err) => error!("{:?} while sending query result: {}", err, &sender),
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

pub async fn write_stream(mut receiver: channel::Receiver<Value>, writer: Arc<TcpStream>) {
    let mut stream = &*writer;
    while let Some(v) = receiver.next().await {
        if let Ok(data) = serde_json::to_string(&v) {
            match send_frame(&mut stream, data.as_bytes()).await {
                Ok(_) => info!("frame successfully sent"),
                Err(e) => error!("unable to send frame {:?}", e),
            }
        }
    }
}
