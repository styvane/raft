//! Server events
use crate::command::{Command, CommandMessage, Value};
use crate::storage::Storage;
use async_std::channel;
use async_std::net::TcpStream;
use async_std::task;
use futures::{select, FutureExt, StreamExt};
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
                    let _ = storage.query(cmd).await;
                },
                msg = events.next().fuse()  => if let Some(event) = msg {
                    match event {
                        Event::Connection { stream } => {
                            if let Ok(addr) = (&*stream).peer_addr() {
                                if let Entry::Vacant(entry) = connections.entry(addr.to_string()) {
                                    let (sender, resp) = channel::bounded(100);
                                    entry.insert(sender);
                                    task::spawn(write_stream(resp, stream));
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
                                    if let Err(err) = ch.send(value).await {
                                        eprintln!("{:?} while sending query result: {}", err, &sender);
                                    }
                                    continue;
                                }
                                let mut value: Value = "unproccessable entity".parse().unwrap();
                                if let Some(consensus) = consensus {
                                    let query = cmd.kind.clone();
                                    if let Err(error) = requests.send(Box::new(cmd)).await {
                                        eprintln!("{:?} while sending client entry to raft broker", error);
                                    }
                                    match consensus.await {
                                        Ok(b) if b => {
                                            value = storage.query(query).await.unwrap();
                                        }
                                        _ => (),
                                    }
                                }
                                if let Err(err) = ch.send(value).await {
                                    eprintln!("{:?} while sending query result: {}", err, &sender);
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
            if let Err(e) = send_frame(&mut stream, data.as_bytes()).await {
                eprintln!("{:?}", e);
            }
        }
    }
}
