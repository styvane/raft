//! Server events
use crate::command::{Command, Value};
use crate::storage::Storage;
use std::{collections::HashMap, str::FromStr};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;

type Consensus = Option<oneshot::Receiver<bool>>;

/// The `Event` type represents the server events.
#[derive(Debug)]
pub enum Event {
    Connection {
        address: String,
        response: Sender<Value>,
    },
    Request {
        sender: String,
        cmd: Command,
        consensus: Consensus,
    },
}

impl Event {
    /// Create new request event.
    pub fn new_connection(address: String, response: Sender<Value>) -> Self {
        Self::Connection { address, response }
    }

    /// Create request event.
    pub fn new_request(cmd: Command, sender: String, consensus: Consensus) -> Self {
        Self::Request {
            cmd,
            sender,
            consensus,
        }
    }

    /// Read incoming query, run the query and send the result.
    pub async fn response_broker(
        mut events: Receiver<Event>,
        mut storage: Storage,
    ) -> anyhow::Result<()> {
        let mut connections = HashMap::new();
        while let Some(event) = events.recv().await {
            match event {
                Event::Connection { address, response } => {
                    connections.insert(address, response);
                }
                Event::Request {
                    sender,
                    cmd,
                    consensus,
                } => {
                    if let Some(ch) = connections.get(&sender) {
                        let mut value = Value::from_str("unable to process request").unwrap();
                        if let Some(consensus) = consensus {
                            match consensus.await {
                                Ok(b) if b => {
                                    value = storage.query(cmd).await.unwrap();
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
        Ok(())
    }
}
