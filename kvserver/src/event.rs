//! Server events
use crate::command::{Command, Value};
use crate::storage::Storage;
use async_std::channel::{Receiver, Sender};
use async_std::stream::StreamExt;
use raft_core::runtime::Runtime;
use std::collections::HashMap;

/// The `Event` type represents the server events.
pub enum Event {
    Connection {
        address: String,
        response: Sender<Value>,
    },
    Request {
        sender: String,
        cmd: Command,
    },
}

impl Event {
    /// Create new request event.
    pub fn new_connection(address: String, response: Sender<Value>) -> Self {
        Self::Connection { address, response }
    }

    /// Create request event.
    pub fn new_request(cmd: Command, sender: String) -> Self {
        Self::Request { cmd, sender }
    }

    /// Read incoming query, run the query and send the result.
    pub async fn response_broker(
        mut events: Receiver<Event>,
        //runtime: Runtime<Command>,
    ) -> anyhow::Result<()> {
        let mut connections = HashMap::new();
        //        let mut storage = Storage::with_runtime(runtime);
        let mut storage = Storage::new();
        while let Some(event) = events.next().await {
            match event {
                Event::Connection { address, response } => {
                    connections.insert(address, response);
                }
                Event::Request { sender, cmd } => {
                    if let Some(ch) = connections.get(&sender) {
                        let value: Value = storage.query(cmd).await.unwrap();
                        if let Err(_) = ch.send(value).await {
                            eprintln!("error send query result: {}", &sender);
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
