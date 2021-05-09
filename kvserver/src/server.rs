//! This module contains the server.

use crate::command::{Command, Value};
use crate::event::Event;
use crate::storage::Storage;
use async_std::channel::{self, Receiver, Sender};
use async_std::net::{TcpListener, TcpStream};
use async_std::stream::StreamExt;
use async_std::task;
use futures::channel::oneshot;
use raft_utils::{recv_frame, send_frame};
use serde::Deserialize;
use std::path::PathBuf;
use std::sync::Arc;
use structopt::StructOpt;

const CONNEXION_MAX: usize = 100;

#[derive(Debug, StructOpt, Deserialize)]
#[structopt(name = "server", rename_all = "kebab-case")]
pub struct ServerOptions {
    #[structopt(long, short, default_value = "127.0.0.1")]
    bind_ip: String,

    #[structopt(long, short, default_value = "21000")]
    port: usize,

    /// The identifier of this node in the cluster configuration.
    #[structopt(short, long)]
    pub node_id: usize,

    /// Path to the configuration file.
    #[structopt(short, long, parse(from_os_str))]
    pub config: PathBuf,
}

/// The `Server` type represents the storage server
pub struct Server {
    options: ServerOptions,
    storage: Option<Storage>,
}

impl Server {
    /// Create a new server.
    pub fn new(options: ServerOptions, storage: Storage) -> Self {
        Server {
            options,
            storage: Some(storage),
        }
    }

    /// Read a stream and send the event throught a channel.

    pub async fn read_stream(sender: Sender<Event>, addr: String, reader: Arc<TcpStream>) {
        loop {
            let mut stream = &*reader;
            match recv_frame(&mut stream).await {
                Err(error) => {
                    eprintln!("{:?}", error);
                }
                Ok(msg) => {
                    if msg.trim().is_empty() {
                        break;
                    }
                    let req = match serde_json::from_str(&msg) {
                        Ok(cmd) => {
                            let mut cmd: Command = cmd;
                            let (tx, rx) = oneshot::channel();
                            cmd.set_consensus(tx);
                            Event::new_request(cmd, addr.clone(), Some(rx))
                        }
                        Err(e) => {
                            let cmd = Command::Invalid(format!("{:?}", e));
                            Event::new_request(cmd, addr.clone(), None)
                        }
                    };

                    if let Err(e) = sender.send(req).await {
                        eprintln!("{:?}", e);
                    }
                }
            }
        }
    }

    /// Listen to incoming connection and serve request.
    pub async fn listen_and_serve(&mut self) -> anyhow::Result<()> {
        let listener =
            TcpListener::bind(format!("{}:{}", self.options.bind_ip, self.options.port)).await?;

        let (broker_tx, broker_rx) = channel::bounded(CONNEXION_MAX);
        let storage = self.storage.take().unwrap();
        let _broker_handle = task::spawn(Event::response_broker(broker_rx, storage));

        while let Ok((stream, addr)) = listener.accept().await {
            let addr = addr.to_string();
            let (query_tx, query_rx) = channel::bounded(CONNEXION_MAX);

            let event = Event::new_connection(addr.clone(), query_tx);

            if let Err(e) = broker_tx.send(event).await {
                eprintln!("{:?}", e);
            }
            let stream = Arc::new(stream);

            let _handle = task::spawn(Self::read_stream(broker_tx.clone(), addr, stream.clone()));
            let _handle = task::spawn(Self::write_stream(query_rx, stream.clone()));
        }

        Ok(())
    }

    pub async fn write_stream(mut receiver: Receiver<Value>, writer: Arc<TcpStream>) {
        let mut stream = &*writer;
        while let Some(v) = receiver.next().await {
            if let Ok(data) = serde_json::to_string(&v) {
                if let Err(e) = send_frame(&mut stream, data.as_bytes()).await {
                    eprintln!("{:?}", e);
                }
            }
        }
    }
}
