//! This module contains the server.

use crate::command::{Command, CommandMessage};
use crate::event::Event;
use crate::storage::Storage;
use async_std::channel;
use async_std::net::{TcpListener, TcpStream};
use async_std::task;
use futures::channel::oneshot;
use raft_core::{runtime, server, Cluster};
use raft_utils::recv_frame;
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

    /// Listen to incoming connection and serve request.
    pub async fn listen_and_serve(&mut self) -> anyhow::Result<()> {
        let listener =
            TcpListener::bind(format!("{}:{}", self.options.bind_ip, self.options.port)).await?;
        let (broker_tx, broker_rx) = channel::bounded(CONNEXION_MAX);
        let storage = self.storage.take().unwrap();

        let (messages, outgoing) = channel::bounded(CONNEXION_MAX);
        let (client_sender, requests) = channel::bounded(CONNEXION_MAX);
        let (commit_tx, commit_rx) = channel::bounded(CONNEXION_MAX);

        let _broker_handle = task::spawn(Event::response_broker(
            storage,
            broker_rx,
            client_sender,
            commit_rx,
        ));
        let cfg = Cluster::from_path(self.options.config.as_path()).unwrap();

        let _runtime_handle = task::spawn(runtime::setup::<Command>(
            outgoing,
            requests,
            server::Server::new(self.options.node_id, cfg, messages, Some(commit_tx)),
        ));

        while let Ok((stream, _)) = listener.accept().await {
            let stream = Arc::new(stream);
            let event = Event::new_connection(stream.clone());
            if let Err(e) = broker_tx.send(event).await {
                eprintln!("{:?}", e);
            }

            let _handle = task::spawn(read_stream(broker_tx.clone(), stream.clone()));
        }

        Ok(())
    }
}

/// Read a stream and send the event throught a channel.
pub async fn read_stream(sender: channel::Sender<Event>, reader: Arc<TcpStream>) {
    let addr = (&*reader).peer_addr().unwrap().to_string();
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
                        let cmd: Command = cmd;
                        let (tx, rx) = oneshot::channel();
                        let cmd = CommandMessage {
                            kind: cmd,
                            response: Some(tx),
                        };
                        Event::new_request(cmd, addr.clone(), Some(rx))
                    }
                    Err(e) => {
                        let cmd = Command::Invalid(format!("{:?}", e));
                        let cmd = CommandMessage {
                            kind: cmd,
                            response: None,
                        };
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
