//! This module contains the server.

use crate::command::{Command, Value};
use crate::event::Event;
use async_std::channel::{self, Receiver, Sender};
use async_std::net::{TcpListener, TcpStream};
use async_std::stream::StreamExt;
use async_std::task;
use raft_core::Transport;
use serde::Deserialize;
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
}

/// The `Server` type represents the storage server
#[allow(dead_code)]
pub struct Server {
    options: ServerOptions,
}

impl Server {
    /// Create a new server.
    pub fn new(options: ServerOptions) -> Self {
        Server { options }
    }

    /// Read a stream and send the event throught a channel.
    pub async fn read_stream(sender: Sender<Event>, stream: Arc<TcpStream>) {
        loop {
            let stream = &*stream;
            let mut trp = Transport::new(stream);
            let message = trp.recv_message().await.unwrap();
            if message.trim().is_empty() {
                break;
            }
            let cmd = match serde_json::from_str(&message) {
                Ok(v) => v,
                Err(e) => Command::Invalid(format!("{:?}", e)),
            };

            let addr = stream.peer_addr().unwrap();
            let addr = format!("{}:{}", addr.ip(), addr.port());
            let event = Event::new_request(cmd, addr);

            if let Err(e) = sender.send(event).await {
                eprintln!("{:?}", e);
            }
        }
    }

    /// Listen to incoming connection and serve request.
    pub async fn listen_and_serve(&mut self) -> anyhow::Result<()> {
        let listener =
            TcpListener::bind(format!("{}:{}", self.options.bind_ip, self.options.port)).await?;

        let (broker_tx, broker_rx) = channel::bounded(CONNEXION_MAX);
        let _broker_handle = task::spawn(Event::response_broker(broker_rx));
        while let Some(stream) = listener.incoming().next().await {
            let stream = stream?;
            let stream = Arc::new(stream);
            let addr = if let Ok(addr) = stream.peer_addr() {
                addr
            } else {
                continue;
            };

            let addr = format!("{}:{}", addr.ip(), addr.port());

            let (query_tx, query_rx) = channel::bounded(CONNEXION_MAX);

            let event = Event::new_connection(addr, query_tx);

            if let Err(e) = broker_tx.send(event).await {
                eprintln!("{:?}", e);
            }

            let _handle = task::spawn(Self::read_stream(broker_tx.clone(), stream.clone()));
            let _handle = task::spawn(Self::write_stream(query_rx.clone(), stream.clone()));
        }

        Ok(())
    }

    pub async fn write_stream(mut receiver: Receiver<Value>, stream: Arc<TcpStream>) {
        let mut trp = Transport::new(&*stream);
        while let Some(v) = receiver.next().await {
            if let Ok(data) = serde_json::to_string(&v) {
                if let Err(e) = trp.send_message(data.as_bytes()).await {
                    eprintln!("{:?}", e);
                }
            }
        }
    }
}
