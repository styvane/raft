//! This module contains the server.

use crate::command::Command;
use crate::storage::Storage;
use anyhow::{self, Context};
use raft_core::Transport;
use serde::Deserialize;
use serde_json;
use std::io;
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;
use structopt::StructOpt;

#[derive(Debug, StructOpt, Deserialize)]
#[structopt(name = "server", rename_all = "kebab-case")]
pub struct ServerOptions {
    #[structopt(long, short, default_value = "127.0.0.1")]
    bind_ip: String,

    #[structopt(long, short, default_value = "21000")]
    port: usize,
}

/// The `Server` type represents the storage server
pub struct Server {
    options: ServerOptions,
    _storage: Storage,
}

impl Server {
    /// Create a new server.
    pub fn new(options: ServerOptions) -> Self {
        Server {
            options,
            _storage: Storage::new(),
        }
    }

    fn handle_client(client: Arc<TcpStream>) {
        loop {
            let mut trp = Transport::new(client.as_ref());
            let message = trp.recv_message().unwrap();
            if message.trim().is_empty() {
                break;
            }
            let _cmd: Command = serde_json::from_str(&message).unwrap();
        }
    }

    /// Listen to incomming connection and serve request.
    pub fn listen_and_serve(&mut self) -> io::Result<()> {
        let listener =
            TcpListener::bind(format!("{}:{}", self.options.bind_ip, self.options.port))?;

        for stream in listener.incoming() {
            let stream = Arc::new(stream.unwrap());
            thread::spawn(move || Self::handle_client(stream));
        }
        Ok(())
    }
}
