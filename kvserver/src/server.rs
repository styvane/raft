//! This module contains the server.

use crate::storage::Storage;
use anyhow;
use async_std::io::{Read, Write};
use async_std::net::{TcpListener, TcpStream};
use async_std::prelude::*;
use async_std::task;
use raft_core::Transport;
use serde::Deserialize;
use serde_json;
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
#[allow(dead_code)]
pub struct Server {
    options: ServerOptions,
    storage: Storage,
}

impl Server {
    /// Create a new server.
    pub fn new(options: ServerOptions) -> Self {
        Server {
            options,
            storage: Storage::new(),
        }
    }

    pub async fn read_stream(stream: impl Read + Write + Unpin + Clone) {
        loop {
            let mut trp = Transport::new(stream.clone());
            let message = trp.recv_message().await.unwrap();
            if message.trim().is_empty() {
                break;
            }
            let _cmd = match serde_json::from_str(&message) {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("{:?}", e);
                    continue;
                }
            };
        }
    }

    /// Listen to incomming connection and serve request.
    pub async fn listen_and_serve(&mut self) -> anyhow::Result<()> {
        let listener =
            TcpListener::bind(format!("{}:{}", self.options.bind_ip, self.options.port)).await?;

        while let Some(stream) = listener.incoming().next().await {
            let stream = stream?;
            let _handle = task::spawn(Self::read_stream(stream));
        }
        Ok(())
    }

    pub async fn write_stream(stream: impl Read + Write + Unpin + Clone) {
        let _trp = Transport::new(stream.clone());
    }
}
