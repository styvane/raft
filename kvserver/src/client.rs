//! Storage client implementation.

use raft_utils::{recv_frame, send_frame};
use rustyline::Editor;
use structopt::StructOpt;
use tokio::net::TcpStream;

/// The type client is the key/value storage client.
#[derive(Debug, StructOpt)]
#[structopt(name = "client", rename_all = "kebab-case")]
pub struct Client {
    /// Port of the running server.
    #[structopt(short, long, default_value = "21000")]
    port: usize,

    /// IP address of the server.
    #[structopt(short, long, default_value = "127.0.0.1")]
    bind_ip: String,
}

impl Client {
    pub fn server_addr(&self) -> String {
        format!("{}:{}", self.bind_ip, self.port)
    }

    /// Connect the client to the server.
    pub async fn connect(&mut self) {
        let addr = self.server_addr();
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let (mut read, mut write) = stream.split();
        loop {
            let mut editor = Editor::<()>::new();
            let command: String = editor.readline("> ").unwrap();
            send_frame(&mut write, command.as_bytes()).await.unwrap();
            let message = recv_frame(&mut read).await.unwrap();
            println!("{:?}", message);
        }
    }
}
