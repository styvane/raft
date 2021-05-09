use anyhow;
use kvserver::{Server, ServerOptions, Storage};
use structopt::StructOpt;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opts = ServerOptions::from_args();
    let storage = Storage::new();
    let mut srv = Server::new(opts, storage);
    srv.listen_and_serve().await
}
