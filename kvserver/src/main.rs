use async_std::channel;
use kvserver::{Server, ServerOptions, Storage};
use structopt::StructOpt;

#[async_std::main]
async fn main() -> anyhow::Result<()> {
    let opts = ServerOptions::from_args();
    let storage = Storage::new();
    let (tx, _rx) = channel::bounded(100);

    let mut srv = Server::new(opts, storage, tx);
    srv.listen_and_serve().await
}
