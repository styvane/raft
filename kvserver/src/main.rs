use kvserver::{init_logger, Server, ServerOptions, Storage};
use raft_core::Cluster;
use structopt::StructOpt;

#[async_std::main]
async fn main() -> anyhow::Result<()> {
    let opts = ServerOptions::from_args();
    let storage = Storage::new();
    let cfg = Cluster::from_path(&opts.config).unwrap();
    init_logger(&cfg);
    let mut srv = Server::new(opts, storage, cfg);
    srv.listen_and_serve().await
}
