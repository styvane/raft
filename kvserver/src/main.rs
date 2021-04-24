use kvserver::{Server, ServerOptions};
use structopt::StructOpt;

#[async_std::main]
async fn main() -> anyhow::Result<()> {
    let opts = ServerOptions::from_args();
    let mut srv = Server::new(opts);
    srv.listen_and_serve().await
}
