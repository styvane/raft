use kvserver::{Server, ServerOptions};
use structopt::StructOpt;

fn main() {
    let opts = ServerOptions::from_args();
    let mut srv = Server::new(opts);
    srv.listen_and_serve().unwrap();
}
