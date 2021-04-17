use kvserver::Client;
use structopt::StructOpt;

fn main() {
    let mut client = Client::from_args();
    client.connect();
}
