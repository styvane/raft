use kvserver::Client;
use structopt::StructOpt;

#[async_std::main]
async fn main() {
    let mut client = Client::from_args();
    client.connect().await;
}
