pub mod client;
pub use client::Client;
pub mod server;
pub use server::Server;
pub use server::ServerOptions;
mod command;
mod event;
mod storage;
pub use storage::Storage;

use log::LevelFilter;
use log4rs::append::console::ConsoleAppender;
use log4rs::append::file::FileAppender;
use log4rs::config::{Appender, Config, Logger, Root};
use raft_core::Cluster;

pub fn init_logger(cluster_cfg: &Cluster) {
    let mut config = Config::builder();
    if cluster_cfg.log_destination() == "console" {
        let kvserver = ConsoleAppender::builder().build();
        let raft = ConsoleAppender::builder().build();
        config = config
            .appender(Appender::builder().build("kvserver", Box::new(kvserver)))
            .appender(Appender::builder().build("raft", Box::new(raft)));
    } else {
        let kvserver = FileAppender::builder()
            .build(cluster_cfg.log_path())
            .unwrap();
        let raft = FileAppender::builder()
            .build(cluster_cfg.log_path())
            .unwrap();
        config = config
            .appender(Appender::builder().build("kvserver", Box::new(kvserver)))
            .appender(Appender::builder().build("raft", Box::new(raft)));
    }

    if cluster_cfg.debug() {
        config = config.logger(Logger::builder().build("raft_core", LevelFilter::Info));
    } else {
        config = config.logger(Logger::builder().build("raft_core", LevelFilter::Error));
    }

    log4rs::init_config(
        config
            .build(
                Root::builder()
                    .appender("kvserver")
                    .build(LevelFilter::Warn),
            )
            .unwrap(),
    )
    .unwrap();
}
