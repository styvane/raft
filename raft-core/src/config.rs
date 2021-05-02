//! Raft cluster configuration

use std::collections::HashMap;

type VecPeer = Vec<String>;

/// Node configuration
#[derive(Debug)]
pub struct Config {
    id: usize,
    addr: String,
    peers: VecPeer,
}

impl Config {
    pub fn new(node_id: usize) -> Self {
        let config = Self::init();
        let addr = config.get(&node_id).clone().unwrap().to_string();

        let peers: Vec<String> = config
            .into_iter()
            .filter(|&(k, _)| k != node_id)
            .map(|(_, v)| v)
            .collect();

        Config {
            id: node_id,
            addr,
            peers,
        }
    }

    fn init() -> HashMap<usize, String> {
        [
            (0, "127.0.0.1:27000"),
            (1, "127.0.0.1:28000"),
            (2, "127.0.0.1:29000"),
            (3, "127.0.0.1:30000"),
            (4, "127.0.0.1:31000"),
            (5, "127.0.0.1:32000"),
            (6, "127.0.0.1:33000"),
        ]
        .iter()
        .map(|&(id, addr)| (id, String::from(addr)))
        .collect()
    }

    pub fn get_addr(&self) -> &str {
        &self.addr
    }

    pub fn peers(&self) -> VecPeer {
        self.peers.clone()
    }

    pub fn cluster_size(&self) -> usize {
        self.peers.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let values: Vec<_> = [
            "127.0.0.1:27000",
            "127.0.0.1:28000",
            "127.0.0.1:29000",
            "127.0.0.1:30000",
            "127.0.0.1:31000",
            "127.0.0.1:32000",
            "127.0.0.1:33000",
        ]
        .iter()
        .map(|&x| String::from(x))
        .collect();

        assert!(Config::new(0).peers.iter().all(|x| values.contains(&x)))
    }
}
