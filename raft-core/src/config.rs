//! Raft cluster configuration

use std::collections::HashMap;

/// Node configuration
#[derive(Debug)]
pub struct Config {
    id: usize,
    addr: String,
    peers: Vec<String>,
}

impl Config {
    pub fn new(id: usize, addr: &str) -> Self {
        let config = Self::init();
        let peers: Vec<String> = config
            .into_iter()
            .filter(|&(k, _)| k != id)
            .map(|(_, v)| v)
            .collect();

        Config {
            id,
            addr: String::from(addr),
            peers,
        }
    }

    pub(super) fn init() -> HashMap<usize, String> {
        [
            (0, "127.0.0.1:27000"),
            (1, "127.0.0.1:28000"),
            (2, "127.0.0.1:29000"),
        ]
        .iter()
        .map(|&(id, addr)| (id, String::from(addr)))
        .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        assert!(Config::new(0, "127.0.0.1:27000").peers.iter().all(|x| [
            String::from("127.0.0.1:28000"),
            String::from("127.0.0.1:29000")
        ]
        .contains(&x)))
    }
}
