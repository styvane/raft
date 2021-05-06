//! Raft cluster configuration

use config::{Config as Configure, File, FileFormat};
use serde::Deserialize;
use std::path::PathBuf;
use std::slice::Iter;

/// Cluster member configuration.
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct Member {
    id: usize,
    host: String,
}

/// Cluster configuration.
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct Cluster {
    id: String,
    members: Vec<Member>,
}

impl Cluster {
    /// Create a cluster configuration from a string.
    pub fn from_str(data: &str, format: FileFormat) -> anyhow::Result<Self> {
        let mut cfg = Configure::new();
        cfg.merge(File::from_str(data, format))?;
        let cfg = cfg.try_into::<Self>()?;
        Ok(cfg)
    }

    /// Create a cluster configuration from a path.
    pub fn from_path(path: PathBuf) -> anyhow::Result<Self> {
        let mut cfg = Configure::new();
        cfg.merge(File::with_name(path.to_str().unwrap()))?;
        let cfg = cfg.try_into::<Self>()?;
        Ok(cfg)
    }

    /// This method returns the configuration of the member associated with
    /// the given Id.
    pub fn get(&self, id: &usize) -> Member {
        self.members[*id].clone()
    }

    /// Return the size of the cluster.
    pub fn size(&self) -> usize {
        self.members.len()
    }

    /// Return an iterator over a slice of all members in the cluster.
    pub fn members_iter(&self) -> Iter<Member> {
        self.members.iter()
    }
}

impl Member {
    /// Return the hostname value.
    pub fn hostname(&self) -> &str {
        &self.host
    }

    /// Return the list of its peers hostname.
    pub fn id(&self) -> usize {
        self.id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let config = Cluster::from_str(
            r#"
	        id = "raft"
                [[members]]
	        id = 0
	        host = "0"

                [[members]]
		id = 1
		host = "1"
            "#,
            FileFormat::Toml,
        );
        assert_eq!(
            config.unwrap().get(&0),
            Member {
                id: 0,
                host: "0".to_string()
            }
        )
    }
}
