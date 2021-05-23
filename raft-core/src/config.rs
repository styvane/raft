//! Raft cluster configuration

use config::{Config as Configure, File, FileFormat};
use serde::Deserialize;
use std::path::Path;
use std::slice::Iter;

/// Cluster member configuration.
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct Member {
    id: usize,
    host: String,

    #[serde(default)]
    me: bool,
}

/// Verbosity configuration.
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct Verbosity {
    #[serde(default)]
    pub election: bool,

    #[serde(default)]
    pub heartbeats: bool,
}

/// Replication configuration.
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct Replication {
    id: String,
    members: Vec<Member>,
    pub verbosity: Verbosity,
}

/// System log configuration.
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct SystemLog {
    #[serde(default)]
    destination: String,

    #[serde(default)]
    pub path: String,

    #[serde(default)]
    pub debug: bool,
}

/// Cluster configuration.
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct Cluster {
    #[serde(rename(deserialize = "systemLog"))]
    pub system_log: SystemLog,
    pub replication: Replication,
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
    pub fn from_path<P>(path: P) -> anyhow::Result<Self>
    where
        P: AsRef<Path>,
    {
        let mut cfg = Configure::new();
        cfg.merge(File::with_name(path.as_ref().to_str().unwrap()))?;
        let cfg = cfg.try_into::<Self>()?;
        Ok(cfg)
    }

    /// This method returns the configuration of the member associated with
    /// the given Id.
    pub fn get(&self, id: &usize) -> Member {
        self.replication
            .members
            .iter()
            .find(|m| m.id == *id)
            .unwrap()
            .clone()
    }

    /// Return the size of the cluster.
    pub fn size(&self) -> usize {
        self.replication.members.len()
    }

    /// Return an iterator over a slice of all members in the cluster.
    pub fn members_iter(&self) -> Iter<Member> {
        self.replication.members.iter()
    }

    /// Return the log path.
    pub fn log_path(&self) -> String {
        self.system_log.path.clone()
    }

    /// Return the log path.
    pub fn log_destination(&self) -> String {
        self.system_log.destination.clone()
    }

    /// Return debug mode flag value.
    pub fn debug(&self) -> bool {
        self.system_log.debug
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
            systemLog:
              destination: "console"
              path: ""
              debug: true
    
            replication:
              id: "raft"
              members:
              - id: 0
                host: "0"
                me: true
              - id: 1
                host: "1"
              verbosity:
                election: true
                heartbeats: true
            "#,
            FileFormat::Yaml,
        );
        assert_eq!(
            config.unwrap().get(&0),
            Member {
                id: 0,
                me: true,
                host: "0".to_string()
            }
        )
    }
}
