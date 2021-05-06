//! Key/Value storage.
//! This module contains a key/value storage implementations.

use crate::command::{Command, Key, Value};
use raft_core::runtime::Runtime;
use std::collections::BTreeMap;
/// The type `Storage` type is a key/value storage.
#[derive(Debug)]
pub struct Storage {
    data: BTreeMap<Key, Value>,
    runtime: Option<Runtime<Command>>,
}

impl Storage {
    /// Create a new storage with runtime.
    pub fn with_runtime(runtime: Runtime<Command>) -> Storage {
        Storage {
            data: BTreeMap::new(),
            runtime: Some(runtime),
        }
    }

    /// Create a new storage
    pub fn new() -> Storage {
        Storage {
            data: BTreeMap::new(),
            runtime: None,
        }
    }

    /// Return a reference to a value corresponding to a key.
    async fn request_get(&self, key: &Key) -> Option<Value> {
        if let Some(value) = self.data.get(key).cloned() {
            Some(value)
        } else {
            Some("NOK".parse::<Value>().unwrap())
        }
    }

    /// Return a reference to a value corresponding to a key.
    async fn request_delete(&mut self, key: &Key) -> &'static str {
        self.data.remove(key);
        "OK"
    }

    /// Set a key value in the store.
    /// If the key already exists, the value will be updated.
    async fn request_set(&mut self, key: Key, value: Value) -> &'static str {
        self.data.insert(key, value);
        "OK"
    }

    /// Return a reference to a value corresponding to a key.
    async fn get(&self, key: &Key) -> Option<Value> {
        if let Some(value) = self.data.get(key).cloned() {
            Some(value)
        } else {
            Some("NOK".parse::<Value>().unwrap())
        }
    }

    /// Return a reference to a value corresponding to a key.
    async fn delete(&mut self, key: &Key) -> &'static str {
        self.data.remove(key);
        "OK"
    }

    /// Set a key value in the store.
    /// If the key already exists, the value will be updated.
    async fn set(&mut self, key: Key, value: Value) -> &'static str {
        self.data.insert(key, value);
        "OK"
    }

    /// Query the storage.
    pub async fn query(&mut self, cmd: Command) -> Option<Value> {
        if let Some(ref mut runtime) = self.runtime {
            println!("{}", runtime.server);
            if let Command::Invalid(_) = cmd.clone() {
                runtime.server.client_append_entry(cmd.clone());
            }
        }
        match cmd {
            Command::Get { key } => self.get(&key).await,
            Command::Delete { key } => {
                let value = self.delete(&key).await;
                let value: Value = value.parse().unwrap();
                Some(value)
            }
            Command::Set { key, value } => {
                let value = self.set(key, value).await;
                let value: Value = value.parse().unwrap();
                Some(value)
            }
            Command::Invalid(value) => Some(value.parse::<Value>().unwrap()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[async_std::test]
    async fn test_set_query() {
        let mut s = Storage::new();
        let cmd = Command::Set {
            key: Key::from_str("foo").unwrap(),
            value: Value::from_str("foo_val").unwrap(),
        };

        assert_eq!(s.query(cmd).await, Some(Value::from_str("OK").unwrap()))
    }

    #[async_std::test]
    async fn test_delete_query() {
        let mut s = Storage::new();
        let cmd = Command::Delete {
            key: Key::from_str("foo").unwrap(),
        };

        assert_eq!(s.query(cmd).await, Some(Value::from_str("OK").unwrap()))
    }

    #[async_std::test]
    async fn test_get_query() {
        let mut s = Storage::new();
        let cmd = Command::Get {
            key: Key::from_str("foo").unwrap(),
        };

        assert_eq!(s.query(cmd).await, Some(Value::from_str("NOK").unwrap()))
    }

    #[async_std::test]
    async fn test_query() {
        let mut s = Storage::new();
        let cmd = Command::Set {
            key: Key::from_str("foo").unwrap(),
            value: Value::from_str("foo_val").unwrap(),
        };

        assert_eq!(s.query(cmd).await, Some(Value::from_str("OK").unwrap()));

        let cmd = Command::Get {
            key: Key::from_str("foo").unwrap(),
        };

        assert_eq!(
            s.query(cmd).await,
            Some(Value::from_str("foo_val").unwrap())
        );

        let cmd = Command::Delete {
            key: Key::from_str("foo").unwrap(),
        };
        assert_eq!(s.query(cmd).await, Some(Value::from_str("OK").unwrap()));

        let cmd = Command::Get {
            key: Key::from_str("foo").unwrap(),
        };

        assert_eq!(s.query(cmd).await, Some(Value::from_str("NOK").unwrap()))
    }
}
