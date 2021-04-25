//! Key/Value storage.
//! This module contains a key/value storage implementations.

use crate::command::{Command, Key, Value};
use async_std;
use std::collections::BTreeMap;
use std::str::FromStr;

/// The type `Storage` type is a key/value storage.
#[derive(Debug)]
pub struct Storage(BTreeMap<Key, Value>);

impl Storage {
    /// Create a new storage
    pub fn new() -> Storage {
        Storage(BTreeMap::new())
    }

    /// Return a reference to a value corresponding to a key.
    async fn perform_get(&self, key: &Key) -> Option<Value> {
        self.0.get(key).cloned()
    }

    /// Return a reference to a value corresponding to a key.
    async fn perform_delete(&mut self, key: &Key) -> &'static str {
        self.0.remove(key);
        "OK"
    }

    /// Set a key value in the store.
    /// If the key already exists, the value will be updated.
    async fn perform_set(&mut self, key: Key, value: Value) -> &'static str {
        self.0.insert(key, value);
        "OK"
    }

    /// Query the storage.
    pub async fn query(&mut self, cmd: Command) -> Option<Value> {
        match cmd {
            Command::Get { key } => self.perform_get(&key).await,
            Command::Delete { key } => {
                let value = self.perform_delete(&key).await;
                let value: Value = value.parse().unwrap();
                Some(value)
            }
            Command::Set { key, value } => {
                let value = self.perform_set(key, value).await;
                let value: Value = value.parse().unwrap();
                Some(value)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

        assert_eq!(s.query(cmd).await, None)
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

        assert_eq!(s.query(cmd).await, None)
    }
}
