//! Key/Value storage.
//! This module contains a key/value storage implementations.

use crate::command::{Key, Value};
use std::collections::BTreeMap;

/// The type `Storage` type is a key/value storage.
#[derive(Debug)]
pub struct Storage(BTreeMap<Key, Value>);

impl Storage {
    /// Create a new storage
    pub fn new() -> Storage {
        Storage(BTreeMap::new())
    }

    /// Return a reference to a value corresponding to a key.
    pub fn get(&self, key: &Key) -> Option<&Value> {
        self.0.get(key)
    }

    /// Return a reference to a value corresponding to a key.
    pub fn delete(&mut self, key: &Key) -> &'static str {
        self.0.remove(key);
        "OK"
    }

    /// Set a key value in the store.
    /// If the key already exists, the value will be updated.
    pub fn set(&mut self, key: Key, value: Value) -> &'static str {
        self.0.insert(key, value);
        "OK"
    }
}
