//! Key/Value server
//! This module contains a key/value server implementations.

use std::collections::BTreeMap;

/// The type `Store` type is a key/value store.
#[derive(Debug)]
pub struct Store<V: Ord>(BTreeMap<String, V>);

impl<V> Store<V>
where
    V: Ord,
{
    /// Create a new store
    pub fn new() -> Store<V> {
        Store(BTreeMap::new())
    }

    /// Return a reference to a value corresponding to a key.
    pub fn get(&self, key: &str) -> Option<&V> {
        self.0.get(key)
    }

    /// Return a reference to a value corresponding to a key.
    pub fn delete(&mut self, key: &str) -> &'static str {
        self.0.remove(key);
        "OK"
    }

    /// Set a key value in the store.
    /// If the key already exists, the value will be updated.
    pub fn set(&mut self, key: String, value: V) -> &'static str {
        self.0.insert(key, value);
        "OK"
    }
}
