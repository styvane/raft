use futures::channel::oneshot;
use raft_core::runtime::{ClientRequest, ConsensusSender};

/// The `Command` type represents the available database command.
use serde::{Deserialize, Serialize};

use std::str::FromStr;
use std::string::ParseError;

#[derive(PartialEq, Eq, Serialize, Deserialize, Clone, Debug, Ord, PartialOrd, Hash)]
pub struct Key(String);

impl FromStr for Key {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(String::from(s)))
    }
}

#[derive(PartialEq, Eq, Serialize, Deserialize, Clone, Debug, Ord, PartialOrd)]
pub struct Value(String);

impl FromStr for Value {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(String::from(s)))
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum Command {
    Get { key: Key },
    Set { key: Key, value: Value },
    Delete { key: Key },
    Invalid(String),
}

#[derive(Debug)]
pub struct CommandMessage {
    pub kind: Command,
    pub response: ConsensusSender,
}

impl CommandMessage {
    pub fn set_consensus(&mut self, sender: oneshot::Sender<bool>) {
        self.response = Some(sender);
    }
}

impl ClientRequest for CommandMessage {
    type Entry = Command;

    fn entry_kind(&self) -> Self::Entry {
        self.kind.clone()
    }

    fn responder(self) -> ConsensusSender {
        self.response
    }
}
