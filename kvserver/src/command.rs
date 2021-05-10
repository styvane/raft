use futures::channel::oneshot;

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

type ConsensusResponse = Option<oneshot::Sender<bool>>;

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
    pub response: ConsensusResponse,
}

impl CommandMessage {
    pub fn set_consensus(&mut self, sender: oneshot::Sender<bool>) {
        self.response = Some(sender);
    }
}
