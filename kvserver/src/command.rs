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

type Consensus = Option<oneshot::Sender<bool>>;

#[derive(Debug, Deserialize, Serialize)]
pub enum Command {
    Get {
        key: Key,

        #[serde(skip)]
        consensus: Consensus,
    },
    Set {
        key: Key,
        value: Value,

        #[serde(skip)]
        consensus: Consensus,
    },
    Delete {
        key: Key,
        #[serde(skip)]
        consensus: Consensus,
    },
    Invalid(String),
}

impl Command {
    pub fn set_consensus(&mut self, sender: oneshot::Sender<bool>) {
        match self {
            Command::Get {
                ref mut consensus, ..
            } => {
                consensus.replace(sender);
            }
            Command::Set {
                ref mut consensus, ..
            } => {
                consensus.replace(sender);
            }
            Command::Delete {
                ref mut consensus, ..
            } => {
                consensus.replace(sender);
            }
            Command::Invalid(..) => {}
        };
    }
}
