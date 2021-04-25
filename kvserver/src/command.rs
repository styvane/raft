/// The `Command` type represents the available database command.
use serde::Deserialize;

use std::str::FromStr;
use std::string::ParseError;

#[derive(PartialEq, Eq, Deserialize, Clone, Debug, Ord, PartialOrd, Hash)]
pub struct Key(String);

#[derive(PartialEq, Eq, Deserialize, Clone, Debug, Ord, PartialOrd)]
pub struct Value(String);

impl FromStr for Value {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(String::from(s)))
    }
}

impl FromStr for Key {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(String::from(s)))
    }
}

#[derive(Debug, Deserialize)]
pub enum Command {
    Get { key: Key },
    Set { key: Key, value: Value },
    Delete { key: Key },
}
