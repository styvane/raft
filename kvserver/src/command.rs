/// The `Command` type represents the available database command.
use serde::Deserialize;

#[derive(PartialEq, Eq, Deserialize, Clone, Debug, Ord, PartialOrd, Hash)]
pub struct Key(String);

#[derive(PartialEq, Eq, Deserialize, Clone, Debug, Ord, PartialOrd)]
pub struct Value(String);

#[derive(Debug, Deserialize)]
pub enum Command {
    Get { key: Key },
    Set { key: Key, value: Value },
    Delete { key: Key },
}
