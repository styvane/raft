//! Server events
use crate::command::{Command, Value};
use async_std::channel::{Receiver, Sender};
use async_std::io;
use std::collections::HashMap;

/// The `Event` type represents the server events.
pub enum Event<'s, T> {
    NewClient { sock: T },
    Request { cmd: Command, sender: &'s str },
}

impl<'s, T> Event<'s, T>
where
    T: io::Read + io::Write,
{
    pub fn new_request(cmd: Command, sender: &'s str) -> Self {
        Self::Request { cmd, sender }
    }
}

/// Event broker.
pub struct Broker<'s, T> {
    receiver: Receiver<Event<'s, T>>,
    clients: HashMap<&'s str, Sender<Value>>,
}
