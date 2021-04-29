//! This module contains the definitions of various events in the Raft
//! network.

use crate::types::Index;
/// Event in the Raft system.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub enum Event {
    AppendEntries { current_term: Index },
}

pub struct Message {
    dest: String,
    event: Event,
}

impl Message {
    pub fn new(dest: &str, event: Event) -> Self {
        Self {
            dest: String::from(dest),
            event,
        }
    }

    pub fn destination(&self) -> String {
        self.dest.clone()
    }

    pub fn inner_event(&self) -> Event {
        self.event.clone()
    }
}
