//! Raft log.

use crate::types::{Index, Term};
use anyhow::{self, bail};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Entry owns the data for the log entry.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
pub struct Entry<Data> {
    pub term: usize,
    pub data: Data,
}

/// A set of log entries.
pub type Entries<Data> = Vec<Entry<Data>>;

impl<Data> Entry<Data> {
    pub const fn new(term: usize, data: Data) -> Self {
        Entry { term, data }
    }
}
/// The `Log` trait defines the
pub trait Log: fmt::Display {
    type Item;

    fn len(&self) -> usize;
    fn previous_term(&self) -> Term;
    fn previous_index(&self) -> Term;
    fn get_entries(&self) -> &[Self::Item];
    fn append_entries(
        &mut self,
        previous_index: Index,
        previous_term: Term,
        entries: &[Self::Item],
    ) -> anyhow::Result<()>;
}

/// The `InMemoryLog` type stores all the Raft server's logs in memory.
#[derive(Debug)]
pub struct InMemory<Data> {
    pub(crate) entries: Vec<Entry<Data>>,
}

impl<Data> InMemory<Data> {
    /// Returns the number of elements in the log.
    /// Returns true if log has length 0.
    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl<Data> fmt::Display for InMemory<Data>
where
    Data: Clone + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", &self.entries)
    }
}
impl<Data> Default for InMemory<Data> {
    fn default() -> Self {
        InMemory {
            entries: Vec::new(),
        }
    }
}

impl<Data> Log for InMemory<Data>
where
    Data: Clone + fmt::Debug,
{
    type Item = Entry<Data>;

    /// Returns the number of entries in the log.
    fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns previous log entry index.
    fn previous_index(&self) -> Index {
        if !self.is_empty() {
            Some(self.len() - 1)
        } else {
            None
        }
    }

    /// Returns the term of the previous log entry.
    fn previous_term(&self) -> Term {
        self.entries.iter().last().map(|entry| entry.term)
    }

    /// Returns a slice of log entries.
    fn get_entries(&self) -> &[Self::Item] {
        &self.entries
    }

    /// Appends new entries to the log.
    fn append_entries(
        &mut self,
        previous_index: Index,
        previous_term: Term,
        entries: &[Self::Item],
    ) -> anyhow::Result<()> {
        if let Some(index) = previous_index {
            // Check whether the previous index received is the same as the current index in the log.
            // because the log is never allowed to have holes.
            if !self.is_empty() && index > self.len() - 1 {
                bail!("log is never allowed to have holes");
            }

            if let Some(term) = previous_term {
                // Check whether the previous term received is the same as the current term of the last
                // log entries.
                if self.entries[index].term != term {
                    bail!("mismatch previous term");
                }
            }
        }

        for (index, entry) in entries
            .iter()
            .enumerate()
            .map(|(ix, val)| (ix + previous_index.unwrap_or(0) + 1, val))
        {
            if index >= self.len() {
                break;
            }

            // Truncate log if new log term is different from current term.
            if self.entries[index].term != entry.term {
                self.entries.truncate(index);
                break;
            }
        }

        // Ignore entries already present in the log.
        match previous_index {
            None => {
                self.entries.truncate(0);
            }
            Some(ix) => {
                if self.len() - 1 > ix {
                    self.entries.truncate(ix + 1);
                }
            }
        }
        self.entries.extend_from_slice(entries);
        Ok(())
    }
}

impl<V: Clone + fmt::Debug> InMemory<V> {
    /// Create a log from existing entries.
    #[allow(dead_code)]
    pub(crate) fn from(entries: Vec<Entry<V>>) -> Self {
        Self { entries }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_append_entries() {
        let mut log = InMemory::default();
        assert!(
            log.append_entries(None, None, &[Entry::new(1, 'x')])
                .is_ok(),
            "append entries failed on empty log"
        );
        assert_eq!(log.entries, [Entry::new(1, 'x')]);
        assert!(
            log.append_entries(None, Some(1), &[Entry::new(1, 'x')])
                .is_ok(),
            "idempotent append entry failed"
        );
        assert_eq!(log.entries, &[Entry::new(1, 'x')]);
        assert!(log
            .append_entries(Some(0), Some(1), &[Entry::new(1, 'y')])
            .is_ok());
        assert!(log
            .append_entries(Some(0), Some(1), &[Entry::new(1, 'y')])
            .is_ok());
        assert!(log
            .append_entries(Some(1), Some(1), &[Entry::new(2, 'y')])
            .is_ok());
        assert!(log
            .append_entries(Some(1), Some(1), &[Entry::new(2, 'y')])
            .is_ok());
        assert_eq!(
            log.entries,
            [Entry::new(1, 'x'), Entry::new(1, 'y'), Entry::new(2, 'y')]
        );
    }

    fn test_setup_leader_paper_fig7() -> InMemory<char> {
        InMemory::from(vec![
            Entry::new(1, 'a'),
            Entry::new(1, 'b'),
            Entry::new(1, 'c'),
            Entry::new(4, 'd'),
            Entry::new(4, 'f'),
            Entry::new(5, 'g'),
            Entry::new(5, 'h'),
            Entry::new(6, 'i'),
            Entry::new(6, 'j'),
            Entry::new(6, 'k'),
            Entry::new(8, 'l'),
        ])
    }

    #[test]
    fn test_scenario_paper_fig7_a() {
        let leader = test_setup_leader_paper_fig7();
        let mut follower_a = InMemory::from(vec![
            Entry::new(1, 'a'),
            Entry::new(1, 'b'),
            Entry::new(1, 'c'),
            Entry::new(4, 'd'),
            Entry::new(4, 'f'),
            Entry::new(5, 'g'),
            Entry::new(5, 'h'),
            Entry::new(6, 'i'),
            Entry::new(6, 'j'),
            Entry::new(6, 'k'),
        ]);
        let result =
            follower_a.append_entries(Some(9), Some(6), &leader.entries[leader.len() - 1..]);
        assert!(result.is_ok());
        assert_eq!(follower_a.entries, leader.entries);
    }

    #[test]
    fn test_scenario_paper_fig7_b() {
        let leader = test_setup_leader_paper_fig7();
        let mut follower_b = InMemory::from(vec![
            Entry::new(1, 'a'),
            Entry::new(1, 'b'),
            Entry::new(1, 'c'),
            Entry::new(4, 'd'),
        ]);

        assert!(follower_b
            .append_entries(Some(9), Some(6), &leader.entries[leader.len() - 1..])
            .is_err());
        assert_ne!(follower_b.entries, leader.entries);
    }

    #[test]
    fn test_scenario_paper_fig7_c() {
        let leader = test_setup_leader_paper_fig7();
        let mut follower_c = InMemory::from(vec![
            Entry::new(1, 'a'),
            Entry::new(1, 'b'),
            Entry::new(1, 'c'),
            Entry::new(4, 'd'),
            Entry::new(4, 'f'),
            Entry::new(5, 'g'),
            Entry::new(5, 'h'),
            Entry::new(6, 'i'),
            Entry::new(6, 'j'),
            Entry::new(6, 'k'),
            Entry::new(6, 'l'),
        ]);

        assert!(follower_c
            .append_entries(Some(9), Some(6), &leader.entries[leader.len() - 1..])
            .is_ok());
        assert_eq!(follower_c.entries, leader.entries);
    }
    #[test]
    fn test_scenario_paper_fig7_d() {
        let leader = test_setup_leader_paper_fig7();
        let mut follower_d = InMemory::from(vec![
            Entry::new(1, 'a'),
            Entry::new(1, 'b'),
            Entry::new(1, 'c'),
            Entry::new(4, 'd'),
            Entry::new(4, 'f'),
            Entry::new(5, 'g'),
            Entry::new(5, 'h'),
            Entry::new(6, 'i'),
            Entry::new(6, 'j'),
            Entry::new(6, 'k'),
            Entry::new(7, 'l'),
            Entry::new(7, 'm'),
        ]);
        assert!(follower_d
            .append_entries(Some(9), Some(6), &leader.entries[leader.len() - 1..])
            .is_ok());
        assert_eq!(follower_d.entries, leader.entries);
    }

    #[test]
    fn test_scenario_paper_fig7_e() {
        let leader = test_setup_leader_paper_fig7();
        let mut follower_e = InMemory::from(vec![
            Entry::new(1, 'a'),
            Entry::new(1, 'b'),
            Entry::new(1, 'c'),
            Entry::new(4, 'd'),
            Entry::new(4, 'f'),
            Entry::new(4, 'g'),
            Entry::new(4, 'h'),
        ]);

        assert!(!follower_e
            .append_entries(Some(9), Some(6), &leader.entries[leader.len() - 1..])
            .is_ok());
        assert_ne!(follower_e.entries, leader.entries);
    }

    #[test]
    fn test_scenario_paper_fig7f() {
        let leader = test_setup_leader_paper_fig7();
        let mut follower_f = InMemory::from(vec![
            Entry::new(1, 'a'),
            Entry::new(1, 'b'),
            Entry::new(1, 'c'),
            Entry::new(2, 'd'),
            Entry::new(2, 'f'),
            Entry::new(2, 'g'),
            Entry::new(3, 'h'),
            Entry::new(3, 'i'),
            Entry::new(3, 'j'),
            Entry::new(3, 'k'),
            Entry::new(3, 'l'),
        ]);

        assert!(!follower_f
            .append_entries(Some(9), Some(6), &leader.entries[leader.len() - 1..])
            .is_ok());
        assert_ne!(follower_f.entries, leader.entries);
    }
}
