//! Raft log.

/// Entry owns the data for the log entry.
#[derive(Debug, PartialOrd, Ord, PartialEq, Eq, Clone)]
pub struct Entry<V> {
    pub term: usize,
    pub value: V,
}

impl<V> Entry<V> {
    pub fn new(term: usize, value: V) -> Self {
        Entry { term, value }
    }
}

/// The `Log` type owns all the log for a Raft server
#[derive(Debug)]
pub struct Log<V> {
    pub(crate) entries: Vec<Entry<V>>,
}

use std::fmt;
impl<V: Clone + fmt::Debug> Log<V> {
    pub fn new() -> Self {
        Log {
            entries: Vec::new(),
        }
    }

    pub(crate) fn from(entries: Vec<Entry<V>>) -> Self {
        Self { entries }
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn append_entries(
        &mut self,
        previous_index: Option<usize>,
        previous_term: Option<usize>,
        entries: &[Entry<V>],
    ) -> bool {
        if let Some(index) = previous_index {
            // Check whether the previous index received is the same as the current index in the log.
            // because the log is never allowed to have holes.
            if !self.is_empty() && index > self.len() - 1 {
                return false;
            }

            if let Some(term) = previous_term {
                // Check whether the previous term received is the same as the current term of the last
                // log entries.
                if self.entries[index].term != term {
                    return false;
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
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_append_entries() {
        let mut log = Log::new();
        assert!(
            log.append_entries(None, None, &[Entry::new(1, 'x')]),
            "append entries failed on empty log"
        );
        assert_eq!(log.entries, [Entry::new(1, 'x')]);
        assert!(
            log.append_entries(None, Some(1), &[Entry::new(1, 'x')]),
            "idempotent append entry failed"
        );
        assert_eq!(log.entries, [Entry::new(1, 'x')]);
        assert!(log.append_entries(Some(0), Some(1), &[Entry::new(1, 'y')]));
        assert!(log.append_entries(Some(0), Some(1), &[Entry::new(1, 'y')]));
        assert_eq!(log.entries, [Entry::new(1, 'x'), Entry::new(1, 'y')]);
    }

    fn test_setup_leader_paper_fig7() -> Log<char> {
        Log::from(vec![
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
        let mut follower_a = Log::from(vec![
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
        let ok = follower_a.append_entries(Some(9), Some(6), &leader.entries[leader.len() - 1..]);
        assert!(ok);
        assert_eq!(follower_a.entries, leader.entries);
    }

    #[test]
    fn test_scenario_paper_fig7_b() {
        let leader = test_setup_leader_paper_fig7();
        let mut follower_b = Log::from(vec![
            Entry::new(1, 'a'),
            Entry::new(1, 'b'),
            Entry::new(1, 'c'),
            Entry::new(4, 'd'),
        ]);

        assert!(!follower_b.append_entries(Some(9), Some(6), &leader.entries[leader.len() - 1..]));
        assert_ne!(follower_b.entries, leader.entries);
    }

    #[test]
    fn test_scenario_paper_fig7_c() {
        let leader = test_setup_leader_paper_fig7();
        let mut follower_c = Log::from(vec![
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

        assert!(follower_c.append_entries(Some(9), Some(6), &leader.entries[leader.len() - 1..]));
        assert_eq!(follower_c.entries, leader.entries);
    }
    #[test]
    fn test_scenario_paper_fig7_d() {
        let leader = test_setup_leader_paper_fig7();
        let mut follower_d = Log::from(vec![
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
        assert!(follower_d.append_entries(Some(9), Some(6), &leader.entries[leader.len() - 1..]));
        assert_eq!(follower_d.entries, leader.entries);
    }

    #[test]
    fn test_scenario_paper_fig7_e() {
        let leader = test_setup_leader_paper_fig7();
        let mut follower_e = Log::from(vec![
            Entry::new(1, 'a'),
            Entry::new(1, 'b'),
            Entry::new(1, 'c'),
            Entry::new(4, 'd'),
            Entry::new(4, 'f'),
            Entry::new(4, 'g'),
            Entry::new(4, 'h'),
        ]);

        assert!(!follower_e.append_entries(Some(9), Some(6), &leader.entries[leader.len() - 1..]));
        assert_ne!(follower_e.entries, leader.entries);
    }

    #[test]
    fn test_scenario_paper_fig7f() {
        let leader = test_setup_leader_paper_fig7();
        let mut follower_f = Log::from(vec![
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

        assert!(!follower_f.append_entries(Some(9), Some(6), &leader.entries[leader.len() - 1..]));
        assert_ne!(follower_f.entries, leader.entries);
    }
}
