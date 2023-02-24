use std::{
    collections::{BTreeMap, HashSet},
    fmt,
};

use casper_execution_engine::storage::trie::TrieRaw;
use casper_hashing::Digest;
use datasize::DataSize;
use derive_more::From;
use serde::Serialize;

use crate::types::TrieOrChunkId;

use super::trie_acquisition::TrieAcquisition;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, DataSize, From)]
pub(crate) struct RootHash(Digest);

impl RootHash {
    pub(crate) fn into_inner(self) -> Digest {
        self.0
    }
}

impl fmt::Display for RootHash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, DataSize, From)]
pub(crate) struct TrieHash(Digest);

impl fmt::Display for TrieHash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, DataSize)]
struct TrieAwaitingChildren {
    trie_raw: TrieRaw,
    missing_children: HashSet<TrieHash>,
}

impl TrieAwaitingChildren {
    fn new(trie_raw: TrieRaw, missing_children: Vec<TrieHash>) -> Self {
        Self {
            trie_raw,
            missing_children: missing_children.into_iter().collect(),
        }
    }

    /// Handles `written_trie` being written to the database - removes the trie as a dependency and
    /// returns the next trie to be downloaded.
    fn trie_written(&mut self, written_trie: TrieHash) {
        self.missing_children.remove(&written_trie);
    }

    fn ready_to_be_written(&self) -> bool {
        self.missing_children.is_empty()
    }

    fn decompose(self) -> (TrieRaw, HashSet<TrieHash>) {
        (self.trie_raw, self.request_root_hashes)
    }

    fn extend_request_hashes(&mut self, request_root_hashes: HashSet<TrieHash>) {
        self.request_root_hashes.extend(request_root_hashes);
    }
}

/*
#[derive(Clone, PartialEq, Eq, DataSize, Debug)]
enum TrieAcquisitionState {
    Acquiring(TrieAcquisition),
    Acquired(TrieRaw),
    PendingStore(TrieRaw, Vec<TrieHash>),
}
*/

#[derive(Clone, PartialEq, Eq, DataSize, Debug)]
pub(crate) struct GlobalStateAcquisition {
    root_hash: RootHash,
    fetch_queue: Vec<TrieAcquisition>,
    tries_awaiting_children: BTreeMap<TrieHash, TrieAwaitingChildren>,
}

impl GlobalStateAcquisition {
    pub(crate) fn new(root_hash: &Digest) -> GlobalStateAcquisition {
        GlobalStateAcquisition {
            root_hash: RootHash(root_hash),
            fetch_queue: vec![TrieAcquisition::Needed {
                trie_hash: root_hash,
            }],
            tries_awaiting_children: BTreeMap::new(),
        }
    }

    pub(crate) fn tries_to_store(&mut self) -> Vec<TrieRaw> {
        // TODO: figure out when to request to store the tries
        for trie_state in self
            .tries
            .iter()
            .filter(|state| matches!(item, TrieAcquisitionState::Acquired {}))
        {}
        Vec::new()
    }

    pub(crate) fn tries_to_fetch(&mut self) -> Vec<TrieOrChunkId> {
        // TODO: This will return _a lot_ of entries for the block synchronizer to choose from
        // find a way to reduce this set of entries
        fetch_queue
            .iter()
            .rev()
            .filter_map(|acq| acq.needs_value_or_chunk())
            .collect()
    }

    pub(super) fn register_pending_trie_fetches(
        &mut self,
        trie_fetches_in_progress: HashSet<Digest>,
    ) {
        // TODO
    }
}
