use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    fmt::{self, Display, Formatter},
};

use casper_execution_engine::{core::engine_state, storage::trie::TrieRaw};
use casper_hashing::Digest;
use datasize::DataSize;
use derive_more::From;
use serde::Serialize;

use crate::types::{TrieOrChunk, TrieOrChunkId};

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

/*
#[derive(Clone, PartialEq, Eq, DataSize, Debug)]
enum TrieAcquisitionState {
    Acquiring(TrieAcquisition),
    Acquired(TrieRaw),
    PendingStore(TrieRaw, Vec<TrieHash>),
}
*/

#[derive(Clone, Copy, PartialEq, Eq, DataSize, Debug)]
pub(crate) enum Error {
    TrieAcquisitionError,
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::TrieAcquisitionError => {
                write!(f, "trie acquisition error",)
            }
        }
    }
}

#[derive(Clone, PartialEq, Eq, DataSize, Debug)]
pub(crate) struct GlobalStateAcquisition {
    root_hash: RootHash,
    fetch_queue: VecDeque<TrieAcquisition>,
    pending_tries: HashMap<TrieHash, TrieAcquisition>,
    acquired_tries: HashMap<TrieHash, TrieRaw>,
    tries_pending_store: HashSet<TrieHash>,
    max_parallel_trie_fetches: usize,
    //tries_awaiting_children: BTreeMap<TrieHash, TrieAwaitingChildren>,
}

impl GlobalStateAcquisition {
    pub(crate) fn new(root_hash: &Digest) -> GlobalStateAcquisition {
        GlobalStateAcquisition {
            root_hash: RootHash(*root_hash),
            fetch_queue: VecDeque::from([TrieAcquisition::Needed {
                trie_hash: *root_hash,
            }]),
            pending_tries: HashMap::new(),
            acquired_tries: HashMap::new(),
            tries_pending_store: HashSet::new(),
            max_parallel_trie_fetches: 500, /*TODO: propagate this from block builder
                                             *tries_awaiting_children: BTreeMap::new(), */
        }
    }

    pub(crate) fn tries_to_fetch(&mut self) -> Vec<TrieOrChunkId> {
        // TODO: This will return _a lot_ of entries for the block synchronizer to choose from
        // find a way to reduce this set of entries
        let mut tries_or_chunks_to_fetch: Vec<TrieOrChunkId> = Vec::new();

        for _ in 0..self.max_parallel_trie_fetches - self.pending_tries.len() {
            if let Some(acq) = self.fetch_queue.pop_back() {
                if let Some(trie_or_chunk) = acq.needs_value_or_chunk() {
                    tries_or_chunks_to_fetch.push(trie_or_chunk);
                    self.pending_tries.insert(TrieHash(acq.trie_hash()), acq);
                } //TODO: do anything for the else case?
            } else {
                break;
            }
        }

        tries_or_chunks_to_fetch
    }

    pub(super) fn register_trie_or_chunk(&mut self, trie_or_chunk: TrieOrChunk) -> Result<(), Error> {
        if let Some(acq) = self
            .pending_tries
            .remove(&TrieHash(*trie_or_chunk.trie_hash()))
        {
            match acq.apply_trie_or_chunk(trie_or_chunk) {
                Ok(acq) => match acq {
                    TrieAcquisition::Needed { .. } | TrieAcquisition::Acquiring { .. } => {
                        self.fetch_queue.push_back(acq);
                        Ok(())
                    }
                    TrieAcquisition::Complete { trie_hash, trie } => {
                        self.acquired_tries.insert(TrieHash(trie_hash), *trie);
                        Ok(())
                    }
                },
                Err(e) => Err(Error::TrieAcquisitionError),
            }
        } else {
            Err(Error::TrieAcquisitionError)
        }
    }

    pub(crate) fn tries_to_store(&mut self) -> Vec<(Digest, TrieRaw)> {
        let tries_to_store: Vec<(Digest, TrieRaw)> = self
            .acquired_tries
            .drain()
            .map(|(trie_hash, trie_raw)| (trie_hash.0, trie_raw))
            .collect();

        for (trie_hash, _) in tries_to_store.iter() {
            self.tries_pending_store.insert(TrieHash(*trie_hash));
        }

        tries_to_store
    }

    pub(super) fn register_pending_trie_fetches(
        &mut self,
        trie_fetches_in_progress: HashSet<Digest>,
    ) {
        todo!()
    }

    pub(super) fn register_put_trie(
        &mut self,
        trie_hash: Digest,
        trie_raw: TrieRaw,
        put_trie_result: Result<Digest, engine_state::Error>,
    ) {
        if self.tries_pending_store.remove(&TrieHash(trie_hash)) {
            match put_trie_result {
                Ok(_) => {}
                Err(engine_state::Error::MissingTrieNodeChildren(missing_children)) => {
                    missing_children.iter().for_each(|child| {
                        self.fetch_queue
                            .push_back(TrieAcquisition::Needed { trie_hash: *child })
                    });
                }
                Err(_) => panic!("Fatal error"), //TODO
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl GlobalStateAcquisition {
        pub(crate) fn root_hash(&self) -> Digest {
            self.root_hash.0
        }
    }
}
