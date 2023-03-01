use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    fmt::{self, Display, Formatter},
    iter::FromIterator,
};

use casper_execution_engine::{core::engine_state, storage::trie::TrieRaw};
use casper_hashing::Digest;
use datasize::DataSize;
use derive_more::From;
use serde::Serialize;
use tracing::debug;

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

impl TrieHash {
    pub(crate) fn into_inner(self) -> Digest {
        self.0
    }
}

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
    PutTrieError,
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::TrieAcquisitionError => {
                write!(f, "trie acquisition error",)
            }
            Error::PutTrieError => {
                write!(f, "put trie error",)
            }
        }
    }
}

#[derive(Clone, PartialEq, Eq, DataSize, Debug)]
pub(crate) struct GlobalStateAcquisition {
    root_hash: RootHash,
    fetch_queue: VecDeque<(TrieAcquisition, Option<Digest>)>,
    pending_tries: HashMap<TrieHash, (TrieAcquisition, Option<Digest>)>,
    acquired_tries: HashMap<TrieHash, (TrieRaw, Option<Digest>)>,
    tries_pending_store: HashMap<TrieHash, Option<Digest>>,
    tries_awaiting_children: HashMap<TrieHash, (TrieRaw, HashSet<Digest>, Option<Digest>)>,
    max_parallel_trie_fetches: usize,
    //tries_awaiting_children: BTreeMap<TrieHash, TrieAwaitingChildren>,
}

impl GlobalStateAcquisition {
    pub(crate) fn new(root_hash: &Digest) -> GlobalStateAcquisition {
        debug!(%root_hash, "XXX Creating GlobalStateAcquisition");
        GlobalStateAcquisition {
            root_hash: RootHash(*root_hash),
            fetch_queue: VecDeque::from([(
                TrieAcquisition::Needed {
                    trie_hash: *root_hash,
                },
                None,
            )]),
            pending_tries: HashMap::new(),
            acquired_tries: HashMap::new(),
            tries_pending_store: HashMap::new(),
            tries_awaiting_children: HashMap::new(),
            max_parallel_trie_fetches: 500, /*TODO: propagate this from block builder
                                             *tries_awaiting_children: BTreeMap::new(), */
        }
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.fetch_queue.is_empty()
            && self.pending_tries.is_empty()
            && self.acquired_tries.is_empty()
            && self.tries_pending_store.is_empty()
            && self.tries_awaiting_children.is_empty()
    }

    pub(crate) fn tries_to_fetch(&mut self) -> Vec<TrieOrChunkId> {
        // TODO: This will return _a lot_ of entries for the block synchronizer to choose from
        // find a way to reduce this set of entries
        let mut tries_or_chunks_to_fetch: Vec<TrieOrChunkId> = Vec::new();
        let mut pending: HashMap<TrieHash, (TrieAcquisition, Option<Digest>)> = HashMap::new();

        for _ in 0..self.max_parallel_trie_fetches - self.pending_tries.len() {
            if let Some((acq, parent)) = self.fetch_queue.pop_back() {
                if let Some(trie_or_chunk) = acq.needs_value_or_chunk() {
                    tries_or_chunks_to_fetch.push(trie_or_chunk);
                    pending.insert(TrieHash(acq.trie_hash()), (acq, parent));
                } //TODO: do anything for the else case?
            } else {
                break;
            }
        }

        // TODO: for finality signatures we also retry pending fetches, but this creates a lot of
        // potentially redundant fetches in this case. We can register back fetch errors to the
        // global state acquisition and re-queue the trie then; this would lead to a minimum number
        // or fetches at the same time (form only one peer) but might be slower than using multiple
        // peers. Alternatively we can return the pending ones and add more fetches (from more peers
        // potentially) which would maybe make it quicker. In this case the errors need to be
        // revisited because somehow there's a storm of peer disqualifies when we do this now.
        // Also it's probably a good idea to only return only from the fetch queue and handle
        // fetch parallelism in the caller.
        /*
        // Retry pending if we have free fetch slots
        if tries_or_chunks_to_fetch.len() < self.max_parallel_trie_fetches {
            tries_or_chunks_to_fetch.extend(
                self.pending_tries
                    .iter()
                    .take(self.max_parallel_trie_fetches - tries_or_chunks_to_fetch.len())
                    .filter(|(_, (acq, _))| acq.needs_value_or_chunk().is_some())
                    .map(|(_, (acq, _))| acq.needs_value_or_chunk().unwrap()),
            )
        }
        */

        self.pending_tries.extend(pending);

        debug!(root_hash=%self.root_hash, ?tries_or_chunks_to_fetch, "XXX GlobalStateAcquisition: returning tries to be fetched");
        tries_or_chunks_to_fetch
    }

    pub(super) fn register_trie_or_chunk(
        &mut self,
        trie_or_chunk: TrieOrChunk,
    ) -> Result<(), Error> {
        debug!(root_hash=%self.root_hash, ?trie_or_chunk, "XXX GlobalStateAcquisition: received a trie or chunk");
        if let Some((acq, parent)) = self
            .pending_tries
            .remove(&TrieHash(*trie_or_chunk.trie_hash()))
        {
            match acq.apply_trie_or_chunk(trie_or_chunk) {
                Ok(acq) => match acq {
                    TrieAcquisition::Needed { .. } | TrieAcquisition::Acquiring { .. } => {
                        self.fetch_queue.push_back((acq.clone(), parent));
                        debug!(root_hash=%self.root_hash, ?acq, "XXX GlobalStateAcquisition: trie still incomplete; moving to fetch queue");
                        Ok(())
                    }
                    TrieAcquisition::Complete { trie_hash, trie } => {
                        self.acquired_tries
                            .insert(TrieHash(trie_hash), (*trie, parent));
                        debug!(root_hash=%self.root_hash, %trie_hash, "XXX GlobalStateAcquisition: trie fetch complete; moving to acquired");
                        Ok(())
                    }
                },
                Err(e) => {
                    debug!(root_hash=%self.root_hash, ?e, "XXX GlobalStateAcquisition: error when trying to apply trie or chunk");
                    Err(Error::TrieAcquisitionError)
                }
            }
        } else {
            debug!(root_hash=%self.root_hash, ?trie_or_chunk, "XXX GlobalStateAcquisition: trie or chunk was not pending; ERROR");
            Err(Error::TrieAcquisitionError)
        }
    }

    pub(crate) fn tries_to_store(&mut self) -> Vec<(Digest, TrieRaw)> {
        let tries_pending_store = &mut self.tries_pending_store;

        let mut tries_to_store: Vec<(Digest, TrieRaw)> = self
            .acquired_tries
            .drain()
            .map(|(trie_hash, (trie_raw, parent))| {
                tries_pending_store.insert(trie_hash, parent);
                (trie_hash.0, trie_raw)
            })
            .collect();

        debug!(root_hash=%self.root_hash, ?tries_to_store, "XXX GlobalStateAcquisition: tries to store from acquired state");

        /*
        let fetch_queue = &self.fetch_queue;
        let pending_tries = &self.pending_tries;
        let acquired_tries = &self.acquired_tries;
        let tries_pending_store = &mut self.tries_pending_store;

        self.tries_awaiting_children
            .retain_mut(|(trie_hash, trie_raw, children)| {
                children.retain(|child| {
                    fetch_queue
                        .iter()
                        .filter(|acq| acq.trie_hash() == *child)
                        .count()
                        != 0
                        || pending_tries.contains_key(&TrieHash(*child))
                        || acquired_tries.contains_key(&TrieHash(*child))
                        || tries_pending_store.contains(&TrieHash(*child))
                        || tries_to_store
                            .iter()
                            .filter(|(hash, _)| hash == child)
                            .count()
                            != 0
                });

                if children.is_empty() {
                    tries_to_store.push((trie_hash.into_inner(), trie_raw.clone()));
                    false
                } else {
                    true
                }
            });
        */

        let mut tries_awaiting_children_to_store: Vec<(Digest, TrieRaw)> = Vec::new();
        self.tries_awaiting_children
            .retain(|&waiting_trie_hash, (trie_raw, children, parent)| {
                if children.is_empty() {
                    tries_awaiting_children_to_store
                        .push((waiting_trie_hash.into_inner(), trie_raw.clone()));
                    tries_pending_store.insert(waiting_trie_hash, *parent);
                    false
                } else {
                    true
                }
            });

        debug!(root_hash=%self.root_hash, ?tries_awaiting_children_to_store, "XXX GlobalStateAcquisition: tries to store that were awaiting children");

        tries_to_store.extend(tries_awaiting_children_to_store);
        tries_to_store
    }

    // TODO: maybe implement this to keep tries_to_fetch immutable; same for tries_to_store
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
    ) -> Result<(), Error> {
        debug!(root_hash=%self.root_hash, %trie_hash, "XXX GlobalStateAcquisition: received put_trie result");

        if let Some((hash, parent)) = self.tries_pending_store.remove_entry(&TrieHash(trie_hash)) {
            match put_trie_result {
                Ok(stored_trie_hash) => {
                    debug!(root_hash=%self.root_hash, trie_hash=%stored_trie_hash, "XXX GlobalStateAcquisition: put_trie was successful");
                    if let Some(parent_hash) = parent {
                        debug!(root_hash=%self.root_hash, trie_hash=%stored_trie_hash, %parent_hash, "XXX GlobalStateAcquisition: found trie to be the child of some other trie");
                        if let Some((elem, children, _)) =
                            self.tries_awaiting_children.get_mut(&TrieHash(parent_hash))
                        {
                            debug!(root_hash=%self.root_hash, trie_hash=%stored_trie_hash, %parent_hash, "XXX GlobalStateAcquisition: removing child from parent dependencies");
                            children.remove(&stored_trie_hash);
                        }
                    }
                    Ok(())
                }
                Err(engine_state::Error::MissingTrieNodeChildren(missing_children)) => {
                    debug!(root_hash=%self.root_hash, %trie_hash, ?missing_children, "XXX GlobalStateAcquisition: trie_result shows that we are missing some children");

                    missing_children.iter().for_each(|child| {
                        debug!(root_hash=%self.root_hash, %child, %trie_hash, "XXX GlobalStateAcquisition: add child to fetch queue");

                        self.fetch_queue
                            .push_back((TrieAcquisition::Needed { trie_hash: *child }, Some(trie_hash)))
                    });
                    debug!(root_hash=%self.root_hash, %trie_hash, ?parent, "XXX GlobalStateAcquisition: putting trie on wait queue until all children are stored");
                    self.tries_awaiting_children.insert(
                        TrieHash(trie_hash),
                        (trie_raw, HashSet::from_iter(missing_children), parent),
                    );
                    Ok(())
                }
                Err(_) => Err(Error::PutTrieError), //TODO
            }
        } else {
            debug!(root_hash=%self.root_hash, %trie_hash, "XXX GlobalStateAcquisition: could not find trie hash in pending stores");
            Err(Error::PutTrieError)
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
