use std::fmt::{Display, Formatter};

use casper_execution_engine::{storage::trie::TrieRaw, core::engine_state};
use casper_hashing::Digest;
use casper_types::system::auction::EraValidators;
use datasize::DataSize;

use crate::types::TrieOrChunk;

use super::global_state_acquisition::{GlobalStateAcquisition, Error as GlobalStateAcquisitionError};

#[derive(Clone, Copy, PartialEq, Eq, DataSize, Debug)]
pub(crate) enum Error {
    NotAcquiring { root_hash: Digest },
    RootHashMismatch { expected: Digest, actual: Digest },
    GlobalStateAcquisition { err: GlobalStateAcquisitionError },
    NotWaitingForGlobalState,
    AlreadyComplete,
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::NotAcquiring { root_hash } => {
                write!(
                    f,
                    "currently not acquiring era validators from root hash {}",
                    root_hash
                )
            }
            Error::AlreadyComplete => {
                write!(f, "acquisition already complete",)
            }
            Error::RootHashMismatch { expected, actual } => {
                write!(
                    f,
                    "root hash mismatch; expected: {}, actual: {}",
                    expected, actual,
                )
            }
            Error::NotWaitingForGlobalState => {
                write!(f, "acquisition not waiting for global state",)
            }
            Error::GlobalStateAcquisition { err } => {
                write!(f, "global state acquisition failed with error {}", err)
            }

        }
    }
}

#[derive(Clone, DataSize, Debug)]
pub(super) enum EraValidatorsAcquisitionState {
    Empty,
    PendingFromStorage {
        state_root_hash: Digest,
    },
    PendingGlobalState {
        global_state_acquisition: GlobalStateAcquisition,
    },
    Complete {
        state_root_hash: Digest,
        era_validators: EraValidators,
    },
}

#[derive(Clone, DataSize, Debug)]
pub(super) struct EraValidatorsAcquisition {
    state: EraValidatorsAcquisitionState,
}

impl EraValidatorsAcquisition {
    pub(super) fn new() -> Self {
        Self {
            state: EraValidatorsAcquisitionState::Empty
        }
    }

    pub(super) fn new_pending_from_storage(state_root_hash: Digest) -> Self {
        Self {
            state: EraValidatorsAcquisitionState::PendingFromStorage { state_root_hash }
        }
    }

    pub(super) fn new_pending_global_state(global_state_acquisition: GlobalStateAcquisition) -> Self {
        Self {
            state: EraValidatorsAcquisitionState::PendingGlobalState { global_state_acquisition }
        }
    }

    pub(super) fn state_mut(&mut self) -> &mut EraValidatorsAcquisitionState {
        &mut self.state
    }

    pub(super) fn is_acquiring_from_root_hash(&self, root_hash: &Digest) -> bool {
        match &self.state {
            EraValidatorsAcquisitionState::Empty => false,
            EraValidatorsAcquisitionState::PendingGlobalState {
                global_state_acquisition,
            } => global_state_acquisition.root_hash() == root_hash,
            EraValidatorsAcquisitionState::Complete {
                state_root_hash, ..
            }
            | EraValidatorsAcquisitionState::PendingFromStorage { state_root_hash } => {
                state_root_hash == root_hash
            }
        }
    }

    pub(super) fn register_era_validators(
        &mut self,
        root_hash: &Digest,
        era_validators: EraValidators,
    ) -> Result<(), Error> {
        match &self.state {
            EraValidatorsAcquisitionState::Empty => Err(Error::NotAcquiring {
                root_hash: *root_hash,
            }),
            EraValidatorsAcquisitionState::Complete {
                state_root_hash, ..
            } => {
                if state_root_hash == root_hash {
                    Err(Error::AlreadyComplete)
                } else {
                    Err(Error::RootHashMismatch {
                        expected: *state_root_hash,
                        actual: *root_hash,
                    })
                }
            }
            EraValidatorsAcquisitionState::PendingFromStorage { state_root_hash } => {
                if state_root_hash != root_hash {
                    Err(Error::RootHashMismatch {
                        expected: *state_root_hash,
                        actual: *root_hash,
                    })
                } else {
                    self.state = EraValidatorsAcquisitionState::Complete {
                        state_root_hash: *state_root_hash,
                        era_validators,
                    };
                    Ok(())
                }
            }
            EraValidatorsAcquisitionState::PendingGlobalState {
                global_state_acquisition,
            } => {
                let state_root_hash = *global_state_acquisition.root_hash();
                if state_root_hash != *root_hash {
                    Err(Error::RootHashMismatch {
                        expected: state_root_hash,
                        actual: *root_hash,
                    })
                } else {
                    self.state = EraValidatorsAcquisitionState::Complete {
                        state_root_hash,
                        era_validators,
                    };
                    Ok(())
                }
            }
        }
    }

    pub(super) fn register_global_state_trie_or_chunk(
        &mut self,
        root_hash: Digest,
        trie_hash: Digest,
        trie_or_chunk: TrieOrChunk
    ) -> Result<(), Error> {
        match &mut self.state {
            EraValidatorsAcquisitionState::Empty => Err(Error::NotAcquiring {
                root_hash,
            }),
            EraValidatorsAcquisitionState::Complete {
                state_root_hash, ..
            } => {
                if *state_root_hash == root_hash {
                    Err(Error::AlreadyComplete)
                } else {
                    Err(Error::RootHashMismatch {
                        expected: *state_root_hash,
                        actual: root_hash,
                    })
                }
            }
            EraValidatorsAcquisitionState::PendingFromStorage { state_root_hash } => {
                if *state_root_hash != root_hash {
                    Err(Error::RootHashMismatch {
                        expected: *state_root_hash,
                        actual: root_hash,
                    })
                } else {
                    Err(Error::NotWaitingForGlobalState)
                }
            }
            EraValidatorsAcquisitionState::PendingGlobalState {
                global_state_acquisition,
            } => {
                let state_root_hash = *global_state_acquisition.root_hash();
                if state_root_hash != root_hash {
                    Err(Error::RootHashMismatch {
                        expected: state_root_hash,
                        actual: root_hash,
                    })
                } else {
                    global_state_acquisition.register_trie_or_chunk(trie_hash, trie_or_chunk).map_err(|err| Error::GlobalStateAcquisition { err })
                }
            }
        }
    }

    pub(super) fn register_global_state_put_trie_result(
        &mut self,
        root_hash: Digest,
        trie_hash: Digest,
        trie_raw: TrieRaw,
        put_trie_result: Result<Digest, engine_state::Error>,
    ) -> Result<(), Error> {
        match &mut self.state {
            EraValidatorsAcquisitionState::Empty => Err(Error::NotAcquiring {
                root_hash,
            }),
            EraValidatorsAcquisitionState::Complete {
                state_root_hash, ..
            } => {
                if *state_root_hash == root_hash {
                    Err(Error::AlreadyComplete)
                } else {
                    Err(Error::RootHashMismatch {
                        expected: *state_root_hash,
                        actual: root_hash,
                    })
                }
            }
            EraValidatorsAcquisitionState::PendingFromStorage { state_root_hash } => {
                if *state_root_hash != root_hash {
                    Err(Error::RootHashMismatch {
                        expected: *state_root_hash,
                        actual: root_hash,
                    })
                } else {
                    Err(Error::NotWaitingForGlobalState)
                }
            }
            EraValidatorsAcquisitionState::PendingGlobalState {
                global_state_acquisition,
            } => {
                let state_root_hash = *global_state_acquisition.root_hash();
                if state_root_hash != root_hash {
                    Err(Error::RootHashMismatch {
                        expected: state_root_hash,
                        actual: root_hash,
                    })
                } else {
                    match global_state_acquisition.register_put_trie(trie_hash, trie_raw, put_trie_result) {
                        Ok(()) => {
                            if global_state_acquisition.is_finished() {
                                self.state = EraValidatorsAcquisitionState::PendingFromStorage { state_root_hash };
                            }
                            Ok(())
                        }
                        Err(err) => Err(Error::GlobalStateAcquisition { err })
                    }
                }
            }
        }
    }
}
