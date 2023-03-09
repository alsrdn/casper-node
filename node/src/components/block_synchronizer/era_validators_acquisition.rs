use std::fmt::{Display, Formatter};

use casper_hashing::Digest;
use casper_types::system::auction::EraValidators;
use datasize::DataSize;

use super::global_state_acquisition::GlobalStateAcquisition;

#[derive(Clone, Copy, PartialEq, Eq, DataSize, Debug)]
pub(crate) enum Error {
    NotAcquiring { root_hash: Digest },
    RootHashMismatch { expected: Digest, actual: Digest },
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
        }
    }
}

#[derive(Clone, DataSize, Debug)]
pub(super) enum EraValidatorsAcquisition {
    Empty,
    PendingFromStorage {
        state_root_hash: Digest,
    },
    PendingGlobalState {
        global_state_acquisition: Box<GlobalStateAcquisition>,
    },
    Complete {
        state_root_hash: Digest,
        era_validators: EraValidators,
    },
}

impl EraValidatorsAcquisition {
    pub(super) fn new() -> Self {
        Self::Empty
    }

    pub(super) fn new_pending_from_storage(state_root_hash: Digest) -> Self {
        Self::PendingFromStorage { state_root_hash }
    }

    pub(super) fn is_acquiring_from_root_hash(&self, root_hash: &Digest) -> bool {
        match self {
            EraValidatorsAcquisition::Empty => false,
            EraValidatorsAcquisition::PendingGlobalState {
                global_state_acquisition,
            } => global_state_acquisition.root_hash() == root_hash,
            EraValidatorsAcquisition::Complete {
                state_root_hash, ..
            }
            | EraValidatorsAcquisition::PendingFromStorage { state_root_hash } => {
                state_root_hash == root_hash
            }
        }
    }

    pub(super) fn register_era_validators(
        self,
        root_hash: &Digest,
        era_validators: EraValidators,
    ) -> Result<Self, Error> {
        match self {
            EraValidatorsAcquisition::Empty => Err(Error::NotAcquiring {
                root_hash: *root_hash,
            }),
            EraValidatorsAcquisition::Complete {
                state_root_hash, ..
            } => {
                if state_root_hash == *root_hash {
                    Err(Error::AlreadyComplete)
                } else {
                    Err(Error::RootHashMismatch {
                        expected: state_root_hash,
                        actual: *root_hash,
                    })
                }
            }
            EraValidatorsAcquisition::PendingFromStorage { state_root_hash } => {
                if state_root_hash != *root_hash {
                    Err(Error::RootHashMismatch {
                        expected: state_root_hash,
                        actual: *root_hash,
                    })
                } else {
                    Ok(EraValidatorsAcquisition::Complete {
                        state_root_hash,
                        era_validators,
                    })
                }
            }
            EraValidatorsAcquisition::PendingGlobalState {
                global_state_acquisition,
            } => {
                let state_root_hash = *global_state_acquisition.root_hash();
                if state_root_hash != *root_hash {
                    Err(Error::RootHashMismatch {
                        expected: state_root_hash,
                        actual: *root_hash,
                    })
                } else {
                    Ok(EraValidatorsAcquisition::Complete {
                        state_root_hash,
                        era_validators,
                    })
                }
            }
        }
    }
}
