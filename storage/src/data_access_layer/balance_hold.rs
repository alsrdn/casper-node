use crate::{data_access_layer::BalanceIdentifier, tracking_copy::TrackingCopyError};
use casper_types::{
    account::AccountHash,
    execution::Effects,
    system::mint::{BalanceHoldAddr, BalanceHoldAddrTag},
    BlockTime, Digest, HoldsEpoch, ProtocolVersion, U512,
};
use std::fmt::{Display, Formatter};
use thiserror::Error;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
pub enum BalanceHoldKind {
    #[default]
    All,
    Tag(BalanceHoldAddrTag),
}

impl BalanceHoldKind {
    /// Returns true of imputed tag applies to instance.
    pub fn matches(&self, balance_hold_addr_tag: BalanceHoldAddrTag) -> bool {
        match self {
            BalanceHoldKind::All => true,
            BalanceHoldKind::Tag(tag) => tag == &balance_hold_addr_tag,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BalanceHoldMode {
    Hold {
        identifier: BalanceIdentifier,
        hold_amount: U512,
        holds_epoch: HoldsEpoch,
        insufficient_handling: InsufficientBalanceHandling,
    },
    Clear {
        identifier: BalanceIdentifier,
        holds_epoch: HoldsEpoch,
    },
}

impl Default for BalanceHoldMode {
    fn default() -> Self {
        BalanceHoldMode::Hold {
            insufficient_handling: InsufficientBalanceHandling::HoldRemaining,
            hold_amount: U512::zero(),
            identifier: BalanceIdentifier::Account(AccountHash::default()),
            holds_epoch: HoldsEpoch::default(),
        }
    }
}

/// How to handle available balance is less than hold amount?
#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
pub enum InsufficientBalanceHandling {
    /// Hold however much balance remains.
    #[default]
    HoldRemaining,
    /// No operation. Aka, do not place a hold.
    Noop,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BalanceHoldRequest {
    state_hash: Digest,
    protocol_version: ProtocolVersion,
    block_time: BlockTime,
    hold_kind: BalanceHoldKind,
    hold_mode: BalanceHoldMode,
}

impl BalanceHoldRequest {
    /// Creates a new [`BalanceHoldRequest`] for adding a gas balance hold.
    #[allow(clippy::too_many_arguments)]
    pub fn new_gas_hold(
        state_hash: Digest,
        protocol_version: ProtocolVersion,
        identifier: BalanceIdentifier,
        hold_amount: U512,
        block_time: BlockTime,
        holds_epoch: HoldsEpoch,
        insufficient_handling: InsufficientBalanceHandling,
    ) -> Self {
        let hold_kind = BalanceHoldKind::Tag(BalanceHoldAddrTag::Gas);
        let hold_mode = BalanceHoldMode::Hold {
            identifier,
            hold_amount,
            holds_epoch,
            insufficient_handling,
        };
        BalanceHoldRequest {
            state_hash,
            protocol_version,
            block_time,
            hold_kind,
            hold_mode,
        }
    }

    /// Creates a new [`BalanceHoldRequest`] for adding a processing balance hold.
    #[allow(clippy::too_many_arguments)]
    pub fn new_processing_hold(
        state_hash: Digest,
        protocol_version: ProtocolVersion,
        identifier: BalanceIdentifier,
        hold_amount: U512,
        block_time: BlockTime,
        holds_epoch: HoldsEpoch,
        insufficient_handling: InsufficientBalanceHandling,
    ) -> Self {
        let hold_kind = BalanceHoldKind::Tag(BalanceHoldAddrTag::Processing);
        let hold_mode = BalanceHoldMode::Hold {
            identifier,
            hold_amount,
            holds_epoch,
            insufficient_handling,
        };
        BalanceHoldRequest {
            state_hash,
            protocol_version,
            block_time,
            hold_kind,
            hold_mode,
        }
    }

    /// Creates a new [`BalanceHoldRequest`] for clearing holds.
    pub fn new_clear(
        state_hash: Digest,
        protocol_version: ProtocolVersion,
        block_time: BlockTime,
        hold_kind: BalanceHoldKind,
        identifier: BalanceIdentifier,
        holds_epoch: HoldsEpoch,
    ) -> Self {
        let hold_mode = BalanceHoldMode::Clear {
            identifier,
            holds_epoch,
        };
        BalanceHoldRequest {
            state_hash,
            protocol_version,
            block_time,
            hold_kind,
            hold_mode,
        }
    }

    /// Returns a state hash.
    pub fn state_hash(&self) -> Digest {
        self.state_hash
    }

    /// Protocol version.
    pub fn protocol_version(&self) -> ProtocolVersion {
        self.protocol_version
    }

    /// Block time.
    pub fn block_time(&self) -> BlockTime {
        self.block_time
    }

    /// Balance hold kind.
    pub fn balance_hold_kind(&self) -> BalanceHoldKind {
        self.hold_kind
    }

    /// Balance hold mode.
    pub fn balance_hold_mode(&self) -> BalanceHoldMode {
        self.hold_mode.clone()
    }
}

/// Possible balance hold errors.
#[derive(Error, Debug, Clone)]
#[non_exhaustive]
pub enum BalanceHoldError {
    TrackingCopy(TrackingCopyError),
    InsufficientBalance { remaining_balance: U512 },
    UnexpectedWildcardVariant, // programmer error
}

impl Display for BalanceHoldError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BalanceHoldError::TrackingCopy(err) => {
                write!(f, "TrackingCopy: {}", err)
            }
            BalanceHoldError::InsufficientBalance { remaining_balance } => {
                write!(f, "InsufficientBalance: {}", remaining_balance)
            }
            BalanceHoldError::UnexpectedWildcardVariant => {
                write!(
                    f,
                    "UnexpectedWildcardVariant: unsupported use of BalanceHoldKind::All"
                )
            }
        }
    }
}

/// Result enum that represents all possible outcomes of a balance hold request.
#[derive(Debug)]
pub enum BalanceHoldResult {
    /// Returned if a passed state root hash is not found.
    RootNotFound,
    /// Balance hold successfully placed.
    Success {
        /// Hold address.
        hold_addr: BalanceHoldAddr,
        /// Purse total balance.
        total_balance: Box<U512>,
        /// Purse available balance after hold placed.
        available_balance: Box<U512>,
        /// How much were we supposed to hold?
        hold: Box<U512>,
        /// How much did we actually hold?
        held: Box<U512>,
        /// Effects of bidding interaction.
        effects: Box<Effects>,
    },
    /// Failed to place balance hold.
    Failure(BalanceHoldError),
}

impl BalanceHoldResult {
    pub fn success(
        hold_addr: BalanceHoldAddr,
        total_balance: U512,
        available_balance: U512,
        hold: U512,
        held: U512,
        effects: Effects,
    ) -> Self {
        BalanceHoldResult::Success {
            hold_addr,
            total_balance: Box::new(total_balance),
            available_balance: Box::new(available_balance),
            hold: Box::new(hold),
            held: Box::new(held),
            effects: Box::new(effects),
        }
    }

    /// Hold address, if any.
    pub fn hold_addr(&self) -> Option<BalanceHoldAddr> {
        match self {
            BalanceHoldResult::RootNotFound | BalanceHoldResult::Failure(_) => None,
            BalanceHoldResult::Success { hold_addr, .. } => Some(*hold_addr),
        }
    }

    /// Was the hold fully covered?
    pub fn is_fully_covered(&self) -> bool {
        match self {
            BalanceHoldResult::RootNotFound | BalanceHoldResult::Failure(_) => false,
            BalanceHoldResult::Success { hold, held, .. } => hold == held,
        }
    }

    /// Was the hold successful?
    pub fn is_success(&self) -> bool {
        matches!(self, BalanceHoldResult::Success { .. })
    }

    /// Was the root not found?
    pub fn is_root_not_found(&self) -> bool {
        matches!(self, BalanceHoldResult::RootNotFound)
    }

    /// The effects, if any.
    pub fn effects(&self) -> Effects {
        match self {
            BalanceHoldResult::RootNotFound | BalanceHoldResult::Failure(_) => Effects::new(),
            BalanceHoldResult::Success { effects, .. } => *effects.clone(),
        }
    }

    pub fn error_message(&self) -> String {
        match self {
            BalanceHoldResult::Success { hold, held, .. } => {
                if hold == held {
                    String::default()
                } else {
                    format!(
                        "insufficient balance to cover hold amount: {}, held remaining amount: {}",
                        hold, held
                    )
                }
            }
            BalanceHoldResult::RootNotFound => "root not found".to_string(),
            BalanceHoldResult::Failure(bhe) => {
                format!("{:?}", bhe)
            }
        }
    }
}
