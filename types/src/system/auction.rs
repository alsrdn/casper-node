//! Contains implementation of a Auction contract functionality.
mod bid;
mod constants;
mod delegator;
mod entry_points;
mod era_info;
mod error;
mod seigniorage_recipient;
mod unbonding_purse;
mod validator_bid;
mod withdraw_purse;

#[cfg(feature = "datasize")]
use datasize::DataSize;
#[cfg(feature = "json-schema")]
use schemars::JsonSchema;

#[cfg(any(all(feature = "std", feature = "testing"), test))]
use alloc::collections::btree_map::Entry;
#[cfg(any(all(feature = "std", feature = "testing"), test))]
use itertools::Itertools;

use alloc::{boxed::Box, collections::BTreeMap, vec::Vec};
use core::fmt::{Debug, Display, Formatter};

use rand::{
    distributions::{Distribution, Standard},
    Rng,
};
use serde::{Deserialize, Serialize};

pub use bid::{Bid, VESTING_SCHEDULE_LENGTH_MILLIS};
pub use constants::*;
pub use delegator::Delegator;
pub use entry_points::auction_entry_points;
pub use era_info::{EraInfo, SeigniorageAllocation};
pub use error::Error;
pub use seigniorage_recipient::SeigniorageRecipient;
pub use unbonding_purse::UnbondingPurse;
pub use validator_bid::ValidatorBid;
pub use withdraw_purse::WithdrawPurse;

#[cfg(any(feature = "testing", test))]
pub(crate) mod gens {
    pub use super::era_info::gens::*;
}

use crate::{
    account::{AccountHash, ACCOUNT_HASH_LENGTH},
    bytesrepr,
    bytesrepr::{FromBytes, ToBytes, U8_SERIALIZED_LENGTH},
    system::auction::bid::VestingSchedule,
    EraId, Key, KeyTag, PublicKey, URef, U512,
};

/// Representation of delegation rate of tokens. Range from 0..=100.
pub type DelegationRate = u8;

/// Validators mapped to their bids.
pub type ValidatorBids = BTreeMap<PublicKey, Box<ValidatorBid>>;

/// Weights of validators. "Weight" in this context means a sum of their stakes.
pub type ValidatorWeights = BTreeMap<PublicKey, U512>;

/// List of era validators
pub type EraValidators = BTreeMap<EraId, ValidatorWeights>;

/// Collection of seigniorage recipients.
pub type SeigniorageRecipients = BTreeMap<PublicKey, SeigniorageRecipient>;

/// Snapshot of `SeigniorageRecipients` for a given era.
pub type SeigniorageRecipientsSnapshot = BTreeMap<EraId, SeigniorageRecipients>;

/// Validators and delegators mapped to their unbonding purses.
pub type UnbondingPurses = BTreeMap<AccountHash, Vec<UnbondingPurse>>;

/// Validators and delegators mapped to their withdraw purses.
pub type WithdrawPurses = BTreeMap<AccountHash, Vec<WithdrawPurse>>;

/// Aggregated representation of validator and associated delegator bids.
pub type Staking = BTreeMap<PublicKey, (ValidatorBid, BTreeMap<PublicKey, Delegator>)>;

/// Serialization tag for BidAddr variants.
#[derive(Default, PartialOrd, Ord, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize)]
#[repr(u8)]
#[cfg_attr(feature = "datasize", derive(DataSize))]
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
pub enum BidAddrTag {
    /// BidAddr for legacy unified bid.
    Unified = 0,
    /// BidAddr for validator bid.
    #[default]
    Validator = 1,
    /// BidAddr for delegator bid.
    Delegator = 2,
}

/// Bid Address
#[derive(Default, PartialOrd, Ord, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize)]
#[cfg_attr(feature = "datasize", derive(DataSize))]
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
pub struct BidAddr(pub AccountHash, pub Option<AccountHash>, BidAddrTag);

impl BidAddr {
    /// The length in bytes of a [`BidAddr`] for a validator bid.
    pub const VALIDATOR_BID_ADDR_LENGTH: usize = ACCOUNT_HASH_LENGTH;

    /// The length in bytes of a [`BidAddr`] for a delegator bid.
    pub const DELEGATOR_BID_ADDR_LENGTH: usize = ACCOUNT_HASH_LENGTH * 2;

    /// Constructs a new [`BidAddr`] instance from a validator's [`AccountHash`].
    pub const fn new_validator_addr(validator: [u8; ACCOUNT_HASH_LENGTH]) -> Self {
        BidAddr(AccountHash::new(validator), None, BidAddrTag::Validator)
    }

    /// Constructs a new [`BidAddr`] instance from the [`AccountHash`] pair of a validator
    /// and a delegator.
    pub const fn new_delegator_addr(
        pair: ([u8; ACCOUNT_HASH_LENGTH], [u8; ACCOUNT_HASH_LENGTH]),
    ) -> Self {
        BidAddr(
            AccountHash::new(pair.0),
            Some(AccountHash::new(pair.1)),
            BidAddrTag::Delegator,
        )
    }

    /// Create a new instance of a [`BidAddr`].
    pub fn new_from_public_keys(
        validator: &PublicKey,
        maybe_delegator: Option<&PublicKey>,
    ) -> Self {
        if let Some(delegator) = maybe_delegator {
            BidAddr(
                AccountHash::from(validator),
                Some(AccountHash::from(delegator)),
                BidAddrTag::Delegator,
            )
        } else {
            BidAddr(AccountHash::from(validator), None, BidAddrTag::Validator)
        }
    }

    #[allow(missing_docs)]
    pub fn legacy(account_hash: AccountHash) -> Self {
        BidAddr(account_hash, None, BidAddrTag::Unified)
    }

    /// Returns the common prefix of all delegators to the cited validator.
    pub fn delegators_prefix(&self) -> Result<Vec<u8>, Error> {
        match self.0.to_bytes() {
            Ok(bytes) => {
                let mut ret = vec![];
                ret.push(KeyTag::Bid as u8);
                ret.push(BidAddrTag::Delegator as u8);
                ret.extend(bytes);
                Ok(ret)
            }
            Err(_) => Err(Error::Serialization),
        }
    }

    /// Validator account hash.
    pub fn validator_account_hash(&self) -> AccountHash {
        self.0
    }

    /// Delegator account hash or none.
    pub fn maybe_delegator_account_hash(&self) -> Option<AccountHash> {
        self.1
    }

    /// If true, this instance is the key for a delegator bid record.
    /// Else, it is the key for a validator bid record.
    pub fn is_delegator_bid_addr(&self) -> bool {
        self.1.is_some()
    }

    /// How long will be the serialized value for this instance.
    pub fn serialized_length(&self) -> usize {
        let validator_len = ToBytes::serialized_length(&self.0) + 1;
        if let Some(delegator) = self.1 {
            let delegator_len = ToBytes::serialized_length(&delegator);
            delegator_len + validator_len
        } else {
            validator_len
        }
    }

    /// Returns the BiddAddrTag of this instance.
    pub fn tag(&self) -> BidAddrTag {
        self.2
    }
}

impl ToBytes for BidAddr {
    fn to_bytes(&self) -> Result<Vec<u8>, bytesrepr::Error> {
        let mut buffer = bytesrepr::allocate_buffer(self)?;
        buffer.push(self.tag() as u8);
        buffer.append(&mut self.0.to_bytes()?);
        if let Some(delegator) = self.1 {
            buffer.append(&mut delegator.to_bytes()?);
        }
        Ok(buffer)
    }

    fn serialized_length(&self) -> usize {
        self.serialized_length()
    }
}

impl FromBytes for BidAddr {
    fn from_bytes(bytes: &[u8]) -> Result<(Self, &[u8]), bytesrepr::Error> {
        let (tag, remainder): (u8, &[u8]) = FromBytes::from_bytes(bytes)?;
        match tag {
            tag if tag == BidAddrTag::Unified as u8 => {
                AccountHash::from_bytes(remainder).map(|(account_hash, remainder)| {
                    (BidAddr(account_hash, None, BidAddrTag::Unified), remainder)
                })
            }
            tag if tag == BidAddrTag::Validator as u8 => {
                AccountHash::from_bytes(remainder).map(|(account_hash, remainder)| {
                    (
                        BidAddr(account_hash, None, BidAddrTag::Validator),
                        remainder,
                    )
                })
            }
            tag if tag == BidAddrTag::Delegator as u8 => {
                let (validator, remainder) = AccountHash::from_bytes(remainder)?;
                let (delegator, remainder) = AccountHash::from_bytes(remainder)?;
                Ok((
                    BidAddr(validator, Some(delegator), BidAddrTag::Delegator),
                    remainder,
                ))
            }
            _ => Err(bytesrepr::Error::Formatting),
        }
    }
}

impl From<BidAddr> for Key {
    fn from(bid_addr: BidAddr) -> Self {
        Key::Bid(bid_addr)
    }
}

impl From<AccountHash> for BidAddr {
    fn from(account_hash: AccountHash) -> Self {
        BidAddr(account_hash, None, BidAddrTag::Validator)
    }
}

impl From<PublicKey> for BidAddr {
    fn from(public_key: PublicKey) -> Self {
        BidAddr(public_key.to_account_hash(), None, BidAddrTag::Validator)
    }
}

impl Display for BidAddr {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        match self.1 {
            Some(delegator) => write!(f, "{}{}", self.0, delegator),
            None => write!(f, "{}", self.0),
        }
    }
}

impl Debug for BidAddr {
    fn fmt(&self, f: &mut Formatter) -> core::fmt::Result {
        match self.1 {
            Some(delegator) => write!(f, "Validator{}-Delegator{}", self.0, delegator),
            None => write!(f, "{:?}", self.0),
        }
    }
}

impl Distribution<BidAddr> for Standard {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> BidAddr {
        BidAddr(AccountHash::new(rng.gen()), None, BidAddrTag::Validator)
    }
}

#[allow(clippy::large_enum_variant)]
#[repr(u8)]
enum BidKindTag {
    Unified = 0,
    Validator = 1,
    Delegator = 2,
}

/// Auction bid variants.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
#[cfg_attr(feature = "datasize", derive(DataSize))]
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
pub enum BidKind {
    /// A unified record indexed on validator data, with an embedded collection of all delegator
    /// bids assigned to that validator. The Unified variant is for legacy retrograde support, new
    /// instances will not be created going forward.
    Unified(Box<Bid>),
    /// A bid record containing only validator data.
    Validator(Box<ValidatorBid>),
    /// A bid record containing only delegator data.
    Delegator(Box<Delegator>),
}

impl BidKind {
    /// Returns validator public key.
    pub fn validator_public_key(&self) -> PublicKey {
        match self {
            BidKind::Unified(bid) => bid.validator_public_key().clone(),
            BidKind::Validator(validator_bid) => validator_bid.validator_public_key().clone(),
            BidKind::Delegator(delegator_bid) => delegator_bid.validator_public_key().clone(),
        }
    }

    /// Is this instance a unified bid?.
    pub fn is_unified(&self) -> bool {
        match self {
            BidKind::Unified(_) => true,
            BidKind::Validator(_) | BidKind::Delegator(_) => false,
        }
    }

    /// Is this instance a validator bid?.
    pub fn is_validator(&self) -> bool {
        match self {
            BidKind::Validator(_) => true,
            BidKind::Unified(_) | BidKind::Delegator(_) => false,
        }
    }

    /// Is this instance a delegator bid?.
    pub fn is_delegator(&self) -> bool {
        match self {
            BidKind::Delegator(_) => true,
            BidKind::Unified(_) | BidKind::Validator(_) => false,
        }
    }

    /// The staked amount.
    pub fn staked_amount(&self) -> U512 {
        match self {
            BidKind::Unified(bid) => *bid.staked_amount(),
            BidKind::Validator(validator_bid) => validator_bid.staked_amount(),
            BidKind::Delegator(delegator) => delegator.staked_amount(),
        }
    }

    /// The bonding purse.
    pub fn bonding_purse(&self) -> URef {
        match self {
            BidKind::Unified(bid) => *bid.bonding_purse(),
            BidKind::Validator(validator_bid) => *validator_bid.bonding_purse(),
            BidKind::Delegator(delegator) => *delegator.bonding_purse(),
        }
    }

    /// The delegator public key, if relevant.
    pub fn delegator_public_key(&self) -> Option<PublicKey> {
        match self {
            BidKind::Unified(_) | BidKind::Validator(_) => None,
            BidKind::Delegator(delegator) => Some(delegator.delegator_public_key().clone()),
        }
    }

    /// Is this bid inactive?
    pub fn inactive(&self) -> bool {
        match self {
            BidKind::Unified(bid) => bid.inactive(),
            BidKind::Validator(validator_bid) => validator_bid.inactive(),
            BidKind::Delegator(delegator) => delegator.staked_amount().is_zero(),
        }
    }

    /// Checks if a bid is still locked under a vesting schedule.
    ///
    /// Returns true if a timestamp falls below the initial lockup period + 91 days release
    /// schedule, otherwise false.
    pub fn is_locked(&self, timestamp_millis: u64) -> bool {
        match self {
            BidKind::Unified(bid) => bid.is_locked(timestamp_millis),
            BidKind::Validator(validator_bid) => validator_bid.is_locked(timestamp_millis),
            BidKind::Delegator(delegator) => delegator.is_locked(timestamp_millis),
        }
    }

    /// Checks if a bid is still locked under a vesting schedule.
    ///
    /// Returns true if a timestamp falls below the initial lockup period + 91 days release
    /// schedule, otherwise false.
    pub fn is_locked_with_vesting_schedule(
        &self,
        timestamp_millis: u64,
        vesting_schedule_period_millis: u64,
    ) -> bool {
        match self {
            BidKind::Unified(bid) => bid
                .is_locked_with_vesting_schedule(timestamp_millis, vesting_schedule_period_millis),
            BidKind::Validator(validator_bid) => validator_bid
                .is_locked_with_vesting_schedule(timestamp_millis, vesting_schedule_period_millis),
            BidKind::Delegator(delegator) => delegator
                .is_locked_with_vesting_schedule(timestamp_millis, vesting_schedule_period_millis),
        }
    }

    /// Returns a reference to the vesting schedule of the provided bid.  `None` if a non-genesis
    /// validator.
    pub fn vesting_schedule(&self) -> Option<&VestingSchedule> {
        match self {
            BidKind::Unified(bid) => bid.vesting_schedule(),
            BidKind::Validator(validator_bid) => validator_bid.vesting_schedule(),
            BidKind::Delegator(delegator) => delegator.vesting_schedule(),
        }
    }

    fn tag(&self) -> BidKindTag {
        match self {
            BidKind::Unified(_) => BidKindTag::Unified,
            BidKind::Validator(_) => BidKindTag::Validator,
            BidKind::Delegator(_) => BidKindTag::Delegator,
        }
    }
}

impl ToBytes for BidKind {
    fn to_bytes(&self) -> Result<Vec<u8>, bytesrepr::Error> {
        let mut result = bytesrepr::allocate_buffer(self)?;
        let (tag, mut serialized_data) = match self {
            BidKind::Unified(bid) => (BidKindTag::Unified, bid.to_bytes()?),
            BidKind::Validator(validator_bid) => (BidKindTag::Validator, validator_bid.to_bytes()?),
            BidKind::Delegator(delegator_bid) => (BidKindTag::Delegator, delegator_bid.to_bytes()?),
        };
        result.push(tag as u8);
        result.append(&mut serialized_data);
        Ok(result)
    }

    fn serialized_length(&self) -> usize {
        U8_SERIALIZED_LENGTH
            + match self {
                BidKind::Unified(bid) => bid.serialized_length(),
                BidKind::Validator(validator_bid) => validator_bid.serialized_length(),
                BidKind::Delegator(delegator_bid) => delegator_bid.serialized_length(),
            }
    }

    fn write_bytes(&self, writer: &mut Vec<u8>) -> Result<(), bytesrepr::Error> {
        writer.push(self.tag() as u8);
        match self {
            //StoredValue::CLValue(cl_value) => cl_value.write_bytes(writer)?,
            BidKind::Unified(bid) => bid.write_bytes(writer)?,
            BidKind::Validator(validator_bid) => validator_bid.write_bytes(writer)?,
            BidKind::Delegator(delegator_bid) => delegator_bid.write_bytes(writer)?,
        };
        Ok(())
    }
}

impl FromBytes for BidKind {
    fn from_bytes(bytes: &[u8]) -> Result<(Self, &[u8]), bytesrepr::Error> {
        let (tag, remainder): (u8, &[u8]) = FromBytes::from_bytes(bytes)?;
        match tag {
            tag if tag == BidKindTag::Unified as u8 => Bid::from_bytes(remainder)
                .map(|(bid, remainder)| (BidKind::Unified(Box::new(bid)), remainder)),
            tag if tag == BidKindTag::Validator as u8 => {
                ValidatorBid::from_bytes(remainder).map(|(validator_bid, remainder)| {
                    (BidKind::Validator(Box::new(validator_bid)), remainder)
                })
            }
            tag if tag == BidKindTag::Delegator as u8 => {
                Delegator::from_bytes(remainder).map(|(delegator_bid, remainder)| {
                    (BidKind::Delegator(Box::new(delegator_bid)), remainder)
                })
            }
            _ => Err(bytesrepr::Error::Formatting),
        }
    }
}

/// Utils for working with a vector of BidKind.
#[cfg(any(all(feature = "std", feature = "testing"), test))]
pub trait BidsExt {
    /// Returns ValidatorBid matching public_key, if present.
    fn validator_bid(&self, public_key: &PublicKey) -> Option<ValidatorBid>;

    /// Returns total validator stake, if present.
    fn validator_total_stake(&self, public_key: &PublicKey) -> Option<U512>;

    /// Returns Delegator entries matching validator public key, if present.
    fn delegators_by_validator_public_key(&self, public_key: &PublicKey) -> Option<Vec<Delegator>>;

    /// Returns Delegator entry by public keys, if present.
    fn delegator_by_public_keys(
        &self,
        validator_public_key: &PublicKey,
        delegator_public_key: &PublicKey,
    ) -> Option<Delegator>;

    /// Returns true if containing any elements matching the provided validator public key.
    fn contains_validator_public_key(&self, public_key: &PublicKey) -> bool;

    /// Removes any items with a public key matching the provided validator public key.
    fn remove_by_validator_public_key(&mut self, public_key: &PublicKey);

    /// Creates a map of Validator public keys to associated Delegator public keys.
    fn public_key_map(&self) -> BTreeMap<PublicKey, Vec<PublicKey>>;
}

#[cfg(any(all(feature = "std", feature = "testing"), test))]
impl BidsExt for Vec<BidKind> {
    fn validator_bid(&self, public_key: &PublicKey) -> Option<ValidatorBid> {
        if let BidKind::Validator(validator_bid) = self
            .iter()
            .find(|x| x.is_validator() && &x.validator_public_key() == public_key)?
        {
            Some(*validator_bid.clone())
        } else {
            None
        }
    }

    fn validator_total_stake(&self, public_key: &PublicKey) -> Option<U512> {
        if let Some(validator_bid) = self.validator_bid(public_key) {
            let delegator_stake = {
                match self.delegators_by_validator_public_key(validator_bid.validator_public_key())
                {
                    None => U512::zero(),
                    Some(delegators) => delegators.iter().map(|x| x.staked_amount()).sum(),
                }
            };
            return Some(validator_bid.staked_amount() + delegator_stake);
        }

        if let BidKind::Unified(bid) = self
            .iter()
            .find(|x| x.is_validator() && &x.validator_public_key() == public_key)?
        {
            return Some(*bid.staked_amount());
        }

        None
    }

    fn delegators_by_validator_public_key(&self, public_key: &PublicKey) -> Option<Vec<Delegator>> {
        let mut ret = vec![];
        for delegator in self
            .iter()
            .filter(|x| x.is_delegator() && &x.validator_public_key() == public_key)
        {
            if let BidKind::Delegator(delegator) = delegator {
                ret.push(*delegator.clone());
            }
        }

        if ret.is_empty() {
            None
        } else {
            Some(ret)
        }
    }

    fn delegator_by_public_keys(
        &self,
        validator_public_key: &PublicKey,
        delegator_public_key: &PublicKey,
    ) -> Option<Delegator> {
        if let BidKind::Delegator(delegator) = self.iter().find(|x| {
            &x.validator_public_key() == validator_public_key
                && x.delegator_public_key() == Some(delegator_public_key.clone())
        })? {
            Some(*delegator.clone())
        } else {
            None
        }
    }

    fn contains_validator_public_key(&self, public_key: &PublicKey) -> bool {
        self.iter().any(|x| &x.validator_public_key() == public_key)
    }

    fn remove_by_validator_public_key(&mut self, public_key: &PublicKey) {
        self.retain(|x| &x.validator_public_key() != public_key)
    }

    fn public_key_map(&self) -> BTreeMap<PublicKey, Vec<PublicKey>> {
        let mut ret = BTreeMap::new();
        let validators = self
            .iter()
            .filter(|x| x.is_validator())
            .cloned()
            .collect_vec();
        for bid_kind in validators {
            ret.insert(bid_kind.validator_public_key().clone(), vec![]);
        }
        let delegators = self
            .iter()
            .filter(|x| x.is_delegator())
            .cloned()
            .collect_vec();
        for bid_kind in delegators {
            if let BidKind::Delegator(delegator) = bid_kind {
                match ret.entry(delegator.validator_public_key().clone()) {
                    Entry::Vacant(ve) => {
                        ve.insert(vec![delegator.delegator_public_key().clone()]);
                    }
                    Entry::Occupied(mut oe) => {
                        let delegators = oe.get_mut();
                        delegators.push(delegator.delegator_public_key().clone())
                    }
                }
            }
        }
        let unified = self
            .iter()
            .filter(|x| x.is_unified())
            .cloned()
            .collect_vec();
        for bid_kind in unified {
            if let BidKind::Unified(unified) = bid_kind {
                let delegators = unified
                    .delegators()
                    .iter()
                    .map(|(_, y)| y.delegator_public_key().clone())
                    .collect();
                ret.insert(unified.validator_public_key().clone(), delegators);
            }
        }
        ret
    }
}

#[cfg(test)]
mod prop_test_validator_addr {
    use proptest::prelude::*;

    use crate::{bytesrepr, gens};

    proptest! {
        #[test]
        fn test_value_bid_addr_validator(validator_bid_addr in gens::bid_addr_validator_arb()) {
            bytesrepr::test_serialization_roundtrip(&validator_bid_addr);
        }
    }
}

#[cfg(test)]
mod prop_test_delegator_addr {
    use proptest::prelude::*;

    use crate::{bytesrepr, gens};

    proptest! {
        #[test]
        fn test_value_bid_addr_delegator(delegator_bid_addr in gens::bid_addr_delegator_arb()) {
            bytesrepr::test_serialization_roundtrip(&delegator_bid_addr);
        }
    }
}
