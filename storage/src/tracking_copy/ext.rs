use std::{
    collections::{btree_map::Entry, BTreeMap, BTreeSet},
    convert::TryInto,
};

use crate::{
    data_access_layer::balance::BalanceHoldsWithProof,
    global_state::{error::Error as GlobalStateError, state::StateReader},
};
use casper_types::{
    account::AccountHash,
    addressable_entity::NamedKeys,
    global_state::TrieMerkleProof,
    system::mint::{BalanceHoldAddr, BalanceHoldAddrTag},
    BlockTime, ByteCode, ByteCodeAddr, ByteCodeHash, CLValue, ChecksumRegistry, EntityAddr,
    HoldsEpoch, Key, KeyTag, Motes, Package, PackageHash, StoredValue, StoredValueTypeMismatch,
    SystemEntityRegistry, URef, URefAddr, U512,
};

use crate::tracking_copy::{TrackingCopy, TrackingCopyError};

/// Higher-level operations on the state via a `TrackingCopy`.
pub trait TrackingCopyExt<R> {
    /// The type for the returned errors.
    type Error;

    /// Reads the entity key for a given account hash.
    fn read_account_key(&mut self, account_hash: AccountHash) -> Result<Key, Self::Error>;

    /// Gets the purse balance key for a given purse.
    fn get_purse_balance_key(&self, purse_key: Key) -> Result<Key, Self::Error>;

    /// Returns the available balance, considering any holds from holds_epoch to now.
    /// If holds_epoch is none, available balance == total balance.
    fn get_available_balance(
        &self,
        balance_key: Key,
        holds_epoch: HoldsEpoch,
    ) -> Result<Motes, Self::Error>;

    /// Gets the purse balance key for a given purse and provides a Merkle proof.
    fn get_purse_balance_key_with_proof(
        &self,
        purse_key: Key,
    ) -> Result<(Key, TrieMerkleProof<Key, StoredValue>), Self::Error>;

    /// Gets the balance at a given balance key and provides a Merkle proof.
    fn get_total_balance_with_proof(
        &self,
        balance_key: Key,
    ) -> Result<(U512, TrieMerkleProof<Key, StoredValue>), Self::Error>;

    /// Clear expired balance holds.
    fn clear_expired_balance_holds(
        &mut self,
        purse_addr: URefAddr,
        tag: BalanceHoldAddrTag,
        holds_epoch: HoldsEpoch,
    ) -> Result<(), Self::Error>;

    /// Gets the balance holds for a given balance, with Merkle proofs.
    fn get_balance_holds_with_proof(
        &self,
        purse_addr: URefAddr,
        holds_epoch: HoldsEpoch,
    ) -> Result<BTreeMap<BlockTime, BalanceHoldsWithProof>, Self::Error>;

    /// Returns the collection of named keys for a given AddressableEntity.
    fn get_named_keys(&mut self, entity_addr: EntityAddr) -> Result<NamedKeys, Self::Error>;

    /// Gets a package by hash.
    fn get_package(&mut self, package_hash: PackageHash) -> Result<Package, Self::Error>;

    /// Gets the system entity registry.
    fn get_system_entity_registry(&mut self) -> Result<SystemEntityRegistry, Self::Error>;

    /// Gets the system checksum registry.
    fn get_checksum_registry(&mut self) -> Result<Option<ChecksumRegistry>, Self::Error>;

    /// Gets byte code by hash.
    fn get_byte_code(&mut self, byte_code_hash: ByteCodeHash) -> Result<ByteCode, Self::Error>;
}

impl<R> TrackingCopyExt<R> for TrackingCopy<R>
where
    R: StateReader<Key, StoredValue, Error = GlobalStateError>,
{
    type Error = TrackingCopyError;

    fn read_account_key(&mut self, account_hash: AccountHash) -> Result<Key, Self::Error> {
        let account_key = Key::Account(account_hash);
        match self.read(&account_key)? {
            Some(StoredValue::CLValue(cl_value)) => Ok(CLValue::into_t(cl_value)?),
            Some(other) => Err(TrackingCopyError::TypeMismatch(
                StoredValueTypeMismatch::new("Account".to_string(), other.type_name()),
            )),
            None => Err(TrackingCopyError::KeyNotFound(account_key)),
        }
    }

    fn get_purse_balance_key(&self, purse_key: Key) -> Result<Key, Self::Error> {
        let balance_key: URef = purse_key
            .into_uref()
            .ok_or(TrackingCopyError::UnexpectedKeyVariant(purse_key))?;
        Ok(Key::Balance(balance_key.addr()))
    }

    fn get_purse_balance_key_with_proof(
        &self,
        purse_key: Key,
    ) -> Result<(Key, TrieMerkleProof<Key, StoredValue>), Self::Error> {
        let balance_key: Key = purse_key
            .uref_to_hash()
            .ok_or(TrackingCopyError::UnexpectedKeyVariant(purse_key))?;
        let proof: TrieMerkleProof<Key, StoredValue> = self
            .read_with_proof(&balance_key)?
            .ok_or(TrackingCopyError::KeyNotFound(purse_key))?;
        let stored_value_ref: &StoredValue = proof.value();
        let cl_value: CLValue = stored_value_ref
            .to_owned()
            .try_into()
            .map_err(TrackingCopyError::TypeMismatch)?;
        let balance_key: Key = cl_value.into_t()?;
        Ok((balance_key, proof))
    }

    fn get_total_balance_with_proof(
        &self,
        key: Key,
    ) -> Result<(U512, TrieMerkleProof<Key, StoredValue>), Self::Error> {
        if let Key::Balance(_) = key {
            let proof: TrieMerkleProof<Key, StoredValue> = self
                .read_with_proof(&key.normalize())?
                .ok_or(TrackingCopyError::KeyNotFound(key))?;
            let cl_value: CLValue = proof
                .value()
                .to_owned()
                .try_into()
                .map_err(TrackingCopyError::TypeMismatch)?;
            let balance = cl_value.into_t()?;
            Ok((balance, proof))
        } else {
            Err(Self::Error::UnexpectedKeyVariant(key))
        }
    }

    fn get_available_balance(
        &self,
        key: Key,
        holds_epoch: HoldsEpoch,
    ) -> Result<Motes, Self::Error> {
        let key = {
            if let Key::URef(uref) = key {
                Key::Balance(uref.addr())
            } else {
                key
            }
        };

        if let Key::Balance(purse_addr) = key {
            let stored_value: StoredValue = self
                .read(&key)?
                .ok_or(TrackingCopyError::KeyNotFound(key))?;
            let cl_value: CLValue = stored_value
                .try_into()
                .map_err(TrackingCopyError::TypeMismatch)?;
            let total_balance = cl_value.into_t::<U512>()?;
            match holds_epoch.value() {
                None => Ok(Motes::new(total_balance)),
                Some(epoch) => {
                    let tagged_keys = {
                        let mut ret = vec![];
                        let key_prefix = Key::serialized_key_prefix_by_tag(KeyTag::BalanceHold)?;
                        let gas_balance_hold_addr_prefix =
                            BalanceHoldAddr::balance_hold_addr_prefix_for_purse(
                                BalanceHoldAddrTag::Gas,
                                purse_addr,
                            )?;
                        let processing_balance_hold_addr_prefix =
                            BalanceHoldAddr::balance_hold_addr_prefix_for_purse(
                                BalanceHoldAddrTag::Processing,
                                purse_addr,
                            )?;

                        let gas_hold_key_prefix: Vec<u8> = key_prefix
                            .clone()
                            .into_iter()
                            .chain(gas_balance_hold_addr_prefix.into_iter())
                            .collect();
                        for key in self.keys_with_prefix(&gas_hold_key_prefix)? {
                            ret.push(key);
                        }
                        let processing_hold_key_prefix: Vec<u8> = key_prefix
                            .clone()
                            .into_iter()
                            .chain(processing_balance_hold_addr_prefix.into_iter())
                            .collect();
                        for key in self.keys_with_prefix(&processing_hold_key_prefix)? {
                            ret.push(key);
                        }
                        ret
                    };

                    let mut total_holds = U512::zero();
                    for hold_key in tagged_keys {
                        if let Some(balance_hold_addr) = hold_key.as_balance_hold() {
                            let block_time = balance_hold_addr.block_time();
                            if block_time.value() < epoch {
                                // skip holds older than imputed epoch
                                //  don't skip holds with a timestamp >= epoch timestamp
                                continue;
                            }
                            let stored_value: StoredValue = self
                                .read(&hold_key)?
                                .ok_or(TrackingCopyError::KeyNotFound(key))?;
                            let cl_value: CLValue = stored_value
                                .try_into()
                                .map_err(TrackingCopyError::TypeMismatch)?;
                            let hold_amount = cl_value.into_t()?;
                            total_holds =
                                total_holds.checked_add(hold_amount).unwrap_or(U512::zero());
                        }
                    }
                    let available = total_balance
                        .checked_sub(total_holds)
                        .unwrap_or(U512::zero());
                    Ok(Motes::new(available))
                }
            }
        } else {
            Err(Self::Error::UnexpectedKeyVariant(key))
        }
    }

    fn get_balance_holds_with_proof(
        &self,
        purse_addr: URefAddr,
        holds_epoch: HoldsEpoch,
    ) -> Result<BTreeMap<BlockTime, BalanceHoldsWithProof>, Self::Error> {
        let tagged_keys = {
            let mut ret = vec![];
            let key_prefix = Key::serialized_key_prefix_by_tag(KeyTag::BalanceHold)?;
            let gas_balance_hold_addr_prefix = BalanceHoldAddr::balance_hold_addr_prefix_for_purse(
                BalanceHoldAddrTag::Gas,
                purse_addr,
            )?;
            let processing_balance_hold_addr_prefix =
                BalanceHoldAddr::balance_hold_addr_prefix_for_purse(
                    BalanceHoldAddrTag::Processing,
                    purse_addr,
                )?;

            let gas_hold_key_prefix: Vec<u8> = key_prefix
                .clone()
                .into_iter()
                .chain(gas_balance_hold_addr_prefix.into_iter())
                .collect();
            for key in self.keys_with_prefix(&gas_hold_key_prefix)? {
                ret.push(key);
            }
            let processing_hold_key_prefix: Vec<u8> = key_prefix
                .clone()
                .into_iter()
                .chain(processing_balance_hold_addr_prefix.into_iter())
                .collect();
            for key in self.keys_with_prefix(&processing_hold_key_prefix)? {
                ret.push(key);
            }
            ret
        };
        let mut ret: BTreeMap<BlockTime, BalanceHoldsWithProof> = BTreeMap::new();
        for hold_key in tagged_keys {
            if let Some(balance_hold_addr) = hold_key.as_balance_hold() {
                let block_time = balance_hold_addr.block_time();
                if let Some(timestamp) = holds_epoch.value() {
                    if block_time.value() < timestamp {
                        // skip holds older than the interval
                        //  don't skip holds with a timestamp >= epoch timestamp
                        continue;
                    }
                }
                let proof: TrieMerkleProof<Key, StoredValue> = self
                    .read_with_proof(&hold_key.normalize())?
                    .ok_or(TrackingCopyError::KeyNotFound(hold_key))?;
                let cl_value: CLValue = proof
                    .value()
                    .to_owned()
                    .try_into()
                    .map_err(TrackingCopyError::TypeMismatch)?;
                let hold_amount = cl_value.into_t()?;
                match ret.entry(block_time) {
                    Entry::Vacant(entry) => {
                        let mut inner = BTreeMap::new();
                        inner.insert(balance_hold_addr.tag(), (hold_amount, proof));
                        entry.insert(inner);
                    }
                    Entry::Occupied(mut occupied_entry) => {
                        let inner = occupied_entry.get_mut();
                        match inner.entry(balance_hold_addr.tag()) {
                            Entry::Vacant(entry) => {
                                entry.insert((hold_amount, proof));
                            }
                            Entry::Occupied(_) => {
                                unreachable!(
                                    "there should be only one entry per (block_time, hold kind)"
                                );
                            }
                        }
                    }
                }
            }
        }
        Ok(ret)
    }

    fn clear_expired_balance_holds(
        &mut self,
        purse_addr: URefAddr,
        tag: BalanceHoldAddrTag,
        holds_epoch: HoldsEpoch,
    ) -> Result<(), Self::Error> {
        let key_prefix = Key::serialized_key_prefix_by_tag(KeyTag::BalanceHold)?;
        let balance_hold_addr_prefix =
            BalanceHoldAddr::balance_hold_addr_prefix_for_purse(tag, purse_addr)?;
        let prefix: Vec<u8> = key_prefix
            .clone()
            .into_iter()
            .chain(balance_hold_addr_prefix.into_iter())
            .collect();

        let immut: &_ = self;
        let hold_keys = immut.keys_with_prefix(&prefix)?;
        println!(
            "clear expired {:?} hold count {} prefix {:?}",
            tag,
            hold_keys.len(),
            prefix
        );
        for hold_key in hold_keys {
            if let Some(balance_hold_addr) = hold_key.as_balance_hold() {
                let hold_block_time = balance_hold_addr.block_time();
                if let Some(earliest_relevant_timestamp) = holds_epoch.value() {
                    if hold_block_time.value() > earliest_relevant_timestamp {
                        // skip still relevant holds
                        //  the expectation is that holds are cleared after balance checks,
                        //  and before payment settlement; if that ordering changes in the
                        //  future this strategy should be reevaluated to determine if it
                        //  remains correct.
                        continue;
                    }
                }
                // prune away holds with a timestamp newer than epoch timestamp
                //  including holds with a timestamp == epoch timestamp
                self.prune(hold_key)
            }
        }
        Ok(())
    }

    fn get_byte_code(&mut self, byte_code_hash: ByteCodeHash) -> Result<ByteCode, Self::Error> {
        let key = Key::ByteCode(ByteCodeAddr::V1CasperWasm(byte_code_hash.value()));
        match self.get(&key)? {
            Some(StoredValue::ByteCode(byte_code)) => Ok(byte_code),
            Some(other) => Err(TrackingCopyError::TypeMismatch(
                StoredValueTypeMismatch::new("ContractWasm".to_string(), other.type_name()),
            )),
            None => Err(TrackingCopyError::KeyNotFound(key)),
        }
    }

    fn get_named_keys(&mut self, entity_addr: EntityAddr) -> Result<NamedKeys, Self::Error> {
        let prefix = entity_addr
            .named_keys_prefix()
            .map_err(Self::Error::BytesRepr)?;

        let mut ret: BTreeSet<Key> = BTreeSet::new();
        let keys = self.reader.keys_with_prefix(&prefix)?;
        let pruned = &self.cache.prunes_cached;
        // don't include keys marked for pruning
        for key in keys {
            if pruned.contains(&key) {
                continue;
            }
            ret.insert(key);
        }

        let cache = self.cache.get_key_tag_muts_cached(&KeyTag::NamedKey);

        // there may be newly inserted keys which have not been committed yet
        if let Some(keys) = cache {
            for key in keys {
                if ret.contains(&key) {
                    continue;
                }
                if key.is_entry_for_base(&entity_addr) {
                    ret.insert(key);
                }
            }
        }

        let mut named_keys = NamedKeys::new();

        for entry_key in ret.iter() {
            match self.read(entry_key)? {
                Some(StoredValue::NamedKey(named_key)) => {
                    let key = named_key.get_key().map_err(TrackingCopyError::CLValue)?;
                    let name = named_key.get_name().map_err(TrackingCopyError::CLValue)?;
                    named_keys.insert(name, key);
                }
                Some(other) => {
                    return Err(TrackingCopyError::TypeMismatch(
                        StoredValueTypeMismatch::new("CLValue".to_string(), other.type_name()),
                    ))
                }
                None => match self.cache.reads_cached.get(entry_key) {
                    Some(StoredValue::NamedKey(named_key_value)) => {
                        let key = named_key_value
                            .get_key()
                            .map_err(TrackingCopyError::CLValue)?;
                        let name = named_key_value
                            .get_name()
                            .map_err(TrackingCopyError::CLValue)?;
                        named_keys.insert(name, key);
                    }
                    Some(_) | None => {
                        return Err(TrackingCopyError::KeyNotFound(*entry_key));
                    }
                },
            };
        }

        Ok(named_keys)
    }

    fn get_package(&mut self, package_hash: PackageHash) -> Result<Package, Self::Error> {
        let key = package_hash.into();
        match self.read(&key)? {
            Some(StoredValue::Package(contract_package)) => Ok(contract_package),
            Some(other) => Err(Self::Error::TypeMismatch(StoredValueTypeMismatch::new(
                "Package".to_string(),
                other.type_name(),
            ))),
            None => match self.read(&Key::Hash(package_hash.value()))? {
                Some(StoredValue::ContractPackage(contract_package)) => {
                    let package: Package = contract_package.into();
                    self.write(
                        Key::Package(package_hash.value()),
                        StoredValue::Package(package.clone()),
                    );
                    Ok(package)
                }
                Some(other) => Err(TrackingCopyError::TypeMismatch(
                    StoredValueTypeMismatch::new("ContractPackage".to_string(), other.type_name()),
                )),
                None => Err(Self::Error::KeyNotFound(key)),
            },
        }
    }

    fn get_system_entity_registry(&mut self) -> Result<SystemEntityRegistry, Self::Error> {
        match self.get(&Key::SystemEntityRegistry)? {
            Some(StoredValue::CLValue(registry)) => {
                let registry: SystemEntityRegistry =
                    CLValue::into_t(registry).map_err(Self::Error::from)?;
                Ok(registry)
            }
            Some(other) => Err(TrackingCopyError::TypeMismatch(
                StoredValueTypeMismatch::new("CLValue".to_string(), other.type_name()),
            )),
            None => Err(TrackingCopyError::KeyNotFound(Key::SystemEntityRegistry)),
        }
    }

    fn get_checksum_registry(&mut self) -> Result<Option<ChecksumRegistry>, Self::Error> {
        match self.get(&Key::ChecksumRegistry)? {
            Some(StoredValue::CLValue(registry)) => {
                let registry: ChecksumRegistry =
                    CLValue::into_t(registry).map_err(Self::Error::from)?;
                Ok(Some(registry))
            }
            Some(other) => Err(TrackingCopyError::TypeMismatch(
                StoredValueTypeMismatch::new("CLValue".to_string(), other.type_name()),
            )),
            None => Ok(None),
        }
    }
}
