//! The consensus component. Provides distributed consensus among the nodes in the network.

#![warn(clippy::integer_arithmetic)]

mod cl_context;
mod config;
mod consensus_protocol;
mod era_supervisor;
#[macro_use]
mod highway_core;
pub(crate) mod error;
mod metrics;
mod protocols;
#[cfg(test)]
mod tests;
mod traits;
mod utils;

use std::{
    convert::Infallible,
    fmt::{self, Debug, Display, Formatter},
    sync::Arc,
    time::Duration,
};

use datasize::DataSize;
use derive_more::From;
use hex_fmt::HexFmt;
use serde::{Deserialize, Serialize};

use casper_types::{EraId, PublicKey};

use crate::{
    components::Component,
    effect::{
        announcements::{BlocklistAnnouncement, ConsensusAnnouncement},
        requests::{
            BlockProposerRequest, BlockValidationRequest, ChainspecLoaderRequest, ConsensusRequest,
            ContractRuntimeRequest, NetworkRequest, StorageRequest,
        },
        EffectBuilder, Effects,
    },
    protocol::Message,
    reactor::ReactorEvent,
    types::{ActivationPoint, BlockHeader, BlockPayload, Timestamp},
    NodeRng,
};

pub(crate) use cl_context::ClContext;
pub(crate) use config::Config;
pub(crate) use consensus_protocol::{BlockContext, EraReport, ProposedBlock};
pub(crate) use era_supervisor::EraSupervisor;
pub(crate) use protocols::highway::HighwayProtocol;
use traits::NodeIdT;
pub(crate) use utils::check_sufficient_finality_signatures;

#[derive(DataSize, Clone, Serialize, Deserialize)]
pub(crate) enum ConsensusMessage {
    /// A protocol message, to be handled by the instance in the specified era.
    Protocol { era_id: EraId, payload: Vec<u8> },
    /// A request for evidence against the specified validator, from any era that is still bonded
    /// in `era_id`.
    EvidenceRequest { era_id: EraId, pub_key: PublicKey },
}

/// An ID to distinguish different timers. What they are used for is specific to each consensus
/// protocol implementation.
#[derive(DataSize, Clone, Copy, Debug, Eq, PartialEq)]
pub struct TimerId(pub u8);

/// An ID to distinguish queued actions. What they are used for is specific to each consensus
/// protocol implementation.
#[derive(DataSize, Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActionId(pub u8);

#[derive(DataSize, Debug, From)]
pub struct NewBlockPayload {
    era_id: EraId,
    block_payload: Arc<BlockPayload>,
    block_context: BlockContext<ClContext>,
}

#[derive(DataSize, Debug, From)]
pub struct ResolveValidity<I> {
    era_id: EraId,
    sender: I,
    proposed_block: ProposedBlock<ClContext>,
    valid: bool,
}

/// Consensus component event.
#[derive(DataSize, Debug, From)]
pub(crate) enum Event<I> {
    /// An incoming network message.
    MessageReceived { sender: I, msg: ConsensusMessage },
    /// A scheduled event to be handled by a specified era.
    Timer {
        era_id: EraId,
        timestamp: Timestamp,
        timer_id: TimerId,
    },
    /// A queued action to be handled by a specific era.
    Action { era_id: EraId, action_id: ActionId },
    /// We are receiving the data we require to propose a new block.
    NewBlockPayload(NewBlockPayload),
    #[from]
    ConsensusRequest(ConsensusRequest),
    /// A new block has been added to the linear chain.
    BlockAdded(Box<BlockHeader>),
    /// The proto-block has been validated.
    ResolveValidity(ResolveValidity<I>),
    /// Deactivate the era with the given ID, unless the number of faulty validators increases.
    DeactivateEra {
        era_id: EraId,
        faulty_num: usize,
        delay: Duration,
    },
    /// Event raised when a new era should be created because a new switch block is available.
    CreateNewEra {
        /// The most recent switch block headers
        switch_blocks: Vec<BlockHeader>,
    },
    /// Got the result of checking for an upgrade activation point.
    GotUpgradeActivationPoint(ActivationPoint),
}

impl Debug for ConsensusMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ConsensusMessage::Protocol { era_id, payload: _ } => {
                write!(f, "Protocol {{ era_id: {:?}, .. }}", era_id)
            }
            ConsensusMessage::EvidenceRequest { era_id, pub_key } => f
                .debug_struct("EvidenceRequest")
                .field("era_id", era_id)
                .field("pub_key", pub_key)
                .finish(),
        }
    }
}

impl Display for ConsensusMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ConsensusMessage::Protocol { era_id, payload } => {
                write!(f, "protocol message {:10} in {}", HexFmt(payload), era_id)
            }
            ConsensusMessage::EvidenceRequest { era_id, pub_key } => write!(
                f,
                "request for evidence of fault by {} in {} or earlier",
                pub_key, era_id,
            ),
        }
    }
}

impl<I: Debug> Display for Event<I> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Event::MessageReceived { sender, msg } => write!(f, "msg from {:?}: {}", sender, msg),
            Event::Timer {
                era_id,
                timestamp,
                timer_id,
            } => write!(
                f,
                "timer (ID {}) for {} scheduled for timestamp {}",
                timer_id.0, era_id, timestamp,
            ),
            Event::Action { era_id, action_id } => {
                write!(f, "action (ID {}) for {}", action_id.0, era_id)
            }
            Event::NewBlockPayload(NewBlockPayload {
                era_id,
                block_payload,
                block_context,
            }) => write!(
                f,
                "New proto-block for era {:?}: {:?}, {:?}",
                era_id, block_payload, block_context
            ),
            Event::ConsensusRequest(request) => write!(
                f,
                "A request for consensus component hash been received: {:?}",
                request
            ),
            Event::BlockAdded(block_header) => write!(
                f,
                "A block has been added to the linear chain: {}",
                block_header.hash(),
            ),
            Event::ResolveValidity(ResolveValidity {
                era_id,
                sender,
                proposed_block,
                valid,
            }) => write!(
                f,
                "Proposed block received from {:?} for {} is {}: {:?}",
                sender,
                era_id,
                if *valid { "valid" } else { "invalid" },
                proposed_block,
            ),
            Event::DeactivateEra {
                era_id, faulty_num, ..
            } => write!(
                f,
                "Deactivate old {} unless additional faults are observed; faults so far: {}",
                era_id, faulty_num
            ),
            Event::CreateNewEra { switch_blocks } => write!(
                f,
                "New era should be created; switch blocks: {:?}",
                switch_blocks
            ),
            Event::GotUpgradeActivationPoint(activation_point) => {
                write!(f, "new upgrade activation point: {:?}", activation_point)
            }
        }
    }
}

/// A helper trait whose bounds represent the requirements for a reactor event that `EraSupervisor`
/// can work with.
pub(crate) trait ReactorEventT<I>:
    ReactorEvent
    + From<Event<I>>
    + Send
    + From<NetworkRequest<I, Message>>
    + From<BlockProposerRequest>
    + From<ConsensusAnnouncement>
    + From<BlockValidationRequest<I>>
    + From<StorageRequest>
    + From<ContractRuntimeRequest>
    + From<ChainspecLoaderRequest>
    + From<BlocklistAnnouncement<I>>
{
}

impl<REv, I> ReactorEventT<I> for REv where
    REv: ReactorEvent
        + From<Event<I>>
        + Send
        + From<NetworkRequest<I, Message>>
        + From<BlockProposerRequest>
        + From<ConsensusAnnouncement>
        + From<BlockValidationRequest<I>>
        + From<StorageRequest>
        + From<ContractRuntimeRequest>
        + From<ChainspecLoaderRequest>
        + From<BlocklistAnnouncement<I>>
{
}

impl<I, REv> Component<REv> for EraSupervisor<I>
where
    I: NodeIdT,
    REv: ReactorEventT<I>,
{
    type Event = Event<I>;
    type ConstructionError = Infallible;

    fn handle_event(
        &mut self,
        effect_builder: EffectBuilder<REv>,
        rng: &mut NodeRng,
        event: Self::Event,
    ) -> Effects<Self::Event> {
        match event {
            Event::Timer {
                era_id,
                timestamp,
                timer_id,
            } => self.handle_timer(effect_builder, rng, era_id, timestamp, timer_id),
            Event::Action { era_id, action_id } => {
                self.handle_action(effect_builder, rng, era_id, action_id)
            }
            Event::MessageReceived { sender, msg } => {
                self.handle_message(effect_builder, rng, sender, msg)
            }
            Event::NewBlockPayload(new_block_payload) => {
                self.handle_new_block_payload(effect_builder, rng, new_block_payload)
            }
            Event::BlockAdded(block_header) => {
                self.handle_block_added(effect_builder, *block_header)
            }
            Event::ResolveValidity(resolve_validity) => {
                self.resolve_validity(effect_builder, rng, resolve_validity)
            }
            Event::DeactivateEra {
                era_id,
                faulty_num,
                delay,
            } => self.handle_deactivate_era(effect_builder, era_id, faulty_num, delay),
            Event::CreateNewEra { switch_blocks } => {
                self.create_new_era(effect_builder, rng, &switch_blocks)
            }
            Event::GotUpgradeActivationPoint(activation_point) => {
                self.got_upgrade_activation_point(activation_point)
            }
            Event::ConsensusRequest(ConsensusRequest::Status(responder)) => self.status(responder),
        }
    }
}
