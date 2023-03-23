pub(crate) mod test_utils;

use assert_matches::assert_matches;
use std::{collections::HashSet, iter, rc::Rc, time::Duration};

use casper_types::testing::TestRng;
use derive_more::From;
use rand::{seq::IteratorRandom, Rng};

use super::*;
use crate::{
    components::consensus::tests::utils::{ALICE_PUBLIC_KEY, ALICE_SECRET_KEY},
    reactor::{EventQueueHandle, QueueKind, Scheduler},
    utils,
};

/// Event for the mock reactor.
#[derive(Debug, From)]
enum MockReactorEvent {
    BlockCompleteConfirmationRequest(BlockCompleteConfirmationRequest),
    BlockFetcherRequest(FetcherRequest<Block>),
    BlockHeaderFetcherRequest(FetcherRequest<BlockHeader>),
    LegacyDeployFetcherRequest(FetcherRequest<LegacyDeploy>),
    DeployFetcherRequest(FetcherRequest<Deploy>),
    FinalitySignatureFetcherRequest(FetcherRequest<FinalitySignature>),
    TrieOrChunkFetcherRequest(FetcherRequest<TrieOrChunk>),
    BlockExecutionResultsOrChunkFetcherRequest(FetcherRequest<BlockExecutionResultsOrChunk>),
    SyncLeapFetcherRequest(FetcherRequest<SyncLeap>),
    NetworkInfoRequest(NetworkInfoRequest),
    BlockAccumulatorRequest(BlockAccumulatorRequest),
    PeerBehaviorAnnouncement(PeerBehaviorAnnouncement),
    StorageRequest(StorageRequest),
    TrieAccumulatorRequest(TrieAccumulatorRequest),
    ContractRuntimeRequest(ContractRuntimeRequest),
    SyncGlobalStateRequest(SyncGlobalStateRequest),
    MakeBlockExecutableRequest(MakeBlockExecutableRequest),
    MetaBlockAnnouncement(MetaBlockAnnouncement),
}

impl From<FetcherRequest<ApprovalsHashes>> for MockReactorEvent {
    fn from(_req: FetcherRequest<ApprovalsHashes>) -> MockReactorEvent {
        unreachable!()
    }
}

struct MockReactor {
    scheduler: &'static Scheduler<MockReactorEvent>,
    effect_builder: EffectBuilder<MockReactorEvent>,
}

impl MockReactor {
    fn new() -> Self {
        let scheduler = utils::leak(Scheduler::new(QueueKind::weights()));
        let event_queue_handle = EventQueueHandle::without_shutdown(scheduler);
        let effect_builder = EffectBuilder::new(event_queue_handle);
        MockReactor {
            scheduler,
            effect_builder,
        }
    }

    fn effect_builder(&self) -> EffectBuilder<MockReactorEvent> {
        self.effect_builder
    }

    async fn crank(&self) -> MockReactorEvent {
        let ((_ancestor, reactor_event), _) = self.scheduler.pop().await;
        reactor_event
    }
}

// Create multiple random peers
fn random_peers(rng: &mut TestRng, num_random_peers: usize) -> HashSet<NodeId> {
    (0..num_random_peers)
        .into_iter()
        .map(|_| NodeId::random(rng))
        .collect()
}

fn check_sync_global_state_event(event: MockReactorEvent, block: &Block) {
    assert!(matches!(
        event,
        MockReactorEvent::SyncGlobalStateRequest { .. }
    ));
    let global_sync_request = match event {
        MockReactorEvent::SyncGlobalStateRequest(req) => req,
        _ => unreachable!(),
    };
    assert_eq!(global_sync_request.block_hash, *block.hash());
    assert_eq!(
        global_sync_request.state_root_hash,
        *block.state_root_hash()
    );
}

// Calls need_next for the block_synchronizer and processes the effects resulted returning a list of
// the new events that were generated
async fn need_next(
    rng: &mut TestRng,
    reactor: &MockReactor,
    block_synchronizer: &mut BlockSynchronizer,
    num_expected_events: usize,
) -> Vec<MockReactorEvent> {
    let mut effects = block_synchronizer.need_next(reactor.effect_builder(), rng);
    assert_eq!(effects.len(), num_expected_events);

    let mut events = Vec::new();
    for effect in effects.drain(0..) {
        tokio::spawn(async move { effect.await });
        let event = reactor.crank().await;
        events.push(event);
    }
    events
}

#[tokio::test]
async fn global_state_sync_wont_stall_with_bad_peers() {
    let mut rng = TestRng::new();
    let mock_reactor = MockReactor::new();

    // Set up a random block that we will use to test synchronization
    let block = Block::random(&mut rng);

    // Set up a validator matrix for the era in which our test block was created
    let mut validator_matrix = ValidatorMatrix::new_with_validator(ALICE_SECRET_KEY.clone());
    validator_matrix.register_validator_weights(
        block.header().era_id(),
        iter::once((ALICE_PUBLIC_KEY.clone(), 100.into())).collect(),
    );

    // Create a block synchronizer with a maximum of 5 simultaneous peers
    let mut block_synchronizer = BlockSynchronizer::new(
        Config::default(),
        Arc::new(Chainspec::random(&mut rng)),
        5,
        validator_matrix,
        prometheus::default_registry(),
    )
    .unwrap();

    // Generate more than 5 peers to see if the peer list changes after a global state sync error
    let num_peers = rng.gen_range(10..20);
    let peers: Vec<NodeId> = random_peers(&mut rng, num_peers).iter().cloned().collect();

    // Set up the synchronizer for the test block such that the next step is getting global state
    block_synchronizer.register_block_by_hash(*block.hash(), true, true);
    assert!(block_synchronizer.historical.is_some()); // we only get global state on historical sync
    block_synchronizer.register_peers(*block.hash(), peers.clone());
    let historical_builder = block_synchronizer.historical.as_mut().unwrap();
    assert!(historical_builder
        .register_block_header(block.header().clone(), None)
        .is_ok());
    historical_builder.register_era_validator_weights(&block_synchronizer.validator_matrix);

    // Generate a finality signature for the test block and register it
    let signature = FinalitySignature::create(
        *block.hash(),
        block.header().era_id(),
        &Rc::new(ALICE_SECRET_KEY.clone()),
        ALICE_PUBLIC_KEY.clone(),
    );
    assert!(signature.is_verified().is_ok());
    assert!(historical_builder
        .register_finality_signature(signature, None)
        .is_ok());
    assert!(historical_builder.register_block(&block, None).is_ok());

    // At this point, the next step the synchronizer takes should be to get global state
    let mut effects = block_synchronizer.need_next(mock_reactor.effect_builder(), &mut rng);
    assert_eq!(effects.len(), 1);
    tokio::spawn(async move { effects.remove(0).await });
    let event = mock_reactor.crank().await;

    // Expect a `SyncGlobalStateRequest` for the `GlobalStateSynchronizer`
    // The peer list that the GlobalStateSynchronizer will use to fetch the tries
    let first_peer_set = peers.iter().copied().choose_multiple(&mut rng, 4);
    check_sync_global_state_event(event, &block);

    // Wait for the latch to reset
    std::thread::sleep(Duration::from_secs(6));

    // Simulate an error form the global_state_synchronizer;
    // make it seem that the `TrieAccumulator` did not find the required tries on any of the peers
    block_synchronizer.global_state_synced(
        *block.hash(),
        Err(GlobalStateSynchronizerError::TrieAccumulator(
            first_peer_set.to_vec(),
        )),
    );

    // At this point we expect that another request for the global state would be made,
    // this time with other peers
    let mut effects = block_synchronizer.need_next(mock_reactor.effect_builder(), &mut rng);
    assert_eq!(effects.len(), 1);
    tokio::spawn(async move { effects.remove(0).await });
    let event = mock_reactor.crank().await;

    let second_peer_set = peers.iter().copied().choose_multiple(&mut rng, 4);
    check_sync_global_state_event(event, &block);

    // Wait for the latch to reset
    std::thread::sleep(Duration::from_secs(6));

    // Simulate a successful global state sync;
    // Although the request was successful, some peers did not have the data.
    let unreliable_peers = second_peer_set.into_iter().choose_multiple(&mut rng, 2);
    block_synchronizer.global_state_synced(
        *block.hash(),
        Ok(GlobalStateSynchronizerResponse::new(
            (*block.state_root_hash()).into(),
            unreliable_peers.clone(),
        )),
    );
    let mut effects = block_synchronizer.need_next(mock_reactor.effect_builder(), &mut rng);
    assert_eq!(effects.len(), 1);
    tokio::spawn(async move { effects.remove(0).await });
    let event = mock_reactor.crank().await;

    // Synchronizer should have progressed
    assert!(false == matches!(event, MockReactorEvent::SyncGlobalStateRequest { .. }));

    // Check if the peers returned by the `GlobalStateSynchronizer` in the response were marked
    // unreliable.
    for peer in unreliable_peers.iter() {
        assert!(block_synchronizer
            .historical
            .as_ref()
            .unwrap()
            .peer_list()
            .is_peer_unreliable(peer));
    }
}

#[test]
fn duplicate_register_block_not_allowed_if_builder_is_not_failed() {
    let mut rng = TestRng::new();

    // Set up a random block that we will use to test synchronization
    let block = Block::random(&mut rng);

    // Set up a validator matrix for the era in which our test block was created
    let mut validator_matrix = ValidatorMatrix::new_with_validator(ALICE_SECRET_KEY.clone());
    validator_matrix.register_validator_weights(
        block.header().era_id(),
        iter::once((ALICE_PUBLIC_KEY.clone(), 100.into())).collect(),
    );

    // Create a block synchronizer with a maximum of 5 simultaneous peers
    let mut block_synchronizer = BlockSynchronizer::new(
        Config::default(),
        Arc::new(Chainspec::random(&mut rng)),
        5,
        validator_matrix,
        prometheus::default_registry(),
    )
    .unwrap();

    // Register block for historical sync
    assert!(block_synchronizer.register_block_by_hash(*block.hash(), false, true));
    assert!(block_synchronizer.forward.is_some()); // we only get global state on historical sync

    // Registering the block again should not be allowed until the sync finishes
    assert!(!block_synchronizer.register_block_by_hash(*block.hash(), false, true));

    // Trying to register a different block should replace the old one
    let new_block = Block::random(&mut rng);
    assert!(block_synchronizer.register_block_by_hash(*new_block.hash(), false, true));
    assert_eq!(
        block_synchronizer.forward.unwrap().block_hash(),
        *new_block.hash()
    );
}

#[tokio::test]
async fn historical_sync_gets_peers_form_both_connected_peers_and_accumulator() {
    const MAX_SIMULTANEOUS_PEERS: u32 = 5;
    let mut rng = TestRng::new();
    let mock_reactor = MockReactor::new();

    // Set up a random block that we will use to test synchronization
    let block = Block::random(&mut rng);

    // Set up a validator matrix for the era in which our test block was created
    let mut validator_matrix = ValidatorMatrix::new_with_validator(ALICE_SECRET_KEY.clone());
    validator_matrix.register_validator_weights(
        block.header().era_id(),
        iter::once((ALICE_PUBLIC_KEY.clone(), 100.into())).collect(),
    );

    // Create a block synchronizer with a maximum of 5 simultaneous peers
    let mut block_synchronizer = BlockSynchronizer::new(
        Config::default(),
        Arc::new(Chainspec::random(&mut rng)),
        MAX_SIMULTANEOUS_PEERS,
        validator_matrix,
        prometheus::default_registry(),
    )
    .unwrap();

    // Register block for historical sync
    assert!(block_synchronizer.register_block_by_hash(*block.hash(), true, true));
    assert!(block_synchronizer.historical.is_some()); // we only get global state on historical sync

    let events = need_next(&mut rng, &mock_reactor, &mut block_synchronizer, 2).await;

    // The first thing the synchronizer should do is get peers.
    // For the historical flow, the synchronizer will get a random sampling of the connected
    // peers and also ask the accumulator to provide peers from which it has received information
    // for the block that is being synchronized.
    assert_matches!(
        events[0],
        MockReactorEvent::NetworkInfoRequest(NetworkInfoRequest::FullyConnectedPeers {
            count,
            ..
        }) if count == MAX_SIMULTANEOUS_PEERS as usize
    );

    assert_matches!(
        events[1],
        MockReactorEvent::BlockAccumulatorRequest(BlockAccumulatorRequest::GetPeersForBlock {
            block_hash,
            ..
        }) if block_hash == *block.hash()
    )
}

#[tokio::test]
async fn sync_starts_with_header_fetch() {
    const MAX_SIMULTANEOUS_PEERS: u32 = 5;
    let mut rng = TestRng::new();
    let mock_reactor = MockReactor::new();

    // Set up a random block that we will use to test synchronization
    let block = Block::random(&mut rng);

    // Set up a validator matrix for the era in which our test block was created
    let mut validator_matrix = ValidatorMatrix::new_with_validator(ALICE_SECRET_KEY.clone());
    validator_matrix.register_validator_weights(
        block.header().era_id(),
        iter::once((ALICE_PUBLIC_KEY.clone(), 100.into())).collect(),
    );

    // Create a block synchronizer with a maximum of 5 simultaneous peers
    let mut block_synchronizer = BlockSynchronizer::new(
        Config::default(),
        Arc::new(Chainspec::random(&mut rng)),
        MAX_SIMULTANEOUS_PEERS,
        validator_matrix,
        prometheus::default_registry(),
    )
    .unwrap();

    // Register block for fwd sync
    assert!(block_synchronizer.register_block_by_hash(*block.hash(), false, true));
    assert!(block_synchronizer.forward.is_some());
    // Generate peers and register them
    let num_peers = rng.gen_range(10..20);
    let peers: Vec<NodeId> = random_peers(&mut rng, num_peers).iter().cloned().collect();
    block_synchronizer.register_peers(*block.hash(), peers.clone());

    let events = need_next(
        &mut rng,
        &mock_reactor,
        &mut block_synchronizer,
        MAX_SIMULTANEOUS_PEERS as usize,
    )
    .await;

    // The first thing needed after the synchronizer has peers is
    // to fetch the block header from peers.
    for event in events {
        assert_matches!(
            event,
            MockReactorEvent::BlockHeaderFetcherRequest(FetcherRequest {
                id,
                peer,
                ..
            }) if peers.contains(&peer) && id == *block.hash()
        );
    }
}
