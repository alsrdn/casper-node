use std::fmt::{self, Display, Formatter};

use casper_hashing::Digest;
use derive_more::From;
use either::Either;
use serde::Serialize;

use casper_execution_engine::{core::engine_state, storage::trie::TrieRaw};

use crate::{
    components::{
        fetcher::FetchResult,
    },
    effect::requests::BlockSynchronizerRequest,
    types::{
        ApprovalsHashes, Block, BlockExecutionResultsOrChunk, BlockHash, BlockHeader, Deploy,
        FinalitySignature, FinalizedBlock, LegacyDeploy, NodeId, TrieOrChunk, TrieOrChunkId,
    },
};

#[derive(From, Debug, Serialize)]
pub(crate) enum Event {
    Initialize,
    #[from]
    Request(BlockSynchronizerRequest),
    DisconnectFromPeer(NodeId),
    #[from]
    MadeFinalizedBlock {
        block_hash: BlockHash,
        result: Option<(FinalizedBlock, Vec<Deploy>)>,
    },
    MarkBlockExecutionEnqueued(BlockHash),
    MarkBlockExecuted(BlockHash),
    MarkBlockCompleted {
        block_hash: BlockHash,
        is_new: bool,
    },
    #[from]
    BlockHeaderFetched(FetchResult<BlockHeader>),
    #[from]
    BlockFetched(FetchResult<Block>),
    #[from]
    ApprovalsHashesFetched(FetchResult<ApprovalsHashes>),
    #[from]
    FinalitySignatureFetched(FetchResult<FinalitySignature>),
    GotExecutionResultsChecksum {
        block_hash: BlockHash,
        #[serde(skip_serializing)]
        result: Result<Option<Digest>, engine_state::Error>,
    },
    DeployFetched {
        block_hash: BlockHash,
        result: Either<FetchResult<LegacyDeploy>, FetchResult<Deploy>>,
    },
    ExecutionResultsFetched {
        block_hash: BlockHash,
        result: FetchResult<BlockExecutionResultsOrChunk>,
    },
    TrieOrChunkFetched {
        trie_id: TrieOrChunkId,
        result: FetchResult<TrieOrChunk>,
    },
    PutTrieResult {
        trie_hash: Digest,
        trie_raw: TrieRaw,
        #[serde(skip)]
        put_trie_result: Result<Digest, engine_state::Error>,
    },
    ExecutionResultsStored(BlockHash),
    AccumulatedPeers(BlockHash, Option<Vec<NodeId>>),
    NetworkPeers(BlockHash, Vec<NodeId>),
}

impl Display for Event {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Event::Request(BlockSynchronizerRequest::NeedNext { .. }) => {
                write!(f, "block synchronizer need next request")
            }
            Event::Request(BlockSynchronizerRequest::SyncGlobalStates(global_states, _)) => {
                write!(f, "global states to be synced: [")?;
                for (block_hash, global_state_hash) in global_states {
                    write!(
                        f,
                        "(block {}, global state {}), ",
                        block_hash, global_state_hash
                    )?;
                }
                write!(f, "]")
            }
            Event::Request(_) => {
                write!(f, "block synchronizer request from effect builder")
            }
            Event::Initialize => {
                write!(f, "initialize this component")
            }
            Event::DisconnectFromPeer(peer) => {
                write!(f, "disconnected from peer {}", peer)
            }
            Event::BlockHeaderFetched(Ok(fetched_item)) => {
                write!(f, "{}", fetched_item)
            }
            Event::BlockHeaderFetched(Err(fetcher_error)) => {
                write!(f, "{}", fetcher_error)
            }
            Event::BlockFetched(Ok(fetched_item)) => {
                write!(f, "{}", fetched_item)
            }
            Event::BlockFetched(Err(fetcher_error)) => {
                write!(f, "{}", fetcher_error)
            }
            Event::ApprovalsHashesFetched(Ok(fetched_item)) => {
                write!(f, "{}", fetched_item)
            }
            Event::ApprovalsHashesFetched(Err(fetcher_error)) => {
                write!(f, "{}", fetcher_error)
            }
            Event::FinalitySignatureFetched(Ok(fetched_item)) => {
                write!(f, "{}", fetched_item)
            }
            Event::FinalitySignatureFetched(Err(fetcher_error)) => {
                write!(f, "{}", fetcher_error)
            }
            Event::GotExecutionResultsChecksum {
                block_hash: _,
                result,
            } => match result {
                Ok(Some(digest)) => write!(f, "got exec results checksum {}", digest),
                Ok(None) => write!(f, "got no exec results checksum"),
                Err(error) => write!(f, "failed to get exec results checksum: {}", error),
            },
            Event::DeployFetched {
                block_hash: _,
                result,
            } => match result {
                Either::Left(Ok(fetched_item)) => write!(f, "{}", fetched_item),
                Either::Left(Err(fetcher_error)) => write!(f, "{}", fetcher_error),
                Either::Right(Ok(fetched_item)) => write!(f, "{}", fetched_item),
                Either::Right(Err(fetcher_error)) => write!(f, "{}", fetcher_error),
            },
            Event::ExecutionResultsFetched {
                block_hash: _,
                result,
            } => match result {
                Ok(fetched_item) => write!(f, "{}", fetched_item),
                Err(fetcher_error) => write!(f, "{}", fetcher_error),
            },
            Event::ExecutionResultsStored { .. } => write!(f, "stored execution results"),
            Event::NetworkPeers(..) => {
                write!(f, "network peers")
            }
            Event::AccumulatedPeers(..) => {
                write!(f, "accumulated peers")
            }
            Event::MadeFinalizedBlock { .. } => {
                write!(f, "made finalized block")
            }
            Event::MarkBlockExecutionEnqueued(..) => {
                write!(f, "mark block enqueued for execution")
            }
            Event::MarkBlockExecuted(..) => {
                write!(f, "block execution complete")
            }
            Event::MarkBlockCompleted { .. } => {
                write!(f, "mark block completed")
            }
            Event::TrieOrChunkFetched { trie_id, result: _ } => {
                write!(f, "fetch response for trie {}", trie_id)
            }
            Event::PutTrieResult {
                trie_hash,
                trie_raw,
                put_trie_result,
            } => {
                write!(f, "put trie result for trie {}", trie_hash)
            }
        }
    }
}
