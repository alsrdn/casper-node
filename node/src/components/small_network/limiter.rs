//! Resource limiters
//!
//! Resource limiters restrict the usable amount of a resource through slowing down the request rate
//! by making each user request an allowance first.

use std::{collections::HashSet, sync::Arc, time::Duration};

use async_trait::async_trait;
use casper_types::PublicKey;
use prometheus::Counter;
use tokio::{
    sync::{mpsc, oneshot},
    time::Instant,
};
use tracing::{debug, info, warn};

use crate::types::NodeId;

/// Amount of resource allowed to buffer in `ClassBasedLimiter`.
const STORED_BUFFER_SECS: Duration = Duration::from_secs(2);

/// A limiter.
///
/// Any consumer of a specific resource is expected to call `create_handle` for every peer and use
/// the returned handle to request a access to a resource.
pub(super) trait Limiter: Send + Sync {
    /// Create a handle for a connection using the given peer and optional validator id.
    fn create_handle(
        &self,
        peer_id: NodeId,
        validator_id: Option<PublicKey>,
    ) -> Box<dyn LimiterHandle>;

    /// Update the validator sets.
    fn update_validators(
        &self,
        active_validators: HashSet<PublicKey>,
        upcoming_validators: HashSet<PublicKey>,
    );
}

/// A per-peer handle for a limiter.
#[async_trait]
pub(super) trait LimiterHandle: Send + Sync {
    /// Waits until the requestor is allocated `amount` additional resources.
    async fn request_allowance(&self, amount: u32);
}

/// An unlimited "limiter".
///
/// Does not restrict resources in any way (`request_allowance` returns immediately).
#[derive(Debug)]
pub(super) struct Unlimited;

/// Handle for `Unlimited`.
struct UnlimitedHandle;

impl Limiter for Unlimited {
    fn create_handle(
        &self,
        _peer_id: NodeId,
        _validator_id: Option<PublicKey>,
    ) -> Box<dyn LimiterHandle> {
        Box::new(UnlimitedHandle)
    }

    fn update_validators(
        &self,
        _active_validators: HashSet<PublicKey>,
        _upcoming_validators: HashSet<PublicKey>,
    ) {
    }
}

#[async_trait]
impl LimiterHandle for UnlimitedHandle {
    async fn request_allowance(&self, _amount: u32) {
        // No limit.
    }
}

/// A limiter dividing resources into multiple classes based on their validator status.
///
/// Imposes a limit on non-validator resources while not limiting active validator resources at all.
#[derive(Debug)]
pub(super) struct ClassBasedLimiter {
    /// Sender for commands to the limiter.
    sender: mpsc::UnboundedSender<ClassBasedCommand>,
}

/// Peer class for the `ClassBasedLimiter`.
enum PeerClass {
    /// Active validators.
    ActiveValidator,
    /// Upcoming validators that are not active yet.
    UpcomingValidator,
    /// Unclassified/low-priority peers.
    Bulk,
}

/// Command sent to the `ClassBasedLimiter`.
enum ClassBasedCommand {
    /// Updates the set of active/upcoming validators.
    UpdateValidators {
        /// The new set of validators active in the current era.
        active_validators: HashSet<PublicKey>,
        /// The new set of validators in future eras.
        upcoming_validators: HashSet<PublicKey>,
    },
    /// Requests a certain amount of a resource.
    RequestResource {
        /// Amount of resource requested.
        amount: u32,
        /// Id of the requesting sender.
        id: Arc<ConsumerId>,
        /// Response channel.
        responder: oneshot::Sender<()>,
    },
    /// Shuts down the worker task
    Shutdown,
}

/// Handle for `ClassBasedLimiter`.
#[derive(Debug)]
struct ClassBasedHandle {
    /// Sender for commands.
    sender: mpsc::UnboundedSender<ClassBasedCommand>,
    /// Consumer ID for the sender holding this handle.
    consumer_id: Arc<ConsumerId>,
}

/// An identity for a consumer.
#[derive(Debug)]
struct ConsumerId {
    /// The peer's ID.
    #[allow(dead_code)]
    peer_id: NodeId,
    /// The remote node's `validator_id`.
    validator_id: Option<PublicKey>,
}

impl ClassBasedLimiter {
    /// Creates a new class based limiter.
    ///
    /// Starts the background worker task as well. Any time the limiter caused a delay,
    /// `wait_time_sec` will be incremented.
    pub(super) fn new(resources_per_second: u32, wait_time_sec: Counter) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();

        tokio::spawn(worker(
            receiver,
            resources_per_second,
            ((resources_per_second as f64) * STORED_BUFFER_SECS.as_secs_f64()) as u32,
            wait_time_sec,
        ));

        ClassBasedLimiter { sender }
    }
}

impl Limiter for ClassBasedLimiter {
    fn create_handle(
        &self,
        peer_id: NodeId,
        validator_id: Option<PublicKey>,
    ) -> Box<dyn LimiterHandle> {
        Box::new(ClassBasedHandle {
            sender: self.sender.clone(),
            consumer_id: Arc::new(ConsumerId {
                peer_id,
                validator_id,
            }),
        })
    }

    fn update_validators(
        &self,
        active_validators: HashSet<PublicKey>,
        upcoming_validators: HashSet<PublicKey>,
    ) {
        if self
            .sender
            .send(ClassBasedCommand::UpdateValidators {
                active_validators,
                upcoming_validators,
            })
            .is_err()
        {
            debug!("could not update validator data set of limiter, channel closed");
        }
    }
}

#[async_trait]
impl LimiterHandle for ClassBasedHandle {
    async fn request_allowance(&self, amount: u32) {
        let (responder, waiter) = oneshot::channel();

        // Send a request to the limiter and await a response. If we do not receive one due to
        // errors, simply ignore it and do not restrict resources.
        //
        // While ignoring it is suboptimal, it is likely that we can continue operation normally in
        // many circumstances, thus the message is downgraded from what would be a `warn!` to
        // `debug!`.
        if self
            .sender
            .send(ClassBasedCommand::RequestResource {
                amount,
                id: self.consumer_id.clone(),
                responder,
            })
            .is_err()
        {
            debug!("worker was shutdown, sending is unlimited");
        } else if waiter.await.is_err() {
            debug!("failed to await resource allowance, unlimited");
        }
    }
}

impl Drop for ClassBasedLimiter {
    fn drop(&mut self) {
        if self.sender.send(ClassBasedCommand::Shutdown).is_err() {
            warn!("error sending shutdown command to class based limiter");
        }
    }
}

/// Background worker for the limiter.
///
/// Will permit any amount of current/future validator resource requests, while restricting
/// non-validators to `resources_per_second`, although with unlimited one-time burst. The latter
/// guarantees that any size request can be satisfied (e.g. a message larger than the burst limit).
///
/// Stores up to `max_stored_resources` during idle times to smooth this process.
async fn worker(
    mut receiver: mpsc::UnboundedReceiver<ClassBasedCommand>,
    resources_per_second: u32,
    max_stored_resource: u32,
    wait_time_sec: Counter,
) {
    let mut active_validators = HashSet::new();
    let mut upcoming_validators = HashSet::new();

    let mut resources_available: i64 = 0;
    let mut last_refill: Instant = Instant::now();

    // Whether or not we emitted a warning that the limiter is currently implicitly disabled.
    let mut logged_uninitialized = false;

    while let Some(msg) = receiver.recv().await {
        match msg {
            ClassBasedCommand::UpdateValidators {
                active_validators: new_active_validators,
                upcoming_validators: new_upcoming_validators,
            } => {
                active_validators = new_active_validators;
                upcoming_validators = new_upcoming_validators;
                debug!(
                    ?active_validators,
                    ?upcoming_validators,
                    "resource classes updated"
                );
            }
            ClassBasedCommand::RequestResource {
                amount,
                id,
                responder,
            } => {
                if active_validators.is_empty() && upcoming_validators.is_empty() {
                    // It is likely that we have not been initialized, thus no node is getting the
                    // reserved resources. In this case, do not limit at all.
                    if !logged_uninitialized {
                        logged_uninitialized = true;
                        info!("empty set of validators, not limiting resources at all");
                    }
                    continue;
                }

                let peer_class = if let Some(ref validator_id) = id.validator_id {
                    if active_validators.contains(validator_id) {
                        PeerClass::ActiveValidator
                    } else if upcoming_validators.contains(validator_id) {
                        PeerClass::UpcomingValidator
                    } else {
                        PeerClass::Bulk
                    }
                } else {
                    PeerClass::Bulk
                };

                match peer_class {
                    PeerClass::ActiveValidator | PeerClass::UpcomingValidator => {
                        // No limit imposed on validators.
                    }
                    PeerClass::Bulk => {
                        while resources_available < 0 {
                            // Determine time delta since last refill.
                            let now = Instant::now();
                            let elapsed = now - last_refill;
                            last_refill = now;

                            // Add appropriate amount of resources, capped at `max_stored_bytes`.
                            resources_available +=
                                ((elapsed.as_nanos() * resources_per_second as u128)
                                    / 1_000_000_000) as i64;
                            resources_available =
                                resources_available.min(max_stored_resource as i64);

                            // If we do not have enough resources available, sleep until we do.
                            if resources_available < 0 {
                                let estimated_time_remaining = Duration::from_millis(
                                    (-resources_available) as u64 * 1000
                                        / resources_per_second as u64,
                                );

                                tokio::time::sleep(estimated_time_remaining).await;
                                wait_time_sec.inc_by(estimated_time_remaining.as_secs_f64());
                            }
                        }

                        // Subtract requested amount.
                        resources_available -= amount as i64;
                    }
                }

                if responder.send(()).is_err() {
                    debug!("resource requester disappeared before we could answer.")
                }
            }
            ClassBasedCommand::Shutdown => {
                // Shutdown the channel, processing only the remaining messages.
                receiver.close();
            }
        }
    }
    debug!("class based worker exiting");
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, time::Duration};

    use prometheus::Counter;
    use tokio::time::Instant;

    use super::{ClassBasedLimiter, Limiter, NodeId, PublicKey, Unlimited};
    use crate::{crypto::AsymmetricKeyExt, testing::init_logging};

    /// Something that happens almost immediately, with some allowance for test jitter.
    const SHORT_TIME: Duration = Duration::from_millis(250);

    /// Creates a new counter for testing.
    fn new_wait_time_sec() -> Counter {
        Counter::new("test_time_waiting", "wait time counter used in tests")
            .expect("could not create new counter")
    }

    #[tokio::test]
    async fn unlimited_limiter_is_unlimited() {
        let mut rng = crate::new_rng();

        let unlimited = Unlimited;

        let handle = unlimited.create_handle(NodeId::random(&mut rng), None);

        let start = Instant::now();
        handle.request_allowance(0).await;
        handle.request_allowance(u32::MAX).await;
        handle.request_allowance(1).await;
        let end = Instant::now();

        assert!(end - start < SHORT_TIME);
    }

    #[tokio::test]
    async fn active_validator_is_unlimited() {
        let mut rng = crate::new_rng();

        let validator_id = PublicKey::random(&mut rng);
        let limiter = ClassBasedLimiter::new(1_000, new_wait_time_sec());

        let mut active_validators = HashSet::new();
        active_validators.insert(validator_id.clone());
        limiter.update_validators(active_validators, HashSet::new());

        let handle = limiter.create_handle(NodeId::random(&mut rng), Some(validator_id));

        let start = Instant::now();
        handle.request_allowance(0).await;
        handle.request_allowance(u32::MAX).await;
        handle.request_allowance(1).await;
        let end = Instant::now();

        assert!(end - start < SHORT_TIME);
    }

    #[tokio::test]
    async fn inactive_validator_limited() {
        let mut rng = crate::new_rng();

        let validator_id = PublicKey::random(&mut rng);
        let limiter = ClassBasedLimiter::new(1_000, new_wait_time_sec());

        // We insert one unrelated active validator to avoid triggering the automatic disabling of
        // the limiter in case there are no active validators.
        let mut active_validators = HashSet::new();
        active_validators.insert(PublicKey::random(&mut rng));
        limiter.update_validators(active_validators, HashSet::new());

        // Try with non-validators or unknown nodes.
        let handles = vec![
            limiter.create_handle(NodeId::random(&mut rng), Some(validator_id)),
            limiter.create_handle(NodeId::random(&mut rng), None),
        ];

        for handle in handles {
            let start = Instant::now();

            // Send 9_0001 bytes, we expect this to take roughly 15 seconds.
            handle.request_allowance(1000).await;
            handle.request_allowance(1000).await;
            handle.request_allowance(1000).await;
            handle.request_allowance(2000).await;
            handle.request_allowance(4000).await;
            handle.request_allowance(1).await;
            let end = Instant::now();

            let diff = end - start;
            assert!(diff >= Duration::from_secs(9));
            assert!(diff <= Duration::from_secs(10));
        }
    }

    #[tokio::test]
    async fn nonvalidators_parallel_limited() {
        let mut rng = crate::new_rng();

        let validator_id = PublicKey::random(&mut rng);
        let wait_metric = new_wait_time_sec();
        let limiter = ClassBasedLimiter::new(1_000, wait_metric.clone());

        let start = Instant::now();

        // We insert one unrelated active validator to avoid triggering the automatic disabling of
        // the limiter in case there are no active validators.
        let mut active_validators = HashSet::new();
        active_validators.insert(PublicKey::random(&mut rng));
        limiter.update_validators(active_validators, HashSet::new());

        // Parallel test, 5 non-validators sharing 1000 bytes per second. Each sends 1001 bytes, so
        // total time is expected to be just over 5 seconds.
        let join_handles = (0..5)
            .map(|_| limiter.create_handle(NodeId::random(&mut rng), Some(validator_id.clone())))
            .map(|handle| {
                tokio::spawn(async move {
                    handle.request_allowance(500).await;
                    handle.request_allowance(150).await;
                    handle.request_allowance(350).await;
                    handle.request_allowance(1).await;
                })
            });

        for join_handle in join_handles {
            join_handle.await.expect("could not join task");
        }

        let end = Instant::now();
        let diff = end - start;
        assert!(diff >= Duration::from_secs(5));
        assert!(diff <= Duration::from_secs(6));

        // Ensure metrics recorded the correct number of seconds.
        assert!(
            wait_metric.get() <= 6.0,
            "wait metric is too large: {}",
            wait_metric.get()
        );

        // Note: The limiting will not apply to all data, so it should be slightly below 5 seconds.
        assert!(
            wait_metric.get() >= 4.5,
            "wait metric is too small: {}",
            wait_metric.get()
        );
    }

    #[tokio::test]
    async fn inactive_validators_unlimited_when_no_validators_known() {
        init_logging();

        let mut rng = crate::new_rng();

        let validator_id = PublicKey::random(&mut rng);
        let wait_metric = new_wait_time_sec();
        let limiter = ClassBasedLimiter::new(1_000, wait_metric.clone());

        limiter.update_validators(HashSet::new(), HashSet::new());

        // Try with non-validators or unknown nodes.
        let handles = vec![
            limiter.create_handle(NodeId::random(&mut rng), Some(validator_id)),
            limiter.create_handle(NodeId::random(&mut rng), None),
        ];

        for handle in handles {
            let start = Instant::now();

            // Send 9_0001 bytes, should now finish instantly.
            handle.request_allowance(1000).await;
            handle.request_allowance(1000).await;
            handle.request_allowance(1000).await;
            handle.request_allowance(2000).await;
            handle.request_allowance(4000).await;
            handle.request_allowance(1).await;
            let end = Instant::now();

            let diff = end - start;
            assert!(diff <= SHORT_TIME);
        }

        // There should have been no time spent waiting.
        assert!(
            wait_metric.get() < SHORT_TIME.as_secs_f64(),
            "wait_metric is too large: {}",
            wait_metric.get()
        );
    }

    /// Regression test for #??? (fill in issue once created)
    #[tokio::test]
    async fn throttling_of_non_validators_does_not_affect_validators() {
        init_logging();

        let mut rng = crate::new_rng();

        let validator_id = PublicKey::random(&mut rng);
        let limiter = ClassBasedLimiter::new(1_000, new_wait_time_sec());

        let mut active_validators = HashSet::new();
        active_validators.insert(validator_id.clone());
        limiter.update_validators(active_validators, HashSet::new());

        let non_validator_handle = limiter.create_handle(NodeId::random(&mut rng), None);
        let validator_handle = limiter.create_handle(NodeId::random(&mut rng), Some(validator_id));

        // We request a large resource at once using a non-validator handle. At the same time,
        // validator requests should be still served, even while waiting for the long-delayed
        // request still blocking.
        let start = Instant::now();
        let background_nv_request = tokio::spawn(async move {
            non_validator_handle.request_allowance(5000).await;
            non_validator_handle.request_allowance(5000).await;

            Instant::now()
        });

        // Allow for a little bit of time to pass to ensure the background task is running.
        tokio::time::sleep(Duration::from_secs(1)).await;

        validator_handle.request_allowance(10000).await;
        validator_handle.request_allowance(10000).await;

        let v_finished = Instant::now();

        let nv_finished = background_nv_request
            .await
            .expect("failed to join background nv task");

        let nv_completed = nv_finished.duration_since(start);
        assert!(
            nv_completed >= Duration::from_millis(4500),
            "non-validator did not delay sufficiently: {:?}",
            nv_completed
        );

        let v_completed = v_finished.duration_since(start);
        assert!(
            v_completed <= Duration::from_millis(1500),
            "validator did not finish quickly enough: {:?}",
            v_completed
        );
    }
}
