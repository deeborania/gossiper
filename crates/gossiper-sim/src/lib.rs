#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! Simulation utilities for gossip protocol implementations.

use core::fmt;
use std::collections::BTreeMap;

use gossiper_core::{
    DeterministicRng, Effect, GossipConfig, GossipEvent, GossipMessage, GossipNode, InsertOutcome,
    MessageId, NodeId, RandomSource, Round, Rumor,
};
use gossiper_transport::{apply_effects, EffectReport, InMemoryTransport};

/// A node managed by a simulation cluster.
#[derive(Clone, Debug)]
pub struct SimNode<T> {
    node: GossipNode<T>,
    rng: DeterministicRng,
}

impl<T> SimNode<T> {
    /// Creates a simulated node.
    pub fn new(node: GossipNode<T>, rng: DeterministicRng) -> Self {
        Self { node, rng }
    }

    /// Returns the inner gossip node.
    pub fn node(&self) -> &GossipNode<T> {
        &self.node
    }

    /// Returns the mutable inner gossip node.
    pub fn node_mut(&mut self) -> &mut GossipNode<T> {
        &mut self.node
    }
}

/// A simulated network partition between two sets of nodes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NetworkPartition {
    side_a: Vec<NodeId>,
    side_b: Vec<NodeId>,
}

impl NetworkPartition {
    /// Creates a partition between two sets of nodes.
    pub fn new(side_a: Vec<NodeId>, side_b: Vec<NodeId>) -> Self {
        Self { side_a, side_b }
    }

    /// Returns the first side of the partition.
    pub fn side_a(&self) -> &[NodeId] {
        &self.side_a
    }

    /// Returns the second side of the partition.
    pub fn side_b(&self) -> &[NodeId] {
        &self.side_b
    }

    fn blocks(&self, source: &NodeId, target: &NodeId) -> bool {
        let source_in_a = self.side_a.contains(source);
        let source_in_b = self.side_b.contains(source);
        let target_in_a = self.side_a.contains(target);
        let target_in_b = self.side_b.contains(target);

        (source_in_a && target_in_b) || (source_in_b && target_in_a)
    }
}

/// A deterministic model of unreliable network behavior.
#[derive(Clone, Debug, PartialEq)]
pub struct NetworkModel {
    loss_rate: f64,
    duplicate_rate: f64,
    delay_rate: f64,
    max_delay_rounds: u64,
    partitions: Vec<NetworkPartition>,
}

impl NetworkModel {
    /// Creates a reliable network model.
    pub fn new() -> Self {
        Self {
            loss_rate: 0.0,
            duplicate_rate: 0.0,
            delay_rate: 0.0,
            max_delay_rounds: 0,
            partitions: Vec::new(),
        }
    }

    /// Returns the simulated packet loss rate.
    pub fn loss_rate(&self) -> f64 {
        self.loss_rate
    }

    /// Returns the simulated duplicate delivery rate.
    pub fn duplicate_rate(&self) -> f64 {
        self.duplicate_rate
    }

    /// Returns the simulated delayed delivery rate.
    pub fn delay_rate(&self) -> f64 {
        self.delay_rate
    }

    /// Returns the maximum number of rounds a delayed message may wait.
    pub fn max_delay_rounds(&self) -> u64 {
        self.max_delay_rounds
    }

    /// Returns active network partitions.
    pub fn partitions(&self) -> &[NetworkPartition] {
        &self.partitions
    }

    /// Returns this model with a simulated packet loss rate.
    pub fn with_loss_rate(mut self, loss_rate: f64) -> Result<Self, ClusterError> {
        if !valid_rate(loss_rate) {
            return Err(ClusterError::InvalidLossRate(loss_rate));
        }

        self.loss_rate = loss_rate;
        Ok(self)
    }

    /// Returns this model with a simulated duplicate delivery rate.
    pub fn with_duplicate_rate(mut self, duplicate_rate: f64) -> Result<Self, ClusterError> {
        if !valid_rate(duplicate_rate) {
            return Err(ClusterError::InvalidDuplicateRate(duplicate_rate));
        }

        self.duplicate_rate = duplicate_rate;
        Ok(self)
    }

    /// Returns this model with a simulated delayed delivery rate.
    pub fn with_delay_rate(
        mut self,
        delay_rate: f64,
        max_delay_rounds: u64,
    ) -> Result<Self, ClusterError> {
        if !valid_rate(delay_rate) {
            return Err(ClusterError::InvalidDelayRate(delay_rate));
        }

        self.delay_rate = delay_rate;
        self.max_delay_rounds = max_delay_rounds;
        Ok(self)
    }

    /// Returns this model with an added network partition.
    pub fn with_partition(mut self, partition: NetworkPartition) -> Self {
        self.partitions.push(partition);
        self
    }

    /// Returns this model with all network partitions removed.
    pub fn without_partitions(mut self) -> Self {
        self.partitions.clear();
        self
    }

    fn blocks(&self, source: &NodeId, target: &NodeId) -> bool {
        self.partitions
            .iter()
            .any(|partition| partition.blocks(source, target))
    }
}

impl Default for NetworkModel {
    fn default() -> Self {
        Self::new()
    }
}

fn valid_rate(rate: f64) -> bool {
    rate.is_finite() && (0.0..=1.0).contains(&rate)
}

fn mean_per_trial(total: usize, trials: usize) -> f64 {
    if trials == 0 {
        return 0.0;
    }

    total as f64 / trials as f64
}

fn ratio(numerator: usize, denominator: usize) -> f64 {
    if denominator == 0 {
        return 0.0;
    }

    numerator as f64 / denominator as f64
}

fn accepted_messages(attempted: usize, dropped: usize) -> usize {
    attempted.saturating_sub(dropped)
}

fn message_copies(attempted: usize, dropped: usize, duplicated: usize) -> usize {
    accepted_messages(attempted, dropped).saturating_add(duplicated)
}

fn generated_node_ids(node_count: usize) -> Vec<NodeId> {
    (0..node_count)
        .map(|index| NodeId::from(format!("node-{index}")))
        .collect()
}

#[derive(Clone, Debug)]
struct PendingSend<T> {
    source: NodeId,
    target: NodeId,
    message: GossipMessage<T>,
}

/// Summary of one simulated cluster tick.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TickReport<Event> {
    attempted: usize,
    sent: usize,
    dropped: usize,
    duplicated: usize,
    delayed: usize,
    received: usize,
    events: Vec<(NodeId, Event)>,
}

impl<Event> TickReport<Event> {
    /// Creates an empty tick report.
    pub fn new() -> Self {
        Self {
            attempted: 0,
            sent: 0,
            dropped: 0,
            duplicated: 0,
            delayed: 0,
            received: 0,
            events: Vec::new(),
        }
    }

    /// Returns the number of original send attempts during the tick.
    pub fn attempted(&self) -> usize {
        self.attempted
    }

    /// Returns the number of messages handed to the simulated transport during the tick.
    pub fn sent(&self) -> usize {
        self.sent
    }

    /// Returns the number of messages dropped by the simulated network during the tick.
    pub fn dropped(&self) -> usize {
        self.dropped
    }

    /// Returns the number of extra duplicate messages created during the tick.
    pub fn duplicated(&self) -> usize {
        self.duplicated
    }

    /// Returns the number of messages delayed for a future tick.
    pub fn delayed(&self) -> usize {
        self.delayed
    }

    /// Returns the number of messages delivered to receiving nodes during the tick.
    pub fn received(&self) -> usize {
        self.received
    }

    /// Returns original send attempts that were not dropped by the simulated network.
    pub fn accepted(&self) -> usize {
        accepted_messages(self.attempted, self.dropped)
    }

    /// Returns message copies created after duplicate expansion.
    pub fn message_copies(&self) -> usize {
        message_copies(self.attempted, self.dropped, self.duplicated)
    }

    /// Returns the observed fraction of original send attempts that were dropped.
    pub fn observed_drop_rate(&self) -> f64 {
        ratio(self.dropped, self.attempted)
    }

    /// Returns the observed fraction of accepted original sends that were duplicated.
    pub fn observed_duplicate_rate(&self) -> f64 {
        ratio(self.duplicated, self.accepted())
    }

    /// Returns the observed fraction of message copies delayed for a future tick.
    pub fn observed_delay_rate(&self) -> f64 {
        ratio(self.delayed, self.message_copies())
    }

    /// Returns the observed fraction of sent messages delivered to receiving nodes.
    pub fn observed_delivery_rate(&self) -> f64 {
        ratio(self.received, self.sent)
    }

    /// Returns the number of new-rumor events emitted during the tick.
    pub fn new_rumors(&self) -> usize {
        self.events.len()
    }

    /// Returns the observed fraction of received messages that produced new rumors.
    pub fn new_rumor_rate(&self) -> f64 {
        ratio(self.new_rumors(), self.received)
    }

    /// Returns events emitted during delivery, paired with the receiving node.
    pub fn events(&self) -> &[(NodeId, Event)] {
        &self.events
    }
}

impl<Event> Default for TickReport<Event> {
    fn default() -> Self {
        Self::new()
    }
}

/// Summary of running a simulation for multiple rounds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunReport<Event> {
    rounds_run: u64,
    attempted: usize,
    sent: usize,
    dropped: usize,
    duplicated: usize,
    delayed: usize,
    received: usize,
    events: Vec<(NodeId, Event)>,
}

impl<Event> RunReport<Event> {
    /// Creates an empty run report.
    pub fn new() -> Self {
        Self {
            rounds_run: 0,
            attempted: 0,
            sent: 0,
            dropped: 0,
            duplicated: 0,
            delayed: 0,
            received: 0,
            events: Vec::new(),
        }
    }

    /// Returns how many rounds were run.
    pub fn rounds_run(&self) -> u64 {
        self.rounds_run
    }

    /// Returns the number of original send attempts during the run.
    pub fn attempted(&self) -> usize {
        self.attempted
    }

    /// Returns the number of messages handed to the simulated transport during the run.
    pub fn sent(&self) -> usize {
        self.sent
    }

    /// Returns the number of messages dropped by the simulated network during the run.
    pub fn dropped(&self) -> usize {
        self.dropped
    }

    /// Returns the number of extra duplicate messages created during the run.
    pub fn duplicated(&self) -> usize {
        self.duplicated
    }

    /// Returns the number of messages delayed for future delivery during the run.
    pub fn delayed(&self) -> usize {
        self.delayed
    }

    /// Returns the number of messages delivered to receiving nodes during the run.
    pub fn received(&self) -> usize {
        self.received
    }

    /// Returns original send attempts that were not dropped by the simulated network.
    pub fn accepted(&self) -> usize {
        accepted_messages(self.attempted, self.dropped)
    }

    /// Returns message copies created after duplicate expansion.
    pub fn message_copies(&self) -> usize {
        message_copies(self.attempted, self.dropped, self.duplicated)
    }

    /// Returns the observed fraction of original send attempts that were dropped.
    pub fn observed_drop_rate(&self) -> f64 {
        ratio(self.dropped, self.attempted)
    }

    /// Returns the observed fraction of accepted original sends that were duplicated.
    pub fn observed_duplicate_rate(&self) -> f64 {
        ratio(self.duplicated, self.accepted())
    }

    /// Returns the observed fraction of message copies delayed for a future tick.
    pub fn observed_delay_rate(&self) -> f64 {
        ratio(self.delayed, self.message_copies())
    }

    /// Returns the observed fraction of sent messages delivered to receiving nodes.
    pub fn observed_delivery_rate(&self) -> f64 {
        ratio(self.received, self.sent)
    }

    /// Returns the number of new-rumor events emitted during the run.
    pub fn new_rumors(&self) -> usize {
        self.events.len()
    }

    /// Returns the observed fraction of received messages that produced new rumors.
    pub fn new_rumor_rate(&self) -> f64 {
        ratio(self.new_rumors(), self.received)
    }

    /// Returns events emitted during the run, paired with the receiving node.
    pub fn events(&self) -> &[(NodeId, Event)] {
        &self.events
    }

    fn record_tick(&mut self, tick: TickReport<Event>) {
        self.rounds_run += 1;
        self.attempted += tick.attempted;
        self.sent += tick.sent;
        self.dropped += tick.dropped;
        self.duplicated += tick.duplicated;
        self.delayed += tick.delayed;
        self.received += tick.received;
        self.events.extend(tick.events);
    }
}

impl<Event> Default for RunReport<Event> {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of running a cluster until a rumor reaches a target number of nodes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReachReport {
    reached: bool,
    rounds_run: u64,
    reached_nodes: usize,
    attempted: usize,
    sent: usize,
    dropped: usize,
    duplicated: usize,
    delayed: usize,
    received: usize,
    new_rumors: usize,
}

impl ReachReport {
    /// Creates a reach report.
    pub fn new(reached: bool, rounds_run: u64, reached_nodes: usize, sent: usize) -> Self {
        Self {
            reached,
            rounds_run,
            reached_nodes,
            attempted: 0,
            sent,
            dropped: 0,
            duplicated: 0,
            delayed: 0,
            received: 0,
            new_rumors: 0,
        }
    }

    fn from_run_report<Event>(
        reached: bool,
        reached_nodes: usize,
        report: RunReport<Event>,
    ) -> Self {
        Self {
            reached,
            rounds_run: report.rounds_run(),
            reached_nodes,
            attempted: report.attempted(),
            sent: report.sent(),
            dropped: report.dropped(),
            duplicated: report.duplicated(),
            delayed: report.delayed(),
            received: report.received(),
            new_rumors: report.new_rumors(),
        }
    }

    /// Returns `true` if the target reach was achieved.
    pub fn reached(&self) -> bool {
        self.reached
    }

    /// Returns how many rounds were run.
    pub fn rounds_run(&self) -> u64 {
        self.rounds_run
    }

    /// Returns how many nodes knew the rumor when the run ended.
    pub fn reached_nodes(&self) -> usize {
        self.reached_nodes
    }

    /// Returns the number of original send attempts while running.
    pub fn attempted(&self) -> usize {
        self.attempted
    }

    /// Returns the total number of messages sent while running.
    pub fn sent(&self) -> usize {
        self.sent
    }

    /// Returns the number of messages dropped by the simulated network while running.
    pub fn dropped(&self) -> usize {
        self.dropped
    }

    /// Returns the number of extra duplicate messages created while running.
    pub fn duplicated(&self) -> usize {
        self.duplicated
    }

    /// Returns the number of messages delayed for future delivery while running.
    pub fn delayed(&self) -> usize {
        self.delayed
    }

    /// Returns the number of messages delivered to receiving nodes while running.
    pub fn received(&self) -> usize {
        self.received
    }

    /// Returns original send attempts that were not dropped by the simulated network.
    pub fn accepted(&self) -> usize {
        accepted_messages(self.attempted, self.dropped)
    }

    /// Returns message copies created after duplicate expansion.
    pub fn message_copies(&self) -> usize {
        message_copies(self.attempted, self.dropped, self.duplicated)
    }

    /// Returns the observed fraction of original send attempts that were dropped.
    pub fn observed_drop_rate(&self) -> f64 {
        ratio(self.dropped, self.attempted)
    }

    /// Returns the observed fraction of accepted original sends that were duplicated.
    pub fn observed_duplicate_rate(&self) -> f64 {
        ratio(self.duplicated, self.accepted())
    }

    /// Returns the observed fraction of message copies delayed for a future tick.
    pub fn observed_delay_rate(&self) -> f64 {
        ratio(self.delayed, self.message_copies())
    }

    /// Returns the observed fraction of sent messages delivered to receiving nodes.
    pub fn observed_delivery_rate(&self) -> f64 {
        ratio(self.received, self.sent)
    }

    /// Returns the number of new-rumor events emitted while running.
    pub fn new_rumors(&self) -> usize {
        self.new_rumors
    }

    /// Returns the observed fraction of received messages that produced new rumors.
    pub fn new_rumor_rate(&self) -> f64 {
        ratio(self.new_rumors, self.received)
    }

    /// Asserts that the target reach was achieved.
    ///
    /// This is intended for simulator examples and tests where a failed
    /// convergence run should fail loudly.
    pub fn assert_reached(&self) {
        assert!(
            self.reached,
            "expected gossip to reach target, but only reached {} nodes after {} rounds",
            self.reached_nodes, self.rounds_run
        );
    }

    /// Asserts that the target reach was achieved within `max_rounds`.
    ///
    /// This is intended for simulator examples and tests where a convergence
    /// budget is part of the expected behavior.
    pub fn assert_reached_within(&self, max_rounds: u64) {
        self.assert_reached();
        assert!(
            self.rounds_run <= max_rounds,
            "expected gossip to reach target within {max_rounds} rounds, but it took {} rounds",
            self.rounds_run
        );
    }
}

/// Error returned when creating an invalid convergence experiment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExperimentError {
    /// Experiment must contain at least one node.
    ZeroNodeCount,

    /// Fanout must be greater than zero.
    ZeroFanout,

    /// Experiment must run at least one trial.
    ZeroTrials,
}

impl fmt::Display for ExperimentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroNodeCount => formatter.write_str("node_count must be greater than zero"),
            Self::ZeroFanout => formatter.write_str("fanout must be greater than zero"),
            Self::ZeroTrials => formatter.write_str("trials must be greater than zero"),
        }
    }
}

impl std::error::Error for ExperimentError {}

/// Configuration for running repeated convergence trials.
#[derive(Clone, Debug, PartialEq)]
pub struct ConvergenceExperiment {
    node_count: usize,
    config: GossipConfig,
    network: NetworkModel,
    max_rounds: u64,
    trials: usize,
    base_seed: u64,
}

impl ConvergenceExperiment {
    /// Creates a convergence experiment.
    pub fn new(
        node_count: usize,
        fanout: usize,
        max_rounds: u64,
        trials: usize,
    ) -> Result<Self, ExperimentError> {
        if node_count == 0 {
            return Err(ExperimentError::ZeroNodeCount);
        }

        if fanout == 0 {
            return Err(ExperimentError::ZeroFanout);
        }

        if trials == 0 {
            return Err(ExperimentError::ZeroTrials);
        }

        let config = GossipConfig::new(fanout, 1_024).expect("fanout was validated");

        Ok(Self {
            node_count,
            config,
            network: NetworkModel::new(),
            max_rounds,
            trials,
            base_seed: 1,
        })
    }

    /// Returns the gossip configuration used for each trial.
    pub fn config(&self) -> &GossipConfig {
        &self.config
    }

    /// Returns a copy of this experiment with a different gossip configuration.
    pub fn with_config(mut self, config: GossipConfig) -> Self {
        self.config = config;
        self
    }

    /// Returns the base seed used to derive per-trial seeds.
    pub fn base_seed(&self) -> u64 {
        self.base_seed
    }

    /// Returns a copy of this experiment with a different base seed.
    pub fn with_seed(mut self, base_seed: u64) -> Self {
        self.base_seed = base_seed;
        self
    }

    /// Returns the network model used for each trial.
    pub fn network_model(&self) -> &NetworkModel {
        &self.network
    }

    /// Returns a copy of this experiment with a different network model.
    pub fn with_network_model(mut self, network: NetworkModel) -> Self {
        self.network = network;
        self
    }

    /// Runs this convergence experiment.
    pub fn run(&self) -> ConvergenceReport {
        let mut successes = 0;
        let mut successful_rounds = Vec::new();
        let mut attempted = 0;
        let mut sent = 0;
        let mut dropped = 0;
        let mut duplicated = 0;
        let mut delayed = 0;
        let mut received = 0;

        for trial in 0..self.trials {
            let node_ids: Vec<_> = (0..self.node_count)
                .map(|index| NodeId::from(format!("trial-{trial}-node-{index}")))
                .collect();

            let config = self.config.clone();
            let origin = node_ids[0].clone();
            let rumor_id = MessageId::new(trial as u128 + 1);
            let rumor = Rumor::new(rumor_id, origin.clone(), Round::ZERO, "experiment");

            let seed = self.base_seed.saturating_add(trial as u64);
            let mut cluster =
                Cluster::with_seed(config, node_ids, seed).with_network_model(self.network.clone());

            cluster
                .insert_rumor(&origin, rumor)
                .expect("origin node should exist");

            let initial_reach = cluster.rumor_reach(rumor_id);
            let mut reached = initial_reach >= self.node_count;
            let mut rounds_run = 0;

            for round in 0..self.max_rounds {
                if reached {
                    break;
                }

                let tick_report = cluster.tick(Round::new(round));

                attempted += tick_report.attempted();
                sent += tick_report.sent();
                dropped += tick_report.dropped();
                duplicated += tick_report.duplicated();
                delayed += tick_report.delayed();
                received += tick_report.received();

                rounds_run = round + 1;
                reached = cluster.rumor_reach(rumor_id) >= self.node_count;
            }

            if reached {
                successes += 1;
                successful_rounds.push(rounds_run);
            }
        }

        ConvergenceReport::new(
            self.trials,
            successes,
            successful_rounds,
            attempted,
            sent,
            dropped,
            duplicated,
            delayed,
            received,
        )
    }
}

/// Aggregate result of repeated convergence trials.
#[derive(Clone, Debug, PartialEq)]
pub struct ConvergenceReport {
    trials: usize,
    successes: usize,
    successful_rounds: Vec<u64>,
    attempted: usize,
    sent: usize,
    dropped: usize,
    duplicated: usize,
    delayed: usize,
    received: usize,
}

impl ConvergenceReport {
    /// Creates a convergence report.
    pub fn new(
        trials: usize,
        successes: usize,
        successful_rounds: Vec<u64>,
        attempted: usize,
        sent: usize,
        dropped: usize,
        duplicated: usize,
        delayed: usize,
        received: usize,
    ) -> Self {
        Self {
            trials,
            successes,
            successful_rounds,
            attempted,
            sent,
            dropped,
            duplicated,
            delayed,
            received,
        }
    }

    /// Returns the total number of trials.
    pub fn trials(&self) -> usize {
        self.trials
    }

    /// Returns the number of successful trials.
    pub fn successes(&self) -> usize {
        self.successes
    }

    /// Returns the number of failed trials.
    pub fn failures(&self) -> usize {
        self.trials.saturating_sub(self.successes)
    }

    /// Returns the fraction of trials that succeeded.
    pub fn success_rate(&self) -> f64 {
        if self.trials == 0 {
            return 0.0;
        }

        self.successes as f64 / self.trials as f64
    }

    /// Returns the fraction of trials that did not converge.
    pub fn failure_rate(&self) -> f64 {
        ratio(self.failures(), self.trials)
    }

    /// Returns the successful round counts.
    pub fn successful_rounds(&self) -> &[u64] {
        &self.successful_rounds
    }

    /// Returns the number of original send attempts across all trials.
    pub fn attempted(&self) -> usize {
        self.attempted
    }

    /// Returns the number of messages handed to simulated transports across all trials.
    pub fn sent(&self) -> usize {
        self.sent
    }

    /// Returns the number of messages dropped by simulated networks across all trials.
    pub fn dropped(&self) -> usize {
        self.dropped
    }

    /// Returns the number of extra duplicate messages created across all trials.
    pub fn duplicated(&self) -> usize {
        self.duplicated
    }

    /// Returns the number of messages delayed for future delivery across all trials.
    pub fn delayed(&self) -> usize {
        self.delayed
    }

    /// Returns the number of messages delivered to receiving nodes across all trials.
    pub fn received(&self) -> usize {
        self.received
    }

    /// Returns original send attempts that were not dropped by the simulated network.
    pub fn accepted(&self) -> usize {
        accepted_messages(self.attempted, self.dropped)
    }

    /// Returns message copies created after duplicate expansion.
    pub fn message_copies(&self) -> usize {
        message_copies(self.attempted, self.dropped, self.duplicated)
    }

    /// Returns the observed fraction of original send attempts that were dropped.
    pub fn observed_drop_rate(&self) -> f64 {
        ratio(self.dropped, self.attempted)
    }

    /// Returns the observed fraction of accepted original sends that were duplicated.
    pub fn observed_duplicate_rate(&self) -> f64 {
        ratio(self.duplicated, self.accepted())
    }

    /// Returns the observed fraction of message copies delayed for a future tick.
    pub fn observed_delay_rate(&self) -> f64 {
        ratio(self.delayed, self.message_copies())
    }

    /// Returns the observed fraction of sent messages delivered to receiving nodes.
    pub fn observed_delivery_rate(&self) -> f64 {
        ratio(self.received, self.sent)
    }

    /// Returns the mean original send attempts per trial.
    pub fn mean_attempted_per_trial(&self) -> f64 {
        mean_per_trial(self.attempted, self.trials)
    }

    /// Returns the mean sent messages per trial.
    pub fn mean_sent_per_trial(&self) -> f64 {
        mean_per_trial(self.sent, self.trials)
    }

    /// Returns the mean dropped messages per trial.
    pub fn mean_dropped_per_trial(&self) -> f64 {
        mean_per_trial(self.dropped, self.trials)
    }

    /// Returns the mean duplicate messages per trial.
    pub fn mean_duplicated_per_trial(&self) -> f64 {
        mean_per_trial(self.duplicated, self.trials)
    }

    /// Returns the mean delayed messages per trial.
    pub fn mean_delayed_per_trial(&self) -> f64 {
        mean_per_trial(self.delayed, self.trials)
    }

    /// Returns the mean received messages per trial.
    pub fn mean_received_per_trial(&self) -> f64 {
        mean_per_trial(self.received, self.trials)
    }

    /// Returns the mean number of rounds among successful trials.
    pub fn mean_successful_rounds(&self) -> Option<f64> {
        if self.successful_rounds.is_empty() {
            return None;
        }

        let total: u64 = self.successful_rounds.iter().sum();

        Some(total as f64 / self.successful_rounds.len() as f64)
    }

    /// Returns a nearest-rank percentile of successful round counts.
    ///
    /// Returns `None` when there are no successful trials or when `percentile`
    /// is outside `0..=100`.
    pub fn percentile_successful_rounds(&self, percentile: f64) -> Option<u64> {
        if self.successful_rounds.is_empty() || !(0.0..=100.0).contains(&percentile) {
            return None;
        }

        if percentile.is_nan() {
            return None;
        }

        let mut rounds = self.successful_rounds.clone();
        rounds.sort_unstable();

        let rank = ((percentile / 100.0) * rounds.len() as f64).ceil() as usize;
        let index = rank.saturating_sub(1);

        rounds.get(index).copied()
    }
}

/// A named convergence experiment.
#[derive(Clone, Debug, PartialEq)]
pub struct ConvergenceScenario {
    label: String,
    experiment: ConvergenceExperiment,
}

impl ConvergenceScenario {
    /// Creates a named convergence scenario.
    pub fn new(label: impl Into<String>, experiment: ConvergenceExperiment) -> Self {
        Self {
            label: label.into(),
            experiment,
        }
    }

    /// Returns the scenario label.
    pub fn label(&self) -> &str {
        &self.label
    }

    /// Returns the convergence experiment for this scenario.
    pub fn experiment(&self) -> &ConvergenceExperiment {
        &self.experiment
    }

    fn run(self) -> ConvergenceScenarioReport {
        ConvergenceScenarioReport::new(self.label, self.experiment.run())
    }
}

/// Runs named convergence experiments and keeps their reports together.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ConvergenceComparison {
    scenarios: Vec<ConvergenceScenario>,
}

impl ConvergenceComparison {
    /// Creates an empty convergence comparison.
    pub fn new() -> Self {
        Self {
            scenarios: Vec::new(),
        }
    }

    /// Adds a named experiment to this comparison.
    pub fn add(mut self, label: impl Into<String>, experiment: ConvergenceExperiment) -> Self {
        self.scenarios
            .push(ConvergenceScenario::new(label, experiment));
        self
    }

    /// Adds an already-created scenario to this comparison.
    pub fn add_scenario(mut self, scenario: ConvergenceScenario) -> Self {
        self.scenarios.push(scenario);
        self
    }

    /// Returns named scenarios that will be run.
    pub fn scenarios(&self) -> &[ConvergenceScenario] {
        &self.scenarios
    }

    /// Returns the number of scenarios in this comparison.
    pub fn len(&self) -> usize {
        self.scenarios.len()
    }

    /// Returns `true` if this comparison contains no scenarios.
    pub fn is_empty(&self) -> bool {
        self.scenarios.is_empty()
    }

    /// Runs every scenario and returns their reports.
    pub fn run(self) -> ConvergenceComparisonReport {
        let results = self
            .scenarios
            .into_iter()
            .map(ConvergenceScenario::run)
            .collect();

        ConvergenceComparisonReport::new(results)
    }
}

/// Aggregate result of running a convergence comparison.
#[derive(Clone, Debug, PartialEq)]
pub struct ConvergenceComparisonReport {
    results: Vec<ConvergenceScenarioReport>,
}

impl ConvergenceComparisonReport {
    /// Creates a convergence comparison report from scenario reports.
    pub fn new(results: Vec<ConvergenceScenarioReport>) -> Self {
        Self { results }
    }

    /// Returns reports for every scenario in insertion order.
    pub fn results(&self) -> &[ConvergenceScenarioReport] {
        &self.results
    }

    /// Returns the number of scenario reports.
    pub fn len(&self) -> usize {
        self.results.len()
    }

    /// Returns `true` if this report contains no scenario reports.
    pub fn is_empty(&self) -> bool {
        self.results.is_empty()
    }
}

/// Result of running one named convergence scenario.
#[derive(Clone, Debug, PartialEq)]
pub struct ConvergenceScenarioReport {
    label: String,
    report: ConvergenceReport,
}

impl ConvergenceScenarioReport {
    /// Creates a named convergence scenario report.
    pub fn new(label: impl Into<String>, report: ConvergenceReport) -> Self {
        Self {
            label: label.into(),
            report,
        }
    }

    /// Returns the scenario label.
    pub fn label(&self) -> &str {
        &self.label
    }

    /// Returns the convergence report for this scenario.
    pub fn report(&self) -> &ConvergenceReport {
        &self.report
    }
}

/// Builder for deterministic simulation clusters.
#[derive(Clone, Debug, PartialEq)]
pub struct ClusterBuilder {
    config: GossipConfig,
    node_ids: Vec<NodeId>,
    seed: u64,
    network: NetworkModel,
}

impl ClusterBuilder {
    /// Creates a cluster builder with no nodes, seed `1`, and a reliable network.
    pub fn new(config: GossipConfig) -> Self {
        Self {
            config,
            node_ids: Vec::new(),
            seed: 1,
            network: NetworkModel::new(),
        }
    }

    /// Returns the gossip configuration used for built clusters.
    pub fn config(&self) -> &GossipConfig {
        &self.config
    }

    /// Returns node IDs that will be used for built clusters.
    pub fn node_ids(&self) -> &[NodeId] {
        &self.node_ids
    }

    /// Returns the seed used for deterministic node and network randomness.
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Returns the simulated network model used for built clusters.
    pub fn network_model(&self) -> &NetworkModel {
        &self.network
    }

    /// Returns this builder with generated node IDs.
    ///
    /// Generated IDs are `node-0`, `node-1`, `node-2`, and so on.
    pub fn with_node_count(mut self, node_count: usize) -> Self {
        self.node_ids = generated_node_ids(node_count);
        self
    }

    /// Returns this builder with explicit node IDs.
    pub fn with_node_ids(mut self, node_ids: Vec<NodeId>) -> Self {
        self.node_ids = node_ids;
        self
    }

    /// Returns this builder with a different deterministic seed.
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// Returns this builder with a different network model.
    pub fn with_network_model(mut self, network: NetworkModel) -> Self {
        self.network = network;
        self
    }

    /// Returns this builder with a simulated packet loss rate.
    pub fn with_loss_rate(mut self, loss_rate: f64) -> Result<Self, ClusterError> {
        self.network = self.network.with_loss_rate(loss_rate)?;
        Ok(self)
    }

    /// Returns this builder with a simulated duplicate delivery rate.
    pub fn with_duplicate_rate(mut self, duplicate_rate: f64) -> Result<Self, ClusterError> {
        self.network = self.network.with_duplicate_rate(duplicate_rate)?;
        Ok(self)
    }

    /// Returns this builder with a simulated delayed delivery rate.
    pub fn with_delay_rate(
        mut self,
        delay_rate: f64,
        max_delay_rounds: u64,
    ) -> Result<Self, ClusterError> {
        self.network = self.network.with_delay_rate(delay_rate, max_delay_rounds)?;
        Ok(self)
    }

    /// Returns this builder with an added network partition.
    pub fn with_partition(mut self, partition: NetworkPartition) -> Self {
        self.network = self.network.with_partition(partition);
        self
    }

    /// Returns this builder with all network partitions removed.
    pub fn without_partitions(mut self) -> Self {
        self.network = self.network.without_partitions();
        self
    }

    /// Builds a cluster where every node peers with every other node.
    pub fn fully_connected<T>(self) -> Cluster<T> {
        let Self {
            config,
            node_ids,
            seed,
            network,
        } = self;

        Cluster::with_seed(config, node_ids, seed).with_network_model(network)
    }

    /// Builds a line topology cluster.
    ///
    /// Each node peers with its immediate neighbor or neighbors in the builder's
    /// node-ID order.
    pub fn line<T>(self) -> Cluster<T> {
        let node_ids = self.node_ids.clone();
        let mut cluster = self.fully_connected();

        for index in 0..node_ids.len() {
            let node_id = &node_ids[index];
            let mut peers = Vec::new();

            if index > 0 {
                peers.push(node_ids[index - 1].clone());
            }

            if index + 1 < node_ids.len() {
                peers.push(node_ids[index + 1].clone());
            }

            if let Some(node) = cluster.node_mut(node_id) {
                node.set_peers(peers);
            }
        }

        cluster
    }
}

/// Error returned when operating on a simulation cluster.
#[derive(Clone, Debug, PartialEq)]
pub enum ClusterError {
    /// The requested node does not exist in the cluster.
    UnknownNode(NodeId),

    /// Packet loss rate must be finite and inside `0.0..=1.0`.
    InvalidLossRate(f64),

    /// Duplicate delivery rate must be finite and inside `0.0..=1.0`.
    InvalidDuplicateRate(f64),

    /// Delayed delivery rate must be finite and inside `0.0..=1.0`.
    InvalidDelayRate(f64),
}

impl fmt::Display for ClusterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownNode(node_id) => write!(formatter, "unknown node: {node_id}"),
            Self::InvalidLossRate(loss_rate) => {
                write!(formatter, "invalid loss rate: {loss_rate}")
            }
            Self::InvalidDuplicateRate(duplicate_rate) => {
                write!(formatter, "invalid duplicate rate: {duplicate_rate}")
            }
            Self::InvalidDelayRate(delay_rate) => {
                write!(formatter, "invalid delay rate: {delay_rate}")
            }
        }
    }
}

impl std::error::Error for ClusterError {}

/// A deterministic in-memory simulation cluster.
#[derive(Clone, Debug)]
pub struct Cluster<T> {
    nodes: BTreeMap<NodeId, SimNode<T>>,
    transport: InMemoryTransport<GossipMessage<T>>,
    network: NetworkModel,
    network_rng: DeterministicRng,
    delayed_messages: BTreeMap<u64, Vec<PendingSend<T>>>,
}

impl<T> Cluster<T> {
    /// Creates a cluster where each node knows every other node as a peer.
    pub fn new(config: GossipConfig, node_ids: Vec<NodeId>) -> Self {
        Self::with_seed(config, node_ids, 1)
    }

    /// Creates a fully connected cluster with generated node IDs.
    pub fn fully_connected(config: GossipConfig, node_count: usize) -> Self {
        ClusterBuilder::new(config)
            .with_node_count(node_count)
            .fully_connected()
    }

    /// Creates a fully connected cluster with generated node IDs and a deterministic seed.
    pub fn fully_connected_with_seed(config: GossipConfig, node_count: usize, seed: u64) -> Self {
        ClusterBuilder::new(config)
            .with_node_count(node_count)
            .with_seed(seed)
            .fully_connected()
    }

    /// Creates a line topology cluster with generated node IDs.
    ///
    /// Each node peers with its immediate neighbor or neighbors:
    ///
    /// `node-0 <-> node-1 <-> node-2 <-> ...`
    pub fn line(config: GossipConfig, node_count: usize) -> Self {
        Self::line_with_seed(config, node_count, 1)
    }

    /// Creates a line topology cluster with generated node IDs and a deterministic seed.
    pub fn line_with_seed(config: GossipConfig, node_count: usize, seed: u64) -> Self {
        ClusterBuilder::new(config)
            .with_node_count(node_count)
            .with_seed(seed)
            .line()
    }

    /// Creates a cluster with deterministic per-node RNG seeds derived from `seed`.
    pub fn with_seed(config: GossipConfig, node_ids: Vec<NodeId>, seed: u64) -> Self {
        let mut nodes = BTreeMap::new();

        for (index, node_id) in node_ids.iter().cloned().enumerate() {
            let peers: Vec<_> = node_ids
                .iter()
                .filter(|peer_id| *peer_id != &node_id)
                .cloned()
                .collect();

            let mut node = GossipNode::new(node_id.clone(), config.clone());
            node.set_peers(peers);

            let node_seed = seed.saturating_add(index as u64);

            nodes.insert(
                node_id,
                SimNode::new(node, DeterministicRng::new(node_seed)),
            );
        }

        Self {
            nodes,
            transport: InMemoryTransport::new(),
            network: NetworkModel::new(),
            network_rng: DeterministicRng::new(seed.saturating_add(node_ids.len() as u64)),
            delayed_messages: BTreeMap::new(),
        }
    }

    /// Returns the simulated network model.
    pub fn network_model(&self) -> &NetworkModel {
        &self.network
    }

    /// Returns a copy of this cluster with a different network model.
    pub fn with_network_model(mut self, network: NetworkModel) -> Self {
        self.network = network;
        self
    }

    /// Returns the simulated packet loss rate.
    pub fn loss_rate(&self) -> f64 {
        self.network.loss_rate()
    }

    /// Returns a copy of this cluster with a simulated packet loss rate.
    ///
    /// `0.0` means no messages are dropped. `1.0` means every sent message is dropped.
    pub fn with_loss_rate(mut self, loss_rate: f64) -> Result<Self, ClusterError> {
        self.network = self.network.with_loss_rate(loss_rate)?;
        Ok(self)
    }

    /// Returns the simulated duplicate delivery rate.
    pub fn duplicate_rate(&self) -> f64 {
        self.network.duplicate_rate()
    }

    /// Returns a copy of this cluster with a simulated duplicate delivery rate.
    ///
    /// `0.0` means messages are never duplicated. `1.0` means every delivered
    /// message is delivered twice.
    pub fn with_duplicate_rate(mut self, duplicate_rate: f64) -> Result<Self, ClusterError> {
        self.network = self.network.with_duplicate_rate(duplicate_rate)?;
        Ok(self)
    }

    /// Returns the simulated delayed delivery rate.
    pub fn delay_rate(&self) -> f64 {
        self.network.delay_rate()
    }

    /// Returns the maximum simulated delivery delay in rounds.
    pub fn max_delay_rounds(&self) -> u64 {
        self.network.max_delay_rounds()
    }

    /// Returns a copy of this cluster with simulated delayed delivery.
    ///
    /// `0.0` means messages are never delayed. `1.0` means every delivered
    /// message is delayed by at least one future tick when `max_delay_rounds` is
    /// greater than zero.
    pub fn with_delay_rate(
        mut self,
        delay_rate: f64,
        max_delay_rounds: u64,
    ) -> Result<Self, ClusterError> {
        self.network = self.network.with_delay_rate(delay_rate, max_delay_rounds)?;
        Ok(self)
    }

    /// Returns active network partitions.
    pub fn partitions(&self) -> &[NetworkPartition] {
        self.network.partitions()
    }

    /// Returns how many messages are currently queued for delayed delivery.
    pub fn pending_delayed_count(&self) -> usize {
        self.delayed_messages.values().map(Vec::len).sum()
    }

    /// Returns delayed-delivery queue sizes by due round.
    pub fn pending_delayed_rounds(&self) -> impl Iterator<Item = (Round, usize)> + '_ {
        self.delayed_messages
            .iter()
            .map(|(round, messages)| (Round::new(*round), messages.len()))
    }

    /// Returns a copy of this cluster with an added network partition.
    pub fn with_partition(mut self, partition: NetworkPartition) -> Self {
        self.network = self.network.with_partition(partition);
        self
    }

    /// Returns a copy of this cluster with all network partitions removed.
    pub fn without_partitions(mut self) -> Self {
        self.network = self.network.without_partitions();
        self
    }

    /// Returns a node by ID.
    pub fn node(&self, node_id: &NodeId) -> Option<&GossipNode<T>> {
        self.nodes.get(node_id).map(SimNode::node)
    }

    /// Returns a mutable node by ID.
    pub fn node_mut(&mut self, node_id: &NodeId) -> Option<&mut GossipNode<T>> {
        self.nodes.get_mut(node_id).map(SimNode::node_mut)
    }

    /// Returns the number of nodes in the cluster.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Returns node IDs in deterministic order.
    pub fn node_ids(&self) -> impl Iterator<Item = &NodeId> {
        self.nodes.keys()
    }

    /// Inserts a rumor into one node.
    pub fn insert_rumor(
        &mut self,
        node_id: &NodeId,
        rumor: Rumor<T>,
    ) -> Result<InsertOutcome, ClusterError> {
        let Some(node) = self.node_mut(node_id) else {
            return Err(ClusterError::UnknownNode(node_id.clone()));
        };

        Ok(node.insert_rumor(rumor))
    }

    /// Publishes a local rumor from one node.
    pub fn publish(
        &mut self,
        node_id: &NodeId,
        rumor_id: MessageId,
        round: Round,
        payload: T,
    ) -> Result<InsertOutcome, ClusterError> {
        let Some(node) = self.node_mut(node_id) else {
            return Err(ClusterError::UnknownNode(node_id.clone()));
        };

        Ok(node.publish(rumor_id, round, payload))
    }
}

impl<T: Clone> Cluster<T> {
    fn should_drop(&mut self) -> bool {
        if self.network.loss_rate() == 0.0 {
            return false;
        }

        if self.network.loss_rate() == 1.0 {
            return true;
        }

        let sample = self.network_rng.next_u64() as f64 / u64::MAX as f64;
        sample < self.network.loss_rate()
    }

    fn should_duplicate(&mut self) -> bool {
        if self.network.duplicate_rate() == 0.0 {
            return false;
        }

        if self.network.duplicate_rate() == 1.0 {
            return true;
        }

        let sample = self.network_rng.next_u64() as f64 / u64::MAX as f64;
        sample < self.network.duplicate_rate()
    }

    fn should_delay(&mut self) -> bool {
        if self.network.max_delay_rounds() == 0 || self.network.delay_rate() == 0.0 {
            return false;
        }

        if self.network.delay_rate() == 1.0 {
            return true;
        }

        let sample = self.network_rng.next_u64() as f64 / u64::MAX as f64;
        sample < self.network.delay_rate()
    }

    fn delay_rounds(&mut self) -> u64 {
        let max_delay_rounds = self.network.max_delay_rounds();

        if max_delay_rounds == 0 {
            return 0;
        }

        (self.network_rng.next_u64() % max_delay_rounds).saturating_add(1)
    }

    fn drain_due_messages(&mut self, round: Round) -> Vec<PendingSend<T>> {
        let due_rounds: Vec<_> = self
            .delayed_messages
            .keys()
            .copied()
            .take_while(|due_round| *due_round <= round.get())
            .collect();
        let mut due_messages = Vec::new();

        for due_round in due_rounds {
            if let Some(mut messages) = self.delayed_messages.remove(&due_round) {
                due_messages.append(&mut messages);
            }
        }

        due_messages
    }

    fn send_to_transport(&mut self, sends: Vec<PendingSend<T>>) -> usize {
        let effects = sends.into_iter().map(|send| Effect::Send {
            target: send.target,
            message: send.message,
        });
        let transport_report: EffectReport<()> = apply_effects(&mut self.transport, effects);

        transport_report.sent()
    }

    fn process_outbound_send(
        &mut self,
        send: PendingSend<T>,
        round: Round,
        report: &mut TickReport<GossipEvent<T>>,
        immediate_sends: &mut Vec<PendingSend<T>>,
    ) {
        report.attempted += 1;

        if self.network.blocks(&send.source, &send.target) || self.should_drop() {
            report.dropped += 1;
            return;
        }

        let duplicate = self.should_duplicate();

        if duplicate {
            report.duplicated += 1;
        }

        let copies = if duplicate {
            vec![send.clone(), send]
        } else {
            vec![send]
        };

        for copy in copies {
            if self.should_delay() {
                let due_round = round.get().saturating_add(self.delay_rounds());
                self.delayed_messages
                    .entry(due_round)
                    .or_default()
                    .push(copy);
                report.delayed += 1;
            } else {
                immediate_sends.push(copy);
            }
        }
    }

    /// Runs one simulation round.
    pub fn tick(&mut self, round: Round) -> TickReport<GossipEvent<T>> {
        let mut report = TickReport::new();
        let due_messages = self.drain_due_messages(round);
        let mut immediate_sends = due_messages;

        let node_ids: Vec<_> = self.nodes.keys().cloned().collect();
        let mut pending_sends = Vec::new();

        for node_id in &node_ids {
            let Some(sim_node) = self.nodes.get_mut(node_id) else {
                continue;
            };
            let effects = sim_node.node.tick(&mut sim_node.rng, round);

            for effect in effects {
                if let Effect::Send { target, message } = effect {
                    pending_sends.push(PendingSend {
                        source: node_id.clone(),
                        target,
                        message,
                    });
                }
            }
        }

        for send in pending_sends {
            self.process_outbound_send(send, round, &mut report, &mut immediate_sends);
        }

        report.sent += self.send_to_transport(immediate_sends);
        for node_id in &node_ids {
            let messages = self.transport.drain(&node_id);

            if let Some(sim_node) = self.nodes.get_mut(node_id) {
                for message in messages {
                    report.received += 1;

                    for effect in sim_node.node.receive(message) {
                        if let Effect::Emit(event) = effect {
                            report.events.push((node_id.clone(), event));
                        }
                    }
                }
            }
        }

        report
    }

    /// Returns how many nodes know a rumor ID.
    pub fn rumor_reach(&self, rumor_id: MessageId) -> usize {
        self.nodes
            .values()
            .filter(|node| node.node.contains_rumor(rumor_id))
            .count()
    }

    /// Returns node IDs that know a rumor ID.
    pub fn known_by(&self, rumor_id: MessageId) -> Vec<NodeId> {
        self.nodes
            .iter()
            .filter_map(|(node_id, node)| {
                node.node.contains_rumor(rumor_id).then(|| node_id.clone())
            })
            .collect()
    }

    /// Returns node IDs that do not know a rumor ID.
    pub fn unknown_by(&self, rumor_id: MessageId) -> Vec<NodeId> {
        self.nodes
            .iter()
            .filter_map(|(node_id, node)| {
                (!node.node.contains_rumor(rumor_id)).then(|| node_id.clone())
            })
            .collect()
    }

    /// Returns how many nodes do not know a rumor ID.
    pub fn missing_count(&self, rumor_id: MessageId) -> usize {
        self.node_count() - self.rumor_reach(rumor_id)
    }

    /// Returns `true` if every node in the cluster knows a rumor ID.
    ///
    /// Returns `false` for an empty cluster.
    pub fn all_know(&self, rumor_id: MessageId) -> bool {
        !self.nodes.is_empty() && self.rumor_reach(rumor_id) == self.node_count()
    }

    /// Runs a fixed number of simulation rounds.
    pub fn run_for_rounds(&mut self, start_round: Round, rounds: u64) -> RunReport<GossipEvent<T>> {
        let mut report = RunReport::new();

        for offset in 0..rounds {
            let round = Round::new(start_round.get().saturating_add(offset));
            report.record_tick(self.tick(round));
        }

        report
    }

    /// Runs rounds until a rumor reaches at least `target_reach` nodes or the budget is exhausted.
    pub fn run_until_reached(
        &mut self,
        rumor_id: MessageId,
        target_reach: usize,
        max_rounds: u64,
    ) -> ReachReport {
        let initial_reach = self.rumor_reach(rumor_id);

        if initial_reach >= target_reach {
            return ReachReport::new(true, 0, initial_reach, 0);
        }

        let mut run_report = RunReport::new();

        for round in 0..max_rounds {
            let tick_report = self.tick(Round::new(round));
            run_report.record_tick(tick_report);

            let reached_nodes = self.rumor_reach(rumor_id);

            if reached_nodes >= target_reach {
                return ReachReport::from_run_report(true, reached_nodes, run_report);
            }
        }

        ReachReport::from_run_report(false, self.rumor_reach(rumor_id), run_report)
    }

    /// Asserts that every node learns a rumor within `max_rounds`.
    ///
    /// Returns the successful reach report so callers can inspect how many rounds
    /// and messages were needed.
    pub fn assert_reaches_all_within(
        &mut self,
        rumor_id: MessageId,
        max_rounds: u64,
    ) -> ReachReport {
        let report = self.run_until_reached(rumor_id, self.node_count(), max_rounds);
        report.assert_reached_within(max_rounds);
        report
    }

    /// Asserts that at least `target_reach` nodes learn a rumor within `max_rounds`.
    ///
    /// Returns the successful reach report so callers can inspect how many rounds
    /// and messages were needed.
    pub fn assert_reaches_at_least_within(
        &mut self,
        rumor_id: MessageId,
        target_reach: usize,
        max_rounds: u64,
    ) -> ReachReport {
        let report = self.run_until_reached(rumor_id, target_reach, max_rounds);
        report.assert_reached_within(max_rounds);
        report
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Cluster, ClusterBuilder, ClusterError, ConvergenceComparison, ConvergenceExperiment,
        ConvergenceReport, ConvergenceScenario, ExperimentError, NetworkModel, NetworkPartition,
        ReachReport, RunReport,
    };
    use gossiper_core::{GossipConfig, InsertOutcome, MessageId, NodeId, Round};

    #[test]
    fn cluster_connects_all_nodes_as_peers() {
        let cluster: Cluster<&str> = Cluster::new(
            GossipConfig::new(1, 10).expect("valid config"),
            vec![
                NodeId::from("node-a"),
                NodeId::from("node-b"),
                NodeId::from("node-c"),
            ],
        );

        assert_eq!(
            cluster
                .node(&NodeId::from("node-a"))
                .expect("node should exist")
                .peers(),
            &[NodeId::from("node-b"), NodeId::from("node-c")]
        );
    }

    #[test]
    fn cluster_exposes_node_count_and_ids() {
        let cluster: Cluster<&str> = Cluster::new(
            GossipConfig::new(1, 10).expect("valid config"),
            vec![
                NodeId::from("node-c"),
                NodeId::from("node-a"),
                NodeId::from("node-b"),
            ],
        );

        let node_ids: Vec<_> = cluster.node_ids().cloned().collect();

        assert_eq!(cluster.node_count(), 3);
        assert_eq!(
            node_ids,
            vec![
                NodeId::from("node-a"),
                NodeId::from("node-b"),
                NodeId::from("node-c"),
            ]
        );
    }

    #[test]
    fn fully_connected_generates_node_ids() {
        let cluster: Cluster<&str> =
            Cluster::fully_connected(GossipConfig::new(1, 10).expect("valid config"), 3);

        let node_ids: Vec<_> = cluster.node_ids().cloned().collect();

        assert_eq!(
            node_ids,
            vec![
                NodeId::from("node-0"),
                NodeId::from("node-1"),
                NodeId::from("node-2"),
            ]
        );

        assert_eq!(
            cluster
                .node(&NodeId::from("node-0"))
                .expect("node should exist")
                .peers(),
            &[NodeId::from("node-1"), NodeId::from("node-2")]
        );
    }

    #[test]
    fn line_generates_neighbor_peers() {
        let cluster: Cluster<&str> =
            Cluster::line(GossipConfig::new(1, 10).expect("valid config"), 4);

        assert_eq!(
            cluster
                .node(&NodeId::from("node-0"))
                .expect("node should exist")
                .peers(),
            &[NodeId::from("node-1")]
        );
        assert_eq!(
            cluster
                .node(&NodeId::from("node-1"))
                .expect("node should exist")
                .peers(),
            &[NodeId::from("node-0"), NodeId::from("node-2")]
        );
        assert_eq!(
            cluster
                .node(&NodeId::from("node-3"))
                .expect("node should exist")
                .peers(),
            &[NodeId::from("node-2")]
        );
    }

    #[test]
    fn cluster_builder_builds_fully_connected_cluster() {
        let network = NetworkModel::new()
            .with_loss_rate(0.25)
            .expect("valid loss rate");

        let cluster: Cluster<&str> =
            ClusterBuilder::new(GossipConfig::new(1, 10).expect("valid config"))
                .with_node_count(3)
                .with_seed(42)
                .with_network_model(network)
                .fully_connected();

        assert_eq!(cluster.node_count(), 3);
        assert_eq!(cluster.loss_rate(), 0.25);
        assert_eq!(
            cluster
                .node(&NodeId::from("node-0"))
                .expect("node should exist")
                .peers(),
            &[NodeId::from("node-1"), NodeId::from("node-2")]
        );
    }

    #[test]
    fn cluster_builder_builds_line_with_custom_node_ids() {
        let cluster: Cluster<&str> =
            ClusterBuilder::new(GossipConfig::new(1, 10).expect("valid config"))
                .with_node_ids(vec![
                    NodeId::from("alpha"),
                    NodeId::from("beta"),
                    NodeId::from("gamma"),
                ])
                .line();

        assert_eq!(
            cluster
                .node(&NodeId::from("alpha"))
                .expect("node should exist")
                .peers(),
            &[NodeId::from("beta")]
        );
        assert_eq!(
            cluster
                .node(&NodeId::from("beta"))
                .expect("node should exist")
                .peers(),
            &[NodeId::from("alpha"), NodeId::from("gamma")]
        );
        assert_eq!(
            cluster
                .node(&NodeId::from("gamma"))
                .expect("node should exist")
                .peers(),
            &[NodeId::from("beta")]
        );
    }

    #[test]
    fn cluster_builder_network_helpers_validate_rates() {
        let builder = ClusterBuilder::new(GossipConfig::new(1, 10).expect("valid config"));

        assert_eq!(
            builder
                .clone()
                .with_loss_rate(1.1)
                .expect_err("invalid loss rate should fail"),
            ClusterError::InvalidLossRate(1.1)
        );
        assert_eq!(
            builder
                .clone()
                .with_duplicate_rate(-0.1)
                .expect_err("invalid duplicate rate should fail"),
            ClusterError::InvalidDuplicateRate(-0.1)
        );
        assert_eq!(
            builder
                .with_delay_rate(f64::INFINITY, 1)
                .expect_err("invalid delay rate should fail"),
            ClusterError::InvalidDelayRate(f64::INFINITY)
        );
    }

    #[test]
    fn cluster_spreads_rumor_over_ticks() {
        let mut cluster = Cluster::new(
            GossipConfig::new(2, 10).expect("valid config"),
            vec![
                NodeId::from("node-a"),
                NodeId::from("node-b"),
                NodeId::from("node-c"),
            ],
        );

        cluster
            .publish(
                &NodeId::from("node-a"),
                MessageId::new(1),
                Round::ZERO,
                "hello",
            )
            .expect("node should exist");

        assert_eq!(cluster.rumor_reach(MessageId::new(1)), 1);

        cluster.tick(Round::new(0));
        cluster.tick(Round::new(1));

        assert_eq!(cluster.rumor_reach(MessageId::new(1)), 3);
    }

    #[test]
    fn cluster_reports_whether_all_nodes_know_rumor() {
        let mut cluster = Cluster::new(
            GossipConfig::new(2, 10).expect("valid config"),
            vec![
                NodeId::from("node-a"),
                NodeId::from("node-b"),
                NodeId::from("node-c"),
            ],
        );

        cluster
            .publish(
                &NodeId::from("node-a"),
                MessageId::new(1),
                Round::ZERO,
                "hello",
            )
            .expect("node should exist");

        assert!(!cluster.all_know(MessageId::new(1)));

        cluster.run_until_reached(MessageId::new(1), 3, 5);

        assert!(cluster.all_know(MessageId::new(1)));
    }

    #[test]
    fn cluster_reports_known_and_unknown_nodes_for_rumor() {
        let mut cluster = Cluster::new(
            GossipConfig::new(1, 10).expect("valid config"),
            vec![
                NodeId::from("node-c"),
                NodeId::from("node-a"),
                NodeId::from("node-b"),
            ],
        );

        cluster
            .publish(
                &NodeId::from("node-a"),
                MessageId::new(1),
                Round::ZERO,
                "hello",
            )
            .expect("node should exist");

        assert_eq!(
            cluster.known_by(MessageId::new(1)),
            vec![NodeId::from("node-a")]
        );
        assert_eq!(
            cluster.unknown_by(MessageId::new(1)),
            vec![NodeId::from("node-b"), NodeId::from("node-c")]
        );
        assert_eq!(cluster.missing_count(MessageId::new(1)), 2);
    }

    #[test]
    fn cluster_reports_every_node_unknown_for_missing_rumor() {
        let cluster: Cluster<&str> = Cluster::new(
            GossipConfig::new(1, 10).expect("valid config"),
            vec![NodeId::from("node-b"), NodeId::from("node-a")],
        );

        assert!(cluster.known_by(MessageId::new(99)).is_empty());
        assert_eq!(
            cluster.unknown_by(MessageId::new(99)),
            vec![NodeId::from("node-a"), NodeId::from("node-b")]
        );
        assert_eq!(cluster.missing_count(MessageId::new(99)), 2);
    }

    #[test]
    fn empty_cluster_does_not_report_all_know() {
        let cluster: Cluster<&str> =
            Cluster::new(GossipConfig::new(1, 10).expect("valid config"), Vec::new());

        assert!(!cluster.all_know(MessageId::new(1)));
    }

    #[test]
    fn cluster_publish_uses_node_as_origin() {
        let mut cluster = Cluster::new(
            GossipConfig::new(1, 10).expect("valid config"),
            vec![NodeId::from("node-a")],
        );

        let outcome = cluster
            .publish(
                &NodeId::from("node-a"),
                MessageId::new(1),
                Round::new(7),
                "hello",
            )
            .expect("node should exist");

        assert_eq!(outcome, InsertOutcome::Inserted);

        let stored = cluster
            .node(&NodeId::from("node-a"))
            .expect("node should exist")
            .get_rumor(MessageId::new(1))
            .expect("rumor should exist");

        assert_eq!(stored.origin(), &NodeId::from("node-a"));
        assert_eq!(stored.created_at(), Round::new(7));
        assert_eq!(stored.payload(), &"hello");
    }

    #[test]
    fn cluster_publish_reports_unknown_node() {
        let mut cluster: Cluster<&str> = Cluster::new(
            GossipConfig::new(1, 10).expect("valid config"),
            vec![NodeId::from("node-a")],
        );

        let error = cluster
            .publish(
                &NodeId::from("missing"),
                MessageId::new(1),
                Round::ZERO,
                "hello",
            )
            .expect_err("missing node should fail");

        assert_eq!(error, ClusterError::UnknownNode(NodeId::from("missing")));
        assert_eq!(error.to_string(), "unknown node: missing");
    }

    #[test]
    fn cluster_with_seed_can_change_peer_selection() {
        let config = GossipConfig::new(1, 10).expect("valid config");
        let node_ids = vec![
            NodeId::from("node-a"),
            NodeId::from("node-b"),
            NodeId::from("node-c"),
            NodeId::from("node-d"),
        ];

        let mut first = Cluster::with_seed(config.clone(), node_ids.clone(), 1);
        let mut second = Cluster::with_seed(config, node_ids, 3);

        first
            .publish(
                &NodeId::from("node-a"),
                MessageId::new(1),
                Round::ZERO,
                "hello",
            )
            .expect("node should exist");
        second
            .publish(
                &NodeId::from("node-a"),
                MessageId::new(1),
                Round::ZERO,
                "hello",
            )
            .expect("node should exist");

        first.tick(Round::new(0));
        second.tick(Round::new(0));

        let first_reached_b = first
            .node(&NodeId::from("node-b"))
            .expect("node should exist")
            .contains_rumor(MessageId::new(1));
        let second_reached_b = second
            .node(&NodeId::from("node-b"))
            .expect("node should exist")
            .contains_rumor(MessageId::new(1));

        assert_ne!(first_reached_b, second_reached_b);
    }

    #[test]
    fn cluster_defaults_to_no_packet_loss() {
        let cluster: Cluster<&str> =
            Cluster::new(GossipConfig::new(1, 10).expect("valid config"), Vec::new());

        assert_eq!(cluster.loss_rate(), 0.0);
    }

    #[test]
    fn cluster_rejects_invalid_loss_rates() {
        let cluster: Cluster<&str> =
            Cluster::new(GossipConfig::new(1, 10).expect("valid config"), Vec::new());

        assert_eq!(
            cluster
                .clone()
                .with_loss_rate(-0.1)
                .expect_err("negative loss rate should fail"),
            ClusterError::InvalidLossRate(-0.1)
        );
        assert_eq!(
            cluster
                .clone()
                .with_loss_rate(1.1)
                .expect_err("loss rate above one should fail"),
            ClusterError::InvalidLossRate(1.1)
        );

        assert!(matches!(
            cluster.with_loss_rate(f64::NAN),
            Err(ClusterError::InvalidLossRate(rate)) if rate.is_nan()
        ));
    }

    #[test]
    fn cluster_with_full_packet_loss_drops_all_messages() {
        let mut cluster = Cluster::new(
            GossipConfig::new(2, 10).expect("valid config"),
            vec![
                NodeId::from("node-a"),
                NodeId::from("node-b"),
                NodeId::from("node-c"),
            ],
        )
        .with_loss_rate(1.0)
        .expect("valid loss rate");

        cluster
            .publish(
                &NodeId::from("node-a"),
                MessageId::new(1),
                Round::ZERO,
                "hello",
            )
            .expect("node should exist");

        let report = cluster.tick(Round::ZERO);

        assert_eq!(report.attempted(), 2);
        assert_eq!(report.sent(), 0);
        assert_eq!(report.dropped(), 2);
        assert_eq!(report.accepted(), 0);
        assert_eq!(report.message_copies(), 0);
        assert_eq!(report.observed_drop_rate(), 1.0);
        assert_eq!(report.observed_duplicate_rate(), 0.0);
        assert_eq!(report.observed_delay_rate(), 0.0);
        assert_eq!(report.observed_delivery_rate(), 0.0);
        assert_eq!(report.new_rumor_rate(), 0.0);
        assert_eq!(cluster.rumor_reach(MessageId::new(1)), 1);
    }

    #[test]
    fn network_model_defaults_to_reliable_delivery() {
        let network = NetworkModel::new();

        assert_eq!(network.loss_rate(), 0.0);
        assert_eq!(network.duplicate_rate(), 0.0);
        assert_eq!(network.delay_rate(), 0.0);
        assert_eq!(network.max_delay_rounds(), 0);
        assert!(network.partitions().is_empty());
    }

    #[test]
    fn cluster_defaults_to_no_duplicate_delivery() {
        let cluster: Cluster<&str> =
            Cluster::new(GossipConfig::new(1, 10).expect("valid config"), Vec::new());

        assert_eq!(cluster.duplicate_rate(), 0.0);
    }

    #[test]
    fn cluster_rejects_invalid_duplicate_rates() {
        let cluster: Cluster<&str> =
            Cluster::new(GossipConfig::new(1, 10).expect("valid config"), Vec::new());

        assert_eq!(
            cluster
                .clone()
                .with_duplicate_rate(-0.1)
                .expect_err("negative duplicate rate should fail"),
            ClusterError::InvalidDuplicateRate(-0.1)
        );
        assert_eq!(
            cluster
                .clone()
                .with_duplicate_rate(1.1)
                .expect_err("duplicate rate above one should fail"),
            ClusterError::InvalidDuplicateRate(1.1)
        );

        assert!(matches!(
            cluster.with_duplicate_rate(f64::NAN),
            Err(ClusterError::InvalidDuplicateRate(rate)) if rate.is_nan()
        ));
    }

    #[test]
    fn cluster_with_full_duplicate_delivery_still_deduplicates_rumors() {
        let mut cluster = Cluster::new(
            GossipConfig::new(1, 10).expect("valid config"),
            vec![NodeId::from("node-a"), NodeId::from("node-b")],
        )
        .with_duplicate_rate(1.0)
        .expect("valid duplicate rate");

        cluster
            .publish(
                &NodeId::from("node-a"),
                MessageId::new(1),
                Round::ZERO,
                "hello",
            )
            .expect("node should exist");

        let report = cluster.tick(Round::ZERO);

        assert_eq!(report.attempted(), 1);
        assert_eq!(report.sent(), 2);
        assert_eq!(report.duplicated(), 1);
        assert_eq!(report.received(), 2);
        assert_eq!(report.new_rumors(), 1);
        assert_eq!(report.accepted(), 1);
        assert_eq!(report.message_copies(), 2);
        assert_eq!(report.observed_drop_rate(), 0.0);
        assert_eq!(report.observed_duplicate_rate(), 1.0);
        assert_eq!(report.observed_delay_rate(), 0.0);
        assert_eq!(report.observed_delivery_rate(), 1.0);
        assert_eq!(report.new_rumor_rate(), 0.5);
        assert_eq!(cluster.rumor_reach(MessageId::new(1)), 2);
        assert_eq!(report.events().len(), 1);
    }

    #[test]
    fn cluster_rejects_invalid_delay_rates() {
        let cluster: Cluster<&str> =
            Cluster::new(GossipConfig::new(1, 10).expect("valid config"), Vec::new());

        assert_eq!(
            cluster
                .clone()
                .with_delay_rate(-0.1, 1)
                .expect_err("negative delay rate should fail"),
            ClusterError::InvalidDelayRate(-0.1)
        );
        assert_eq!(
            cluster
                .clone()
                .with_delay_rate(1.1, 1)
                .expect_err("delay rate above one should fail"),
            ClusterError::InvalidDelayRate(1.1)
        );

        assert!(matches!(
            cluster.with_delay_rate(f64::NAN, 1),
            Err(ClusterError::InvalidDelayRate(rate)) if rate.is_nan()
        ));
    }

    #[test]
    fn cluster_with_full_delay_holds_messages_until_future_tick() {
        let mut cluster = Cluster::new(
            GossipConfig::new(1, 10).expect("valid config"),
            vec![NodeId::from("node-a"), NodeId::from("node-b")],
        )
        .with_delay_rate(1.0, 1)
        .expect("valid delay rate");

        cluster
            .publish(
                &NodeId::from("node-a"),
                MessageId::new(1),
                Round::ZERO,
                "hello",
            )
            .expect("node should exist");

        let first = cluster.tick(Round::ZERO);

        assert_eq!(first.attempted(), 1);
        assert_eq!(first.sent(), 0);
        assert_eq!(first.delayed(), 1);
        assert_eq!(first.received(), 0);
        assert_eq!(first.accepted(), 1);
        assert_eq!(first.message_copies(), 1);
        assert_eq!(first.observed_delay_rate(), 1.0);
        assert_eq!(cluster.pending_delayed_count(), 1);
        assert_eq!(
            cluster.pending_delayed_rounds().collect::<Vec<_>>(),
            vec![(Round::new(1), 1)]
        );
        assert_eq!(cluster.rumor_reach(MessageId::new(1)), 1);

        let second = cluster.tick(Round::new(1));

        assert_eq!(second.sent(), 1);
        assert_eq!(second.received(), 1);
        assert_eq!(second.new_rumors(), 1);
        assert_eq!(second.observed_delivery_rate(), 1.0);
        assert_eq!(second.new_rumor_rate(), 1.0);
        assert_eq!(cluster.pending_delayed_count(), 1);
        assert_eq!(
            cluster.pending_delayed_rounds().collect::<Vec<_>>(),
            vec![(Round::new(2), 1)]
        );
        assert_eq!(cluster.rumor_reach(MessageId::new(1)), 2);
    }

    #[test]
    fn network_partition_blocks_cross_partition_messages_until_healed() {
        let partition =
            NetworkPartition::new(vec![NodeId::from("node-a")], vec![NodeId::from("node-b")]);
        let mut cluster = Cluster::new(
            GossipConfig::new(1, 10).expect("valid config"),
            vec![NodeId::from("node-a"), NodeId::from("node-b")],
        )
        .with_partition(partition);

        cluster
            .publish(
                &NodeId::from("node-a"),
                MessageId::new(1),
                Round::ZERO,
                "hello",
            )
            .expect("node should exist");

        let blocked = cluster.tick(Round::ZERO);

        assert_eq!(cluster.partitions().len(), 1);
        assert_eq!(blocked.attempted(), 1);
        assert_eq!(blocked.dropped(), 1);
        assert_eq!(blocked.sent(), 0);
        assert_eq!(cluster.rumor_reach(MessageId::new(1)), 1);

        cluster = cluster.without_partitions();

        let healed = cluster.tick(Round::new(1));

        assert!(cluster.partitions().is_empty());
        assert_eq!(healed.sent(), 1);
        assert_eq!(healed.received(), 1);
        assert_eq!(cluster.rumor_reach(MessageId::new(1)), 2);
    }

    #[test]
    fn reach_report_exposes_sent_count() {
        let report = ReachReport::new(true, 2, 3, 7);

        assert!(report.reached());
        assert_eq!(report.rounds_run(), 2);
        assert_eq!(report.reached_nodes(), 3);
        assert_eq!(report.attempted(), 0);
        assert_eq!(report.sent(), 7);
        assert_eq!(report.dropped(), 0);
        assert_eq!(report.duplicated(), 0);
        assert_eq!(report.delayed(), 0);
        assert_eq!(report.received(), 0);
        assert_eq!(report.new_rumors(), 0);
    }

    #[test]
    fn reach_report_assertions_pass_for_successful_convergence() {
        let report = ReachReport::new(true, 2, 3, 7);

        report.assert_reached();
        report.assert_reached_within(2);
    }

    #[test]
    fn run_report_starts_empty() {
        let report: RunReport<&str> = RunReport::new();

        assert_eq!(report.rounds_run(), 0);
        assert_eq!(report.attempted(), 0);
        assert_eq!(report.sent(), 0);
        assert_eq!(report.dropped(), 0);
        assert_eq!(report.duplicated(), 0);
        assert_eq!(report.delayed(), 0);
        assert_eq!(report.received(), 0);
        assert_eq!(report.accepted(), 0);
        assert_eq!(report.message_copies(), 0);
        assert_eq!(report.observed_drop_rate(), 0.0);
        assert_eq!(report.observed_duplicate_rate(), 0.0);
        assert_eq!(report.observed_delay_rate(), 0.0);
        assert_eq!(report.observed_delivery_rate(), 0.0);
        assert_eq!(report.new_rumors(), 0);
        assert_eq!(report.new_rumor_rate(), 0.0);
        assert!(report.events().is_empty());
    }

    #[test]
    fn cluster_can_run_for_fixed_round_count() {
        let mut cluster = Cluster::new(
            GossipConfig::new(1, 10).expect("valid config"),
            vec![NodeId::from("node-a"), NodeId::from("node-b")],
        );

        cluster
            .publish(
                &NodeId::from("node-a"),
                MessageId::new(1),
                Round::ZERO,
                "hello",
            )
            .expect("node should exist");

        let report = cluster.run_for_rounds(Round::ZERO, 2);

        assert_eq!(report.rounds_run(), 2);
        assert!(report.attempted() >= 1);
        assert!(report.sent() >= 1);
        assert_eq!(cluster.rumor_reach(MessageId::new(1)), 2);
    }

    #[test]
    fn run_until_reached_reports_success() {
        let mut cluster = Cluster::new(
            GossipConfig::new(2, 10).expect("valid config"),
            vec![
                NodeId::from("node-a"),
                NodeId::from("node-b"),
                NodeId::from("node-c"),
            ],
        );

        cluster
            .publish(
                &NodeId::from("node-a"),
                MessageId::new(1),
                Round::ZERO,
                "hello",
            )
            .expect("node should exist");

        let report = cluster.run_until_reached(MessageId::new(1), 3, 5);

        assert!(report.reached());
        assert!(report.rounds_run() <= 5);
        assert_eq!(report.reached_nodes(), 3);
        assert!(report.attempted() > 0);
        assert!(report.sent() > 0);
        assert!(report.received() > 0);
        assert!(report.new_rumors() > 0);
        assert_eq!(report.observed_delivery_rate(), 1.0);
    }

    #[test]
    fn run_until_reached_reports_failure_after_budget() {
        let mut cluster = Cluster::new(
            GossipConfig::new(1, 10).expect("valid config"),
            vec![NodeId::from("node-a")],
        );

        cluster
            .publish(
                &NodeId::from("node-a"),
                MessageId::new(1),
                Round::ZERO,
                "hello",
            )
            .expect("node should exist");

        let report = cluster.run_until_reached(MessageId::new(1), 2, 3);

        assert!(!report.reached());
        assert_eq!(report.rounds_run(), 3);
        assert_eq!(report.reached_nodes(), 1);
        assert_eq!(report.sent(), 0);
        assert_eq!(report.received(), 0);
    }

    #[test]
    fn run_until_reached_returns_immediately_if_already_reached() {
        let mut cluster = Cluster::new(
            GossipConfig::new(1, 10).expect("valid config"),
            vec![NodeId::from("node-a")],
        );

        cluster
            .publish(
                &NodeId::from("node-a"),
                MessageId::new(1),
                Round::ZERO,
                "hello",
            )
            .expect("node should exist");

        let report = cluster.run_until_reached(MessageId::new(1), 1, 3);

        assert!(report.reached());
        assert_eq!(report.rounds_run(), 0);
        assert_eq!(report.reached_nodes(), 1);
        assert_eq!(report.sent(), 0);
    }

    #[test]
    fn cluster_asserts_all_nodes_reached_within_budget() {
        let mut cluster = Cluster::new(
            GossipConfig::new(2, 10).expect("valid config"),
            vec![
                NodeId::from("node-a"),
                NodeId::from("node-b"),
                NodeId::from("node-c"),
            ],
        );

        cluster
            .publish(
                &NodeId::from("node-a"),
                MessageId::new(1),
                Round::ZERO,
                "hello",
            )
            .expect("node should exist");

        let report = cluster.assert_reaches_all_within(MessageId::new(1), 5);

        assert!(report.reached());
        assert_eq!(report.reached_nodes(), 3);
    }

    #[test]
    fn cluster_asserts_target_reach_within_budget() {
        let mut cluster = Cluster::new(
            GossipConfig::new(1, 10).expect("valid config"),
            vec![
                NodeId::from("node-a"),
                NodeId::from("node-b"),
                NodeId::from("node-c"),
            ],
        );

        cluster
            .publish(
                &NodeId::from("node-a"),
                MessageId::new(1),
                Round::ZERO,
                "hello",
            )
            .expect("node should exist");

        let report = cluster.assert_reaches_at_least_within(MessageId::new(1), 2, 5);

        assert!(report.reached());
        assert!(report.reached_nodes() >= 2);
    }

    #[test]
    fn convergence_report_calculates_success_rate() {
        let report = ConvergenceReport::new(4, 3, vec![2, 3, 4], 10, 9, 1, 2, 3, 8);

        assert_eq!(report.trials(), 4);
        assert_eq!(report.successes(), 3);
        assert_eq!(report.failures(), 1);
        assert_eq!(report.success_rate(), 0.75);
        assert_eq!(report.failure_rate(), 0.25);
        assert_eq!(report.successful_rounds(), &[2, 3, 4]);
        assert_eq!(report.mean_successful_rounds(), Some(3.0));
        assert_eq!(report.attempted(), 10);
        assert_eq!(report.sent(), 9);
        assert_eq!(report.dropped(), 1);
        assert_eq!(report.duplicated(), 2);
        assert_eq!(report.delayed(), 3);
        assert_eq!(report.received(), 8);
        assert_eq!(report.accepted(), 9);
        assert_eq!(report.message_copies(), 11);
        assert_eq!(report.observed_drop_rate(), 0.1);
        assert_eq!(report.observed_duplicate_rate(), 2.0 / 9.0);
        assert_eq!(report.observed_delay_rate(), 3.0 / 11.0);
        assert_eq!(report.observed_delivery_rate(), 8.0 / 9.0);
        assert_eq!(report.mean_attempted_per_trial(), 2.5);
        assert_eq!(report.mean_sent_per_trial(), 2.25);
        assert_eq!(report.mean_dropped_per_trial(), 0.25);
        assert_eq!(report.mean_duplicated_per_trial(), 0.5);
        assert_eq!(report.mean_delayed_per_trial(), 0.75);
        assert_eq!(report.mean_received_per_trial(), 2.0);
    }

    #[test]
    fn convergence_report_handles_zero_trials_and_no_successes() {
        let report = ConvergenceReport::new(0, 0, Vec::new(), 0, 0, 0, 0, 0, 0);

        assert_eq!(report.success_rate(), 0.0);
        assert_eq!(report.failure_rate(), 0.0);
        assert_eq!(report.mean_successful_rounds(), None);
        assert_eq!(report.observed_drop_rate(), 0.0);
        assert_eq!(report.observed_duplicate_rate(), 0.0);
        assert_eq!(report.observed_delay_rate(), 0.0);
        assert_eq!(report.observed_delivery_rate(), 0.0);
        assert_eq!(report.mean_attempted_per_trial(), 0.0);
        assert_eq!(report.mean_sent_per_trial(), 0.0);
        assert_eq!(report.mean_dropped_per_trial(), 0.0);
        assert_eq!(report.mean_duplicated_per_trial(), 0.0);
        assert_eq!(report.mean_delayed_per_trial(), 0.0);
        assert_eq!(report.mean_received_per_trial(), 0.0);
    }

    #[test]
    fn convergence_report_calculates_percentiles() {
        let report = ConvergenceReport::new(5, 5, vec![10, 2, 4, 3, 2], 0, 0, 0, 0, 0, 0);

        assert_eq!(report.percentile_successful_rounds(0.0), Some(2));
        assert_eq!(report.percentile_successful_rounds(50.0), Some(3));
        assert_eq!(report.percentile_successful_rounds(95.0), Some(10));
        assert_eq!(report.percentile_successful_rounds(100.0), Some(10));
    }

    #[test]
    fn convergence_report_percentile_returns_none_for_invalid_input() {
        let report = ConvergenceReport::new(0, 0, Vec::new(), 0, 0, 0, 0, 0, 0);

        assert_eq!(report.percentile_successful_rounds(50.0), None);

        let report = ConvergenceReport::new(3, 3, vec![1, 2, 3], 0, 0, 0, 0, 0, 0);

        assert_eq!(report.percentile_successful_rounds(-1.0), None);
        assert_eq!(report.percentile_successful_rounds(101.0), None);
        assert_eq!(report.percentile_successful_rounds(f64::NAN), None);
    }

    #[test]
    fn convergence_experiment_runs_trials() {
        let experiment = ConvergenceExperiment::new(5, 2, 5, 4).expect("valid experiment");

        let report = experiment.run();

        assert_eq!(report.trials(), 4);
        assert!(report.successes() <= 4);
        assert!(report.success_rate() >= 0.0);
        assert!(report.success_rate() <= 1.0);
        assert_eq!(report.successful_rounds().len(), report.successes());
    }

    #[test]
    fn convergence_experiment_uses_default_config_from_fanout() {
        let experiment = ConvergenceExperiment::new(5, 2, 5, 4).expect("valid experiment");

        assert_eq!(experiment.config().fanout(), 2);
        assert_eq!(experiment.config().max_rumors(), 1_024);
    }

    #[test]
    fn convergence_experiment_can_override_config() {
        let config = GossipConfig::new(3, 64)
            .expect("valid config")
            .with_max_rumors_per_message(4)
            .expect("valid per-message limit");

        let experiment = ConvergenceExperiment::new(5, 2, 5, 4)
            .expect("valid experiment")
            .with_config(config);

        assert_eq!(experiment.config().fanout(), 3);
        assert_eq!(experiment.config().max_rumors(), 64);
        assert_eq!(experiment.config().max_rumors_per_message(), 4);
    }

    #[test]
    fn convergence_experiment_has_default_base_seed() {
        let experiment = ConvergenceExperiment::new(5, 2, 5, 4).expect("valid experiment");

        assert_eq!(experiment.base_seed(), 1);
    }

    #[test]
    fn convergence_experiment_can_override_base_seed() {
        let experiment = ConvergenceExperiment::new(5, 2, 5, 4)
            .expect("valid experiment")
            .with_seed(42);

        assert_eq!(experiment.base_seed(), 42);
    }

    #[test]
    fn convergence_experiment_uses_reliable_network_by_default() {
        let experiment = ConvergenceExperiment::new(5, 2, 5, 4).expect("valid experiment");

        assert_eq!(experiment.network_model().loss_rate(), 0.0);
        assert_eq!(experiment.network_model().duplicate_rate(), 0.0);
        assert_eq!(experiment.network_model().delay_rate(), 0.0);
        assert!(experiment.network_model().partitions().is_empty());
    }

    #[test]
    fn convergence_experiment_can_override_network_model() {
        let network = NetworkModel::new()
            .with_loss_rate(1.0)
            .expect("valid loss rate");

        let experiment = ConvergenceExperiment::new(5, 2, 5, 4)
            .expect("valid experiment")
            .with_network_model(network);

        assert_eq!(experiment.network_model().loss_rate(), 1.0);
    }

    #[test]
    fn convergence_experiment_respects_network_model() {
        let network = NetworkModel::new()
            .with_loss_rate(1.0)
            .expect("valid loss rate");

        let experiment = ConvergenceExperiment::new(5, 2, 5, 4)
            .expect("valid experiment")
            .with_network_model(network);

        let report = experiment.run();

        assert_eq!(report.trials(), 4);
        assert_eq!(report.successes(), 0);
        assert_eq!(report.success_rate(), 0.0);
    }

    #[test]
    fn convergence_experiment_reports_network_metrics() {
        let network = NetworkModel::new()
            .with_loss_rate(1.0)
            .expect("valid loss rate");

        let report = ConvergenceExperiment::new(5, 2, 3, 4)
            .expect("valid experiment")
            .with_network_model(network)
            .run();

        assert_eq!(report.trials(), 4);
        assert_eq!(report.successes(), 0);
        assert!(report.attempted() > 0);
        assert_eq!(report.sent(), 0);
        assert_eq!(report.dropped(), report.attempted());
        assert_eq!(report.received(), 0);
    }

    #[test]
    fn convergence_scenario_exposes_label_and_experiment() {
        let experiment = ConvergenceExperiment::new(5, 2, 3, 4).expect("valid experiment");
        let scenario = ConvergenceScenario::new("reliable", experiment);

        assert_eq!(scenario.label(), "reliable");
        assert_eq!(scenario.experiment().config().fanout(), 2);
    }

    #[test]
    fn convergence_comparison_runs_named_experiments_in_order() {
        let reliable = ConvergenceExperiment::new(5, 2, 5, 3)
            .expect("valid experiment")
            .with_seed(10);
        let lossy_network = NetworkModel::new()
            .with_loss_rate(1.0)
            .expect("valid loss rate");
        let lossy = ConvergenceExperiment::new(5, 2, 5, 3)
            .expect("valid experiment")
            .with_seed(10)
            .with_network_model(lossy_network);

        let comparison = ConvergenceComparison::new()
            .add("reliable", reliable)
            .add("lossy", lossy);

        assert_eq!(comparison.len(), 2);
        assert!(!comparison.is_empty());
        assert_eq!(comparison.scenarios()[0].label(), "reliable");

        let report = comparison.run();

        assert_eq!(report.len(), 2);
        assert!(!report.is_empty());
        assert_eq!(report.results()[0].label(), "reliable");
        assert_eq!(report.results()[1].label(), "lossy");
        assert_eq!(report.results()[0].report().trials(), 3);
        assert_eq!(report.results()[1].report().successes(), 0);
    }

    #[test]
    fn convergence_comparison_can_add_prebuilt_scenarios() {
        let experiment = ConvergenceExperiment::new(3, 1, 3, 2).expect("valid experiment");
        let scenario = ConvergenceScenario::new("custom", experiment);
        let comparison = ConvergenceComparison::new().add_scenario(scenario);

        let report = comparison.run();

        assert_eq!(report.results()[0].label(), "custom");
        assert_eq!(report.results()[0].report().trials(), 2);
    }

    #[test]
    fn convergence_experiment_rejects_zero_nodes() {
        let error = ConvergenceExperiment::new(0, 2, 5, 4).expect_err("zero nodes should fail");

        assert_eq!(error, ExperimentError::ZeroNodeCount);
        assert_eq!(error.to_string(), "node_count must be greater than zero");
    }

    #[test]
    fn convergence_experiment_rejects_zero_fanout() {
        let error = ConvergenceExperiment::new(5, 0, 5, 4).expect_err("zero fanout should fail");

        assert_eq!(error, ExperimentError::ZeroFanout);
        assert_eq!(error.to_string(), "fanout must be greater than zero");
    }

    #[test]
    fn convergence_experiment_rejects_zero_trials() {
        let error = ConvergenceExperiment::new(5, 2, 5, 0).expect_err("zero trials should fail");

        assert_eq!(error, ExperimentError::ZeroTrials);
        assert_eq!(error.to_string(), "trials must be greater than zero");
    }
}
