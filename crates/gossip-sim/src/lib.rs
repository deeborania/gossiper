#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! Simulation utilities for gossip protocol implementations.

use core::fmt;
use std::collections::BTreeMap;

use gossip_core::{
    DeterministicRng, Effect, GossipConfig, GossipEvent, GossipMessage, GossipNode, MessageId,
    NodeId, Round, Rumor,
};
use gossip_transport::{apply_effects, EffectReport, InMemoryTransport};

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

/// Summary of one simulated cluster tick.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TickReport<Event> {
    sent: usize,
    events: Vec<(NodeId, Event)>,
}

impl<Event> TickReport<Event> {
    /// Creates an empty tick report.
    pub fn new() -> Self {
        Self {
            sent: 0,
            events: Vec::new(),
        }
    }

    /// Returns the number of messages sent during the tick.
    pub fn sent(&self) -> usize {
        self.sent
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

/// Result of running a cluster until a rumor reaches a target number of nodes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReachReport {
    reached: bool,
    rounds_run: u64,
    reached_nodes: usize,
}

impl ReachReport {
    /// Creates a reach report.
    pub fn new(reached: bool, rounds_run: u64, reached_nodes: usize) -> Self {
        Self {
            reached,
            rounds_run,
            reached_nodes,
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
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConvergenceExperiment {
    node_count: usize,
    config: GossipConfig,
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

    /// Runs this convergence experiment.
    pub fn run(&self) -> ConvergenceReport {
        let mut successes = 0;
        let mut successful_rounds = Vec::new();

        for trial in 0..self.trials {
            let node_ids: Vec<_> = (0..self.node_count)
                .map(|index| NodeId::from(format!("trial-{trial}-node-{index}")))
                .collect();

            let config = self.config.clone();
            let origin = node_ids[0].clone();
            let rumor_id = MessageId::new(trial as u128 + 1);
            let rumor = Rumor::new(rumor_id, origin.clone(), Round::ZERO, "experiment");

            let seed = self.base_seed.saturating_add(trial as u64);
            let mut cluster = Cluster::with_seed(config, node_ids, seed);

            cluster.insert_rumor(&origin, rumor);

            let report = cluster.run_until_reached(rumor_id, self.node_count, self.max_rounds);

            if report.reached() {
                successes += 1;
                successful_rounds.push(report.rounds_run());
            }
        }

        ConvergenceReport::new(self.trials, successes, successful_rounds)
    }
}

/// Aggregate result of repeated convergence trials.
#[derive(Clone, Debug, PartialEq)]
pub struct ConvergenceReport {
    trials: usize,
    successes: usize,
    successful_rounds: Vec<u64>,
}

impl ConvergenceReport {
    /// Creates a convergence report.
    pub fn new(trials: usize, successes: usize, successful_rounds: Vec<u64>) -> Self {
        Self {
            trials,
            successes,
            successful_rounds,
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

    /// Returns the fraction of trials that succeeded.
    pub fn success_rate(&self) -> f64 {
        if self.trials == 0 {
            return 0.0;
        }

        self.successes as f64 / self.trials as f64
    }

    /// Returns the successful round counts.
    pub fn successful_rounds(&self) -> &[u64] {
        &self.successful_rounds
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

/// A deterministic in-memory simulation cluster.
#[derive(Clone, Debug)]
pub struct Cluster<T> {
    nodes: BTreeMap<NodeId, SimNode<T>>,
    transport: InMemoryTransport<GossipMessage<T>>,
}

impl<T> Cluster<T> {
    /// Creates a cluster where each node knows every other node as a peer.
    pub fn new(config: GossipConfig, node_ids: Vec<NodeId>) -> Self {
        Self::with_seed(config, node_ids, 1)
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
        }
    }

    /// Returns a node by ID.
    pub fn node(&self, node_id: &NodeId) -> Option<&GossipNode<T>> {
        self.nodes.get(node_id).map(SimNode::node)
    }

    /// Returns a mutable node by ID.
    pub fn node_mut(&mut self, node_id: &NodeId) -> Option<&mut GossipNode<T>> {
        self.nodes.get_mut(node_id).map(SimNode::node_mut)
    }

    /// Inserts a rumor into one node.
    pub fn insert_rumor(&mut self, node_id: &NodeId, rumor: Rumor<T>) -> bool {
        let Some(node) = self.node_mut(node_id) else {
            return false;
        };

        !matches!(
            node.insert_rumor(rumor),
            gossip_core::InsertOutcome::Duplicate
        )
    }
}

impl<T: Clone> Cluster<T> {
    /// Runs one simulation round.
    pub fn tick(&mut self, round: Round) -> TickReport<GossipEvent<T>> {
        let mut report = TickReport::new();

        for sim_node in self.nodes.values_mut() {
            let effects = sim_node.node.tick(&mut sim_node.rng, round);
            let transport_report: EffectReport<()> = apply_effects(&mut self.transport, effects);

            report.sent += transport_report.sent();
        }

        let node_ids: Vec<_> = self.nodes.keys().cloned().collect();

        for node_id in node_ids {
            let messages = self.transport.drain(&node_id);

            if let Some(sim_node) = self.nodes.get_mut(&node_id) {
                for message in messages {
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

    /// Runs rounds until a rumor reaches at least `target_reach` nodes or the budget is exhausted.
    pub fn run_until_reached(
        &mut self,
        rumor_id: MessageId,
        target_reach: usize,
        max_rounds: u64,
    ) -> ReachReport {
        let initial_reach = self.rumor_reach(rumor_id);

        if initial_reach >= target_reach {
            return ReachReport::new(true, 0, initial_reach);
        }

        for round in 0..max_rounds {
            self.tick(Round::new(round));

            let reached_nodes = self.rumor_reach(rumor_id);

            if reached_nodes >= target_reach {
                return ReachReport::new(true, round + 1, reached_nodes);
            }
        }

        ReachReport::new(false, max_rounds, self.rumor_reach(rumor_id))
    }
}

#[cfg(test)]
mod tests {
    use super::{Cluster, ConvergenceExperiment, ConvergenceReport, ExperimentError};
    use gossip_core::{GossipConfig, MessageId, NodeId, Round, Rumor};

    fn rumor(id: u128, payload: &'static str) -> Rumor<&'static str> {
        Rumor::new(
            MessageId::new(id),
            NodeId::from("node-a"),
            Round::new(0),
            payload,
        )
    }

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
    fn cluster_spreads_rumor_over_ticks() {
        let mut cluster = Cluster::new(
            GossipConfig::new(2, 10).expect("valid config"),
            vec![
                NodeId::from("node-a"),
                NodeId::from("node-b"),
                NodeId::from("node-c"),
            ],
        );

        cluster.insert_rumor(&NodeId::from("node-a"), rumor(1, "hello"));

        assert_eq!(cluster.rumor_reach(MessageId::new(1)), 1);

        cluster.tick(Round::new(0));
        cluster.tick(Round::new(1));

        assert_eq!(cluster.rumor_reach(MessageId::new(1)), 3);
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

        first.insert_rumor(&NodeId::from("node-a"), rumor(1, "hello"));
        second.insert_rumor(&NodeId::from("node-a"), rumor(1, "hello"));

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
    fn run_until_reached_reports_success() {
        let mut cluster = Cluster::new(
            GossipConfig::new(2, 10).expect("valid config"),
            vec![
                NodeId::from("node-a"),
                NodeId::from("node-b"),
                NodeId::from("node-c"),
            ],
        );

        cluster.insert_rumor(&NodeId::from("node-a"), rumor(1, "hello"));

        let report = cluster.run_until_reached(MessageId::new(1), 3, 5);

        assert!(report.reached());
        assert!(report.rounds_run() <= 5);
        assert_eq!(report.reached_nodes(), 3);
    }

    #[test]
    fn run_until_reached_reports_failure_after_budget() {
        let mut cluster = Cluster::new(
            GossipConfig::new(1, 10).expect("valid config"),
            vec![NodeId::from("node-a")],
        );

        cluster.insert_rumor(&NodeId::from("node-a"), rumor(1, "hello"));

        let report = cluster.run_until_reached(MessageId::new(1), 2, 3);

        assert!(!report.reached());
        assert_eq!(report.rounds_run(), 3);
        assert_eq!(report.reached_nodes(), 1);
    }

    #[test]
    fn run_until_reached_returns_immediately_if_already_reached() {
        let mut cluster = Cluster::new(
            GossipConfig::new(1, 10).expect("valid config"),
            vec![NodeId::from("node-a")],
        );

        cluster.insert_rumor(&NodeId::from("node-a"), rumor(1, "hello"));

        let report = cluster.run_until_reached(MessageId::new(1), 1, 3);

        assert!(report.reached());
        assert_eq!(report.rounds_run(), 0);
        assert_eq!(report.reached_nodes(), 1);
    }

    #[test]
    fn convergence_report_calculates_success_rate() {
        let report = ConvergenceReport::new(4, 3, vec![2, 3, 4]);

        assert_eq!(report.trials(), 4);
        assert_eq!(report.successes(), 3);
        assert_eq!(report.success_rate(), 0.75);
        assert_eq!(report.successful_rounds(), &[2, 3, 4]);
        assert_eq!(report.mean_successful_rounds(), Some(3.0));
    }

    #[test]
    fn convergence_report_handles_zero_trials_and_no_successes() {
        let report = ConvergenceReport::new(0, 0, Vec::new());

        assert_eq!(report.success_rate(), 0.0);
        assert_eq!(report.mean_successful_rounds(), None);
    }

    #[test]
    fn convergence_report_calculates_percentiles() {
        let report = ConvergenceReport::new(5, 5, vec![10, 2, 4, 3, 2]);

        assert_eq!(report.percentile_successful_rounds(0.0), Some(2));
        assert_eq!(report.percentile_successful_rounds(50.0), Some(3));
        assert_eq!(report.percentile_successful_rounds(95.0), Some(10));
        assert_eq!(report.percentile_successful_rounds(100.0), Some(10));
    }

    #[test]
    fn convergence_report_percentile_returns_none_for_invalid_input() {
        let report = ConvergenceReport::new(0, 0, Vec::new());

        assert_eq!(report.percentile_successful_rounds(50.0), None);

        let report = ConvergenceReport::new(3, 3, vec![1, 2, 3]);

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
