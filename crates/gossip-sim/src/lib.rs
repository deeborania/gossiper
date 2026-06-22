#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! Simulation utilities for gossip protocol implementations.

use core::fmt;
use std::collections::BTreeMap;

use gossip_core::{
    DeterministicRng, Effect, GossipConfig, GossipEvent, GossipMessage, GossipNode, InsertOutcome,
    MessageId, NodeId, RandomSource, Round, Rumor,
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

    /// Returns the number of new-rumor events emitted during the tick.
    pub fn new_rumors(&self) -> usize {
        self.events.len()
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
    sent: usize,
}

impl ReachReport {
    /// Creates a reach report.
    pub fn new(reached: bool, rounds_run: u64, reached_nodes: usize, sent: usize) -> Self {
        Self {
            reached,
            rounds_run,
            reached_nodes,
            sent,
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

    /// Returns the total number of messages sent while running.
    pub fn sent(&self) -> usize {
        self.sent
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

            cluster
                .insert_rumor(&origin, rumor)
                .expect("origin node should exist");

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

        let mut sent = 0;

        for round in 0..max_rounds {
            let tick_report = self.tick(Round::new(round));
            sent += tick_report.sent();

            let reached_nodes = self.rumor_reach(rumor_id);

            if reached_nodes >= target_reach {
                return ReachReport::new(true, round + 1, reached_nodes, sent);
            }
        }

        ReachReport::new(false, max_rounds, self.rumor_reach(rumor_id), sent)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Cluster, ClusterError, ConvergenceExperiment, ConvergenceReport, ExperimentError,
        NetworkModel, NetworkPartition, ReachReport,
    };
    use gossip_core::{GossipConfig, InsertOutcome, MessageId, NodeId, Round};

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
        assert_eq!(cluster.rumor_reach(MessageId::new(1)), 1);

        let second = cluster.tick(Round::new(1));

        assert_eq!(second.sent(), 1);
        assert_eq!(second.received(), 1);
        assert_eq!(second.new_rumors(), 1);
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
        assert_eq!(report.sent(), 7);
    }

    #[test]
    fn reach_report_assertions_pass_for_successful_convergence() {
        let report = ReachReport::new(true, 2, 3, 7);

        report.assert_reached();
        report.assert_reached_within(2);
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
        assert!(report.sent() > 0);
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
