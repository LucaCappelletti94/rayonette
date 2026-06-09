//! The serializable event stream and the in-memory run state (PLAN.md Phase 5).
//!
//! The core emits a single stream of [`Event`]s, the one source of truth that
//! every renderer subscribes to. [`EventBus`] is the
//! lossy broadcast: a slow observer drops events but can never backpressure the
//! run. [`RunState`] reduces the stream into the live per-node and per-task
//! picture a renderer draws. Events are `Serialize` so a renderer
//! can be in-process or out-of-process.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::capability::{NodeProfile, Role};
use crate::protocol::TaskId;

/// A node's place in its lifecycle, grown per phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeState {
    /// Confirming the host responds (the `uname` probe).
    Probing,
    /// Installing the rust toolchain user-locally via rustup.
    Installing,
    /// Shipping and unpacking the crate source.
    Syncing,
    /// Compiling the agent on the host.
    Building,
    /// Built and ready to receive tasks.
    Ready,
    /// Running at least one task.
    Working,
    /// Connected and ready but with no task in flight.
    Idle,
    /// Paused by an operator: still connected, but assigned no new work until
    /// resumed (any in-flight task finishes first).
    Paused,
    /// Being killed after its current tasks drain: assigned no new work, and
    /// dropped once its in-flight tasks finish.
    Draining,
    /// The run finished on this node.
    Done,
    /// The node's connection dropped; its in-flight work was requeued.
    Lost,
}

/// A point-in-time sample of a node's live resource use, reported by the agent.
///
/// Percentages are whole numbers in `0..=100` so the type stays `Eq` (and small on
/// the wire), matching how the rest of the event stream avoids floats. GPU fields
/// are absent when the host has no GPU (or no `nvidia-smi`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeTelemetry {
    /// CPU utilisation since the previous sample, as a percentage.
    cpu_pct: u8,
    /// Memory in use, as a percentage of total.
    mem_pct: u8,
    /// GPU compute utilisation, if a GPU was sampled.
    gpu_pct: Option<u8>,
    /// GPU memory utilisation, if a GPU was sampled.
    gpu_mem_pct: Option<u8>,
    /// Tasks running on the node at the moment of the sample.
    in_flight: usize,
    /// The node's own non-loopback IPv4 addresses, as it sees them (its interface
    /// IPs, including any overlay like Tailscale). Empty when none were read.
    /// Defaulted on decode so traces recorded before this field still load.
    #[serde(default)]
    interfaces: Vec<String>,
}

impl NodeTelemetry {
    /// A live resource sample: CPU and memory percentages, optional GPU compute
    /// and memory percentages, the `in_flight` task count, and the node's own
    /// `interfaces` (its non-loopback IPv4 addresses).
    #[must_use]
    pub const fn new(
        cpu_pct: u8,
        mem_pct: u8,
        gpu_pct: Option<u8>,
        gpu_mem_pct: Option<u8>,
        in_flight: usize,
        interfaces: Vec<String>,
    ) -> Self {
        Self {
            cpu_pct,
            mem_pct,
            gpu_pct,
            gpu_mem_pct,
            in_flight,
            interfaces,
        }
    }

    /// CPU utilisation since the previous sample, as a percentage.
    #[must_use]
    pub const fn cpu_pct(&self) -> u8 {
        self.cpu_pct
    }

    /// Memory in use, as a percentage of total.
    #[must_use]
    pub const fn mem_pct(&self) -> u8 {
        self.mem_pct
    }

    /// GPU compute utilisation, if a GPU was sampled.
    #[must_use]
    pub const fn gpu_pct(&self) -> Option<u8> {
        self.gpu_pct
    }

    /// Tasks running on the node at the moment of the sample.
    #[must_use]
    pub const fn in_flight(&self) -> usize {
        self.in_flight
    }

    /// The node's own non-loopback IPv4 addresses, as it sees them.
    #[must_use]
    pub fn interfaces(&self) -> &[String] {
        &self.interfaces
    }
}

/// One observability event. The stream carries node lifecycle and task
/// lifecycle; log lines and richer progress are deferred.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Event {
    /// The run began with this many tasks (the progress denominator).
    RunStarted {
        /// Total number of tasks in the run.
        tasks: usize,
    },
    /// A node entered a new [`NodeState`].
    Node {
        /// The host the event is about.
        host: String,
        /// The state the host just entered.
        state: NodeState,
    },
    /// A host was probed for its capabilities and assigned a scheduling role.
    Profiled {
        /// The host that was probed (its path id from the root).
        host: String,
        /// A stable id for the physical node, so the same node reached by two
        /// paths is recognized as one (the basis for redundant-path dedup).
        id: String,
        /// The capabilities the probe found.
        profile: NodeProfile,
        /// The role the fleet filter assigned from that profile.
        role: Role,
        /// Round-trip latency of the discovery probe, in microseconds: the weight
        /// of the link from this node's parent, used to pick redundant paths.
        latency_us: u64,
    },
    /// A task began running on a host.
    TaskStarted {
        /// The host running the task.
        host: String,
        /// The task identifier.
        task: TaskId,
    },
    /// A task finished on a host, successfully or not.
    TaskFinished {
        /// The host that ran the task.
        host: String,
        /// The task identifier.
        task: TaskId,
        /// Whether the task completed successfully.
        ok: bool,
    },
    /// A live resource sample an agent reported about itself. The agent sends it
    /// with an empty host; each hop up the tree prefixes the sender's label, so it
    /// arrives carrying the node's path (see [`Event::prefix_host`]).
    Telemetry {
        /// The host the sample is about (its path id, once prefixed).
        host: String,
        /// The sampled utilisation.
        telemetry: NodeTelemetry,
    },
}

impl Event {
    /// Build a node-state-change event for `host`.
    #[must_use]
    pub fn node(host: &str, state: NodeState) -> Self {
        Self::Node {
            host: host.to_string(),
            state,
        }
    }

    /// Build a capability-and-role event for `host` (a node with stable `id`),
    /// carrying the measured `latency_us` of the link from its parent.
    #[must_use]
    pub fn profiled(
        host: &str,
        id: &str,
        profile: NodeProfile,
        role: Role,
        latency_us: u64,
    ) -> Self {
        Self::Profiled {
            host: host.to_string(),
            id: id.to_string(),
            profile,
            role,
            latency_us,
        }
    }

    /// Prepend `prefix/` to this event's host, turning a relay's local child
    /// label into a path from the root as the event is forwarded up one hop.
    /// [`Event::RunStarted`] has no host and is left unchanged.
    pub fn prefix_host(&mut self, prefix: &str) {
        let host = match self {
            Self::Node { host, .. }
            | Self::Profiled { host, .. }
            | Self::TaskStarted { host, .. }
            | Self::TaskFinished { host, .. }
            | Self::Telemetry { host, .. } => host,
            Self::RunStarted { .. } => return,
        };
        // An agent reports its own telemetry with an empty host, so the first hop's
        // prefix becomes the whole label rather than leaving a leading slash.
        *host = if host.is_empty() {
            prefix.to_string()
        } else {
            format!("{prefix}/{host}")
        };
    }
}

/// One line of a recorded event stream: an [`Event`] plus the time it was emitted.
///
/// A run can append these to a file (the docker consumer does, behind
/// `RAYONETTE_EVENT_LOG`), and a viewer can replay them, pacing playback by
/// `elapsed_ms` (milliseconds since the run started), to watch the run unfold. The
/// timestamp lives here rather than on [`Event`] so the live protocol is unchanged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordedEvent {
    /// Milliseconds from the start of the run to when this event was emitted.
    elapsed_ms: u64,
    /// The event itself.
    event: Event,
}

impl RecordedEvent {
    /// Pair an `event` with the `elapsed_ms` (milliseconds from the start of the
    /// run) at which it was emitted, the unit a recorded trace is made of.
    #[must_use]
    pub const fn new(elapsed_ms: u64, event: Event) -> Self {
        Self { elapsed_ms, event }
    }

    /// Milliseconds from the start of the run to when this event was emitted.
    #[must_use]
    pub const fn elapsed_ms(&self) -> u64 {
        self.elapsed_ms
    }

    /// The event itself.
    #[must_use]
    pub const fn event(&self) -> &Event {
        &self.event
    }
}

/// A consumer of the observability event stream.
pub trait EventSink: Send + Sync {
    /// Record one event. Must not block the run.
    fn emit(&self, event: Event);
}

/// An [`EventSink`] that discards every event: the default for an unobserved run.
#[derive(Debug, Default)]
pub struct NoopSink;

impl EventSink for NoopSink {
    fn emit(&self, _event: Event) {}
}

/// The lossy broadcast every renderer subscribes to.
///
/// Backed by a bounded broadcast channel: [`EventBus::emit`] never blocks, and a
/// receiver that falls behind silently drops the events it missed rather than
/// slowing the producer.
#[derive(Debug)]
pub struct EventBus {
    sender: broadcast::Sender<Event>,
}

impl EventBus {
    /// Create a bus buffering up to `capacity` events per receiver.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self { sender }
    }

    /// Subscribe a new receiver to the stream.
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.sender.subscribe()
    }
}

impl EventSink for EventBus {
    fn emit(&self, event: Event) {
        // Non-blocking: `send` returns immediately. An error means no live
        // receivers, which is fine; lagging receivers drop on their own end.
        let _ = self.sender.send(event);
    }
}

/// The parent of a path id (everything before the last `/`), or `None` for a
/// top-level node. Node ids are `/`-joined paths from the root, so the tree
/// structure is read straight off the id.
#[must_use]
pub(crate) fn parent_of(id: &str) -> Option<&str> {
    id.rsplit_once('/').map(|(parent, _)| parent)
}

/// A path id's depth: the number of `/` separators (0 for a top-level node).
#[must_use]
pub fn depth(id: &str) -> usize {
    id.bytes().filter(|&b| b == b'/').count()
}

/// The last segment of a path id (the node's own label), for display.
#[must_use]
pub fn leaf_of(id: &str) -> &str {
    id.rsplit_once('/').map_or(id, |(_, leaf)| leaf)
}

/// Join a path `head` with a `tail` sub-path: `head` alone when `tail` is empty,
/// else `head/tail`. The building block for attributing a relayed completion to the
/// deep leaf, mirroring how [`Event::prefix_host`] extends a host one hop.
#[must_use]
pub(crate) fn join_label(head: &str, tail: &str) -> String {
    if tail.is_empty() {
        head.to_string()
    } else {
        format!("{head}/{tail}")
    }
}

/// The live, reduced picture of a run: per-node state and task tallies.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunState {
    /// Total tasks in the run (the progress denominator).
    total_tasks: usize,
    /// Tasks that finished successfully.
    completed: usize,
    /// Tasks that finished with an error.
    failed: usize,
    /// Per-host view, ordered by host name.
    nodes: BTreeMap<String, NodeView>,
}

impl RunState {
    /// Total tasks in the run (the progress denominator).
    #[must_use]
    pub const fn total_tasks(&self) -> usize {
        self.total_tasks
    }

    /// Tasks that finished successfully.
    #[must_use]
    pub const fn completed(&self) -> usize {
        self.completed
    }

    /// Tasks that finished with an error.
    #[must_use]
    pub const fn failed(&self) -> usize {
        self.failed
    }

    /// Per-host view, ordered by host name.
    #[must_use]
    pub const fn nodes(&self) -> &BTreeMap<String, NodeView> {
        &self.nodes
    }
}

/// One host's reduced view: its current state and how much it has finished.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeView {
    /// The host's current lifecycle state.
    state: NodeState,
    /// Tasks this host has finished (ok or err).
    completed: usize,
    /// The host's probed capabilities, once it has been profiled.
    profile: Option<NodeProfile>,
    /// The role the fleet filter assigned, once it has been profiled.
    role: Option<Role>,
    /// The physical node's stable id, once profiled (two paths sharing an id are
    /// the same node, reachable redundantly).
    id: Option<String>,
    /// The measured latency (microseconds) of the link from this node's parent,
    /// once profiled.
    latency_us: Option<u64>,
    /// The node's most recent live resource sample, once it has reported one.
    telemetry: Option<NodeTelemetry>,
}

impl NodeView {
    /// The host's current lifecycle state.
    #[must_use]
    pub const fn state(&self) -> NodeState {
        self.state
    }

    /// Tasks this host has finished (ok or err).
    #[must_use]
    pub const fn completed(&self) -> usize {
        self.completed
    }

    /// The host's probed capabilities, once it has been profiled.
    #[must_use]
    pub const fn profile(&self) -> Option<&NodeProfile> {
        self.profile.as_ref()
    }

    /// The role the fleet filter assigned, once it has been profiled.
    #[must_use]
    pub const fn role(&self) -> Option<Role> {
        self.role
    }

    /// The physical node's stable id, once profiled.
    #[must_use]
    pub fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }

    /// The measured latency (microseconds) of the link from this node's parent.
    #[must_use]
    pub const fn latency_us(&self) -> Option<u64> {
        self.latency_us
    }

    /// The node's most recent live resource sample, once it has reported one.
    #[must_use]
    pub const fn telemetry(&self) -> Option<&NodeTelemetry> {
        self.telemetry.as_ref()
    }
}

impl RunState {
    /// Fold one event into the state.
    pub fn apply(&mut self, event: &Event) {
        match event {
            Event::RunStarted { tasks } => self.total_tasks = *tasks,
            Event::Node { host, state } => {
                self.node(host).state = *state;
            }
            Event::Profiled {
                host,
                id,
                profile,
                role,
                latency_us,
            } => {
                let view = self.node(host);
                view.profile = Some(profile.clone());
                view.role = Some(*role);
                view.id = Some(id.clone());
                view.latency_us = Some(*latency_us);
            }
            Event::TaskStarted { host, .. } => {
                self.node(host);
            }
            Event::Telemetry { host, telemetry } => {
                self.node(host).telemetry = Some(telemetry.clone());
            }
            Event::TaskFinished { host, ok, .. } => {
                self.node(host).completed += 1;
                if *ok {
                    self.completed += 1;
                } else {
                    self.failed += 1;
                }
            }
        }
    }

    /// The top-level nodes (the coordinator's direct children), in id order.
    #[must_use]
    pub fn roots(&self) -> Vec<&str> {
        self.nodes
            .keys()
            .filter(|id| parent_of(id).is_none())
            .map(String::as_str)
            .collect()
    }

    /// Tasks finished anywhere in `host`'s subtree: its own count plus every
    /// descendant's. A relay computes nothing itself, so this rolls its leaves'
    /// work up to it for a per-subtree total, while a leaf (no descendants) reports
    /// just its own. Matches descendants by the `host/` path prefix, so `relayA`
    /// does not absorb `relayAB`.
    #[must_use]
    pub fn subtree_completed(&self, host: &str) -> usize {
        let prefix = format!("{host}/");
        self.nodes
            .iter()
            .filter(|(path, _)| path.as_str() == host || path.starts_with(&prefix))
            .map(|(_, view)| view.completed)
            .sum()
    }

    /// The direct children of `id`, in id order.
    #[must_use]
    pub fn children_of(&self, id: &str) -> Vec<&str> {
        self.nodes
            .keys()
            .filter(|child| parent_of(child) == Some(id))
            .map(String::as_str)
            .collect()
    }

    /// Path ids grouped by their physical node id, in id order. A group with more
    /// than one path is a node reachable redundantly through several relays.
    #[must_use]
    pub fn paths_by_id(&self) -> BTreeMap<String, Vec<&str>> {
        let mut by_id: BTreeMap<String, Vec<&str>> = BTreeMap::new();
        for (path, view) in &self.nodes {
            if let Some(id) = &view.id {
                by_id.entry(id.clone()).or_default().push(path);
            }
        }
        by_id
    }

    /// The state to display for `path`, accounting for a dead ancestor.
    ///
    /// A node reached only through a relay that went [`NodeState::Lost`] is
    /// stranded: its own last-reported state is stale (no further event can reach
    /// it through the dead relay), so it is shown `Lost`. A node whose ancestors
    /// are all alive keeps its own reported state, which is how a reroute stays
    /// visible: the surviving path's copy of the node still completes. An unknown
    /// path takes the first-sighting default of [`NodeState::Working`].
    #[must_use]
    pub fn effective_state(&self, path: &str) -> NodeState {
        let own = self
            .nodes
            .get(path)
            .map_or(NodeState::Working, |view| view.state);
        let mut ancestor = parent_of(path);
        while let Some(id) = ancestor {
            if self.nodes.get(id).map(|view| view.state) == Some(NodeState::Lost) {
                return NodeState::Lost;
            }
            ancestor = parent_of(id);
        }
        own
    }

    /// The view for `host`, created (defaulting to [`NodeState::Working`]) on
    /// first sighting if a task event arrives before any node-state event.
    fn node(&mut self, host: &str) -> &mut NodeView {
        self.nodes.entry(host.to_string()).or_insert(NodeView {
            state: NodeState::Working,
            completed: 0,
            profile: None,
            role: None,
            id: None,
            latency_us: None,
            telemetry: None,
        })
    }
}

/// A headless renderer: turns the event stream into a deterministic sequence of
/// plain lines, for non-terminal runs, logs, and CI.
#[derive(Debug, Default)]
pub struct PlainRenderer {
    state: RunState,
}

impl PlainRenderer {
    /// A fresh renderer with empty state.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold `event` into the state and return the line it should print, if any.
    pub fn render(&mut self, event: &Event) -> Option<String> {
        self.state.apply(event);
        match event {
            Event::RunStarted { tasks } => Some(format!("run started: {tasks} tasks")),
            Event::Node { host, state } => Some(format!(
                "{}{}: {state:?}",
                "  ".repeat(depth(host)),
                leaf_of(host)
            )),
            Event::Profiled {
                host,
                profile,
                role,
                ..
            } => Some(format!(
                "{}{}: {role:?} ({:?}, {} cores, {} MB RAM, {} GPUs)",
                "  ".repeat(depth(host)),
                leaf_of(host),
                profile.os(),
                profile.cores(),
                profile.ram_mb(),
                profile.gpus().len()
            )),
            Event::TaskStarted { .. } | Event::Telemetry { .. } => None,
            Event::TaskFinished { .. } => Some(format!(
                "progress: {}/{}",
                self.state.completed + self.state.failed,
                self.state.total_tasks
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Event, EventBus, EventSink, NodeState, NodeView, RunState};
    use crate::capability::{NodeProfile, Os, Role};

    fn finished(host: &str, task: u64, ok: bool) -> Event {
        Event::TaskFinished {
            host: host.to_string(),
            task,
            ok,
        }
    }

    #[tokio::test]
    async fn slow_observer_never_backpressures_the_run() {
        let bus = EventBus::new(4);
        let _stalled = bus.subscribe(); // subscribed but never drained

        // Emit far more than the buffer holds; emit must never block.
        for task in 0..1000 {
            bus.emit(finished("h", task, true));
        }

        // A fresh reader still receives subsequent events (lossy, not broken).
        let mut live = bus.subscribe();
        bus.emit(Event::RunStarted { tasks: 7 });
        assert_eq!(live.recv().await.unwrap(), Event::RunStarted { tasks: 7 });
    }

    #[test]
    fn path_ids_form_a_tree() {
        use super::{depth, parent_of};
        assert_eq!(parent_of("a"), None);
        assert_eq!(parent_of("a/b"), Some("a"));
        assert_eq!(parent_of("a/b/c"), Some("a/b"));
        assert_eq!(depth("a"), 0);
        assert_eq!(depth("a/b/c"), 2);
    }

    #[test]
    fn join_label_extends_a_path_one_hop() {
        use super::join_label;
        // A relay prepends its child label onto the sub-path beneath it; an empty
        // tail (the leaf ran it itself) leaves the head alone, so no trailing slash.
        assert_eq!(join_label("relay", ""), "relay");
        assert_eq!(join_label("relay", "leaf"), "relay/leaf");
        assert_eq!(join_label("relay", "sub/leaf"), "relay/sub/leaf");
    }

    #[test]
    fn run_state_exposes_roots_and_children() {
        let mut state = RunState::default();
        for id in ["a", "a/b", "a/c", "a/b/d", "e"] {
            state.apply(&Event::node(id, NodeState::Working));
        }
        assert_eq!(state.roots(), vec!["a", "e"]);
        assert_eq!(state.children_of("a"), vec!["a/b", "a/c"]);
        assert_eq!(state.children_of("a/b"), vec!["a/b/d"]);
        assert!(state.children_of("e").is_empty());
    }

    #[test]
    fn subtree_completed_rolls_descendants_up() {
        let mut state = RunState::default();
        // relayA fronts a leaf that did 6; relayAB (a different node) did 1; the
        // prefix match must not let relayA absorb relayAB.
        for (host, done) in [("relayA", 0), ("relayA/leaf", 6), ("relayAB", 1)] {
            for task in 0..done {
                state.apply(&Event::TaskFinished {
                    host: host.to_string(),
                    task,
                    ok: true,
                });
            }
        }
        // relayA rolls up its leaf (its own 0 plus the leaf's 6), not relayAB.
        assert_eq!(state.subtree_completed("relayA"), 6);
        // A leaf with no descendants reports just its own count.
        assert_eq!(state.subtree_completed("relayA/leaf"), 6);
        assert_eq!(state.subtree_completed("relayAB"), 1);
    }

    #[test]
    fn effective_state_strands_a_child_of_a_lost_relay() {
        let mut state = RunState::default();
        // gatewayA dies; gatewayB survives. Both front the same shared leaf, so
        // the work reroutes to gatewayB and completes there.
        state.apply(&Event::node("gatewayA", NodeState::Lost));
        state.apply(&Event::node("gatewayA/shared", NodeState::Working));
        state.apply(&Event::node("gatewayB", NodeState::Done));
        state.apply(&Event::node("gatewayB/shared", NodeState::Done));

        // A child reached only through a Lost relay is stranded: shown Lost, even
        // though no event ever updated its own (now stale Working) state.
        assert_eq!(state.effective_state("gatewayA/shared"), NodeState::Lost);
        // A child whose ancestors are all alive keeps its own reported state, so
        // the reroute is visible as gatewayB/shared completing.
        assert_eq!(state.effective_state("gatewayB/shared"), NodeState::Done);
        // A node reports its own state when it has no ancestor at all.
        assert_eq!(state.effective_state("gatewayA"), NodeState::Lost);
        // An unknown path falls back to Working, matching first-sighting defaults.
        assert_eq!(state.effective_state("nope"), NodeState::Working);
    }

    #[test]
    fn run_state_records_a_profile_and_role() {
        let profile = NodeProfile::new(
            Os::Linux,
            String::new(),
            crate::capability::CpuArch::unknown(),
            8,
            16_000,
            Vec::new(),
        );
        let mut state = RunState::default();
        state.apply(&Event::profiled(
            "host-a",
            "node-1",
            profile.clone(),
            Role::Compute,
            1_500,
        ));

        let view = &state.nodes["host-a"];
        assert_eq!(view.profile, Some(profile));
        assert_eq!(view.role, Some(Role::Compute));
        assert_eq!(view.id.as_deref(), Some("node-1"));
        assert_eq!(view.latency_us, Some(1_500));
    }

    #[test]
    fn paths_by_id_groups_a_redundantly_reachable_node() {
        let profile = NodeProfile::new(
            Os::Linux,
            String::new(),
            crate::capability::CpuArch::unknown(),
            8,
            16_000,
            Vec::new(),
        );
        let mut state = RunState::default();
        // The same physical node ("shared") is reached via two relays.
        state.apply(&Event::profiled(
            "a/shared",
            "shared",
            profile.clone(),
            Role::Compute,
            0,
        ));
        state.apply(&Event::profiled(
            "b/shared",
            "shared",
            profile.clone(),
            Role::Compute,
            0,
        ));
        state.apply(&Event::profiled(
            "a/solo",
            "solo",
            profile,
            Role::Compute,
            0,
        ));

        let by_id = state.paths_by_id();
        assert_eq!(by_id["shared"], vec!["a/shared", "b/shared"]);
        assert_eq!(by_id["solo"], vec!["a/solo"]);
    }

    #[test]
    fn plain_renderer_summarizes_a_profile() {
        let profile = NodeProfile::new(
            Os::Linux,
            String::new(),
            crate::capability::CpuArch::unknown(),
            8,
            16_000,
            Vec::new(),
        );
        let mut renderer = super::PlainRenderer::new();
        let line = renderer
            .render(&Event::profiled(
                "host-a",
                "node-1",
                profile,
                Role::Compute,
                0,
            ))
            .expect("profiled events print a line");
        assert!(line.contains("host-a"), "{line}");
        assert!(line.contains("Compute"), "{line}");
    }

    #[test]
    fn run_state_reduces_node_and_task_events() {
        let mut state = RunState::default();
        for event in [
            Event::RunStarted { tasks: 3 },
            Event::node("fast", NodeState::Working),
            Event::TaskStarted {
                host: "slow".to_string(),
                task: 2,
            },
            finished("fast", 0, true),
            finished("fast", 1, true),
            finished("slow", 2, false),
            Event::node("fast", NodeState::Done),
        ] {
            state.apply(&event);
        }

        assert_eq!(state.total_tasks, 3);
        assert_eq!(state.completed, 2);
        assert_eq!(state.failed, 1);
        assert_eq!(state.nodes["fast"].completed, 2);
        assert_eq!(state.nodes["fast"].state, NodeState::Done);
        assert_eq!(state.nodes["slow"].completed, 1);
        assert_eq!(state.nodes["slow"].state, NodeState::Working);
    }

    #[test]
    fn plain_renderer_produces_the_expected_line_sequence() {
        let mut renderer = super::PlainRenderer::new();
        let script = [
            Event::RunStarted { tasks: 3 },
            Event::node("leaf-a", NodeState::Building),
            Event::node("leaf-a", NodeState::Ready),
            Event::TaskStarted {
                host: "leaf-a".to_string(),
                task: 0,
            },
            finished("leaf-a", 0, true),
            finished("leaf-a", 1, false),
            Event::node("leaf-a", NodeState::Done),
        ];
        let lines: Vec<String> = script.iter().filter_map(|e| renderer.render(e)).collect();
        assert_eq!(
            lines,
            vec![
                "run started: 3 tasks",
                "leaf-a: Building",
                "leaf-a: Ready",
                "progress: 1/3",
                "progress: 2/3",
                "leaf-a: Done",
            ]
        );
    }

    #[test]
    fn plain_renderer_indents_a_multi_level_tree() {
        let mut renderer = super::PlainRenderer::new();
        let script = [
            Event::node("relay", NodeState::Ready),
            Event::node("relay/leaf-a", NodeState::Working),
            Event::node("relay/leaf-a", NodeState::Done),
        ];
        let lines: Vec<String> = script.iter().filter_map(|e| renderer.render(e)).collect();
        assert_eq!(
            lines,
            vec!["relay: Ready", "  leaf-a: Working", "  leaf-a: Done"]
        );
    }

    #[test]
    fn node_state_variants_round_trip_and_format() {
        for state in [
            NodeState::Probing,
            NodeState::Installing,
            NodeState::Syncing,
            NodeState::Building,
            NodeState::Ready,
            NodeState::Working,
            NodeState::Idle,
            NodeState::Done,
            NodeState::Lost,
        ] {
            let bytes = postcard::to_allocvec(&state).unwrap();
            let back: NodeState = postcard::from_bytes(&bytes).unwrap();
            assert_eq!(back, state);
            assert!(!format!("{state:?}").is_empty());
        }
    }

    #[test]
    fn events_round_trip_through_serde() {
        let events = [
            Event::RunStarted { tasks: 5 },
            Event::node("h", NodeState::Building),
            Event::TaskStarted {
                host: "h".to_string(),
                task: 9,
            },
            finished("h", 9, false),
        ];
        for event in &events {
            let bytes = postcard::to_allocvec(event).unwrap();
            let back: Event = postcard::from_bytes(&bytes).unwrap();
            assert_eq!(&back, event);
        }
    }

    #[test]
    fn telemetry_reduces_and_prefixes() {
        use super::NodeTelemetry;
        let sample = NodeTelemetry {
            cpu_pct: 73,
            mem_pct: 41,
            gpu_pct: Some(88),
            gpu_mem_pct: Some(50),
            in_flight: 2,
            interfaces: vec!["100.64.0.3".to_string()],
        };

        // A node's latest sample lands on its view.
        let mut state = RunState::default();
        state.apply(&Event::Telemetry {
            host: "relay/leaf".to_string(),
            telemetry: sample.clone(),
        });
        assert_eq!(state.nodes["relay/leaf"].telemetry, Some(sample.clone()));

        // A relay prefixes an agent's self-reported (empty-host) telemetry into a
        // path rather than leaving a leading slash, then the next hop extends it.
        let mut event = Event::Telemetry {
            host: String::new(),
            telemetry: sample,
        };
        event.prefix_host("leaf");
        event.prefix_host("relay");
        let Event::Telemetry { host, .. } = &event else {
            unreachable!("still a telemetry event")
        };
        assert_eq!(host, "relay/leaf");

        // It round-trips on the wire.
        let bytes = postcard::to_allocvec(&event).unwrap();
        assert_eq!(postcard::from_bytes::<Event>(&bytes).unwrap(), event);
    }

    #[test]
    fn recorded_event_round_trips() {
        use super::RecordedEvent;
        let record = RecordedEvent::new(1234, Event::node("relay/leaf", NodeState::Working));
        assert_eq!(record.elapsed_ms(), 1234);
        assert_eq!(
            record.event(),
            &Event::node("relay/leaf", NodeState::Working)
        );
        let bytes = postcard::to_allocvec(&record).unwrap();
        let back: RecordedEvent = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(back, record);
        assert!(format!("{record:?}").contains("RecordedEvent"));
    }

    #[test]
    fn observability_types_expose_debug_and_clone() {
        let event = Event::RunStarted { tasks: 1 };
        let cloned = event.clone();
        assert_eq!(event, cloned);
        assert!(format!("{event:?}").contains("RunStarted"));

        let mut state = RunState::default();
        state.apply(&Event::node("h", NodeState::Idle));
        let state_clone = state.clone();
        assert_eq!(state, state_clone);
        assert!(format!("{state:?}").contains("nodes"));

        let view: NodeView = state.nodes["h"].clone();
        assert!(format!("{view:?}").contains("Idle"));

        let bus = EventBus::new(2);
        assert!(format!("{bus:?}").contains("EventBus"));
    }
}
