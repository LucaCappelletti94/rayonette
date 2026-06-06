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
    /// The run finished on this node.
    Done,
    /// The node's connection dropped; its in-flight work was requeued.
    Lost,
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
            | Self::TaskFinished { host, .. } => host,
            Self::RunStarted { .. } => return,
        };
        *host = format!("{prefix}/{host}");
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
pub fn parent_of(id: &str) -> Option<&str> {
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

/// The live, reduced picture of a run: per-node state and task tallies.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunState {
    /// Total tasks in the run (the progress denominator).
    pub total_tasks: usize,
    /// Tasks that finished successfully.
    pub completed: usize,
    /// Tasks that finished with an error.
    pub failed: usize,
    /// Per-host view, ordered by host name.
    pub nodes: BTreeMap<String, NodeView>,
}

/// One host's reduced view: its current state and how much it has finished.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeView {
    /// The host's current lifecycle state.
    pub state: NodeState,
    /// Tasks this host has finished (ok or err).
    pub completed: usize,
    /// The host's probed capabilities, once it has been profiled.
    pub profile: Option<NodeProfile>,
    /// The role the fleet filter assigned, once it has been profiled.
    pub role: Option<Role>,
    /// The physical node's stable id, once profiled (two paths sharing an id are
    /// the same node, reachable redundantly).
    pub id: Option<String>,
    /// The measured latency (microseconds) of the link from this node's parent,
    /// once profiled.
    pub latency_us: Option<u64>,
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
                profile.os,
                profile.cores,
                profile.ram_mb,
                profile.gpus.len()
            )),
            Event::TaskStarted { .. } => None,
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
    fn run_state_records_a_profile_and_role() {
        let profile = NodeProfile {
            os: Os::Linux,
            arch: crate::capability::CpuArch::unknown(),
            cores: 8,
            ram_mb: 16_000,
            gpus: Vec::new(),
        };
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
        let profile = NodeProfile {
            os: Os::Linux,
            arch: crate::capability::CpuArch::unknown(),
            cores: 8,
            ram_mb: 16_000,
            gpus: Vec::new(),
        };
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
        let profile = NodeProfile {
            os: Os::Linux,
            arch: crate::capability::CpuArch::unknown(),
            cores: 8,
            ram_mb: 16_000,
            gpus: Vec::new(),
        };
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
