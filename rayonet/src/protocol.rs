//! The rayonet wire message set.

use serde::{Deserialize, Serialize};

/// Bumped when the wire protocol changes. Because the agent is compiled from the
/// same source as the coordinator (whole-crate compile), this is a sanity
/// assertion rather than a true negotiation.
pub const PROTOCOL_VERSION: u32 = 11;

/// Identifies a task within a run.
pub type TaskId = u64;

/// One child a relay has discovered and built, advertised up for path selection.
///
/// The coordinator decides which paths to run from these. A node reached through
/// two relays appears once under each, with the same `id`, which is how the
/// coordinator dedups it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChildAd {
    /// The child's local label under this relay (its path segment).
    label: String,
    /// The child's stable physical node id (shared across redundant paths).
    id: String,
    /// How many tasks the child can hold in flight (its own advertised slots).
    slots: usize,
    /// Measured latency (microseconds) of the relay's link to this child, the
    /// weight used to pick the primary among redundant paths.
    latency_us: u64,
}

impl ChildAd {
    /// Advertise a built child: its local `label` under the relay, its stable
    /// physical node `id`, its advertised `slots`, and the relay's measured link
    /// `latency_us` to it.
    #[must_use]
    pub const fn new(label: String, id: String, slots: usize, latency_us: u64) -> Self {
        Self {
            label,
            id,
            slots,
            latency_us,
        }
    }

    /// The child's local label under its relay (its path segment).
    #[must_use]
    pub fn label(&self) -> &str {
        &self.label
    }

    /// The child's stable physical node id, shared across redundant paths.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Measured latency (microseconds) of the relay's link to this child.
    #[must_use]
    pub const fn latency_us(&self) -> u64 {
        self.latency_us
    }
}

/// Coordinator to agent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ToAgent {
    /// Handshake naming the single function this whole job runs.
    Hello {
        /// Wire protocol version of the coordinator (see [`PROTOCOL_VERSION`]).
        protocol_version: u32,
        /// The `type_name` selector identifying the task function to run.
        fn_key: String,
        /// The run's liveness cadence, so every node agrees on the ping interval
        /// and the silence timeout. A relay passes it on to its own children.
        heartbeat: crate::heartbeat::HeartbeatConfig,
    },
    /// One unit of work.
    Assign {
        /// Identifier the agent echoes back in `Completed` or `Failed`.
        task_id: TaskId,
        /// Postcard-encoded task input.
        payload: Vec<u8>,
    },
    /// The coordinator's answer to a relay's `Discovered`: the labels of the
    /// children to run now. Any discovered child not named is held as a built but
    /// idle standby (a redundant path the coordinator deduped away), ready to be
    /// brought in later with `Promote`. The relay replies `Ready` with the active
    /// capacity.
    Activate {
        /// Labels of the children to schedule to.
        active: Vec<String>,
    },
    /// Bring a standby child into the active set after its primary path died, so
    /// the relay starts scheduling to it. The relay replies `Capacity` with its
    /// new total.
    Promote {
        /// Label of the standby child to activate.
        child: String,
    },
    /// An operator control routed down the tree (pause, resume, or kill a node).
    /// `target` is the path from this agent down to the node to act on (its first
    /// segment names a direct child); a relay applies the action to that child
    /// when the path ends there, or forwards a deeper path one hop further down.
    Control {
        /// The remaining path from this agent to the node to control.
        target: String,
        /// What to do to it.
        action: crate::control::ControlAction,
    },
    /// A liveness probe a parent sends its children on the heartbeat interval; the
    /// child answers with [`FromAgent::Pong`]. Receiving it (like any message)
    /// keeps the child from giving its parent up.
    Ping,
    /// Stop serving and exit cleanly (sent once every result is in).
    Shutdown,
}

/// Agent to coordinator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FromAgent {
    /// Handshake reply: the agent is built, connected, and ready for work. It
    /// advertises how many tasks it can hold in flight at once: a leaf reports
    /// 1 (one task at a time), a relay reports the number of ready compute slots
    /// across its subtree, so the coordinator keeps that subtree fed.
    Ready {
        /// Concurrent tasks this agent can hold in flight.
        slots: usize,
    },
    /// A task has begun executing (lets a view show it in flight).
    Started {
        /// The task now running.
        task_id: TaskId,
    },
    /// A task finished successfully.
    Completed {
        /// The task that finished.
        task_id: TaskId,
        /// Postcard-encoded task output.
        output: Vec<u8>,
        /// The path from this agent down to the leaf that actually ran the task,
        /// empty when this agent ran it itself. Each relay prepends its child's
        /// label as it forwards the result up, so the coordinator can credit the
        /// completion to the deep leaf rather than to the relay it heard it from.
        via: String,
    },
    /// A task panicked. Terminal: never retried.
    Failed {
        /// The task that panicked.
        task_id: TaskId,
        /// Captured panic message.
        error: String,
        /// The path down to the leaf that ran the task (see [`FromAgent::Completed`]).
        via: String,
    },
    /// A relay's built children, sent in place of `Ready` so the coordinator can
    /// dedup redundant paths and choose which to run before any task flows. The
    /// relay then waits for `Activate`. A leaf never sends this (it just readies).
    Discovered {
        /// Every child this relay built, with its id and slots.
        children: Vec<ChildAd>,
    },
    /// A relay's updated in-flight capacity after a `Promote` brought a standby
    /// child into its active set, so the coordinator feeds the larger subtree.
    Capacity {
        /// The relay's new total concurrent slots across its active children.
        slots: usize,
    },
    /// An observability event about this agent's subtree, forwarded up so the
    /// top coordinator can see the whole tree. A relay sends these for its
    /// children (and passes its grandchildren's up); a leaf never sends one. The
    /// receiver prefixes the event's host with the sending child's label, so the
    /// host becomes a path from the root (the parent is the path prefix).
    Observe(crate::observability::Event),
    /// A child's reply to a [`ToAgent::Ping`]: proof it is still alive. Carries no
    /// data; the parent only notes that the child was heard from.
    Pong,
}

#[cfg(test)]
mod tests {
    use super::{ChildAd, FromAgent, ToAgent, PROTOCOL_VERSION};
    use crate::control::{ControlAction, KillMode};
    use crate::observability::{Event, NodeState};
    use proptest::prelude::*;

    fn roundtrip_to_agent(msg: &ToAgent) {
        let bytes = postcard::to_allocvec(msg).expect("encode");
        let back: ToAgent = postcard::from_bytes(&bytes).expect("decode");
        assert_eq!(msg, &back);
    }

    fn roundtrip_from_agent(msg: &FromAgent) {
        let bytes = postcard::to_allocvec(msg).expect("encode");
        let back: FromAgent = postcard::from_bytes(&bytes).expect("decode");
        assert_eq!(msg, &back);
    }

    #[test]
    fn to_agent_variants_roundtrip() {
        for msg in [
            ToAgent::Hello {
                protocol_version: PROTOCOL_VERSION,
                fn_key: "my_crate::evolve".to_string(),
                heartbeat: crate::heartbeat::HeartbeatConfig::default(),
            },
            ToAgent::Assign {
                task_id: 7,
                payload: vec![1, 2, 3, 255, 0],
            },
            ToAgent::Activate {
                active: vec!["leaf-a".to_string(), "leaf-b".to_string()],
            },
            ToAgent::Promote {
                child: "leaf-b".to_string(),
            },
            ToAgent::Control {
                target: "leaf-a".to_string(),
                action: ControlAction::Pause,
            },
            ToAgent::Control {
                target: "leaf-a/deep".to_string(),
                action: ControlAction::Kill {
                    mode: KillMode::AfterCurrent,
                },
            },
            ToAgent::Ping,
            ToAgent::Shutdown,
        ] {
            roundtrip_to_agent(&msg);
        }
    }

    #[test]
    fn from_agent_variants_roundtrip() {
        for msg in [
            FromAgent::Ready { slots: 1 },
            FromAgent::Started { task_id: 9 },
            FromAgent::Completed {
                task_id: 9,
                output: vec![42; 64],
                via: "relay/leaf".to_string(),
            },
            FromAgent::Failed {
                task_id: 9,
                error: "panicked at 'boom'".to_string(),
                via: String::new(),
            },
            FromAgent::Observe(Event::node("relay/leaf", NodeState::Working)),
            FromAgent::Discovered {
                children: vec![ChildAd {
                    label: "leaf-a".to_string(),
                    id: "node-1".to_string(),
                    slots: 2,
                    latency_us: 1_200,
                }],
            },
            FromAgent::Capacity { slots: 3 },
            FromAgent::Pong,
        ] {
            roundtrip_from_agent(&msg);
        }
    }

    fn to_agent_strategy() -> impl Strategy<Value = ToAgent> {
        prop_oneof![
            (any::<u32>(), any::<String>()).prop_map(|(protocol_version, fn_key)| {
                ToAgent::Hello {
                    protocol_version,
                    fn_key,
                    heartbeat: crate::heartbeat::HeartbeatConfig::default(),
                }
            }),
            (any::<u64>(), any::<Vec<u8>>())
                .prop_map(|(task_id, payload)| ToAgent::Assign { task_id, payload }),
            prop::collection::vec(any::<String>(), 0..4)
                .prop_map(|active| ToAgent::Activate { active }),
            any::<String>().prop_map(|child| ToAgent::Promote { child }),
            (
                any::<String>(),
                prop_oneof![
                    Just(ControlAction::Pause),
                    Just(ControlAction::Resume),
                    Just(ControlAction::Kill {
                        mode: KillMode::Now
                    }),
                    Just(ControlAction::Kill {
                        mode: KillMode::AfterCurrent
                    }),
                ],
            )
                .prop_map(|(target, action)| ToAgent::Control { target, action }),
            Just(ToAgent::Ping),
            Just(ToAgent::Shutdown),
        ]
    }

    fn from_agent_strategy() -> impl Strategy<Value = FromAgent> {
        prop_oneof![
            any::<usize>().prop_map(|slots| FromAgent::Ready { slots }),
            any::<u64>().prop_map(|task_id| FromAgent::Started { task_id }),
            (any::<u64>(), any::<Vec<u8>>(), any::<String>()).prop_map(|(task_id, output, via)| {
                FromAgent::Completed {
                    task_id,
                    output,
                    via,
                }
            }),
            (any::<u64>(), any::<String>(), any::<String>()).prop_map(|(task_id, error, via)| {
                FromAgent::Failed {
                    task_id,
                    error,
                    via,
                }
            }),
            any::<usize>().prop_map(|tasks| FromAgent::Observe(Event::RunStarted { tasks })),
            prop::collection::vec(
                (
                    any::<String>(),
                    any::<String>(),
                    any::<usize>(),
                    any::<u64>()
                )
                    .prop_map(|(label, id, slots, latency_us)| ChildAd {
                        label,
                        id,
                        slots,
                        latency_us,
                    },),
                0..4,
            )
            .prop_map(|children| FromAgent::Discovered { children }),
            any::<usize>().prop_map(|slots| FromAgent::Capacity { slots }),
            Just(FromAgent::Pong),
        ]
    }

    proptest! {
        #[test]
        fn arbitrary_to_agent_roundtrips(msg in to_agent_strategy()) {
            let bytes = postcard::to_allocvec(&msg).unwrap();
            let back: ToAgent = postcard::from_bytes(&bytes).unwrap();
            prop_assert_eq!(msg, back);
        }

        #[test]
        fn arbitrary_from_agent_roundtrips(msg in from_agent_strategy()) {
            let bytes = postcard::to_allocvec(&msg).unwrap();
            let back: FromAgent = postcard::from_bytes(&bytes).unwrap();
            prop_assert_eq!(msg, back);
        }
    }
}
