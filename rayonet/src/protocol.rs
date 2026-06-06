//! The rayonet wire message set.

use serde::{Deserialize, Serialize};

/// Bumped when the wire protocol changes. Because the agent is compiled from the
/// same source as the coordinator (whole-crate compile), this is a sanity
/// assertion rather than a true negotiation.
pub const PROTOCOL_VERSION: u32 = 2;

/// Identifies a task within a run.
pub type TaskId = u64;

/// Coordinator to agent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ToAgent {
    /// Handshake naming the single function this whole job runs.
    Hello {
        /// Wire protocol version of the coordinator (see [`PROTOCOL_VERSION`]).
        protocol_version: u32,
        /// The `type_name` selector identifying the task function to run.
        fn_key: String,
    },
    /// One unit of work.
    Assign {
        /// Identifier the agent echoes back in `Completed` or `Failed`.
        task_id: TaskId,
        /// Postcard-encoded task input.
        payload: Vec<u8>,
    },
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
    },
    /// A task panicked. Terminal: never retried.
    Failed {
        /// The task that panicked.
        task_id: TaskId,
        /// Captured panic message.
        error: String,
    },
}

#[cfg(test)]
mod tests {
    use super::{FromAgent, ToAgent, PROTOCOL_VERSION};
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
            },
            ToAgent::Assign {
                task_id: 7,
                payload: vec![1, 2, 3, 255, 0],
            },
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
            },
            FromAgent::Failed {
                task_id: 9,
                error: "panicked at 'boom'".to_string(),
            },
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
                }
            }),
            (any::<u64>(), any::<Vec<u8>>())
                .prop_map(|(task_id, payload)| ToAgent::Assign { task_id, payload }),
            Just(ToAgent::Shutdown),
        ]
    }

    fn from_agent_strategy() -> impl Strategy<Value = FromAgent> {
        prop_oneof![
            any::<usize>().prop_map(|slots| FromAgent::Ready { slots }),
            any::<u64>().prop_map(|task_id| FromAgent::Started { task_id }),
            (any::<u64>(), any::<Vec<u8>>())
                .prop_map(|(task_id, output)| FromAgent::Completed { task_id, output }),
            (any::<u64>(), any::<String>())
                .prop_map(|(task_id, error)| FromAgent::Failed { task_id, error }),
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
