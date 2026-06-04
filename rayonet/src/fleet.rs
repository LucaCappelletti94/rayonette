//! The user-facing fleet and its distributed map (PLAN.md Phase 3).
//!
//! A [`Fleet`] is a homogeneous set of agent launchers. [`Fleet::map`] derives
//! the wire key from the task function (via `type_name`), launches one agent per
//! launcher, runs the job, and returns the outputs in input order. The launcher
//! abstraction lets the same API back onto in-process pipes (tests), spawned
//! subprocesses, or ssh (Phase 4).

use std::future::Future;

use serde::{de::DeserializeOwned, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::agent::fn_key;
use crate::coordinator::run_job;
use crate::framing::Connection;

/// Launches one agent and yields a connection to it plus a lifecycle guard kept
/// alive for the duration of a run.
pub trait Launch {
    /// The byte stream the connection runs over.
    type Stream: AsyncRead + AsyncWrite + Unpin + Send + 'static;
    /// A handle held for the run's duration (a process or task lifecycle).
    type Guard: Send;

    /// Launch one agent.
    ///
    /// # Errors
    /// Returns an error if the agent cannot be launched.
    fn launch(
        &self,
    ) -> impl Future<Output = std::io::Result<(Connection<Self::Stream>, Self::Guard)>> + Send;
}

/// A homogeneous set of agent launchers.
pub struct Fleet<L> {
    launchers: Vec<L>,
}

impl<L> std::fmt::Debug for Fleet<L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Fleet")
            .field("agents", &self.launchers.len())
            .finish()
    }
}

impl<L: Launch> Fleet<L> {
    /// Build a fleet from a set of launchers.
    #[must_use]
    pub const fn new(launchers: Vec<L>) -> Self {
        Self { launchers }
    }

    /// Run `f` over `inputs` across the fleet, returning outputs in input order.
    ///
    /// The function is used only for its `type_name` key; the agents already
    /// hold the matching handler (registered under the same key).
    ///
    /// # Errors
    /// Returns an error if an agent cannot be launched or the run fails.
    pub async fn map<F, I, O>(
        &self,
        f: F,
        inputs: Vec<I>,
    ) -> std::io::Result<Vec<Result<O, String>>>
    where
        F: Fn(I) -> O,
        I: Serialize,
        O: DeserializeOwned,
    {
        let key = fn_key(&f);
        let mut connections = Vec::with_capacity(self.launchers.len());
        let mut guards = Vec::with_capacity(self.launchers.len());
        for launcher in &self.launchers {
            let (connection, guard) = launcher.launch().await?;
            connections.push(connection);
            guards.push(guard);
        }
        let result = run_job(connections, key, inputs).await;
        drop(guards); // agents already shut down via the job's `Shutdown`
        result
    }
}

#[cfg(test)]
mod tests {
    use super::{Fleet, Launch};
    use crate::agent::{serve, Registry};
    use crate::framing::Connection;
    use crate::testing::connection_pair;
    use tokio::io::DuplexStream;
    use tokio::task::JoinHandle;

    /// A launcher that runs an agent in-process over a duplex pipe.
    struct InProcess {
        registry: Registry,
        capacity: u32,
    }

    impl Launch for InProcess {
        type Stream = DuplexStream;
        type Guard = JoinHandle<std::io::Result<()>>;

        async fn launch(&self) -> std::io::Result<(Connection<DuplexStream>, Self::Guard)> {
            let (client, server) = connection_pair(256);
            let task = tokio::spawn(serve(server, self.registry.clone(), self.capacity));
            Ok((client, task))
        }
    }

    fn double(x: u32) -> u32 {
        x * 2
    }

    #[tokio::test]
    async fn map_runs_a_function_across_the_fleet() {
        let launchers = (0..3)
            .map(|_| InProcess {
                registry: Registry::new().with_fn(double),
                capacity: 2,
            })
            .collect();
        let fleet = Fleet::new(launchers);

        let out: Vec<Result<u32, String>> = fleet.map(double, (0..20u32).collect()).await.unwrap();

        assert_eq!(out, (0..20u32).map(|x| Ok(x * 2)).collect::<Vec<_>>());
        assert!(format!("{fleet:?}").contains("Fleet"));
    }
}
