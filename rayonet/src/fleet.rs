//! The user-facing fleet and its distributed map (PLAN.md Phase 3).
//!
//! A [`Fleet`] is a homogeneous set of agent launchers. The [`NetMapExt`] iterator
//! adapter (`inputs.net_map_with_fleet(map, &fleet)`) derives the wire key from
//! the task function (via `type_name`), launches one agent per launcher, runs
//! the job, and gathers the outputs in input order ([`NetMap::collect`]) or
//! folds them ([`NetMap::net_reduce`]). The launcher abstraction lets the same
//! API back onto in-process pipes (tests), spawned subprocesses, or ssh.

use std::future::Future;
use std::sync::Arc;

use serde::{de::DeserializeOwned, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::agent::fn_key;
use crate::coordinator::run_job;
use crate::framing::Connection;
use crate::observability::{EventSink, NoopSink};

/// Launches one agent and yields a connection to it plus a lifecycle guard kept
/// alive for the duration of a run.
pub trait Launch {
    /// The byte stream the connection runs over.
    type Stream: AsyncRead + AsyncWrite + Unpin + Send + 'static;
    /// A handle held for the run's duration (a process or task lifecycle).
    type Guard: Send;

    /// A stable name for the host this launcher targets, used to attribute
    /// observability events.
    fn label(&self) -> String;

    /// Launch one agent, emitting any provisioning progress to `events`.
    ///
    /// # Errors
    /// Returns an error if the agent cannot be launched.
    fn launch(
        &self,
        events: &dyn EventSink,
    ) -> impl Future<Output = std::io::Result<(Connection<Self::Stream>, Self::Guard)>> + Send;
}

/// A homogeneous set of agent launchers, with an optional observer for the run.
pub struct Fleet<L> {
    launchers: Vec<L>,
    events: Arc<dyn EventSink>,
}

impl<L> std::fmt::Debug for Fleet<L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Fleet")
            .field("agents", &self.launchers.len())
            .finish_non_exhaustive()
    }
}

impl<L: Launch> Fleet<L> {
    /// Build a fleet from a set of launchers, with the run unobserved.
    #[must_use]
    pub fn new(launchers: Vec<L>) -> Self {
        Self {
            launchers,
            events: Arc::new(NoopSink),
        }
    }

    /// Build a fleet whose run emits its event stream to `events`.
    #[must_use]
    pub fn observed(launchers: Vec<L>, events: Arc<dyn EventSink>) -> Self {
        Self { launchers, events }
    }

    /// Launch one agent per launcher, run `f` over `inputs`, and gather the
    /// per-task results in input order. The shared engine behind the `net_map`
    /// terminals; the function is used only for its `type_name` key (the agents
    /// already hold the matching handler).
    async fn run_map<F, I, O>(
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
        let events = self.events.as_ref();
        let mut agents = Vec::with_capacity(self.launchers.len());
        let mut guards = Vec::with_capacity(self.launchers.len());
        let mut failures = Vec::new();
        // A host that fails to launch (for example a cold host that cannot
        // install the toolchain) is dropped; its tasks are simply scheduled
        // onto the survivors by the pull scheduler.
        for launcher in &self.launchers {
            match launcher.launch(events).await {
                Ok((connection, guard)) => {
                    agents.push((launcher.label(), connection));
                    guards.push(guard);
                }
                Err(failure) => failures.push(failure),
            }
        }
        if agents.is_empty() {
            let detail = failures
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("; ");
            return Err(std::io::Error::other(format!(
                "rayonet: every host failed to launch: {detail}"
            )));
        }
        let result = run_job(agents, key, inputs, events).await;
        drop(guards); // agents already shut down via the job's `Shutdown`
        result
    }
}

/// A pending distributed map over a fleet, built by
/// [`NetMapExt::net_map_with_fleet`]. Nothing runs until a terminal:
/// [`NetMap::collect`] gathers every result, [`NetMap::net_reduce`] folds them.
#[must_use = "a NetMap runs nothing until `.collect()` or `.net_reduce()`"]
pub struct NetMap<'fleet, L, F, I> {
    fleet: &'fleet Fleet<L>,
    map: F,
    inputs: Vec<I>,
}

impl<L, F, I> std::fmt::Debug for NetMap<'_, L, F, I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NetMap")
            .field("tasks", &self.inputs.len())
            .finish_non_exhaustive()
    }
}

impl<L: Launch, F, I> NetMap<'_, L, F, I>
where
    I: Serialize,
{
    /// Gather every task's result, in input order: `Ok(output)` for a task that
    /// returned, `Err(message)` for one that panicked.
    ///
    /// # Errors
    /// Returns an error if an agent cannot be launched or the run fails.
    pub async fn collect<O>(self) -> std::io::Result<Vec<Result<O, String>>>
    where
        F: Fn(I) -> O,
        O: DeserializeOwned,
    {
        self.fleet.run_map(self.map, self.inputs).await
    }

    /// Fold the task outputs into one value on the coordinator with `op`, like
    /// [`Iterator::reduce`]: `None` if there were no tasks, otherwise the
    /// combined result. The first task to panic short-circuits to an error.
    ///
    /// `op` runs only on the coordinator and may capture. It should be
    /// associative; outputs are folded in input order.
    ///
    /// # Errors
    /// Returns an error if an agent cannot be launched, the run fails, or any
    /// task panics (the first panic message is surfaced).
    pub async fn net_reduce<O>(self, op: impl Fn(O, O) -> O) -> std::io::Result<Option<O>>
    where
        F: Fn(I) -> O,
        O: DeserializeOwned,
    {
        let mut outputs = self.fleet.run_map(self.map, self.inputs).await?.into_iter();
        let Some(first) = outputs.next() else {
            return Ok(None);
        };
        let mut acc = first.map_err(std::io::Error::other)?;
        for output in outputs {
            acc = op(acc, output.map_err(std::io::Error::other)?);
        }
        Ok(Some(acc))
    }
}

/// Distributed map as an iterator adapter.
///
/// `inputs.net_map_with_fleet(map, &fleet)` runs `map` over the items across
/// `fleet`. Implemented for every [`IntoIterator`], so a `Vec`, a range, or any
/// iterator of task inputs works.
pub trait NetMapExt: IntoIterator + Sized {
    /// Map `map` over these items, distributed across `fleet`. Terminate with
    /// [`NetMap::collect`] or [`NetMap::net_reduce`].
    ///
    /// `map` is the task shipped to the agents, keyed by its `type_name`, so it
    /// must be a named function or a non-capturing closure (enforced at compile
    /// time). A future `net_map(map)` will target a global fleet implicitly.
    ///
    /// # Examples
    /// A capturing closure is rejected at compile time:
    /// ```compile_fail
    /// use rayonet::fleet::{Fleet, NetMapExt, Subprocess};
    /// let fleet: Fleet<Subprocess> = Fleet::new(vec![]);
    /// let captured = 10u32;
    /// let _ = std::iter::once(1u32).net_map_with_fleet(move |x: u32| x + captured, &fleet);
    /// ```
    fn net_map_with_fleet<F, L, O>(self, map: F, fleet: &Fleet<L>) -> NetMap<'_, L, F, Self::Item>
    where
        F: Fn(Self::Item) -> O,
        L: Launch,
    {
        // Reject capturing closures at compile time: a named function or
        // non-capturing closure is zero-sized; captured state is not. This keeps
        // the unique fn-item type that the `type_name` key relies on.
        const {
            assert!(
                size_of::<F>() == 0,
                "a `.net_map` task function must not capture; use a named function or a non-capturing closure"
            );
        }
        NetMap {
            fleet,
            map,
            inputs: self.into_iter().collect(),
        }
    }
}

impl<T: IntoIterator> NetMapExt for T {}

/// Launches an agent by spawning a command as a subprocess.
///
/// Typically the consumer's own binary (one binary, two roles).
pub struct Subprocess {
    program: std::ffi::OsString,
}

impl std::fmt::Debug for Subprocess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Subprocess").finish_non_exhaustive()
    }
}

impl Subprocess {
    /// Launch the current executable as the agent (the common case: the
    /// consumer's binary is both coordinator and agent).
    ///
    /// # Errors
    /// Returns an error if the current executable path cannot be determined.
    pub fn current_exe() -> std::io::Result<Self> {
        Ok(Self {
            program: std::env::current_exe()?.into_os_string(),
        })
    }

    /// Launch the binary at `program` as the agent.
    #[must_use]
    pub fn command(program: impl Into<std::ffi::OsString>) -> Self {
        Self {
            program: program.into(),
        }
    }
}

impl Launch for Subprocess {
    type Stream = tokio::io::Join<tokio::process::ChildStdout, tokio::process::ChildStdin>;
    type Guard = crate::process::AgentProcess;

    fn label(&self) -> String {
        self.program.to_string_lossy().into_owned()
    }

    // A local subprocess does not provision, so there is no progress to emit.
    async fn launch(
        &self,
        _events: &dyn EventSink,
    ) -> std::io::Result<(Connection<Self::Stream>, Self::Guard)> {
        crate::process::spawn(tokio::process::Command::new(&self.program))
    }
}

#[cfg(feature = "rayon")]
mod rayon_bridge {
    use super::{Fleet, Launch, NetMapExt};
    use rayon::iter::ParallelIterator;
    use serde::{de::DeserializeOwned, Serialize};
    use std::marker::PhantomData;

    /// Adds `.net_map(f)` to any rayon `ParallelIterator`: the ordered barrier bridge.
    ///
    /// It drains the upstream iterator, runs `f` distributed over a fleet, and
    /// returns outputs in input order so the chain can re-enter rayon.
    pub trait RayonNetMapExt: ParallelIterator + Sized {
        /// Begin a distributed map of `f` over this iterator's items; terminate
        /// with [`NetMapJob::run`].
        fn net_map<F, O>(self, f: F) -> NetMapJob<Self, F, O>
        where
            F: Fn(Self::Item) -> O,
        {
            NetMapJob {
                upstream: self,
                f,
                _output: PhantomData,
            }
        }
    }

    impl<P: ParallelIterator> RayonNetMapExt for P {}

    /// A pending distributed map built by [`RayonNetMapExt::net_map`].
    pub struct NetMapJob<P, F, O> {
        upstream: P,
        f: F,
        _output: PhantomData<O>,
    }

    impl<P, F, O> std::fmt::Debug for NetMapJob<P, F, O> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("NetMapJob").finish_non_exhaustive()
        }
    }

    impl<P, F, O> NetMapJob<P, F, O>
    where
        P: ParallelIterator,
        P::Item: Serialize + Send,
        F: Fn(P::Item) -> O,
        O: DeserializeOwned + Send,
    {
        /// Drain the upstream iterator and run the job over `fleet`. Blocks on a
        /// fresh runtime, so it must be called outside a tokio runtime.
        ///
        /// # Errors
        /// Returns an error if the runtime cannot start or the run fails.
        pub fn run<L: Launch>(self, fleet: &Fleet<L>) -> std::io::Result<Vec<Result<O, String>>> {
            let inputs: Vec<P::Item> = self.upstream.collect();
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(inputs.net_map_with_fleet(self.f, fleet).collect())
        }
    }
}

#[cfg(feature = "rayon")]
pub use rayon_bridge::{NetMapJob, RayonNetMapExt};

#[cfg(test)]
mod tests {
    use super::{EventSink, Fleet, Launch, NetMapExt};
    use crate::agent::{serve, Registry};
    use crate::framing::Connection;
    use crate::testing::connection_pair;
    use tokio::io::DuplexStream;
    use tokio::task::JoinHandle;

    /// A launcher that runs an agent in-process over a duplex pipe. A `None`
    /// registry simulates a host that fails to launch (for example a cold host
    /// that cannot be provisioned).
    struct InProcess {
        registry: Option<Registry>,
    }

    impl InProcess {
        fn serving(double: bool) -> Self {
            let registry = double.then(|| Registry::new().with_fn(self::double));
            Self { registry }
        }

        fn serving_registry(registry: Registry) -> Self {
            Self {
                registry: Some(registry),
            }
        }
    }

    impl Launch for InProcess {
        type Stream = DuplexStream;
        type Guard = JoinHandle<std::io::Result<()>>;

        fn label(&self) -> String {
            "in-process".to_string()
        }

        async fn launch(
            &self,
            _events: &dyn EventSink,
        ) -> std::io::Result<(Connection<DuplexStream>, Self::Guard)> {
            let Some(registry) = &self.registry else {
                return Err(std::io::Error::other("simulated launch failure"));
            };
            let (client, server) = connection_pair(256);
            let task = tokio::spawn(serve(server, registry.clone()));
            Ok((client, task))
        }
    }

    fn double(x: u32) -> u32 {
        x * 2
    }

    fn boom(x: u32) -> u32 {
        assert!(x < 5, "too big: {x}");
        x
    }

    // A named reduce op shared across the net_reduce tests, so the empty-input
    // case (which never folds) does not leave an uncovered closure behind.
    fn add(a: u32, b: u32) -> u32 {
        a + b
    }

    #[tokio::test]
    async fn netreduce_folds_outputs_on_the_coordinator() {
        let launchers = (0..3).map(|_| InProcess::serving(true)).collect();
        let fleet = Fleet::new(launchers);

        let job = (0..10u32).net_map_with_fleet(double, &fleet);
        assert!(format!("{job:?}").contains("NetMap"));
        let sum = job.net_reduce(add).await.unwrap();

        assert_eq!(sum, Some((0..10u32).map(|x| x * 2).sum()));
    }

    #[tokio::test]
    async fn netreduce_is_none_for_empty_input() {
        let launchers = (0..2).map(|_| InProcess::serving(true)).collect();
        let fleet = Fleet::new(launchers);

        let sum = Vec::<u32>::new()
            .net_map_with_fleet(double, &fleet)
            .net_reduce(add)
            .await
            .unwrap();

        assert_eq!(sum, None);
    }

    #[tokio::test]
    async fn netreduce_short_circuits_on_the_first_task_failure() {
        let launchers = vec![
            InProcess::serving_registry(Registry::new().with_fn(boom)),
            InProcess::serving_registry(Registry::new().with_fn(boom)),
        ];
        let fleet = Fleet::new(launchers);

        let err = (0..10u32)
            .net_map_with_fleet(boom, &fleet)
            .net_reduce(add)
            .await
            .unwrap_err();

        assert!(err.to_string().contains("too big"), "{err}");
    }

    #[tokio::test]
    async fn netmap_runs_a_function_across_the_fleet() {
        let launchers = (0..3).map(|_| InProcess::serving(true)).collect();
        let fleet = Fleet::new(launchers);

        let out: Vec<Result<u32, String>> = (0..20u32)
            .net_map_with_fleet(double, &fleet)
            .collect()
            .await
            .unwrap();

        assert_eq!(out, (0..20u32).map(|x| Ok(x * 2)).collect::<Vec<_>>());
        assert!(format!("{fleet:?}").contains("Fleet"));
    }

    #[tokio::test]
    async fn an_observed_fleet_emits_the_run_event_stream() {
        use crate::observability::{NodeState, RunState};
        use crate::testing::EventRecorder;
        use std::sync::Arc;

        let sink = Arc::new(EventRecorder::default());
        let launchers = (0..2).map(|_| InProcess::serving(true)).collect();
        let fleet = Fleet::observed(launchers, sink.clone());

        let out: Vec<Result<u32, String>> = (0..10u32)
            .net_map_with_fleet(double, &fleet)
            .collect()
            .await
            .unwrap();
        assert_eq!(out.len(), 10);

        let mut state = RunState::default();
        for event in &sink.events() {
            state.apply(event);
        }
        assert_eq!(state.total_tasks, 10);
        assert_eq!(state.completed, 10);
        assert_eq!(state.nodes["in-process"].state, NodeState::Done);
    }

    #[tokio::test]
    async fn netmap_proceeds_when_some_hosts_fail_to_launch() {
        // One healthy host, two that fail to launch: the job still completes,
        // the survivor absorbing every task.
        let launchers = vec![
            InProcess::serving(false),
            InProcess::serving(true),
            InProcess::serving(false),
        ];
        let fleet = Fleet::new(launchers);

        let out: Vec<Result<u32, String>> = (0..15u32)
            .net_map_with_fleet(double, &fleet)
            .collect()
            .await
            .unwrap();

        assert_eq!(out, (0..15u32).map(|x| Ok(x * 2)).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn netmap_errors_when_every_host_fails_to_launch() {
        let launchers = vec![InProcess::serving(false), InProcess::serving(false)];
        let fleet = Fleet::new(launchers);

        let error = (0..5u32)
            .net_map_with_fleet(double, &fleet)
            .collect::<u32>()
            .await
            .unwrap_err();
        assert!(error.to_string().contains("every host failed"), "{error}");
    }

    #[cfg(feature = "rayon")]
    #[test]
    fn netmap_composes_inside_a_rayon_chain() {
        use super::RayonNetMapExt;
        use rayon::iter::{IntoParallelIterator, ParallelIterator};

        let launchers = (0..2).map(|_| InProcess::serving(true)).collect();
        let fleet = Fleet::new(launchers);

        // rayon before -> distributed net_map -> rayon after.
        let job = (0..10u32).into_par_iter().map(|x| x + 1).net_map(double);
        assert!(format!("{job:?}").contains("NetMapJob"));
        let out: Vec<u32> = job
            .run(&fleet)
            .unwrap()
            .into_par_iter()
            .map(|r| r.unwrap() + 100)
            .collect();

        let expected: Vec<u32> = (0..10u32).map(|x| (x + 1) * 2 + 100).collect();
        assert_eq!(out, expected);
    }
}
