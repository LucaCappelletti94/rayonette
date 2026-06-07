//! The user-facing fleet and its distributed map (PLAN.md Phase 3).
//!
//! A [`Fleet`] is a homogeneous set of agent launchers. The [`NetMapExt`] iterator
//! adapter (`inputs.net_map_with_fleet(map, &fleet)`) derives the wire key from
//! the task function (via `type_name`), launches one agent per launcher, runs
//! the job, and gathers the outputs in input order ([`NetMap::collect`]) or
//! folds them ([`NetMap::net_reduce`]). The launcher abstraction lets the same
//! API back onto in-process pipes (tests), spawned subprocesses, or ssh.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use serde::{de::DeserializeOwned, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;

use crate::agent::fn_key;
use crate::capability::{pred::Predicate, Filter, NodeProfile, Role};
use crate::coordinator::{
    decode_output, handshake_join, run_job_raw_with_joins, serialize_inputs, Joiner, RunOptions,
};
use crate::framing::Connection;
use crate::observability::{Event, EventSink, NoopSink};

/// How the rejoin driver retries a host that was unreachable at launch: how long
/// to wait between attempts, and how many attempts before giving that host up.
/// Bounded attempts, no wall clock: the run errors only when no agent is alive and
/// every candidate is exhausted.
#[derive(Clone, Copy)]
struct RejoinPolicy {
    backoff: Duration,
    max_attempts: usize,
}

/// The production rejoin tuning. Tests pass a tighter policy for speed.
const REJOIN_POLICY: RejoinPolicy = RejoinPolicy {
    backoff: Duration::from_millis(200),
    max_attempts: 150,
};

/// Launches one agent in three phases so a host's capabilities can be probed and
/// filtered before the expensive provisioning step: connect, then probe, then
/// activate.
pub trait Launch {
    /// The byte stream the connection runs over.
    type Stream: AsyncRead + AsyncWrite + Unpin + Send + 'static;
    /// A handle held for the run's duration (a process or task lifecycle).
    type Guard: Send;
    /// The live transport to a host, established by [`Launch::connect`] and
    /// consumed by [`Launch::activate`] (an ssh session, or a trivial handle for
    /// a local launcher).
    type Session: Send;

    /// A stable name for the host this launcher targets, used to attribute
    /// observability events.
    fn label(&self) -> String;

    /// Phase 1: establish the transport to the host. Cheap, so it runs before
    /// any capability filtering.
    ///
    /// # Errors
    /// Returns an error if the host cannot be reached.
    fn connect(&self) -> impl Future<Output = std::io::Result<Self::Session>> + Send;

    /// Phase 2: probe the host's [`NodeProfile`] over an established session.
    /// Defaults to an unknown profile, for launchers with no real host to probe
    /// (a local subprocess or an in-process test agent).
    ///
    /// # Errors
    /// Returns an error if the probe cannot run.
    fn probe(
        &self,
        _session: &Self::Session,
    ) -> impl Future<Output = std::io::Result<NodeProfile>> + Send {
        std::future::ready(Ok(NodeProfile::unknown()))
    }

    /// A stable id for the physical node behind this launcher, so the same node
    /// reached by two paths is recognized as one (redundant-path dedup). Probed
    /// over the session, best-effort: it defaults to the [`Launch::label`], which
    /// the real ssh launcher overrides with the host's machine id.
    fn node_id(&self, _session: &Self::Session) -> impl Future<Output = String> + Send {
        std::future::ready(self.label())
    }

    /// Phase 3: provision and spawn the agent over the session, emitting any
    /// progress to `events`.
    ///
    /// # Errors
    /// Returns an error if the agent cannot be provisioned or spawned.
    fn activate(
        &self,
        session: Self::Session,
        events: &dyn EventSink,
    ) -> impl Future<Output = std::io::Result<(Connection<Self::Stream>, Self::Guard)>> + Send;
}

/// A homogeneous set of agent launchers, with an optional observer for the run
/// and an optional capability filter assigning each host a [`Role`].
pub struct Fleet<L> {
    launchers: Vec<L>,
    events: Arc<dyn EventSink>,
    filter: Option<Filter>,
}

impl<L> std::fmt::Debug for Fleet<L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Fleet")
            .field("agents", &self.launchers.len())
            .field("filtered", &self.filter.is_some())
            .finish_non_exhaustive()
    }
}

impl<L: Launch> Fleet<L> {
    /// Build a fleet from a set of launchers, with the run unobserved and every
    /// host treated as [`Role::Compute`].
    #[must_use]
    pub fn new(launchers: Vec<L>) -> Self {
        Self {
            launchers,
            events: Arc::new(NoopSink),
            filter: None,
        }
    }

    /// Build a fleet whose run emits its event stream to `events`.
    #[must_use]
    pub fn observed(launchers: Vec<L>, events: Arc<dyn EventSink>) -> Self {
        Self {
            launchers,
            events,
            filter: None,
        }
    }

    /// Map each host to a [`Role`] from its probed capabilities: only hosts the
    /// `filter` assigns [`Role::Compute`] run tasks; the rest are dropped before
    /// they are provisioned. Without a filter every host is `Compute`.
    #[must_use]
    pub fn with_filter(mut self, filter: Filter) -> Self {
        self.filter = Some(filter);
        self
    }
}

/// The raw, byte-level result of running a job: each task's output bytes or its
/// error message, in input order.
type RawResults = std::io::Result<Vec<Result<Vec<u8>, String>>>;

/// The agents a [`launch_all`] pass brought up, plus the guards that keep them
/// alive and the failures of the hosts it dropped.
pub(crate) struct Launched<L: Launch> {
    /// Each ready agent's label and live connection, ready to hand to a
    /// coordinator ([`run_job_raw`]) or a relay (`crate::relay`).
    pub agents: Vec<(String, Connection<L::Stream>)>,
    /// Each ready agent's stable physical node id, parallel to `agents`, so a
    /// relay can advertise its children by id for redundant-path dedup.
    pub ids: Vec<String>,
    /// Each ready agent's measured link latency (microseconds), parallel to
    /// `agents`, the weight redundant paths are chosen by.
    pub latencies: Vec<u64>,
    /// A guard per ready agent, held for the run's duration.
    pub guards: Vec<L::Guard>,
    /// Why each dropped host did not join (for the no-eligible-host message).
    pub failures: Vec<std::io::Error>,
}

/// A host discovery has brought up: identified and eligible to run, holding the
/// session [`provision_all`] will consume to activate it. Discovery identifies
/// every host in a layer before any is activated, so a node reached by two relays
/// can be deduped by id before either path provisions it (a payoff that lands in
/// later redundancy work. Here every eligible host is simply provisioned).
struct Discovered<'a, L: Launch> {
    /// The launcher that discovered (and will activate) this host.
    launcher: &'a L,
    /// The host's label, used to attribute its observability events.
    label: String,
    /// The host's stable physical node id, carried through to [`Launched::ids`].
    id: String,
    /// The measured link latency (microseconds), carried to [`Launched::latencies`].
    latency_us: u64,
    /// The open session from [`Launch::connect`], handed to [`Launch::activate`].
    session: L::Session,
}

/// Discovery: connect, probe, and identify every launcher, emitting each as a
/// `Profiled` fact, and return those eligible to run (assigned [`Role::Compute`]
/// and meeting the job `requires`) holding their open sessions. A host that fails
/// to connect or probe is dropped with its error kept (for the no-eligible-host
/// message) and returned as a retry candidate: it may yet come online, so the
/// rejoin driver retries it (R6 elastic membership). A filtered or unneeded host
/// is dropped without error and is not retried: its exclusion is permanent. No
/// host is activated here: identity precedes provisioning.
async fn discover_all<'a, L: Launch + Send + Sync>(
    launchers: &'a [L],
    filter: Option<&Filter>,
    requires: Option<&Predicate>,
    events: &dyn EventSink,
) -> (Vec<Discovered<'a, L>>, Vec<std::io::Error>, Vec<&'a L>) {
    let mut eligible = Vec::with_capacity(launchers.len());
    let mut failures = Vec::new();
    let mut retry = Vec::new();
    for launcher in launchers {
        let label = launcher.label();
        let session = match launcher.connect().await {
            Ok(session) => session,
            Err(failure) => {
                failures.push(failure);
                retry.push(launcher);
                continue;
            }
        };
        // Time the probe round-trip as the link's latency, the weight redundant
        // paths are chosen by.
        let probe_started = std::time::Instant::now();
        let profile = match launcher.probe(&session).await {
            Ok(profile) => profile,
            Err(failure) => {
                failures.push(failure);
                retry.push(launcher);
                continue;
            }
        };
        let latency_us = u64::try_from(probe_started.elapsed().as_micros()).unwrap_or(u64::MAX);
        let role = filter.map_or(Role::Compute, |filter| filter.role_of(&profile));
        // A job requirement narrows the compute hosts further: a host the fleet
        // runs tasks on, but whose capabilities this job does not need, is
        // skipped for this run only.
        let meets_requirement = requires.is_none_or(|predicate| predicate.eval(&profile));
        let id = launcher.node_id(&session).await;
        events.emit(Event::profiled(&label, &id, profile, role, latency_us));
        if role != Role::Compute || !meets_requirement {
            continue;
        }
        eligible.push(Discovered {
            launcher,
            label,
            id,
            latency_us,
            session,
        });
    }
    (eligible, failures, retry)
}

/// Provisioning: activate and spawn the agent on each discovered host, consuming
/// its session. A host whose activation fails is dropped (its error added to the
/// discovery `failures`), so a cold host never blocks the run.
async fn provision_all<L: Launch + Send + Sync>(
    discovered: Vec<Discovered<'_, L>>,
    mut failures: Vec<std::io::Error>,
    events: &dyn EventSink,
) -> Launched<L> {
    let mut agents = Vec::with_capacity(discovered.len());
    let mut ids = Vec::with_capacity(discovered.len());
    let mut latencies = Vec::with_capacity(discovered.len());
    let mut guards = Vec::with_capacity(discovered.len());
    for host in discovered {
        match host.launcher.activate(host.session, events).await {
            Ok((connection, guard)) => {
                agents.push((host.label, connection));
                ids.push(host.id);
                latencies.push(host.latency_us);
                guards.push(guard);
            }
            Err(failure) => failures.push(failure),
        }
    }
    Launched {
        agents,
        ids,
        latencies,
        guards,
        failures,
    }
}

/// Bring up a set of launchers in two passes: discover every host (connect,
/// probe, identify), dropping any the `filter` does not assign [`Role::Compute`]
/// or that fail the job `requires`, then provision (activate) the survivors. A
/// host that fails any phase is dropped (its error kept), so a cold or filtered
/// host never blocks the run. Splitting discovery from provisioning means every
/// host's identity is known before any is activated. Shared by the fleet
/// coordinator and the relay (which launches its own children the same way).
pub(crate) async fn launch_all<L: Launch + Send + Sync>(
    launchers: &[L],
    filter: Option<&Filter>,
    requires: Option<&Predicate>,
    events: &dyn EventSink,
) -> Launched<L> {
    // The relay launches a fixed child set, so it does not retry: the rejoin
    // candidates are for the coordinator's re-entrant discovery only.
    let (discovered, failures, _retry) = discover_all(launchers, filter, requires, events).await;
    provision_all(discovered, failures, events).await
}

/// One attempt to bring a candidate that was unreachable at launch into a running
/// job: `Joined` if it came online and handshaked, `Retry` if it is still down
/// (try again later), or `GiveUp` if it is now permanently excluded (the filter
/// or the job requirement rules it out on this attempt).
enum JoinAttempt<L: Launch> {
    Joined(Joiner<L::Stream>, L::Guard),
    Retry,
    GiveUp,
}

/// Re-entrant discovery (R6 elastic membership): retry every candidate that was
/// unreachable at launch on a backoff, feeding each one that comes online into
/// the run's `joins_tx`, and hold the guards that keep them alive. A candidate is
/// retried up to `policy.max_attempts` times, then given up; one ruled out by the
/// filter or requirement is given up at once. Returns when no candidate remains
/// (or the run ended), dropping `joins_tx` so the run learns no node can join.
///
/// The per-attempt handshake (connect, probe, identify, filter, provision,
/// handshake) is inlined rather than a helper, so it does not monomorphize into a
/// separate function per launcher type that a fixed fleet would never exercise.
async fn rejoin_driver<L: Launch + Send + Sync>(
    candidates: Vec<&L>,
    fn_key: &str,
    filter: Option<&Filter>,
    requires: Option<&Predicate>,
    joins_tx: mpsc::UnboundedSender<Joiner<L::Stream>>,
    policy: RejoinPolicy,
    events: &dyn EventSink,
) -> Vec<L::Guard> {
    let mut guards = Vec::new();
    // Each candidate carries how many attempts it has left.
    let mut pending: Vec<(&L, usize)> = candidates
        .into_iter()
        .map(|launcher| (launcher, policy.max_attempts))
        .collect();
    while !pending.is_empty() && !joins_tx.is_closed() {
        let mut still = Vec::new();
        for (launcher, remaining) in pending {
            // Transport hiccups (connect, probe, activate, handshake) are
            // retryable; a filter or requirement exclusion is permanent.
            let attempt: JoinAttempt<L> = 'attempt: {
                let label = launcher.label();
                let Ok(session) = launcher.connect().await else {
                    break 'attempt JoinAttempt::Retry;
                };
                let probe_started = std::time::Instant::now();
                let Ok(profile) = launcher.probe(&session).await else {
                    break 'attempt JoinAttempt::Retry;
                };
                let latency_us =
                    u64::try_from(probe_started.elapsed().as_micros()).unwrap_or(u64::MAX);
                let role = filter.map_or(Role::Compute, |filter| filter.role_of(&profile));
                let meets_requirement = requires.is_none_or(|predicate| predicate.eval(&profile));
                let id = launcher.node_id(&session).await;
                events.emit(Event::profiled(&label, &id, profile, role, latency_us));
                if role != Role::Compute || !meets_requirement {
                    break 'attempt JoinAttempt::GiveUp;
                }
                let Ok((connection, guard)) = launcher.activate(session, events).await else {
                    break 'attempt JoinAttempt::Retry;
                };
                handshake_join(label, connection, fn_key)
                    .await
                    .map_or(JoinAttempt::Retry, |joiner| {
                        JoinAttempt::Joined(joiner, guard)
                    })
            };
            match attempt {
                JoinAttempt::Joined(joiner, guard) => {
                    // A send error means the run already ended; drop the agent.
                    if joins_tx.send(joiner).is_ok() {
                        guards.push(guard);
                    }
                }
                JoinAttempt::Retry if remaining > 1 => still.push((launcher, remaining - 1)),
                JoinAttempt::Retry | JoinAttempt::GiveUp => {}
            }
        }
        pending = still;
        if pending.is_empty() || joins_tx.is_closed() {
            break;
        }
        tokio::time::sleep(policy.backoff).await;
    }
    guards
}

/// The no-eligible-host error message from a [`launch_all`] pass that left no
/// agents, naming why each candidate dropped out.
pub(crate) fn no_eligible_host(failures: &[std::io::Error]) -> std::io::Error {
    let detail = failures
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("; ");
    std::io::Error::other(format!(
        "rayonet: no eligible host (every host failed to launch or was filtered out): {detail}"
    ))
}

/// A fleet behind a type-erased, byte-level interface.
///
/// A process-global (the implicit fleet behind [`NetMapExt::net_map`]) cannot
/// name a launcher type `L`, so it stores `Arc<dyn ErasedFleet>` instead. The
/// erasure boundary is bytes (the `type_name` key plus serialized inputs in, raw
/// outputs out), which is exactly what already crosses the wire.
trait ErasedFleet: Send + Sync {
    /// Launch the fleet, run `fn_key` over `payloads`, and return each task's raw
    /// output bytes or error, in input order.
    fn run_erased<'a>(
        &'a self,
        fn_key: &'a str,
        payloads: Vec<Vec<u8>>,
        requires: Option<&'a Predicate>,
        options: RunOptions,
    ) -> Pin<Box<dyn Future<Output = RawResults> + Send + 'a>>;
}

impl<L: Launch + Send + Sync> ErasedFleet for Fleet<L> {
    fn run_erased<'a>(
        &'a self,
        fn_key: &'a str,
        payloads: Vec<Vec<u8>>,
        requires: Option<&'a Predicate>,
        options: RunOptions,
    ) -> Pin<Box<dyn Future<Output = RawResults> + Send + 'a>> {
        Box::pin(async move {
            let events = self.events.as_ref();
            let filter = self.filter.as_ref();
            // Discover the layer, then provision the eligible hosts. A host that
            // fails to launch or that the filter/requirement excludes is dropped;
            // one that was merely unreachable becomes a rejoin candidate.
            let (eligible, failures, retry) =
                discover_all(&self.launchers, filter, requires, events).await;
            let Launched {
                agents,
                latencies,
                guards,
                failures,
                ..
            } = provision_all(eligible, failures, events).await;
            // A run needs at least one live host to start; with none, no node
            // could have joined a run that never began, so this is a hard failure.
            if agents.is_empty() {
                return Err(no_eligible_host(&failures));
            }

            // Run the job while a rejoin driver retries the unreachable candidates
            // and feeds any that come online into the live run. The two run
            // concurrently in one task (so the driver may borrow the launchers);
            // the job's result returns as soon as it completes, and the driver's
            // guards (the agents it joined) are held until then.
            let (joins_tx, joins_rx) = mpsc::unbounded_channel();
            let job = run_job_raw_with_joins(
                agents, fn_key, payloads, &latencies, options, joins_rx, events,
            );
            let driver = rejoin_driver(
                retry,
                fn_key,
                filter,
                requires,
                joins_tx,
                REJOIN_POLICY,
                events,
            );
            tokio::pin!(job);
            tokio::pin!(driver);
            let mut driver_done = false;
            let mut joiner_guards = Vec::new();
            let result = loop {
                tokio::select! {
                    job_result = &mut job => break job_result,
                    driver_guards = &mut driver, if !driver_done => {
                        joiner_guards = driver_guards;
                        driver_done = true;
                    }
                }
            };
            drop(guards); // agents already shut down via the job's `Shutdown`
            drop(joiner_guards);
            result
        })
    }
}

/// The process-global fleet that [`NetMapExt::net_map`] runs against. Installed
/// once with [`install_fleet`]; replaceable.
static GLOBAL_FLEET: RwLock<Option<Arc<dyn ErasedFleet>>> = RwLock::new(None);

/// Install (or replace) the process-global fleet that bare `net_map(map)` runs
/// against, so the common case can drop the explicit `&fleet`.
///
/// Without this, `inputs.net_map(map)` errors when run; use
/// [`NetMapExt::net_map_with_fleet`] to pass a fleet explicitly instead.
pub fn install_fleet<L: Launch + Send + Sync + 'static>(fleet: Fleet<L>) {
    *GLOBAL_FLEET
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::new(fleet));
}

/// The currently installed global fleet, if any.
fn global_fleet() -> Option<Arc<dyn ErasedFleet>> {
    GLOBAL_FLEET
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
}

/// Which fleet a [`NetMap`] runs against: one borrowed explicitly, or the
/// process-global installed with [`install_fleet`].
enum RunnerRef<'a> {
    Borrowed(&'a dyn ErasedFleet),
    Global,
}

/// A pending distributed map over a fleet.
///
/// Built by [`NetMapExt::net_map`] or [`NetMapExt::net_map_with_fleet`]. Nothing
/// runs until a terminal: [`NetMap::collect`] gathers every result,
/// [`NetMap::net_reduce`] / [`NetMap::net_fold`] fold them.
#[must_use = "a NetMap runs nothing until `.collect()`, `.net_reduce()`, or `.net_fold()`"]
pub struct NetMap<'a, F, I> {
    runner: RunnerRef<'a>,
    map: F,
    inputs: Vec<I>,
    requires: Option<Predicate>,
    require_redundancy: bool,
    speculative: bool,
}

impl<F, I> std::fmt::Debug for NetMap<'_, F, I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NetMap")
            .field("tasks", &self.inputs.len())
            .finish_non_exhaustive()
    }
}

impl<F, I> NetMap<'_, F, I> {
    /// Run this job only on hosts whose [`NodeProfile`] satisfies `predicate`
    /// (for example `requires(pred::rocm())` for a job that needs `ROCm`).
    ///
    /// This narrows within the fleet's compute hosts for this run only: it is the
    /// per-job counterpart to the fleet-wide [`Fleet::with_filter`]. A job no host
    /// can satisfy fails with the no-eligible-host error.
    pub fn requires(mut self, predicate: Predicate) -> Self {
        self.requires = Some(predicate);
        self
    }

    /// Refuse to run if any compute node is reachable through only one relay, so a
    /// single relay's death could strand it. The run fails before any task starts,
    /// naming the nodes that lack a redundant path.
    pub const fn require_redundancy(mut self) -> Self {
        self.require_redundancy = true;
        self
    }

    /// Race the tail of the run: when no task is pending but some are still in
    /// flight, let an idle node re-run a straggler, first result winning (deduped).
    /// Off by default; it trades extra compute for a shorter tail when a node
    /// lags. Safe by the same idempotency contract reruns already rely on.
    pub const fn speculative(mut self) -> Self {
        self.speculative = true;
        self
    }
}

impl<F, I> NetMap<'_, F, I>
where
    I: Serialize,
{
    /// Serialize the inputs, run the job against the chosen fleet, and decode
    /// each output. The shared engine behind every terminal.
    async fn run<O>(self) -> std::io::Result<Vec<Result<O, String>>>
    where
        F: Fn(I) -> O,
        O: DeserializeOwned,
    {
        let key = fn_key(&self.map);
        let payloads = serialize_inputs(&self.inputs)?;
        let requires = self.requires;
        let options = RunOptions {
            require_redundancy: self.require_redundancy,
            speculative: self.speculative,
        };
        let raw = match self.runner {
            RunnerRef::Borrowed(fleet) => {
                fleet
                    .run_erased(key, payloads, requires.as_ref(), options)
                    .await?
            }
            RunnerRef::Global => {
                let fleet = global_fleet().ok_or_else(|| {
                    std::io::Error::other(
                        "rayonet: no global fleet installed; call rayonet::install_fleet(fleet) or use net_map_with_fleet",
                    )
                })?;
                fleet
                    .run_erased(key, payloads, requires.as_ref(), options)
                    .await?
            }
        };
        Ok(raw.into_iter().map(decode_output::<O>).collect())
    }

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
        self.run().await
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
        let mut outputs = self.run::<O>().await?.into_iter();
        let Some(first) = outputs.next() else {
            return Ok(None);
        };
        let mut acc = first.map_err(std::io::Error::other)?;
        for output in outputs {
            acc = op(acc, output.map_err(std::io::Error::other)?);
        }
        Ok(Some(acc))
    }

    /// Fold the task outputs into an accumulator on the coordinator with `op`,
    /// like [`Iterator::fold`]: starting from `init`, each output is folded in
    /// input order. Unlike [`NetMap::net_reduce`] the accumulator type `B` may
    /// differ from the task output, and an empty run returns `init`. The first
    /// task to panic short-circuits to an error.
    ///
    /// `op` runs only on the coordinator and may capture. The fold is a strict
    /// left fold over the inputs, so `op` need not be associative.
    ///
    /// # Errors
    /// Returns an error if an agent cannot be launched, the run fails, or any
    /// task panics (the first panic message is surfaced).
    pub async fn net_fold<O, B>(self, init: B, op: impl Fn(B, O) -> B) -> std::io::Result<B>
    where
        F: Fn(I) -> O,
        O: DeserializeOwned,
    {
        let mut acc = init;
        for output in self.run::<O>().await? {
            acc = op(acc, output.map_err(std::io::Error::other)?);
        }
        Ok(acc)
    }
}

/// Distributed map as an iterator adapter.
///
/// `inputs.net_map(map)` runs `map` over the items across the global fleet (see
/// [`install_fleet`]); `inputs.net_map_with_fleet(map, &fleet)` runs against an
/// explicit one. Implemented for every [`IntoIterator`], so a `Vec`, a range, or
/// any iterator of task inputs works.
pub trait NetMapExt: IntoIterator + Sized {
    /// Map `map` over these items across the process-global fleet (installed
    /// with [`install_fleet`]). Terminate with [`NetMap::collect`],
    /// [`NetMap::net_reduce`], or [`NetMap::net_fold`].
    ///
    /// `map` is the task shipped to the agents, keyed by its `type_name`, so it
    /// must be a named function or a non-capturing closure (enforced at compile
    /// time). Running this with no global fleet installed errors at the terminal.
    ///
    /// # Examples
    /// A capturing closure is rejected at compile time:
    /// ```compile_fail
    /// use rayonet::fleet::NetMapExt;
    /// let captured = 10u32;
    /// let _ = std::iter::once(1u32).net_map(move |x: u32| x + captured);
    /// ```
    fn net_map<F, O>(self, map: F) -> NetMap<'static, F, Self::Item>
    where
        F: Fn(Self::Item) -> O,
    {
        const {
            assert!(
                size_of::<F>() == 0,
                "a `.net_map` task function must not capture; use a named function or a non-capturing closure"
            );
        }
        NetMap {
            runner: RunnerRef::Global,
            map,
            inputs: self.into_iter().collect(),
            requires: None,
            require_redundancy: false,
            speculative: false,
        }
    }

    /// Map `map` over these items, distributed across an explicit `fleet`.
    /// Otherwise identical to [`NetMapExt::net_map`].
    fn net_map_with_fleet<F, L, O>(self, map: F, fleet: &Fleet<L>) -> NetMap<'_, F, Self::Item>
    where
        F: Fn(Self::Item) -> O,
        L: Launch + Send + Sync,
    {
        const {
            assert!(
                size_of::<F>() == 0,
                "a `.net_map` task function must not capture; use a named function or a non-capturing closure"
            );
        }
        NetMap {
            runner: RunnerRef::Borrowed(fleet),
            map,
            inputs: self.into_iter().collect(),
            requires: None,
            require_redundancy: false,
            speculative: false,
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
    type Session = ();

    fn label(&self) -> String {
        self.program.to_string_lossy().into_owned()
    }

    // A local subprocess has no transport to establish and uses the default
    // (unknown) profile; it spawns directly in `activate`.
    async fn connect(&self) -> std::io::Result<()> {
        Ok(())
    }

    async fn activate(
        &self,
        _session: (),
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
        pub fn run<L: Launch + Send + Sync>(
            self,
            fleet: &Fleet<L>,
        ) -> std::io::Result<Vec<Result<O, String>>> {
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
    use super::{EventSink, Fleet, Launch, NetMapExt, NodeProfile};
    use crate::agent::{serve, Registry};
    use crate::framing::Connection;
    use crate::testing::connection_pair;
    use tokio::io::DuplexStream;
    use tokio::task::JoinHandle;

    /// A launcher that runs an agent in-process over a duplex pipe. A `None`
    /// registry simulates a host that fails to launch (for example a cold host
    /// that cannot be provisioned). An optional `profile` overrides the default
    /// (unknown) probe result so the role filter can be exercised.
    struct InProcess {
        registry: Option<Registry>,
        label: String,
        profile: Option<NodeProfile>,
        probe_fails: bool,
        /// A shared counter and a threshold: `connect` fails while the count is
        /// below the threshold, then succeeds, simulating a host unreachable at
        /// launch that comes online after a few attempts.
        connect_gate: Option<(std::sync::Arc<std::sync::atomic::AtomicUsize>, usize)>,
        /// When set, `activate` fails, simulating a host that connects and probes
        /// but cannot be provisioned (a cold host whose build never finishes).
        activate_fails: bool,
    }

    impl InProcess {
        fn serving(double: bool) -> Self {
            let registry = double.then(|| Registry::new().with_fn(self::double));
            Self {
                registry,
                label: "in-process".to_string(),
                profile: None,
                probe_fails: false,
                connect_gate: None,
                activate_fails: false,
            }
        }

        fn serving_registry(registry: Registry) -> Self {
            Self {
                registry: Some(registry),
                label: "in-process".to_string(),
                profile: None,
                probe_fails: false,
                connect_gate: None,
                activate_fails: false,
            }
        }

        /// Make `connect` fail until it has been called `fail_below` times (sharing
        /// `counter`), then succeed: a host that is unreachable at launch and the
        /// first few rejoin attempts, then comes online.
        fn flaky_connect(
            mut self,
            counter: std::sync::Arc<std::sync::atomic::AtomicUsize>,
            fail_below: usize,
        ) -> Self {
            self.connect_gate = Some((counter, fail_below));
            self
        }

        /// Make `activate` fail: a host that connects and probes but cannot be
        /// provisioned, so it is dropped before it can run anything.
        fn activate_failing(mut self) -> Self {
            self.activate_fails = true;
            self
        }

        /// Override the host label, so a multi-host fleet has distinct nodes.
        fn named(mut self, label: &str) -> Self {
            self.label = label.to_string();
            self
        }

        /// Override the probed profile, so the role filter has something to act on.
        fn with_profile(mut self, profile: NodeProfile) -> Self {
            self.profile = Some(profile);
            self
        }

        /// Make the probe fail, simulating a host that connects but cannot be
        /// inspected, so it is dropped before activation.
        fn probe_failing(mut self) -> Self {
            self.probe_fails = true;
            self
        }
    }

    impl Launch for InProcess {
        type Stream = DuplexStream;
        type Guard = JoinHandle<std::io::Result<()>>;
        // The session carries the registry to serve; a `None` registry fails the
        // connect, simulating a host that cannot be launched.
        type Session = Registry;

        fn label(&self) -> String {
            self.label.clone()
        }

        async fn connect(&self) -> std::io::Result<Registry> {
            if let Some((counter, fail_below)) = &self.connect_gate {
                if counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst) < *fail_below {
                    return Err(std::io::Error::other("simulated unreachable host"));
                }
            }
            self.registry
                .clone()
                .ok_or_else(|| std::io::Error::other("simulated launch failure"))
        }

        async fn probe(&self, _session: &Registry) -> std::io::Result<NodeProfile> {
            if self.probe_fails {
                return Err(std::io::Error::other("simulated probe failure"));
            }
            Ok(self.profile.clone().unwrap_or_else(NodeProfile::unknown))
        }

        async fn activate(
            &self,
            registry: Registry,
            _events: &dyn EventSink,
        ) -> std::io::Result<(Connection<DuplexStream>, Self::Guard)> {
            if self.activate_fails {
                return Err(std::io::Error::other("simulated activation failure"));
            }
            let (client, server) = connection_pair(256);
            let task = tokio::spawn(serve(server, registry));
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

    /// A launcher that records the order of its `probe` and `activate` calls into
    /// a shared timeline, so a test can prove discovery (probe and identify) of a
    /// whole layer precedes provisioning (activate) of any of it.
    struct Timeline {
        label: String,
        log: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
        registry: Registry,
    }

    impl Launch for Timeline {
        type Stream = DuplexStream;
        type Guard = JoinHandle<std::io::Result<()>>;
        type Session = ();

        fn label(&self) -> String {
            self.label.clone()
        }

        async fn connect(&self) -> std::io::Result<()> {
            Ok(())
        }

        async fn probe(&self, _session: &()) -> std::io::Result<NodeProfile> {
            self.log
                .lock()
                .unwrap()
                .push(format!("probe {}", self.label));
            Ok(NodeProfile::unknown())
        }

        async fn activate(
            &self,
            _session: (),
            _events: &dyn EventSink,
        ) -> std::io::Result<(Connection<DuplexStream>, Self::Guard)> {
            self.log
                .lock()
                .unwrap()
                .push(format!("activate {}", self.label));
            let (client, server) = connection_pair(256);
            let task = tokio::spawn(serve(server, self.registry.clone()));
            Ok((client, task))
        }
    }

    #[tokio::test]
    async fn a_layer_is_fully_discovered_before_any_of_it_is_provisioned() {
        // Identity must be known before activation, so a node reached by two
        // relays is not double-activated: every host in a layer is probed and
        // identified before any host is activated. With the old interleaved
        // launch (probe a, activate a, probe b, ...) host b's probe would land
        // after host a's activate. The split orders all probes before any
        // activate.
        let log = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let launchers = vec![
            Timeline {
                label: "a".to_string(),
                log: log.clone(),
                registry: Registry::new().with_fn(double),
            },
            Timeline {
                label: "b".to_string(),
                log: log.clone(),
                registry: Registry::new().with_fn(double),
            },
        ];
        let fleet = Fleet::new(launchers);

        let out: Vec<Result<u32, String>> = (0..6u32)
            .net_map_with_fleet(double, &fleet)
            .collect()
            .await
            .unwrap();
        assert_eq!(out, (0..6u32).map(|x| Ok(x * 2)).collect::<Vec<_>>());

        let log = log.lock().unwrap().clone();
        let last_probe = log
            .iter()
            .rposition(|e| e.starts_with("probe"))
            .expect("both hosts are probed");
        let first_activate = log
            .iter()
            .position(|e| e.starts_with("activate"))
            .expect("both hosts are activated");
        assert!(
            last_probe < first_activate,
            "every host must be discovered before any is provisioned: {log:?}"
        );
    }

    #[tokio::test]
    async fn net_reduce_folds_outputs_on_the_coordinator() {
        let launchers = (0..3).map(|_| InProcess::serving(true)).collect();
        let fleet = Fleet::new(launchers);

        let job = (0..10u32).net_map_with_fleet(double, &fleet);
        assert!(format!("{job:?}").contains("NetMap"));
        let sum = job.net_reduce(add).await.unwrap();

        assert_eq!(sum, Some((0..10u32).map(|x| x * 2).sum()));
    }

    #[tokio::test]
    async fn net_reduce_is_none_for_empty_input() {
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
    async fn net_reduce_short_circuits_on_the_first_task_failure() {
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

    // A named fold op for the net_fold tests: accumulates u32 outputs into a u64
    // (the accumulator type differs from the task output), shared so the empty
    // case leaves no uncovered closure.
    fn add_u64(acc: u64, x: u32) -> u64 {
        acc + u64::from(x)
    }

    #[tokio::test]
    async fn net_fold_accumulates_into_a_seed() {
        let launchers = (0..3).map(|_| InProcess::serving(true)).collect();
        let fleet = Fleet::new(launchers);

        let total = (0..10u32)
            .net_map_with_fleet(double, &fleet)
            .net_fold(100u64, add_u64)
            .await
            .unwrap();

        // seed 100 plus the sum of the doubled inputs, accumulated as u64.
        assert_eq!(
            total,
            100 + (0..10u32).map(|x| u64::from(x * 2)).sum::<u64>()
        );
    }

    #[tokio::test]
    async fn net_fold_returns_the_seed_for_empty_input() {
        let launchers = (0..2).map(|_| InProcess::serving(true)).collect();
        let fleet = Fleet::new(launchers);

        let total = Vec::<u32>::new()
            .net_map_with_fleet(double, &fleet)
            .net_fold(42u64, add_u64)
            .await
            .unwrap();

        assert_eq!(total, 42);
    }

    #[tokio::test]
    async fn net_fold_short_circuits_on_the_first_task_failure() {
        let launchers = vec![
            InProcess::serving_registry(Registry::new().with_fn(boom)),
            InProcess::serving_registry(Registry::new().with_fn(boom)),
        ];
        let fleet = Fleet::new(launchers);

        let err = (0..10u32)
            .net_map_with_fleet(boom, &fleet)
            .net_fold(0u64, add_u64)
            .await
            .unwrap_err();

        assert!(err.to_string().contains("too big"), "{err}");
    }

    // This is the only test that touches the process-global fleet, so its state
    // is deterministic: unset at the start, installed partway through.
    #[tokio::test]
    async fn net_map_uses_the_installed_global_fleet() {
        // With no global installed, a bare `net_map` errors at the terminal.
        let err = (0..4u32)
            .net_map(double)
            .collect::<u32>()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("no global fleet"), "{err}");

        // Install one, and the same bare call now runs against it.
        let launchers = (0..2).map(|_| InProcess::serving(true)).collect();
        super::install_fleet(Fleet::new(launchers));

        let out: Vec<Result<u32, String>> = (0..4u32).net_map(double).collect().await.unwrap();
        assert_eq!(out, (0..4u32).map(|x| Ok(x * 2)).collect::<Vec<_>>());

        // Terminals other than collect work over the global fleet too.
        let sum = (0..4u32).net_map(double).net_reduce(add).await.unwrap();
        assert_eq!(sum, Some((0..4u32).map(|x| x * 2).sum()));
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
    async fn with_filter_keeps_only_compute_hosts() {
        use crate::capability::{pred, Filter, NodeProfile, Os, Role};
        use crate::observability::{NodeState, RunState};
        use crate::testing::EventRecorder;
        use std::sync::Arc;

        let linux = NodeProfile {
            os: Os::Linux,
            arch: crate::capability::CpuArch::unknown(),
            cores: 8,
            ram_mb: 16_000,
            gpus: Vec::new(),
        };
        let mac = NodeProfile {
            os: Os::MacOs,
            arch: crate::capability::CpuArch::unknown(),
            cores: 8,
            ram_mb: 16_000,
            gpus: Vec::new(),
        };

        // A filter that keeps Linux as Compute and excludes everything else, so
        // the macOS host is dropped before it is ever activated.
        let filter = Filter::new().compute(pred::os_is(Os::Linux));
        let sink = Arc::new(EventRecorder::default());
        let launchers = vec![
            InProcess::serving(true)
                .named("linux")
                .with_profile(linux.clone()),
            InProcess::serving(true)
                .named("mac")
                .with_profile(mac.clone()),
        ];
        let fleet = Fleet::observed(launchers, sink.clone()).with_filter(filter);

        let out: Vec<Result<u32, String>> = (0..8u32)
            .net_map_with_fleet(double, &fleet)
            .collect()
            .await
            .unwrap();
        assert_eq!(out, (0..8u32).map(|x| Ok(x * 2)).collect::<Vec<_>>());

        let mut state = RunState::default();
        for event in &sink.events() {
            state.apply(event);
        }
        // The macOS host was profiled and excluded, never ran a task; the Linux
        // host ran every task to completion.
        assert_eq!(state.nodes["mac"].role, Some(Role::Excluded));
        assert_eq!(state.nodes["mac"].completed, 0);
        assert_eq!(state.nodes["linux"].role, Some(Role::Compute));
        assert_eq!(state.nodes["linux"].state, NodeState::Done);
        // The node id (the label, for an in-process launcher) flows into the
        // profile event and is recorded per node.
        assert_eq!(state.nodes["linux"].id.as_deref(), Some("linux"));
    }

    #[tokio::test]
    async fn requires_narrows_a_run_to_matching_hosts() {
        use crate::capability::{pred, Gpu, GpuRuntime, GpuVendor, NodeProfile, Os};
        use crate::observability::{NodeState, RunState};
        use crate::testing::EventRecorder;
        use std::sync::Arc;

        let rocm_box = NodeProfile {
            os: Os::Linux,
            arch: crate::capability::CpuArch::unknown(),
            cores: 32,
            ram_mb: 128_000,
            gpus: vec![Gpu {
                vendor: GpuVendor::Amd,
                runtime: Some(GpuRuntime::Rocm),
                model: "Instinct".to_string(),
                vram_mb: Some(65_536),
            }],
        };
        let cpu_box = NodeProfile {
            os: Os::Linux,
            arch: crate::capability::CpuArch::unknown(),
            cores: 8,
            ram_mb: 16_000,
            gpus: Vec::new(),
        };

        let sink = Arc::new(EventRecorder::default());
        let launchers = vec![
            InProcess::serving(true)
                .named("rocm")
                .with_profile(rocm_box),
            InProcess::serving(true).named("cpu").with_profile(cpu_box),
        ];
        let fleet = Fleet::observed(launchers, sink.clone());

        // The job needs ROCm; only the ROCm host runs it, the CPU host is dropped.
        let out: Vec<Result<u32, String>> = (0..6u32)
            .net_map_with_fleet(double, &fleet)
            .requires(pred::rocm())
            .collect()
            .await
            .unwrap();
        assert_eq!(out, (0..6u32).map(|x| Ok(x * 2)).collect::<Vec<_>>());

        let mut state = RunState::default();
        for event in &sink.events() {
            state.apply(event);
        }
        // The CPU host was profiled but, failing the requirement, ran nothing.
        assert_eq!(state.nodes["cpu"].completed, 0);
        assert_eq!(state.nodes["rocm"].state, NodeState::Done);
    }

    #[tokio::test]
    async fn requires_errors_when_no_host_satisfies_it() {
        use crate::capability::pred;

        // The only host has no GPU, so a ROCm requirement leaves no eligible host.
        let fleet = Fleet::new(vec![InProcess::serving(true)]);

        let err = (0..4u32)
            .net_map_with_fleet(double, &fleet)
            .requires(pred::rocm())
            .collect::<u32>()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("no eligible host"), "{err}");
    }

    #[tokio::test]
    async fn require_redundancy_admits_direct_hosts() {
        // Direct compute hosts sit behind no relay, so requiring redundancy does
        // not reject them and the run proceeds.
        let fleet = Fleet::new((0..2).map(|_| InProcess::serving(true)).collect());
        let out: Vec<Result<u32, String>> = (0..6u32)
            .net_map_with_fleet(double, &fleet)
            .require_redundancy()
            .collect()
            .await
            .unwrap();
        assert_eq!(out, (0..6u32).map(|x| Ok(x * 2)).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn netmap_drops_a_host_whose_probe_fails() {
        // One host's probe errors (it connects but cannot be inspected); it is
        // dropped and the survivor runs every task.
        let launchers = vec![
            InProcess::serving(true).named("healthy"),
            InProcess::serving(true)
                .named("unprobeable")
                .probe_failing(),
        ];
        let fleet = Fleet::new(launchers);

        let out: Vec<Result<u32, String>> = (0..6u32)
            .net_map_with_fleet(double, &fleet)
            .collect()
            .await
            .unwrap();

        assert_eq!(out, (0..6u32).map(|x| Ok(x * 2)).collect::<Vec<_>>());
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

    /// A handler slow enough that work is still pending when a late host joins.
    fn slow_double(x: u32) -> u32 {
        std::thread::sleep(std::time::Duration::from_millis(15));
        x * 2
    }

    #[tokio::test]
    async fn a_host_unreachable_at_start_joins_on_a_later_attempt() {
        use crate::observability::RunState;
        use crate::testing::EventRecorder;
        use std::sync::atomic::AtomicUsize;
        use std::sync::Arc;

        // A healthy host runs from the start; a second host's first two connects
        // fail (it is unreachable at launch), so the rejoin driver retries it and
        // it joins mid-run, taking pending tasks. Every result is still correct.
        let counter = Arc::new(AtomicUsize::new(0));
        let launchers = vec![
            InProcess::serving_registry(Registry::new().with_fn(slow_double)).named("healthy"),
            InProcess::serving_registry(Registry::new().with_fn(slow_double))
                .named("late")
                .flaky_connect(counter, 2),
        ];
        let sink = Arc::new(EventRecorder::default());
        let fleet = Fleet::observed(launchers, sink.clone());

        let out: Vec<Result<u32, String>> = (0..40u32)
            .net_map_with_fleet(slow_double, &fleet)
            .collect()
            .await
            .unwrap();
        assert_eq!(out, (0..40u32).map(|x| Ok(x * 2)).collect::<Vec<_>>());

        let mut state = RunState::default();
        for event in &sink.events() {
            state.apply(event);
        }
        assert!(
            state.nodes.contains_key("late"),
            "the late host should have joined the run"
        );
        assert!(
            state.nodes["late"].completed >= 1,
            "the late host should have run pending tasks once it joined"
        );
    }

    #[tokio::test]
    async fn the_rejoin_driver_gives_up_on_a_permanently_unreachable_candidate() {
        use tokio::sync::mpsc;

        // A candidate whose connect never succeeds is retried up to the attempt
        // cap, then given up: the driver returns no guards and closes the joins
        // channel without ever sending a joiner.
        let bad = InProcess::serving(false).named("never");
        let (joins_tx, mut joins_rx) =
            mpsc::unbounded_channel::<crate::coordinator::Joiner<DuplexStream>>();
        let guards = super::rejoin_driver(
            vec![&bad],
            "double",
            None,
            None,
            joins_tx,
            super::RejoinPolicy {
                backoff: std::time::Duration::from_millis(1),
                max_attempts: 3,
            },
            &super::NoopSink,
        )
        .await;
        assert!(
            guards.is_empty(),
            "a permanently dead candidate yields nothing"
        );
        assert!(
            joins_rx.recv().await.is_none(),
            "no joiner was sent and the channel is closed"
        );
    }

    #[tokio::test]
    async fn the_rejoin_driver_drops_unprobeable_excluded_and_unprovisionable_candidates() {
        use crate::capability::{pred, Filter, NodeProfile, Os};
        use tokio::sync::mpsc;

        let linux = NodeProfile {
            os: Os::Linux,
            ..NodeProfile::unknown()
        };
        let mac = NodeProfile {
            os: Os::MacOs,
            ..NodeProfile::unknown()
        };
        // Three candidates, each exercising one non-join outcome: one that cannot
        // be probed (retry then give up), one the filter excludes (give up at
        // once), and one that probes fine but cannot be provisioned (retry then
        // give up). None joins.
        let unprobeable = InProcess::serving(true)
            .named("unprobeable")
            .probe_failing();
        let excluded = InProcess::serving(true).named("excluded").with_profile(mac);
        let unprovisionable = InProcess::serving(true)
            .named("unprovisionable")
            .with_profile(linux)
            .activate_failing();
        let filter = Filter::new().compute(pred::os_is(Os::Linux));
        // A job requirement the surviving Linux candidate satisfies, so the
        // requirement check runs (the unprovisionable host passes it, then fails
        // to activate).
        let requires = pred::os_is(Os::Linux);
        let (joins_tx, mut joins_rx) =
            mpsc::unbounded_channel::<crate::coordinator::Joiner<DuplexStream>>();
        let guards = super::rejoin_driver(
            vec![&unprobeable, &excluded, &unprovisionable],
            "double",
            Some(&filter),
            Some(&requires),
            joins_tx,
            super::RejoinPolicy {
                backoff: std::time::Duration::from_millis(1),
                max_attempts: 2,
            },
            &super::NoopSink,
        )
        .await;
        assert!(guards.is_empty());
        assert!(joins_rx.recv().await.is_none(), "no candidate joined");
    }

    #[tokio::test]
    async fn a_speculative_run_returns_correct_results() {
        // Speculation is opt-in via .speculative(); with healthy hosts the run is
        // unchanged (it just permits racing a straggler), so results are correct.
        let fleet = Fleet::new((0..2).map(|_| InProcess::serving(true)).collect());
        let out: Vec<Result<u32, String>> = (0..12u32)
            .net_map_with_fleet(double, &fleet)
            .speculative()
            .collect()
            .await
            .unwrap();
        assert_eq!(out, (0..12u32).map(|x| Ok(x * 2)).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn a_host_that_cannot_be_provisioned_is_dropped_at_launch() {
        // A host connects and probes but fails to activate (a cold host whose
        // build never finishes): it is dropped at launch and the survivor runs
        // every task. A provisioning failure is not an unreachable host, so it is
        // not retried.
        let launchers = vec![
            InProcess::serving(true).named("healthy"),
            InProcess::serving(true)
                .named("unprovisionable")
                .activate_failing(),
        ];
        let fleet = Fleet::new(launchers);
        let out: Vec<Result<u32, String>> = (0..8u32)
            .net_map_with_fleet(double, &fleet)
            .collect()
            .await
            .unwrap();
        assert_eq!(out, (0..8u32).map(|x| Ok(x * 2)).collect::<Vec<_>>());
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
