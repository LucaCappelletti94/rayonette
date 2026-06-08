//! The coordinator side of a run.
//!
//! Hands a job's inputs to a set of agents, schedules them by pull (each agent
//! is kept filled to its advertised capacity from the pending pool, one slot for
//! a leaf and more for a relay fronting a subtree), and assembles the results in
//! input order (PLAN.md Phase 1).
//!
//! Each agent connection is split: the coordinator keeps the send half to issue
//! `Assign`/`Shutdown`, and a per-agent reader task drains the receive half into
//! one central event channel so all agents are serviced concurrently.

use std::collections::{HashMap, VecDeque};

use serde::{de::DeserializeOwned, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::framing::{Connection, Receiver, Sender};
use crate::observability::{join_label, Event as Obs, EventSink, NodeState};
use crate::protocol::{ChildAd, FromAgent, TaskId, ToAgent, PROTOCOL_VERSION};

/// What a reader task forwards to the coordinator's central loop. A read error
/// and a clean disconnect are treated alike: the agent is lost. Shared with the
/// relay (`crate::relay`), which reuses [`connect_agents`] and the same reader
/// channel to multiplex its children.
pub(crate) enum Event {
    Message(usize, FromAgent),
    Lost(usize),
}

pub(crate) fn spawn_reader<S>(
    mut rx: Receiver<S>,
    agent: usize,
    events: mpsc::UnboundedSender<Event>,
) -> JoinHandle<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        // Forward each message; on a clean end-of-stream or a read error, report
        // the agent lost and stop. A failed send means the coordinator is gone.
        loop {
            let Ok(Some(msg)) = rx.recv::<FromAgent>().await else {
                let _ = events.send(Event::Lost(agent));
                break;
            };
            let _ = events.send(Event::Message(agent, msg));
        }
    })
}

/// How a parent decides which of a relay's discovered children to activate now.
pub(crate) enum ActivationPolicy {
    /// The coordinator's global dedup: the first relay to claim a physical node id
    /// runs it, and a later relay reaching the same node holds it as a standby
    /// (the basis of redundant-path reroute).
    DedupById,
    /// A relay over its own children, with no global view, activates them all.
    ApproveAll,
}

/// An agent's first handshake reply: a leaf readies with its slot count, a relay
/// first describes the children it built so the parent can choose which to run.
enum FirstReport {
    Leaf(usize),
    Relay(Vec<ChildAd>),
}

/// The labels each agent should activate now, by `policy`, parallel to `reports`.
/// `ApproveAll` runs every child (a relay has no global view). `DedupById` chooses
/// the metric-best path for a node reachable through several relays and holds the
/// others standby (see [`crate::graph::choose_active`]).
fn active_labels(
    reports: &[FirstReport],
    labels: &[String],
    latencies: &[u64],
    policy: &ActivationPolicy,
) -> Vec<Vec<String>> {
    match policy {
        ActivationPolicy::ApproveAll => reports
            .iter()
            .map(|report| match report {
                FirstReport::Leaf(_) => Vec::new(),
                FirstReport::Relay(children) => children
                    .iter()
                    .map(|child| child.label().to_string())
                    .collect(),
            })
            .collect(),
        ActivationPolicy::DedupById => dedup_active(reports, labels, latencies),
    }
}

/// Per-agent active labels with redundant paths deduped by the shortest-latency
/// path through the sharing relays (leaves get none). Latency is the only link
/// metric measured so far, so the widest-bandwidth metric waits on a bandwidth
/// probe.
fn dedup_active(reports: &[FirstReport], labels: &[String], latencies: &[u64]) -> Vec<Vec<String>> {
    let (relay_agents, relays) = relay_reports(reports, labels, latencies);
    let chosen = crate::graph::choose_active(&relays, crate::graph::Metric::ShortestLatency);
    let mut actives = vec![Vec::new(); reports.len()];
    for (slot, agent) in relay_agents.into_iter().enumerate() {
        actives[agent].clone_from(&chosen[slot]);
    }
    actives
}

/// The relay agents among `reports` as [`graph::RelayReport`]s for path analysis,
/// paired with their agent indices (so results scatter back to the right agent).
fn relay_reports(
    reports: &[FirstReport],
    labels: &[String],
    latencies: &[u64],
) -> (Vec<usize>, Vec<crate::graph::RelayReport>) {
    let mut relay_agents = Vec::new();
    let mut relays = Vec::new();
    for (agent, report) in reports.iter().enumerate() {
        if let FirstReport::Relay(children) = report {
            relay_agents.push(agent);
            relays.push(crate::graph::RelayReport::new(
                labels[agent].clone(),
                latencies.get(agent).copied().unwrap_or(0),
                children.clone(),
            ));
        }
    }
    (relay_agents, relays)
}

/// An `InvalidData` error for an unexpected handshake reply.
fn unexpected(want: &str, got: Option<&FromAgent>) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("expected {want}, got {got:?}"),
    )
}

/// The per-agent state produced by the handshake.
pub(crate) struct Agents<S> {
    pub(crate) senders: Vec<Sender<S>>,
    pub(crate) labels: Vec<String>,
    /// Each agent's advertised concurrency: how many tasks it can hold at once
    /// (1 for a leaf, the active-slot sum for a relay).
    pub(crate) capacity: Vec<usize>,
    /// Per agent, the children it holds as built-but-idle standbys (a node the
    /// coordinator deduped onto another path), for reroute on a relay's death.
    pub(crate) standbys: Vec<Vec<ChildAd>>,
    pub(crate) readers: Vec<JoinHandle<()>>,
}

/// A single handshaked agent ready to be spliced into a live run (R6 elastic
/// membership). A node that comes online after the run started is handshaked on
/// its own (with [`handshake_join`]) and handed to the central loop, which
/// appends it to the [`Job`] and starts feeding it pending work.
pub(crate) struct Joiner<S> {
    /// The host's label, used to attribute its observability events.
    pub(crate) label: String,
    /// The send half, for issuing `Assign`/`Shutdown`.
    pub(crate) tx: Sender<S>,
    /// The receive half, drained by a reader once the agent is spliced in.
    pub(crate) rx: Receiver<S>,
    /// The agent's advertised concurrency (its `slots`).
    pub(crate) capacity: usize,
}

/// Handshake one agent that is joining a run already in progress, so it can be
/// spliced in. Mirrors the per-agent steps of [`connect_agents`] for a single
/// connection: greet it, learn its capacity, and (if it is a relay) activate all
/// of its own children, since a single late joiner has no layer to dedup against.
/// Called by the rejoin driver (`crate::fleet`) for each host that comes online
/// mid-run.
///
/// # Errors
/// Returns an error on a handshake or transport failure.
pub(crate) async fn handshake_join<S>(
    label: String,
    conn: Connection<S>,
    fn_key: &str,
) -> std::io::Result<Joiner<S>>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut tx, mut rx) = conn.split();
    tx.send(&ToAgent::Hello {
        protocol_version: PROTOCOL_VERSION,
        fn_key: fn_key.to_string(),
    })
    .await?;
    let capacity = match rx.recv::<FromAgent>().await? {
        Some(FromAgent::Ready { slots }) => slots,
        Some(FromAgent::Discovered { children }) => {
            let active = children
                .iter()
                .map(|child| child.label().to_string())
                .collect();
            tx.send(&ToAgent::Activate { active }).await?;
            match rx.recv::<FromAgent>().await? {
                Some(FromAgent::Ready { slots }) => slots,
                other => return Err(unexpected("Ready after Activate", other.as_ref())),
            }
        }
        other => return Err(unexpected("Ready or Discovered", other.as_ref())),
    };
    Ok(Joiner {
        label,
        tx,
        rx,
        capacity,
    })
}

/// Handshake with every agent and spawn its reader task. A leaf replies `Ready`;
/// a relay replies `Discovered`, is told which children to `Activate` by `policy`,
/// then replies `Ready` with its active capacity. Children a relay does not
/// activate are kept as standbys.
pub(crate) async fn connect_agents<S>(
    agents: Vec<(String, Connection<S>)>,
    fn_key: &str,
    events_tx: &mpsc::UnboundedSender<Event>,
    agent_latencies: &[u64],
    require_redundancy: bool,
    policy: &ActivationPolicy,
) -> std::io::Result<Agents<S>>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    // Phase 1: greet every agent and read its first reply.
    let mut parts = Vec::with_capacity(agents.len());
    let mut reports = Vec::with_capacity(agents.len());
    for (label, conn) in agents {
        let (mut tx, mut rx) = conn.split();
        tx.send(&ToAgent::Hello {
            protocol_version: PROTOCOL_VERSION,
            fn_key: fn_key.to_string(),
        })
        .await?;
        let report = match rx.recv::<FromAgent>().await? {
            Some(FromAgent::Ready { slots }) => FirstReport::Leaf(slots),
            Some(FromAgent::Discovered { children }) => FirstReport::Relay(children),
            other => return Err(unexpected("Ready or Discovered", other.as_ref())),
        };
        reports.push(report);
        parts.push((label, tx, rx));
    }

    // Phase 2: choose each relay's active children (deduping redundant paths by
    // the link metric), tell each relay, then read its readiness.
    let labels: Vec<String> = parts.iter().map(|(label, _, _)| label.clone()).collect();
    if require_redundancy {
        let (_, relays) = relay_reports(&reports, &labels, agent_latencies);
        let gaps = crate::graph::redundancy_gaps(&relays);
        if !gaps.is_empty() {
            return Err(std::io::Error::other(format!(
                "rayonet: require_redundancy: compute reachable through only one relay: {}",
                gaps.join(", ")
            )));
        }
    }
    let actives = active_labels(&reports, &labels, agent_latencies, policy);
    let mut out = Agents {
        senders: Vec::with_capacity(parts.len()),
        labels: Vec::with_capacity(parts.len()),
        capacity: Vec::with_capacity(parts.len()),
        standbys: Vec::with_capacity(parts.len()),
        readers: Vec::with_capacity(parts.len()),
    };
    for (agent_id, ((label, mut tx, mut rx), report)) in parts.into_iter().zip(reports).enumerate()
    {
        let active = &actives[agent_id];
        let (slots, standby) = match report {
            FirstReport::Leaf(slots) => (slots, Vec::new()),
            FirstReport::Relay(children) => {
                tx.send(&ToAgent::Activate {
                    active: active.clone(),
                })
                .await?;
                let slots = match rx.recv::<FromAgent>().await? {
                    Some(FromAgent::Ready { slots }) => slots,
                    other => return Err(unexpected("Ready after Activate", other.as_ref())),
                };
                let standby = children
                    .into_iter()
                    .filter(|child| !active.iter().any(|l| l == child.label()))
                    .collect();
                (slots, standby)
            }
        };
        out.capacity.push(slots);
        out.standbys.push(standby);
        out.senders.push(tx);
        out.labels.push(label);
        out.readers
            .push(spawn_reader(rx, agent_id, events_tx.clone()));
    }
    Ok(out)
}

/// Per-run policy flags, bundled so the run entry points stay within a sane
/// argument count.
#[derive(Clone, Copy, Default)]
pub(crate) struct RunOptions {
    /// Refuse to start unless every compute node has a redundant path.
    pub(crate) require_redundancy: bool,
    /// When no task is pending but some are still in flight, let an idle node
    /// re-run a straggler and race it (first result wins, deduped). Off by default.
    pub(crate) speculative: bool,
}

/// Mutable scheduling state for one run.
struct Job<S> {
    senders: Vec<Sender<S>>,
    /// Each agent's label, used to attribute its observability events. Grows in
    /// lockstep with the other per-agent vectors when a node joins mid-run.
    labels: Vec<String>,
    /// One reader task per agent, draining its receive half into the central
    /// event channel. A joining agent appends its reader here too.
    readers: Vec<JoinHandle<()>>,
    /// How many tasks each agent is currently running (kept below `capacity`).
    in_flight: Vec<usize>,
    /// How many tasks each agent may hold in flight at once (its advertised
    /// `slots`): 1 for a leaf, more for a relay fronting a subtree.
    capacity: Vec<usize>,
    /// Per agent, the children it holds as built-but-idle standbys (redundant
    /// paths the coordinator deduped away). On a relay's death these are promoted
    /// so a survivor takes over the orphaned subtree.
    standbys: Vec<Vec<ChildAd>>,
    /// Whether each agent's connection is still up; a lost one takes no tasks.
    alive: Vec<bool>,
    pending: VecDeque<usize>,
    /// `task_id` -> (input index, the agents running it). Usually one agent; with
    /// speculation a straggler is raced on a second, so the first terminal frees
    /// every agent that held it and the rest are deduped.
    assigned: HashMap<TaskId, (usize, Vec<usize>)>,
    results: Vec<Option<Result<Vec<u8>, String>>>,
    remaining: usize,
    payloads: Vec<Vec<u8>>,
    /// Whether an idle node may re-run a straggler (R6 speculative re-execution).
    speculative: bool,
}

impl<S> Job<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// Fill an agent: hand it pending tasks up to its capacity, then, if
    /// speculation is on and nothing is pending, let it race a straggler. A relay
    /// fronting many slots is kept fed while a leaf still holds one task; a lost
    /// agent takes nothing.
    async fn fill(&mut self, agent: usize) -> std::io::Result<()> {
        self.assign_up_to_capacity(agent).await?;
        if self.speculative {
            self.speculate(agent).await?;
        }
        Ok(())
    }

    /// Hand `agent` pending tasks up to its capacity. A lost agent takes nothing.
    async fn assign_up_to_capacity(&mut self, agent: usize) -> std::io::Result<()> {
        while self.alive[agent] && self.in_flight[agent] < self.capacity[agent] {
            let Some(idx) = self.pending.pop_front() else {
                return Ok(());
            };
            let task_id = idx as TaskId;
            self.senders[agent]
                .send(&ToAgent::Assign {
                    task_id,
                    payload: self.payloads[idx].clone(),
                })
                .await?;
            self.assigned.insert(task_id, (idx, vec![agent]));
            self.in_flight[agent] += 1;
        }
        Ok(())
    }

    /// With nothing pending, fill `agent`'s free slots by re-running stragglers:
    /// the oldest in-flight tasks running on exactly one other agent, so each is
    /// raced at most twice and the idle node helps drain the tail. The first
    /// terminal wins; the rest dedup.
    async fn speculate(&mut self, agent: usize) -> std::io::Result<()> {
        while self.alive[agent] && self.in_flight[agent] < self.capacity[agent] {
            let Some(task_id) = self
                .assigned
                .iter()
                .filter(|(_, (_, agents))| agents.len() == 1 && !agents.contains(&agent))
                .map(|(task, _)| *task)
                .min()
            else {
                return Ok(());
            };
            let (idx, agents) = self.assigned.get_mut(&task_id).expect("task is assigned");
            agents.push(agent);
            let payload = self.payloads[*idx].clone();
            self.in_flight[agent] += 1;
            self.senders[agent]
                .send(&ToAgent::Assign { task_id, payload })
                .await?;
        }
        Ok(())
    }

    /// Record a terminal outcome. Returns the agents freed (the one that finished
    /// plus any that were racing the same task), or an empty list if the `task_id`
    /// was already resolved (a duplicate, deduped here so it runs once per task).
    fn record(&mut self, task_id: TaskId, result: Result<Vec<u8>, String>) -> Vec<usize> {
        // `assigned` holds each task_id at most once (removed on first record), so a
        // duplicate finds nothing and returns empty, running exactly once per task.
        let Some((idx, agents)) = self.assigned.remove(&task_id) else {
            return Vec::new();
        };
        for &agent in &agents {
            self.in_flight[agent] -= 1;
        }
        self.results[idx] = Some(result);
        self.remaining -= 1;
        agents
    }

    /// Mark `agent` lost and return its in-flight tasks to the pending pool so
    /// survivors re-run them. A task it had actually
    /// finished is already out of `assigned`, so only genuinely lost work moves;
    /// re-running a task is safe by the idempotency contract and deduped on
    /// completion by `record`.
    fn requeue_lost(&mut self, agent: usize) {
        self.alive[agent] = false;
        let tasks: Vec<TaskId> = self.assigned.keys().copied().collect();
        for task in tasks {
            let (idx, agents) = self.assigned.get_mut(&task).expect("task is assigned");
            agents.retain(|other| *other != agent);
            // A task still running on another agent (a speculative replica) is not
            // lost; only one with no runner left is requeued for the survivors.
            if agents.is_empty() {
                let idx = *idx;
                self.assigned.remove(&task);
                self.pending.push_back(idx);
            }
        }
        self.in_flight[agent] = 0;
    }

    /// Whether any agent is still alive to take work.
    fn any_alive(&self) -> bool {
        self.alive.iter().any(|alive| *alive)
    }

    /// React to a relay's death by promoting every standby a survivor holds, so a
    /// node the dead relay was the primary path to is brought up on its alternate
    /// path. Each promoted relay replies `Capacity` with its larger total, which
    /// the loop folds in before feeding it the requeued work. A node with no
    /// surviving path (behind a dead articulation relay) has no standby to
    /// promote, so its work simply finds no taker and the run fails clearly.
    async fn reroute(&mut self) -> std::io::Result<()> {
        for agent in 0..self.senders.len() {
            if !self.alive[agent] {
                continue;
            }
            for child in std::mem::take(&mut self.standbys[agent]) {
                self.senders[agent]
                    .send(&ToAgent::Promote {
                        child: child.label().to_string(),
                    })
                    .await?;
            }
        }
        Ok(())
    }

    /// Fold one event from the central channel into the run: record a terminal,
    /// surface a task or subtree event, fold in a promoted relay's larger
    /// capacity, or react to a lost agent by rerouting onto standbys. The caller
    /// decides when to give up (no survivor and no node still able to join), so
    /// this only requeues and reroutes a lost agent's work.
    ///
    /// # Errors
    /// Returns an error on a protocol violation or a transport failure.
    async fn on_event(&mut self, event: Event, events: &dyn EventSink) -> std::io::Result<()> {
        match event {
            Event::Message(_, FromAgent::Ready { .. } | FromAgent::Discovered { .. }) => {
                // Ready and Discovered are handshake messages, so seeing one on
                // the live channel is a protocol error.
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "agent sent a handshake message more than once",
                ));
            }
            // A promoted relay reports its larger capacity after reroute: fold it
            // in and feed it the requeued work, marking it working if it took any.
            Event::Message(agent, FromAgent::Capacity { slots }) => {
                self.capacity[agent] = slots;
                self.fill(agent).await?;
                if self.in_flight[agent] > 0 {
                    events.emit(Obs::node(&self.labels[agent], NodeState::Working));
                }
            }
            Event::Message(agent, FromAgent::Started { task_id }) => {
                // Attribute the start to the agent that reported it (a speculative
                // replica reports its own start), only if the task is still live.
                if self.assigned.contains_key(&task_id) {
                    events.emit(Obs::TaskStarted {
                        host: self.labels[agent].clone(),
                        task: task_id,
                    });
                }
            }
            Event::Message(
                agent,
                FromAgent::Completed {
                    task_id,
                    output,
                    via,
                },
            ) => {
                // The raw bytes are kept as-is; decoding happens in the typed
                // layer, so a `Completed` is `ok` at the protocol level here.
                self.finish(task_id, Ok(output), true, agent, via, events)
                    .await?;
            }
            Event::Message(
                agent,
                FromAgent::Failed {
                    task_id,
                    error,
                    via,
                },
            ) => {
                self.finish(task_id, Err(error), false, agent, via, events)
                    .await?;
            }
            // A subtree event from a relay child: prefix its host with that
            // child's label so it carries a full path from the root, then
            // re-emit it so the whole tree surfaces in the run's event stream.
            Event::Message(agent, FromAgent::Observe(mut event)) => {
                event.prefix_host(&self.labels[agent]);
                events.emit(event);
            }
            // A dropped or erroring agent is abandoned: mark it lost, requeue its
            // in-flight tasks, promote standbys so a survivor takes over the
            // orphaned subtree, and feed the survivors.
            Event::Lost(agent) => {
                events.emit(Obs::node(&self.labels[agent], NodeState::Lost));
                self.requeue_lost(agent);
                self.reroute().await?;
                for survivor in 0..self.senders.len() {
                    self.fill(survivor).await?;
                }
            }
        }
        Ok(())
    }

    /// Record a terminal outcome, emit its task event, and refill every agent it
    /// freed (the finisher plus any that were racing the same task). Emits an
    /// `Idle` node event for a freed agent left with no work.
    async fn finish(
        &mut self,
        task_id: TaskId,
        result: Result<Vec<u8>, String>,
        ok: bool,
        completer: usize,
        via: String,
        events: &dyn EventSink,
    ) -> std::io::Result<()> {
        let freed = self.record(task_id, result);
        if freed.is_empty() {
            return Ok(());
        }
        // Credit the completion to the deep leaf that ran it, not the relay we
        // heard it from: `via` is that path within the completer's subtree.
        events.emit(Obs::TaskFinished {
            host: join_label(&self.labels[completer], &via),
            task: task_id,
            ok,
        });
        for agent in freed {
            self.fill(agent).await?;
            if self.in_flight[agent] == 0 {
                events.emit(Obs::node(&self.labels[agent], NodeState::Idle));
            }
        }
        Ok(())
    }
}

/// Serialize each task input to its wire payload.
///
/// # Errors
/// Returns an error if an input cannot be serialized.
pub(crate) fn serialize_inputs<I: Serialize>(inputs: &[I]) -> std::io::Result<Vec<Vec<u8>>> {
    inputs
        .iter()
        .map(|i| {
            postcard::to_allocvec(i)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
        })
        .collect()
}

/// Decode one raw task result into `O`, turning a decode failure into the task's
/// error string (the agent and coordinator share a compile, so this is only a
/// guard against a corrupt frame).
pub(crate) fn decode_output<O: DeserializeOwned>(
    raw: Result<Vec<u8>, String>,
) -> Result<O, String> {
    raw.and_then(|bytes| {
        postcard::from_bytes::<O>(&bytes).map_err(|e| format!("decode output: {e}"))
    })
}

/// Run one job to completion over already-serialized payloads, returning each
/// task's raw output bytes or its failure message, in input order.
///
/// The byte-level core: agnostic to the input and output types, so a type-erased
/// fleet (see [`crate::fleet`]) can drive it. [`run_job`] is the typed wrapper.
/// This is the static-membership form: it runs with the agents it starts with
/// and never absorbs a new one. [`run_job_raw_with_joins`] is the elastic core.
///
/// # Errors
/// Returns an error on a handshake or transport failure, or if every agent is
/// lost before the job completes.
pub(crate) async fn run_job_raw<S>(
    agents: Vec<(String, Connection<S>)>,
    fn_key: &str,
    payloads: Vec<Vec<u8>>,
    agent_latencies: &[u64],
    options: RunOptions,
    events: &dyn EventSink,
) -> std::io::Result<Vec<Result<Vec<u8>, String>>>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    // No node can join: an immediately-closed channel makes the elastic core
    // behave exactly like the static run.
    let (joins_tx, joins_rx) = mpsc::unbounded_channel::<Joiner<S>>();
    drop(joins_tx);
    run_job_raw_with_joins(
        agents,
        fn_key,
        payloads,
        agent_latencies,
        options,
        joins_rx,
        events,
    )
    .await
}

/// Run one job to completion, absorbing nodes that join mid-run (R6 elastic
/// membership). Identical to [`run_job_raw`] but for the `joins` channel: each
/// [`Joiner`] sent on it (by the rejoin driver, or a test) is spliced into the
/// live schedule and starts pulling pending work. The run ends successfully when
/// every task is done, and fails only when no agent is alive AND the joins
/// channel has closed (no node can still join).
///
/// # Errors
/// Returns an error on a handshake or transport failure, or if every agent is
/// lost with no node left able to join before the job completes.
pub(crate) async fn run_job_raw_with_joins<S>(
    agents: Vec<(String, Connection<S>)>,
    fn_key: &str,
    payloads: Vec<Vec<u8>>,
    agent_latencies: &[u64],
    options: RunOptions,
    mut joins: mpsc::UnboundedReceiver<Joiner<S>>,
    events: &dyn EventSink,
) -> std::io::Result<Vec<Result<Vec<u8>, String>>>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let num_tasks = payloads.len();

    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let Agents {
        senders,
        labels,
        capacity,
        standbys,
        readers,
    } = connect_agents(
        agents,
        fn_key,
        &events_tx,
        agent_latencies,
        options.require_redundancy,
        &ActivationPolicy::DedupById,
    )
    .await?;

    let agent_count = senders.len();
    let mut job: Job<S> = Job {
        senders,
        labels,
        readers,
        in_flight: vec![0; agent_count],
        capacity,
        standbys,
        alive: vec![true; agent_count],
        pending: (0..num_tasks).collect(),
        assigned: HashMap::new(),
        results: (0..num_tasks).map(|_| None).collect(),
        remaining: num_tasks,
        payloads,
        speculative: options.speculative,
    };

    events.emit(Obs::RunStarted { tasks: num_tasks });
    for agent in 0..job.senders.len() {
        job.fill(agent).await?;
        if job.in_flight[agent] > 0 {
            events.emit(Obs::node(&job.labels[agent], NodeState::Working));
        }
    }

    // The loop holds `events_tx` so it can spawn a reader for any node that joins;
    // a closed `joins` channel means no more nodes can join, which is how a run
    // with no rejoin driver (and the static [`run_job_raw`]) behaves.
    let mut joins_open = true;
    loop {
        if job.remaining == 0 {
            break;
        }
        if !job.any_alive() && !joins_open {
            return Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "every agent was lost and no node could join before the job completed",
            ));
        }
        tokio::select! {
            maybe_event = events_rx.recv() => {
                let event = maybe_event
                    .expect("the loop holds an events sender, so the channel stays open");
                job.on_event(event, events).await?;
            }
            maybe_join = joins.recv(), if joins_open => match maybe_join {
                // Splice a node that joined mid-run into the live schedule: append
                // it to every per-agent vector (agents are only appended, never
                // removed, so existing indices stay valid), spawn its reader, then
                // feed it pending work and mark it working if it took any.
                Some(joiner) => {
                    let agent = job.senders.len();
                    job.senders.push(joiner.tx);
                    job.labels.push(joiner.label);
                    job.capacity.push(joiner.capacity);
                    // A late joiner activates all its own children, so none standby.
                    job.standbys.push(Vec::new());
                    job.alive.push(true);
                    job.in_flight.push(0);
                    job.readers
                        .push(spawn_reader(joiner.rx, agent, events_tx.clone()));
                    job.fill(agent).await?;
                    if job.in_flight[agent] > 0 {
                        events.emit(Obs::node(&job.labels[agent], NodeState::Working));
                    }
                }
                None => joins_open = false,
            },
        }
    }

    // Release the loop's events sender so the trailing drain ends once the readers
    // do, then shut the live agents down and complete the tree view.
    drop(events_tx);
    for agent in 0..job.senders.len() {
        if job.alive[agent] {
            events.emit(Obs::node(&job.labels[agent], NodeState::Done));
        }
    }
    for agent in 0..job.senders.len() {
        if job.alive[agent] {
            let _ = job.senders[agent].send(&ToAgent::Shutdown).await;
        }
    }
    // Drain trailing subtree observability before the readers end: a relay
    // flushes its children's final states (their `Done`) as it shuts down, so
    // re-emitting these completes the tree view. Other late messages are ignored.
    while let Some(event) = events_rx.recv().await {
        if let Event::Message(agent, FromAgent::Observe(mut observed)) = event {
            observed.prefix_host(&job.labels[agent]);
            events.emit(observed);
        }
    }
    for reader in std::mem::take(&mut job.readers) {
        let _ = reader.await;
    }

    // Every slot is `Some` because the loop only exits at `remaining == 0`.
    Ok(job.results.into_iter().flatten().collect())
}

/// Typed wrapper over [`run_job_raw`]: serialize `inputs`, run, then decode each
/// successful output into `O` (a decode failure becomes that task's error).
///
/// # Errors
/// Returns an error on a handshake or transport failure, or if every agent is
/// lost before the job completes.
pub async fn run_job<S, I, O>(
    agents: Vec<(String, Connection<S>)>,
    fn_key: &str,
    inputs: Vec<I>,
    events: &dyn EventSink,
) -> std::io::Result<Vec<Result<O, String>>>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    I: Serialize,
    O: DeserializeOwned,
{
    let payloads = serialize_inputs(&inputs)?;
    // The typed entry carries no measured latencies and uses default options (no
    // redundancy requirement, no speculation). The fleet's real path supplies both.
    let raw = run_job_raw(agents, fn_key, payloads, &[], RunOptions::default(), events).await?;
    Ok(raw.into_iter().map(decode_output::<O>).collect())
}

#[cfg(test)]
mod tests {
    use super::run_job;
    use crate::agent::{fn_key, handler, serve, Registry};
    use crate::framing::Connection;
    use crate::observability::NoopSink;
    use crate::protocol::{FromAgent, ToAgent};
    use crate::testing::{connection_pair, FaultInjector};
    use proptest::prelude::*;
    use serde::{de::DeserializeOwned, Serialize};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::io::{AsyncRead, AsyncWrite};

    /// Run a job over unlabeled connections, discarding events: most tests care
    /// only about results, not the observability stream.
    async fn run<S, I, O>(
        conns: Vec<Connection<S>>,
        key: &str,
        inputs: Vec<I>,
    ) -> std::io::Result<Vec<Result<O, String>>>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
        I: Serialize,
        O: DeserializeOwned,
    {
        let labeled = conns
            .into_iter()
            .enumerate()
            .map(|(i, c)| (format!("a{i}"), c))
            .collect();
        run_job(labeled, key, inputs, &NoopSink).await
    }

    #[tokio::test]
    async fn ordered_results_from_one_agent() {
        let (client, server) = connection_pair(256);
        let agent = tokio::spawn(serve(
            server,
            Registry::new().with("sq", handler(|x: u32| x * x)),
        ));

        let inputs: Vec<u32> = (0u32..10).collect();
        let out: Vec<Result<u32, String>> = run(vec![client], "sq", inputs.clone()).await.unwrap();

        let expected: Vec<Result<u32, String>> = inputs.iter().map(|x| Ok(x * x)).collect();
        assert_eq!(out, expected);
        agent.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn failures_land_in_place_among_successes() {
        let (client, server) = connection_pair(256);
        let agent = tokio::spawn(serve(
            server,
            Registry::new().with(
                "evens",
                handler(|x: u32| -> u32 {
                    assert!(x.is_multiple_of(2), "odd input");
                    x
                }),
            ),
        ));

        let inputs: Vec<u32> = (0u32..6).collect();
        let out: Vec<Result<u32, String>> = run(vec![client], "evens", inputs).await.unwrap();
        agent.await.unwrap().unwrap();

        for (i, r) in out.iter().enumerate() {
            let i = u32::try_from(i).unwrap();
            if i.is_multiple_of(2) {
                assert_eq!(r.as_ref().unwrap(), &i);
            } else {
                assert!(r.as_ref().unwrap_err().contains("odd"));
            }
        }
    }

    #[tokio::test]
    async fn each_agent_runs_one_task_at_a_time() {
        // An agent pulls its next task only after reporting the current one, so
        // a single agent never has two tasks in flight at once.
        let current = Arc::new(AtomicUsize::new(0));
        let high_water = Arc::new(AtomicUsize::new(0));
        let (cur, hw) = (current.clone(), high_water.clone());
        let task = handler(move |x: u32| {
            let now = cur.fetch_add(1, Ordering::SeqCst) + 1;
            hw.fetch_max(now, Ordering::SeqCst);
            std::thread::sleep(std::time::Duration::from_millis(5));
            cur.fetch_sub(1, Ordering::SeqCst);
            x
        });

        let (client, server) = connection_pair(256);
        let agent = tokio::spawn(serve(server, Registry::new().with("id", task)));

        let inputs: Vec<u32> = (0u32..8).collect();
        let out: Vec<Result<u32, String>> = run(vec![client], "id", inputs).await.unwrap();
        agent.await.unwrap().unwrap();

        assert_eq!(out, (0u32..8).map(Ok).collect::<Vec<_>>());
        assert_eq!(
            high_water.load(Ordering::SeqCst),
            1,
            "a single agent must never run two tasks at once"
        );
    }

    #[tokio::test]
    async fn faster_agent_takes_more_work() {
        let fast_n = Arc::new(AtomicUsize::new(0));
        let slow_n = Arc::new(AtomicUsize::new(0));
        let (fc, sc) = (fast_n.clone(), slow_n.clone());
        let fast = handler(move |x: u32| {
            fc.fetch_add(1, Ordering::SeqCst);
            x
        });
        let slow = handler(move |x: u32| {
            sc.fetch_add(1, Ordering::SeqCst);
            std::thread::sleep(std::time::Duration::from_millis(10));
            x
        });

        let (client_a, server_a) = connection_pair(256);
        let (client_b, server_b) = connection_pair(256);
        let agent_a = tokio::spawn(serve(server_a, Registry::new().with("id", fast)));
        let agent_b = tokio::spawn(serve(server_b, Registry::new().with("id", slow)));

        let inputs: Vec<u32> = (0u32..50).collect();
        let out: Vec<Result<u32, String>> =
            run(vec![client_a, client_b], "id", inputs).await.unwrap();
        agent_a.await.unwrap().unwrap();
        agent_b.await.unwrap().unwrap();

        assert_eq!(out.len(), 50);
        let (f, s) = (fast_n.load(Ordering::SeqCst), slow_n.load(Ordering::SeqCst));
        assert_eq!(f + s, 50);
        assert!(f > s, "fast agent ({f}) should out-produce slow ({s})");
    }

    #[tokio::test]
    async fn the_event_stream_attributes_work_per_host() {
        use crate::observability::{NodeState, RunState};
        use crate::testing::EventRecorder;

        let fast = handler(|x: u32| x);
        let slow = handler(|x: u32| {
            std::thread::sleep(std::time::Duration::from_millis(5));
            x
        });
        let (client_a, server_a) = connection_pair(256);
        let (client_b, server_b) = connection_pair(256);
        let agent_a = tokio::spawn(serve(server_a, Registry::new().with("id", fast)));
        let agent_b = tokio::spawn(serve(server_b, Registry::new().with("id", slow)));

        let collector = EventRecorder::default();
        let agents = vec![
            ("fast".to_string(), client_a),
            ("slow".to_string(), client_b),
        ];
        let out: Vec<Result<u32, String>> = run_job(agents, "id", (0..40u32).collect(), &collector)
            .await
            .unwrap();
        agent_a.await.unwrap().unwrap();
        agent_b.await.unwrap().unwrap();
        assert_eq!(out.len(), 40);

        let mut state = RunState::default();
        for event in &collector.events() {
            state.apply(event);
        }

        assert_eq!(state.total_tasks(), 40);
        assert_eq!(state.completed(), 40);
        assert_eq!(
            state.nodes()["fast"].completed() + state.nodes()["slow"].completed(),
            40
        );
        let (fast, slow) = (
            state.nodes()["fast"].completed(),
            state.nodes()["slow"].completed(),
        );
        assert!(fast > slow, "fast {fast} vs slow {slow}");
        assert_eq!(state.nodes()["fast"].state(), NodeState::Done);
        assert_eq!(state.nodes()["slow"].state(), NodeState::Done);

        // The node-state projection drops the task events from the mixed stream.
        let states = collector.states();
        assert!(states.contains(&NodeState::Done));
        assert!(states
            .iter()
            .all(|s| { matches!(s, NodeState::Working | NodeState::Idle | NodeState::Done) }));
    }

    #[tokio::test]
    async fn a_dropped_agents_tasks_are_redistributed_and_run_once() {
        use tokio::io::duplex;

        // Agent A's coordinator-read is severed mid-run; agent B stays healthy.
        let (raw_a, server_a) = duplex(256);
        let (raw_b, server_b) = duplex(256);
        let client_a = Connection::new(FaultInjector::cut_reads_after(raw_a, 50));
        let client_b = Connection::new(FaultInjector::cut_reads_after(raw_b, usize::MAX));

        let agent_a = tokio::spawn(serve(
            Connection::new(server_a),
            Registry::new().with("id", handler(|x: u32| x)),
        ));
        let agent_b = tokio::spawn(serve(
            Connection::new(server_b),
            Registry::new().with("id", handler(|x: u32| x)),
        ));

        let agents = vec![("a".to_string(), client_a), ("b".to_string(), client_b)];
        let out: Vec<Result<u32, String>> = run_job(agents, "id", (0..20u32).collect(), &NoopSink)
            .await
            .unwrap();

        // Every task completed exactly once, in input order, despite the drop.
        assert_eq!(out, (0..20u32).map(Ok).collect::<Vec<_>>());

        let _ = agent_a.await; // A's serve may error once its peer read is cut.
        let _ = agent_b.await;
    }

    #[tokio::test]
    async fn a_failed_result_survives_its_agents_death() {
        // Agent A fails task 0, then drops. The failure is terminal: it is not
        // requeued to the survivor, and the error stands in the result.
        let (client_a, server_a) = connection_pair(64);
        let (client_b, server_b) = connection_pair(256);
        let fake_a = tokio::spawn(async move {
            let (mut tx, mut rx) = server_a.split();
            let _hello: ToAgent = rx.recv().await.unwrap().unwrap();
            tx.send(&FromAgent::Ready { slots: 1 }).await.unwrap();
            let _assign: ToAgent = rx.recv().await.unwrap().unwrap();
            tx.send(&FromAgent::Failed {
                task_id: 0,
                error: "boom".to_string(),
                via: String::new(),
            })
            .await
            .unwrap();
            // Drop without a shutdown: the coordinator sees A's stream end.
        });
        let agent_b = tokio::spawn(serve(
            server_b,
            Registry::new().with("id", handler(|x: u32| x)),
        ));

        let agents = vec![("a".to_string(), client_a), ("b".to_string(), client_b)];
        let out: Vec<Result<u32, String>> = run_job(agents, "id", vec![0u32, 1], &NoopSink)
            .await
            .unwrap();

        assert!(out[0].as_ref().unwrap_err().contains("boom"));
        assert_eq!(out[1], Ok(1));
        fake_a.await.unwrap();
        let _ = agent_b.await;
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(24))]

        /// However early one of two agents is severed, every task still
        /// completes exactly once, in input order.
        #[test]
        fn a_severed_agent_never_loses_or_duplicates_a_task(cut in 10usize..400) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let (raw_a, server_a) = tokio::io::duplex(256);
                let (raw_b, server_b) = tokio::io::duplex(256);
                let client_a = Connection::new(FaultInjector::cut_reads_after(raw_a, cut));
                let client_b = Connection::new(FaultInjector::cut_reads_after(raw_b, usize::MAX));
                let agent_a = tokio::spawn(serve(
                    Connection::new(server_a),
                    Registry::new().with("id", handler(|x: u32| x)),
                ));
                let agent_b = tokio::spawn(serve(
                    Connection::new(server_b),
                    Registry::new().with("id", handler(|x: u32| x)),
                ));
                let agents = vec![("a".to_string(), client_a), ("b".to_string(), client_b)];
                let out: Vec<Result<u32, String>> =
                    run_job(agents, "id", (0..30u32).collect(), &NoopSink).await.unwrap();
                prop_assert_eq!(out, (0..30u32).map(Ok).collect::<Vec<_>>());
                let _ = agent_a.await;
                let _ = agent_b.await;
                Ok(())
            })?;
        }
    }

    #[tokio::test]
    async fn a_subtree_observe_is_reemitted_with_a_prefixed_host() {
        use crate::observability::{Event as Obs, NodeState, RunState};
        use crate::protocol::FromAgent;
        use crate::testing::EventRecorder;

        // A relay-like agent reports a subtree node via Observe; the coordinator
        // re-emits it with the host prefixed by the agent's label, so the deep
        // node appears in the run state at its full path.
        let (client, server) = connection_pair(256);
        let fake = tokio::spawn(async move {
            let (mut tx, mut rx) = server.split();
            let _hello: ToAgent = rx.recv().await.unwrap().unwrap();
            tx.send(&FromAgent::Ready { slots: 1 }).await.unwrap();
            tx.send(&FromAgent::Observe(Obs::node("leaf", NodeState::Working)))
                .await
                .unwrap();
            tx.send(&FromAgent::Observe(Obs::node("leaf", NodeState::Done)))
                .await
                .unwrap();
            let _assign: ToAgent = rx.recv().await.unwrap().unwrap();
            tx.send(&FromAgent::Completed {
                task_id: 0,
                output: postcard::to_allocvec(&0u32).unwrap(),
                via: String::new(),
            })
            .await
            .unwrap();
            let _shutdown: ToAgent = rx.recv().await.unwrap().unwrap();
        });

        let recorder = EventRecorder::default();
        let out: Vec<Result<u32, String>> = run_job(
            vec![("relay".to_string(), client)],
            "k",
            vec![7u32],
            &recorder,
        )
        .await
        .unwrap();
        fake.await.unwrap();
        assert_eq!(out.len(), 1);

        let mut state = RunState::default();
        for event in &recorder.events() {
            state.apply(event);
        }
        // The subtree node surfaces at its full path with its last reported state.
        assert_eq!(state.nodes()["relay/leaf"].state(), NodeState::Done);
    }

    #[tokio::test]
    async fn an_agent_is_filled_to_its_advertised_capacity() {
        // An agent advertising three slots is handed three tasks up front,
        // before it completes any: proof the coordinator keeps a multi-slot
        // agent (a relay fronting a subtree) fed rather than one-at-a-time.
        let (client, server) = connection_pair(256);
        let fake = tokio::spawn(async move {
            let (mut tx, mut rx) = server.split();
            let _hello: ToAgent = rx.recv().await.unwrap().unwrap();
            tx.send(&FromAgent::Ready { slots: 3 }).await.unwrap();

            // Read three assignments before reporting any completion. With a
            // one-slot agent this would deadlock (the coordinator would wait for
            // a completion before sending the second), so reaching three proves
            // the capacity is honored.
            let mut ids = Vec::new();
            for _ in 0..3 {
                match rx.recv::<ToAgent>().await.unwrap().unwrap() {
                    ToAgent::Assign { task_id, .. } => ids.push(task_id),
                    other => panic!("expected Assign, got {other:?}"),
                }
            }
            for id in ids {
                tx.send(&FromAgent::Completed {
                    task_id: id,
                    output: postcard::to_allocvec(&0u32).unwrap(),
                    via: String::new(),
                })
                .await
                .unwrap();
            }
            // The remaining two tasks arrive as slots free; complete each.
            loop {
                match rx.recv::<ToAgent>().await.unwrap() {
                    Some(ToAgent::Assign { task_id, .. }) => tx
                        .send(&FromAgent::Completed {
                            task_id,
                            output: postcard::to_allocvec(&0u32).unwrap(),
                            via: String::new(),
                        })
                        .await
                        .unwrap(),
                    Some(ToAgent::Shutdown) | None => break,
                    other => panic!("unexpected {other:?}"),
                }
            }
        });

        let out: Vec<Result<u32, String>> =
            run(vec![client], "k", (0..5u32).collect()).await.unwrap();
        assert_eq!(out, vec![Ok(0); 5]);
        fake.await.unwrap();
    }

    #[tokio::test]
    async fn errors_when_agent_readies_twice() {
        let (client, server) = connection_pair(64);
        let fake = tokio::spawn(async move {
            let (mut tx, mut rx) = server.split();
            let _hello: ToAgent = rx.recv().await.unwrap().unwrap();
            tx.send(&FromAgent::Ready { slots: 1 }).await.unwrap();
            tx.send(&FromAgent::Ready { slots: 1 }).await.unwrap();
            let _ = rx.recv::<ToAgent>().await;
        });
        let res = run::<_, u32, u32>(vec![client], "k", vec![1u32, 2, 3]).await;
        assert!(res.is_err());
        fake.await.unwrap();
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]

        /// Every task completes exactly once, in input order, none lost.
        #[test]
        fn every_task_completes_once_in_order(n in 0usize..40) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let (client, server) = connection_pair(512);
                let agent = tokio::spawn(serve(
                    server,
                    Registry::new().with("id", handler(|x: u64| x)),
                ));
                let inputs: Vec<u64> = (0..n as u64).collect();
                let out: Vec<Result<u64, String>> =
                    run(vec![client], "id", inputs.clone()).await.unwrap();
                agent.await.unwrap().unwrap();
                let got: Vec<u64> = out.into_iter().map(Result::unwrap).collect();
                prop_assert_eq!(got, inputs);
                Ok(())
            })?;
        }
    }

    #[tokio::test]
    async fn errors_when_agent_skips_ready() {
        let (client, server) = connection_pair(64);
        let fake = tokio::spawn(async move {
            let (mut tx, mut rx) = server.split();
            let _hello: ToAgent = rx.recv().await.unwrap().unwrap();
            tx.send(&FromAgent::Started { task_id: 0 }).await.unwrap();
        });
        let res = run::<_, u32, u32>(vec![client], "k", vec![1u32]).await;
        assert!(res.is_err());
        fake.await.unwrap();
    }

    #[tokio::test]
    async fn errors_when_agent_disconnects_mid_run() {
        let (client, server) = connection_pair(64);
        let fake = tokio::spawn(async move {
            let (mut tx, mut rx) = server.split();
            let _hello: ToAgent = rx.recv().await.unwrap().unwrap();
            tx.send(&FromAgent::Ready { slots: 1 }).await.unwrap();
            let _assign: ToAgent = rx.recv().await.unwrap().unwrap();
            // Drop without completing: the coordinator sees end-of-stream.
        });
        let res = run::<_, u32, u32>(vec![client], "k", vec![1u32, 2, 3]).await;
        assert!(res.is_err());
        fake.await.unwrap();
    }

    #[tokio::test]
    async fn errors_with_no_agents_but_pending_work() {
        let res = run::<tokio::io::DuplexStream, u32, u32>(vec![], "k", vec![1u32, 2, 3]).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn a_decode_failure_becomes_a_task_error() {
        let (client, server) = connection_pair(64);
        let fake = tokio::spawn(async move {
            let (mut tx, mut rx) = server.split();
            let _hello: ToAgent = rx.recv().await.unwrap().unwrap();
            tx.send(&FromAgent::Ready { slots: 1 }).await.unwrap();
            let _assign: ToAgent = rx.recv().await.unwrap().unwrap();
            // Output bytes that are not a valid u32 encoding.
            tx.send(&FromAgent::Completed {
                task_id: 0,
                output: vec![0xFF; 6],
                via: String::new(),
            })
            .await
            .unwrap();
            let _shutdown: ToAgent = rx.recv().await.unwrap().unwrap();
        });
        let out: Vec<Result<u32, String>> = run(vec![client], "k", vec![7u32]).await.unwrap();
        fake.await.unwrap();
        assert_eq!(out.len(), 1);
        assert!(out[0].as_ref().unwrap_err().contains("decode output"));
    }

    #[tokio::test]
    async fn a_garbage_frame_surfaces_as_an_error() {
        let (client, server) = connection_pair(64);
        let fake = tokio::spawn(async move {
            let (mut tx, mut rx) = server.split();
            let _hello: ToAgent = rx.recv().await.unwrap().unwrap();
            tx.send(&FromAgent::Ready { slots: 1 }).await.unwrap();
            tx.send(&255u8).await.unwrap(); // not a valid FromAgent frame
                                            // Stay connected until the coordinator reacts (it sends an Assign,
                                            // then drops); receiving that or EOF ends this task with no dead line.
            let _ = rx.recv::<ToAgent>().await;
        });
        let res = run::<_, u32, u32>(vec![client], "k", vec![1u32, 2, 3]).await;
        assert!(res.is_err());
        fake.await.unwrap();
    }

    #[tokio::test]
    async fn a_duplicate_completion_is_deduplicated() {
        let (client, server) = connection_pair(64);
        let fake = tokio::spawn(async move {
            let (mut tx, mut rx) = server.split();
            let _hello: ToAgent = rx.recv().await.unwrap().unwrap();
            tx.send(&FromAgent::Ready { slots: 1 }).await.unwrap();

            // First task completes, then a duplicate completion for it arrives.
            let _a0: ToAgent = rx.recv().await.unwrap().unwrap();
            tx.send(&FromAgent::Completed {
                task_id: 0,
                output: postcard::to_allocvec(&100u32).unwrap(),
                via: String::new(),
            })
            .await
            .unwrap();
            tx.send(&FromAgent::Completed {
                task_id: 0,
                output: postcard::to_allocvec(&999u32).unwrap(),
                via: String::new(),
            })
            .await
            .unwrap();

            // Second task (assigned after the first completed).
            let _a1: ToAgent = rx.recv().await.unwrap().unwrap();
            tx.send(&FromAgent::Completed {
                task_id: 1,
                output: postcard::to_allocvec(&200u32).unwrap(),
                via: String::new(),
            })
            .await
            .unwrap();

            let _shutdown: ToAgent = rx.recv().await.unwrap().unwrap();
        });

        let out: Vec<Result<u32, String>> = run(vec![client], "k", vec![10u32, 20]).await.unwrap();
        fake.await.unwrap();
        // The duplicate (999) is ignored; the first result (100) stands.
        assert_eq!(out, vec![Ok(100), Ok(200)]);
    }

    #[tokio::test]
    async fn a_function_is_keyed_by_its_type_name() {
        fn triple(x: u32) -> u32 {
            x * 3
        }
        let (client, server) = connection_pair(256);
        let agent = tokio::spawn(serve(server, Registry::new().with_fn(triple)));

        // The coordinator derives the same key the agent registered under.
        let key = fn_key(&triple);
        let out: Vec<Result<u32, String>> =
            run(vec![client], key, (0..5u32).collect()).await.unwrap();
        agent.await.unwrap().unwrap();

        assert_eq!(out, (0..5u32).map(|x| Ok(x * 3)).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn a_serialize_error_fails_the_run() {
        use crate::testing::FailsToSerialize;
        let res = run::<tokio::io::DuplexStream, FailsToSerialize, u32>(
            vec![],
            "k",
            vec![FailsToSerialize],
        )
        .await;
        assert!(res.is_err());
    }

    /// A handler slow enough that work is still pending when a node joins, so the
    /// join lands mid-run rather than after the original agent has drained it all.
    fn slow_double(x: u32) -> u32 {
        std::thread::sleep(std::time::Duration::from_millis(3));
        x * 2
    }

    #[tokio::test]
    async fn speculation_reruns_a_straggler_on_an_idle_node() {
        use crate::observability::RunState;
        use crate::testing::EventRecorder;
        use tokio::sync::mpsc;

        fn slow(x: u32) -> u32 {
            std::thread::sleep(std::time::Duration::from_millis(40));
            x
        }
        fn fast(x: u32) -> u32 {
            x
        }

        // The first agent is assigned the lone task and runs it slowly; the second
        // is idle with nothing pending. With speculation on, the idle agent re-runs
        // the straggler and wins, so the run finishes without waiting on the slow
        // one, and the result is recorded exactly once.
        let (slow_client, slow_server) = connection_pair(256);
        let (fast_client, fast_server) = connection_pair(256);
        tokio::spawn(serve(slow_server, Registry::new().with("k", handler(slow))));
        tokio::spawn(serve(fast_server, Registry::new().with("k", handler(fast))));
        let recorder = EventRecorder::default();
        let (joins_tx, joins_rx) = mpsc::unbounded_channel();
        drop(joins_tx);
        let agents = vec![
            ("slow".to_string(), slow_client),
            ("fast".to_string(), fast_client),
        ];
        let payloads = super::serialize_inputs(&[7u32]).unwrap();
        let raw = super::run_job_raw_with_joins(
            agents,
            "k",
            payloads,
            &[],
            super::RunOptions {
                require_redundancy: false,
                speculative: true,
            },
            joins_rx,
            &recorder,
        )
        .await
        .unwrap();
        let outs: Vec<Result<u32, String>> =
            raw.into_iter().map(super::decode_output::<u32>).collect();
        assert_eq!(outs, vec![Ok(7)], "exactly one result, no duplicate");

        let mut state = RunState::default();
        for event in &recorder.events() {
            state.apply(event);
        }
        assert_eq!(
            state.nodes()["fast"].completed(),
            1,
            "the idle agent re-ran the straggler and won"
        );
    }

    #[tokio::test]
    async fn a_node_joining_mid_run_takes_pending_work() {
        use crate::observability::RunState;
        use crate::testing::EventRecorder;
        use tokio::sync::mpsc;

        // One slow agent starts the run; a second node is spliced in over the
        // joins channel partway through and must pull some of the pending tasks,
        // with every result still produced exactly once and in input order.
        let (joins_tx, joins_rx) = mpsc::unbounded_channel();
        let (orig_client, orig_server) = connection_pair(256);
        tokio::spawn(serve(
            orig_server,
            Registry::new().with("slow", handler(slow_double)),
        ));
        let recorder = EventRecorder::default();
        let agents = vec![("orig".to_string(), orig_client)];
        let payloads = super::serialize_inputs(&(0..30u32).collect::<Vec<_>>()).unwrap();

        let run = super::run_job_raw_with_joins(
            agents,
            "slow",
            payloads,
            &[],
            super::RunOptions::default(),
            joins_rx,
            &recorder,
        );
        let driver = async {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            let (join_client, join_server) = connection_pair(256);
            tokio::spawn(serve(
                join_server,
                Registry::new().with("slow", handler(slow_double)),
            ));
            let joiner = super::handshake_join("joiner".to_string(), join_client, "slow")
                .await
                .unwrap();
            joins_tx.send(joiner).unwrap();
            drop(joins_tx);
        };
        let (raw, ()) = tokio::join!(run, driver);

        let outs: Vec<Result<u32, String>> = raw
            .unwrap()
            .into_iter()
            .map(super::decode_output::<u32>)
            .collect();
        assert_eq!(outs, (0..30u32).map(|x| Ok(x * 2)).collect::<Vec<_>>());

        let mut state = RunState::default();
        for event in &recorder.events() {
            state.apply(event);
        }
        assert!(
            state.nodes()["joiner"].completed() >= 1,
            "the joined node should have run at least one task"
        );
    }

    #[tokio::test]
    async fn a_relay_node_can_join_mid_run() {
        use crate::protocol::ChildAd;
        use tokio::sync::mpsc;

        // A joining node may itself be a relay: it replies `Discovered`, is told to
        // activate all of its own children, then readies. handshake_join must drive
        // that two-step handshake, and the spliced relay must run pending tasks.
        let (joins_tx, joins_rx) = mpsc::unbounded_channel();
        let (orig_client, orig_server) = connection_pair(256);
        tokio::spawn(serve(
            orig_server,
            Registry::new().with("slow", handler(slow_double)),
        ));
        let agents = vec![("orig".to_string(), orig_client)];
        let payloads = super::serialize_inputs(&(0..20u32).collect::<Vec<_>>()).unwrap();

        let run = super::run_job_raw_with_joins(
            agents,
            "slow",
            payloads,
            &[],
            super::RunOptions::default(),
            joins_rx,
            &NoopSink,
        );
        let driver = async {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            let (relay_client, relay_server) = connection_pair(256);
            // A fake relay: describes one child, accepts the activate, readies, then
            // completes every task it is assigned until shut down.
            tokio::spawn(async move {
                let (mut tx, mut rx) = relay_server.split();
                let _hello = rx.recv::<ToAgent>().await;
                tx.send(&FromAgent::Discovered {
                    children: vec![ChildAd::new("child".to_string(), "child".to_string(), 1, 0)],
                })
                .await
                .unwrap();
                let _activate = rx.recv::<ToAgent>().await; // Activate all children
                tx.send(&FromAgent::Ready { slots: 1 }).await.unwrap();
                // Complete each assigned task; anything else (Shutdown, a clean
                // end, or a read error) stops the relay, dropping its connection
                // so the coordinator's drain ends.
                while let Ok(Some(ToAgent::Assign { task_id, .. })) = rx.recv::<ToAgent>().await {
                    tx.send(&FromAgent::Completed {
                        task_id,
                        output: postcard::to_allocvec(&0u32).unwrap(),
                        via: String::new(),
                    })
                    .await
                    .unwrap();
                }
            });
            let joiner = super::handshake_join("relay".to_string(), relay_client, "slow")
                .await
                .unwrap();
            joins_tx.send(joiner).unwrap();
            drop(joins_tx);
        };
        let (raw, ()) = tokio::join!(run, driver);
        // Every task completes (the splice ran a relay joiner end to end).
        assert_eq!(raw.unwrap().len(), 20);
    }

    #[tokio::test]
    async fn handshake_join_rejects_a_bad_first_reply() {
        // The joiner's first reply must be Ready or Discovered; anything else is
        // a protocol error the handshake surfaces rather than splicing a bad node.
        let (client, server) = connection_pair(256);
        tokio::spawn(async move {
            let (mut tx, mut rx) = server.split();
            let _hello = rx.recv::<ToAgent>().await;
            tx.send(&FromAgent::Completed {
                task_id: 0,
                output: Vec::new(),
                via: String::new(),
            })
            .await
            .unwrap();
        });
        let Err(err) = super::handshake_join("bad".to_string(), client, "id").await else {
            panic!("a bad first reply must be rejected");
        };
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn handshake_join_rejects_a_relay_that_does_not_ready_after_activate() {
        use crate::protocol::ChildAd;
        // A joining relay that, after being told to activate, replies with
        // something other than Ready is rejected.
        let (client, server) = connection_pair(256);
        tokio::spawn(async move {
            let (mut tx, mut rx) = server.split();
            let _hello = rx.recv::<ToAgent>().await;
            tx.send(&FromAgent::Discovered {
                children: vec![ChildAd::new("c".to_string(), "c".to_string(), 1, 0)],
            })
            .await
            .unwrap();
            let _activate = rx.recv::<ToAgent>().await;
            tx.send(&FromAgent::Started { task_id: 0 }).await.unwrap();
        });
        let Err(err) = super::handshake_join("relay".to_string(), client, "id").await else {
            panic!("a relay that does not ready after activate must be rejected");
        };
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn a_run_waits_for_a_join_then_errors_when_the_channel_closes_with_no_survivor() {
        use tokio::sync::mpsc;

        // The only agent readies, then dies leaving work pending. With the joins
        // channel still open the run must wait (a node might yet join), and only
        // once the channel closes with no survivor does it give up.
        let (joins_tx, joins_rx) =
            mpsc::unbounded_channel::<super::Joiner<tokio::io::DuplexStream>>();
        let (client, server) = connection_pair(256);
        tokio::spawn(async move {
            let (mut tx, mut rx) = server.split();
            let _hello = rx.recv::<ToAgent>().await;
            tx.send(&FromAgent::Ready { slots: 1 }).await.unwrap();
            // Accept the first task, then drop: the agent dies mid-task, leaving
            // work pending and no survivor.
            let _assign = rx.recv::<ToAgent>().await;
        });
        let agents = vec![("dies".to_string(), client)];
        let payloads = super::serialize_inputs(&(0..4u32).collect::<Vec<_>>()).unwrap();

        let run = super::run_job_raw_with_joins(
            agents,
            "id",
            payloads,
            &[],
            super::RunOptions::default(),
            joins_rx,
            &NoopSink,
        );
        let driver = async {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            drop(joins_tx); // no node ever joins; closing the channel ends the wait
        };
        let (res, ()) = tokio::join!(run, driver);
        let err = res.unwrap_err();
        assert!(err.to_string().contains("no node could join"), "{err}");
    }
}
