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
use crate::observability::{Event as Obs, EventSink, NodeState};
use crate::protocol::{ChildAd, FromAgent, TaskId, ToAgent, PROTOCOL_VERSION};

/// What a reader task forwards to the coordinator's central loop. A read error
/// and a clean disconnect are treated alike: the agent is lost. Shared with the
/// relay (`crate::relay`), which reuses [`connect_agents`] and the same reader
/// channel to multiplex its children.
pub(crate) enum Event {
    Message(usize, FromAgent),
    Lost(usize),
}

fn spawn_reader<S>(
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
/// Under `DedupById` a node id already claimed by an earlier relay is dropped, so
/// it stays a standby on this path.
fn active_labels(reports: &[FirstReport], policy: &ActivationPolicy) -> Vec<Vec<String>> {
    let mut claimed = std::collections::HashSet::new();
    reports
        .iter()
        .map(|report| match report {
            FirstReport::Leaf(_) => Vec::new(),
            FirstReport::Relay(children) => children
                .iter()
                .filter(|child| match policy {
                    ActivationPolicy::ApproveAll => true,
                    ActivationPolicy::DedupById => claimed.insert(child.id.clone()),
                })
                .map(|child| child.label.clone())
                .collect(),
        })
        .collect()
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

/// Handshake with every agent and spawn its reader task. A leaf replies `Ready`;
/// a relay replies `Discovered`, is told which children to `Activate` by `policy`,
/// then replies `Ready` with its active capacity. Children a relay does not
/// activate are kept as standbys.
pub(crate) async fn connect_agents<S>(
    agents: Vec<(String, Connection<S>)>,
    fn_key: &str,
    events_tx: &mpsc::UnboundedSender<Event>,
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

    // Phase 2: tell each relay which children to run, then read its readiness.
    let actives = active_labels(&reports, policy);
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
                    .filter(|child| !active.contains(&child.label))
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

/// Mutable scheduling state for one run.
struct Job<S> {
    senders: Vec<Sender<S>>,
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
    /// `task_id` -> (agent, input index), for reassignment and dedup on completion.
    assigned: HashMap<TaskId, (usize, usize)>,
    results: Vec<Option<Result<Vec<u8>, String>>>,
    remaining: usize,
    payloads: Vec<Vec<u8>>,
}

impl<S> Job<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// Fill an agent up to its capacity from the pending pool, so a relay
    /// fronting many slots is kept fed while a leaf still holds one task. A lost
    /// agent takes nothing.
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
            self.assigned.insert(task_id, (agent, idx));
            self.in_flight[agent] += 1;
        }
        Ok(())
    }

    /// Record a terminal outcome. Returns the freed agent, or `None` if the
    /// `task_id` was unknown or already resolved (dedup of a re-run task).
    fn record(&mut self, task_id: TaskId, result: Result<Vec<u8>, String>) -> Option<usize> {
        // `assigned` holds each task_id at most once (removed on first record),
        // so a duplicate returns `None` here and this runs exactly once per task.
        let (agent, idx) = self.assigned.remove(&task_id)?;
        self.in_flight[agent] -= 1;
        self.results[idx] = Some(result);
        self.remaining -= 1;
        Some(agent)
    }

    /// Mark `agent` lost and return its in-flight tasks to the pending pool so
    /// survivors re-run them. A task it had actually
    /// finished is already out of `assigned`, so only genuinely lost work moves;
    /// re-running a task is safe by the idempotency contract and deduped on
    /// completion by `record`.
    fn requeue_lost(&mut self, agent: usize) {
        self.alive[agent] = false;
        let lost: Vec<TaskId> = self
            .assigned
            .iter()
            .filter(|(_, (owner, _))| *owner == agent)
            .map(|(task, _)| *task)
            .collect();
        for task in lost {
            if let Some((_, idx)) = self.assigned.remove(&task) {
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
                    .send(&ToAgent::Promote { child: child.label })
                    .await?;
            }
        }
        Ok(())
    }

    /// Fold one event from the central channel into the run: record a terminal,
    /// surface a task or subtree event, fold in a promoted relay's larger
    /// capacity, or react to a lost agent by rerouting onto standbys. Returns an
    /// error on a protocol violation or when no agent is left to make progress.
    async fn on_event(
        &mut self,
        event: Option<Event>,
        labels: &[String],
        events: &dyn EventSink,
    ) -> std::io::Result<()> {
        match event {
            Some(Event::Message(_, FromAgent::Ready { .. } | FromAgent::Discovered { .. })) => {
                // Ready and Discovered are handshake messages, so seeing one on
                // the live channel is a protocol error.
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "agent sent a handshake message more than once",
                ));
            }
            // A promoted relay reports its larger capacity after reroute: fold it
            // in and feed it the requeued work, marking it working if it took any.
            Some(Event::Message(agent, FromAgent::Capacity { slots })) => {
                self.capacity[agent] = slots;
                self.assign_up_to_capacity(agent).await?;
                if self.in_flight[agent] > 0 {
                    events.emit(Obs::node(&labels[agent], NodeState::Working));
                }
            }
            Some(Event::Message(_, FromAgent::Started { task_id })) => {
                if let Some((agent, _)) = self.assigned.get(&task_id) {
                    events.emit(Obs::TaskStarted {
                        host: labels[*agent].clone(),
                        task: task_id,
                    });
                }
            }
            Some(Event::Message(_, FromAgent::Completed { task_id, output })) => {
                // The raw bytes are kept as-is; decoding happens in the typed
                // layer, so a `Completed` is `ok` at the protocol level here.
                self.finish(task_id, Ok(output), true, labels, events)
                    .await?;
            }
            Some(Event::Message(_, FromAgent::Failed { task_id, error })) => {
                self.finish(task_id, Err(error), false, labels, events)
                    .await?;
            }
            // A subtree event from a relay child: prefix its host with that
            // child's label so it carries a full path from the root, then
            // re-emit it so the whole tree surfaces in the run's event stream.
            Some(Event::Message(agent, FromAgent::Observe(mut event))) => {
                event.prefix_host(&labels[agent]);
                events.emit(event);
            }
            // A dropped or erroring agent is abandoned: mark it lost, requeue its
            // in-flight tasks, promote standbys so a survivor takes over the
            // orphaned subtree, and feed the survivors. The run fails only when no
            // agent is left.
            Some(Event::Lost(agent)) => {
                events.emit(Obs::node(&labels[agent], NodeState::Lost));
                self.requeue_lost(agent);
                if !self.any_alive() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::ConnectionReset,
                        "every agent was lost before the job completed",
                    ));
                }
                self.reroute().await?;
                for survivor in 0..self.senders.len() {
                    self.assign_up_to_capacity(survivor).await?;
                }
            }
            None => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "all agents gone before completion",
                ));
            }
        }
        Ok(())
    }

    /// Record a terminal outcome, emit its task event, and hand the freed agent
    /// its next task. Emits an `Idle` node event when no work is left for it.
    async fn finish(
        &mut self,
        task_id: TaskId,
        result: Result<Vec<u8>, String>,
        ok: bool,
        labels: &[String],
        events: &dyn EventSink,
    ) -> std::io::Result<()> {
        if let Some(agent) = self.record(task_id, result) {
            events.emit(Obs::TaskFinished {
                host: labels[agent].clone(),
                task: task_id,
                ok,
            });
            self.assign_up_to_capacity(agent).await?;
            if self.in_flight[agent] == 0 {
                events.emit(Obs::node(&labels[agent], NodeState::Idle));
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
///
/// # Errors
/// Returns an error on a handshake or transport failure, or if every agent is
/// lost before the job completes.
pub(crate) async fn run_job_raw<S>(
    agents: Vec<(String, Connection<S>)>,
    fn_key: &str,
    payloads: Vec<Vec<u8>>,
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
    } = connect_agents(agents, fn_key, &events_tx, &ActivationPolicy::DedupById).await?;
    drop(events_tx); // the channel ends when every reader does

    let agent_count = senders.len();
    let mut job: Job<S> = Job {
        senders,
        in_flight: vec![0; agent_count],
        capacity,
        standbys,
        alive: vec![true; agent_count],
        pending: (0..num_tasks).collect(),
        assigned: HashMap::new(),
        results: (0..num_tasks).map(|_| None).collect(),
        remaining: num_tasks,
        payloads,
    };

    events.emit(Obs::RunStarted { tasks: num_tasks });
    for (agent, label) in labels.iter().enumerate() {
        job.assign_up_to_capacity(agent).await?;
        if job.in_flight[agent] > 0 {
            events.emit(Obs::node(label, NodeState::Working));
        }
    }

    while job.remaining > 0 {
        let event = events_rx.recv().await;
        job.on_event(event, &labels, events).await?;
    }

    for (agent, label) in labels.iter().enumerate() {
        if job.alive[agent] {
            events.emit(Obs::node(label, NodeState::Done));
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
            observed.prefix_host(&labels[agent]);
            events.emit(observed);
        }
    }
    for reader in readers {
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
    let raw = run_job_raw(agents, fn_key, payloads, events).await?;
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

        assert_eq!(state.total_tasks, 40);
        assert_eq!(state.completed, 40);
        assert_eq!(
            state.nodes["fast"].completed + state.nodes["slow"].completed,
            40
        );
        let (fast, slow) = (state.nodes["fast"].completed, state.nodes["slow"].completed);
        assert!(fast > slow, "fast {fast} vs slow {slow}");
        assert_eq!(state.nodes["fast"].state, NodeState::Done);
        assert_eq!(state.nodes["slow"].state, NodeState::Done);

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
        assert_eq!(state.nodes["relay/leaf"].state, NodeState::Done);
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
            })
            .await
            .unwrap();
            tx.send(&FromAgent::Completed {
                task_id: 0,
                output: postcard::to_allocvec(&999u32).unwrap(),
            })
            .await
            .unwrap();

            // Second task (assigned after the first completed).
            let _a1: ToAgent = rx.recv().await.unwrap().unwrap();
            tx.send(&FromAgent::Completed {
                task_id: 1,
                output: postcard::to_allocvec(&200u32).unwrap(),
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
}
