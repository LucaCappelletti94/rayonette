//! The coordinator side of a run.
//!
//! Hands a job's inputs to a set of agents, schedules them by pull (a free
//! capacity slot gets the next pending task), and assembles the results in
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
use crate::protocol::{FromAgent, TaskId, ToAgent, PROTOCOL_VERSION};

/// What a reader task forwards to the coordinator's central loop. A read error
/// and a clean disconnect are treated alike: the agent is lost.
enum Event {
    Message(FromAgent),
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
            let _ = events.send(Event::Message(msg));
        }
    })
}

/// The per-agent state produced by the handshake.
struct Agents<S> {
    senders: Vec<Sender<S>>,
    capacities: Vec<u32>,
    labels: Vec<String>,
    readers: Vec<JoinHandle<()>>,
}

/// Handshake with every agent (send `Hello`, receive `Ready`) and spawn each
/// agent's reader task feeding the central channel.
async fn connect_agents<S>(
    agents: Vec<(String, Connection<S>)>,
    fn_key: &str,
    events_tx: &mpsc::UnboundedSender<Event>,
) -> std::io::Result<Agents<S>>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut out = Agents {
        senders: Vec::with_capacity(agents.len()),
        capacities: Vec::with_capacity(agents.len()),
        labels: Vec::with_capacity(agents.len()),
        readers: Vec::with_capacity(agents.len()),
    };
    for (agent_id, (label, conn)) in agents.into_iter().enumerate() {
        let (mut tx, mut rx) = conn.split();
        tx.send(&ToAgent::Hello {
            protocol_version: PROTOCOL_VERSION,
            fn_key: fn_key.to_string(),
        })
        .await?;
        let capacity = match rx.recv::<FromAgent>().await? {
            Some(FromAgent::Ready { capacity }) => capacity,
            other => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("expected Ready, got {other:?}"),
                ));
            }
        };
        out.senders.push(tx);
        out.capacities.push(capacity);
        out.labels.push(label);
        out.readers
            .push(spawn_reader(rx, agent_id, events_tx.clone()));
    }
    Ok(out)
}

/// Mutable scheduling state for one run.
struct Job<S, O> {
    senders: Vec<Sender<S>>,
    capacities: Vec<u32>,
    inflight: Vec<u32>,
    /// Whether each agent's connection is still up; a lost one takes no tasks.
    alive: Vec<bool>,
    pending: VecDeque<usize>,
    /// `task_id` -> (agent, input index), for refill and dedup on completion.
    assigned: HashMap<TaskId, (usize, usize)>,
    results: Vec<Option<Result<O, String>>>,
    remaining: usize,
    payloads: Vec<Vec<u8>>,
}

impl<S, O> Job<S, O>
where
    S: AsyncRead + AsyncWrite + Unpin,
    O: DeserializeOwned,
{
    /// Fill an agent up to its capacity with pending tasks. A lost agent takes
    /// nothing.
    async fn fill(&mut self, agent: usize) -> std::io::Result<()> {
        if !self.alive[agent] {
            return Ok(());
        }
        while self.inflight[agent] < self.capacities[agent] {
            let Some(idx) = self.pending.pop_front() else {
                break;
            };
            let task_id = idx as TaskId;
            self.senders[agent]
                .send(&ToAgent::Assign {
                    task_id,
                    payload: self.payloads[idx].clone(),
                })
                .await?;
            self.assigned.insert(task_id, (agent, idx));
            self.inflight[agent] += 1;
        }
        Ok(())
    }

    /// Record a terminal outcome. Returns the agent to refill, or `None` if the
    /// `task_id` was unknown or already resolved (dedup of a re-run task).
    fn record(&mut self, task_id: TaskId, result: Result<O, String>) -> Option<usize> {
        // `assigned` holds each task_id at most once (removed on first record),
        // so a duplicate returns `None` above and this runs exactly once per task.
        let (agent, idx) = self.assigned.remove(&task_id)?;
        self.inflight[agent] -= 1;
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
    }

    /// Whether any agent is still alive to take work.
    fn any_alive(&self) -> bool {
        self.alive.iter().any(|alive| *alive)
    }

    /// Record a terminal outcome, emit its task event, and refill the freed
    /// agent. Emits an `Idle` node event when the agent runs dry.
    async fn finish(
        &mut self,
        task_id: TaskId,
        result: Result<O, String>,
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
            self.fill(agent).await?;
            if self.inflight[agent] == 0 {
                events.emit(Obs::node(&labels[agent], NodeState::Idle));
            }
        }
        Ok(())
    }
}

/// Run one job to completion, returning the outputs in input order.
///
/// Ships `inputs` to `agents` (all running the function named by `fn_key`); each
/// result is either the decoded output or a failure message (a panic or a decode
/// error).
///
/// # Errors
/// Returns an error on a handshake failure or transport error, or (in v1) if an
/// agent disconnects before the job completes (requeue/reconnect is Phase 6).
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
    let payloads: Vec<Vec<u8>> = inputs
        .iter()
        .map(|i| {
            postcard::to_allocvec(i)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
        })
        .collect::<Result<_, _>>()?;
    let num_tasks = payloads.len();

    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let Agents {
        senders,
        capacities,
        labels,
        readers,
    } = connect_agents(agents, fn_key, &events_tx).await?;
    drop(events_tx); // the channel ends when every reader does

    let agent_count = senders.len();
    let mut job: Job<S, O> = Job {
        senders,
        capacities,
        inflight: vec![0; agent_count],
        alive: vec![true; agent_count],
        pending: (0..num_tasks).collect(),
        assigned: HashMap::new(),
        results: (0..num_tasks).map(|_| None).collect(),
        remaining: num_tasks,
        payloads,
    };

    events.emit(Obs::RunStarted { tasks: num_tasks });
    for (agent, label) in labels.iter().enumerate() {
        job.fill(agent).await?;
        if job.inflight[agent] > 0 {
            events.emit(Obs::node(label, NodeState::Working));
        }
    }

    while job.remaining > 0 {
        let event = events_rx.recv().await;
        match event {
            Some(Event::Message(FromAgent::Ready { .. })) => {
                // Ready is a handshake message; a second one is a protocol error.
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "agent sent Ready more than once",
                ));
            }
            Some(Event::Message(FromAgent::Started { task_id })) => {
                if let Some((agent, _)) = job.assigned.get(&task_id) {
                    events.emit(Obs::TaskStarted {
                        host: labels[*agent].clone(),
                        task: task_id,
                    });
                }
            }
            Some(Event::Message(FromAgent::Completed { task_id, output })) => {
                let result =
                    postcard::from_bytes::<O>(&output).map_err(|e| format!("decode output: {e}"));
                let ok = result.is_ok();
                job.finish(task_id, result, ok, &labels, events).await?;
            }
            Some(Event::Message(FromAgent::Failed { task_id, error })) => {
                job.finish(task_id, Err(error), false, &labels, events)
                    .await?;
            }
            // A dropped or erroring agent is abandoned: mark it lost, requeue
            // its in-flight tasks, and let the survivors absorb them. The run
            // fails only when no agent is left.
            Some(Event::Lost(agent)) => {
                events.emit(Obs::node(&labels[agent], NodeState::Lost));
                job.requeue_lost(agent);
                if !job.any_alive() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::ConnectionReset,
                        "every agent was lost before the job completed",
                    ));
                }
                for survivor in 0..agent_count {
                    job.fill(survivor).await?;
                }
            }
            None => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "all agents gone before completion",
                ));
            }
        }
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
    for reader in readers {
        let _ = reader.await;
    }

    // Every slot is `Some` because the loop only exits at `remaining == 0`.
    Ok(job.results.into_iter().flatten().collect())
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
            4,
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
            2,
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
    async fn never_exceeds_capacity_but_runs_concurrently() {
        let current = Arc::new(AtomicUsize::new(0));
        let high_water = Arc::new(AtomicUsize::new(0));
        let (cur, hw) = (current.clone(), high_water.clone());
        let task = handler(move |x: u32| {
            let now = cur.fetch_add(1, Ordering::SeqCst) + 1;
            hw.fetch_max(now, Ordering::SeqCst);
            std::thread::sleep(std::time::Duration::from_millis(20));
            cur.fetch_sub(1, Ordering::SeqCst);
            x
        });

        let (client, server) = connection_pair(256);
        let agent = tokio::spawn(serve(server, Registry::new().with("id", task), 3));

        let inputs: Vec<u32> = (0u32..12).collect();
        let out: Vec<Result<u32, String>> = run(vec![client], "id", inputs).await.unwrap();
        agent.await.unwrap().unwrap();

        assert_eq!(out.len(), 12);
        assert!(out.iter().all(Result::is_ok));
        let peak = high_water.load(Ordering::SeqCst);
        assert!(peak <= 3, "peak concurrency {peak} exceeded capacity 3");
        assert!(peak >= 2, "expected real concurrency, peak was {peak}");
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
        let agent_a = tokio::spawn(serve(server_a, Registry::new().with("id", fast), 1));
        let agent_b = tokio::spawn(serve(server_b, Registry::new().with("id", slow), 1));

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
        let agent_a = tokio::spawn(serve(server_a, Registry::new().with("id", fast), 1));
        let agent_b = tokio::spawn(serve(server_b, Registry::new().with("id", slow), 1));

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
            1,
        ));
        let agent_b = tokio::spawn(serve(
            Connection::new(server_b),
            Registry::new().with("id", handler(|x: u32| x)),
            1,
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
            tx.send(&FromAgent::Ready { capacity: 1 }).await.unwrap();
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
            1,
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
                    1,
                ));
                let agent_b = tokio::spawn(serve(
                    Connection::new(server_b),
                    Registry::new().with("id", handler(|x: u32| x)),
                    1,
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
    async fn errors_when_agent_readies_twice() {
        let (client, server) = connection_pair(64);
        let fake = tokio::spawn(async move {
            let (mut tx, mut rx) = server.split();
            let _hello: ToAgent = rx.recv().await.unwrap().unwrap();
            tx.send(&FromAgent::Ready { capacity: 1 }).await.unwrap();
            tx.send(&FromAgent::Ready { capacity: 1 }).await.unwrap();
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
        fn every_task_completes_once_in_order(n in 0usize..40, cap in 1u32..4) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let (client, server) = connection_pair(512);
                let agent = tokio::spawn(serve(
                    server,
                    Registry::new().with("id", handler(|x: u64| x)),
                    cap,
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
            tx.send(&FromAgent::Ready { capacity: 1 }).await.unwrap();
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
            tx.send(&FromAgent::Ready { capacity: 1 }).await.unwrap();
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
            tx.send(&FromAgent::Ready { capacity: 1 }).await.unwrap();
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
            tx.send(&FromAgent::Ready { capacity: 1 }).await.unwrap();

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
        let agent = tokio::spawn(serve(server, Registry::new().with_fn(triple), 2));

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
