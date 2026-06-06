//! The relay: a node that is an agent to its parent and a coordinator to its own
//! children (PLAN.md R2).
//!
//! A relay handshakes its parent like a leaf, but instead of running tasks it
//! launches a sub-fleet of children, advertises their combined capacity upward
//! (so the parent keeps the whole subtree fed), and splices the two sides: a
//! task assigned from above is dispatched to a free child, and a child's
//! `Started`/`Completed`/`Failed` is forwarded straight up with its `task_id`
//! intact. The global task id flows through unchanged, so the top coordinator
//! collects results in input order without knowing the tree's shape.

use std::collections::{HashMap, VecDeque};
use std::io;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;

use crate::agent::recv_hello;
use crate::coordinator::{connect_agents, Agents, Event as ChildEvent};
use crate::fleet::{launch_all, Launch, Launched};
use crate::framing::{Connection, Sender};
use crate::observability::{Event, EventSink, NodeState};
use crate::protocol::{FromAgent, TaskId, ToAgent};

/// An [`EventSink`] that forwards each event onto the relay's uplink channel, so
/// a child's observability (its `Profiled` and provisioning ladder) can be sent
/// up to the parent from the relay's async loop (the sink's `emit` is sync).
struct UplinkSink {
    tx: mpsc::UnboundedSender<Event>,
}

impl EventSink for UplinkSink {
    fn emit(&self, event: Event) {
        let _ = self.tx.send(event);
    }
}

/// The relay's child-side scheduling state: which child holds each in-flight
/// task, how loaded each child is against its capacity, and the pending backlog.
struct Relay<S> {
    senders: Vec<Sender<S>>,
    /// Each child's local label, used to attribute its observability events.
    labels: Vec<String>,
    /// Each child's advertised capacity (slots it can hold in flight).
    capacity: Vec<usize>,
    /// Whether each child is still up; a lost one takes no more tasks.
    alive: Vec<bool>,
    /// How many tasks each child is currently running.
    load: Vec<usize>,
    /// Tasks waiting for a free child slot, with their payloads.
    pending: VecDeque<(TaskId, Vec<u8>)>,
    /// `task_id` -> (child, payload), kept so a lost child's work can be requeued.
    inflight: HashMap<TaskId, (usize, Vec<u8>)>,
    /// Subtree observability events queued for the parent (drained by the loop).
    uplink: mpsc::UnboundedSender<Event>,
}

impl<S> Relay<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn new(
        senders: Vec<Sender<S>>,
        labels: Vec<String>,
        capacity: Vec<usize>,
        uplink: mpsc::UnboundedSender<Event>,
    ) -> Self {
        let n = senders.len();
        Self {
            senders,
            labels,
            capacity,
            alive: vec![true; n],
            load: vec![0; n],
            pending: VecDeque::new(),
            inflight: HashMap::new(),
            uplink,
        }
    }

    /// Queue a node-state event for `child` to be reported up to the parent.
    fn report(&self, child: usize, state: NodeState) {
        let _ = self.uplink.send(Event::node(&self.labels[child], state));
    }

    /// Hand pending tasks to children with a free slot, first free child first,
    /// until no task or no free slot is left. A child going from idle to busy is
    /// reported as `Working`.
    async fn dispatch(&mut self) -> io::Result<()> {
        while !self.pending.is_empty() {
            let Some(child) =
                (0..self.senders.len()).find(|&c| self.alive[c] && self.load[c] < self.capacity[c])
            else {
                return Ok(());
            };
            let (task_id, payload) = self.pending.pop_front().expect("pending is non-empty");
            let was_idle = self.load[child] == 0;
            self.senders[child]
                .send(&ToAgent::Assign {
                    task_id,
                    payload: payload.clone(),
                })
                .await?;
            self.load[child] += 1;
            self.inflight.insert(task_id, (child, payload));
            if was_idle {
                self.report(child, NodeState::Working);
            }
        }
        Ok(())
    }

    /// Record a child's terminal outcome, freeing its slot. Returns the freed
    /// child, or `None` for an unknown `task_id` (a duplicate already resolved),
    /// which is not forwarded.
    fn on_terminal(&mut self, task_id: TaskId) -> Option<usize> {
        let (child, _) = self.inflight.remove(&task_id)?;
        self.load[child] -= 1;
        Some(child)
    }

    /// Mark a child lost and requeue its in-flight tasks onto the front of the
    /// pending backlog so the survivors re-run them.
    fn on_lost(&mut self, child: usize) {
        self.alive[child] = false;
        let orphaned: Vec<TaskId> = self
            .inflight
            .iter()
            .filter(|(_, (owner, _))| *owner == child)
            .map(|(task, _)| *task)
            .collect();
        for task in orphaned {
            if let Some((_, payload)) = self.inflight.remove(&task) {
                self.pending.push_front((task, payload));
            }
        }
        self.load[child] = 0;
        self.report(child, NodeState::Lost);
    }

    /// Shut the live children down cleanly, reporting each `Done`.
    async fn shutdown(&mut self) {
        for child in 0..self.senders.len() {
            if self.alive[child] {
                self.report(child, NodeState::Done);
                let _ = self.senders[child].send(&ToAgent::Shutdown).await;
            }
        }
    }
}

/// Run as a relay over `parent`, coordinating `children`.
///
/// A relay is an agent to its parent and a coordinator to its children: it
/// handshakes the parent (learning the job's `fn_key`), launches and handshakes
/// the children, advertises their combined capacity, then forwards work down and
/// `Started`/`Completed`/`Failed` straight back up (task ids pass through intact)
/// until the parent sends `Shutdown` or the connection closes. A child that drops
/// mid-run has its in-flight tasks requeued onto the survivors; with none left
/// the relay tears down so the parent requeues the whole subtree (no redundancy
/// in R2).
///
/// Subtree observability flows up too: the relay reports each child's role,
/// profile, and lifecycle state to the parent as `Observe` events (prefixing a
/// grandchild's path with the child's label), so the top coordinator sees the
/// whole tree.
///
/// # Errors
/// Returns an error on a protocol violation or a transport failure on either side.
pub async fn relay<P, L>(parent: Connection<P>, children: Vec<L>) -> io::Result<()>
where
    P: AsyncRead + AsyncWrite + Unpin + Send,
    L: Launch + Send + Sync,
{
    let (mut parent_tx, mut parent_rx) = parent.split();

    // Parent handshake: learn the job's function key (a leaf does the same in
    // `agent::serve`, sharing `recv_hello`).
    let fn_key = recv_hello(&mut parent_rx).await?;

    // The uplink carries this subtree's observability up to the parent: the
    // children's `Profiled` and provisioning ladder are emitted to it during
    // launch, and the relay reports its children's task lifecycle to it too.
    let (uplink_tx, mut uplink_rx) = mpsc::unbounded_channel::<Event>();
    let uplink_sink = UplinkSink {
        tx: uplink_tx.clone(),
    };

    // Launch the children (the provisioning cascade in the real path), then
    // handshake them and spawn one reader per child feeding a central channel.
    let Launched {
        agents,
        guards,
        failures,
    } = launch_all(&children, None, None, &uplink_sink).await;
    let (child_events_tx, mut child_events_rx) = mpsc::unbounded_channel();
    let Agents {
        senders,
        labels,
        capacity,
        readers,
    } = connect_agents(agents, &fn_key, &child_events_tx).await?;
    drop(child_events_tx); // the channel ends when every child reader does

    // Advertise the subtree's combined capacity so the parent keeps it fed. A
    // relay whose children all failed to launch has nothing to offer; it errors
    // (naming why each child dropped) rather than advertising zero, which would
    // stall the parent; the parent then treats the dead subtree as a lost host.
    let total_slots: usize = capacity.iter().sum();
    if total_slots == 0 {
        return Err(no_usable_children(&failures));
    }
    parent_tx
        .send(&FromAgent::Ready { slots: total_slots })
        .await?;

    // Splice the two sides until the parent is done or the subtree dies.
    let mut sched = Relay::new(senders, labels, capacity, uplink_tx);
    loop {
        tokio::select! {
            from_parent = parent_rx.recv::<ToAgent>() => match from_parent? {
                Some(ToAgent::Assign { task_id, payload }) => {
                    sched.pending.push_back((task_id, payload));
                    sched.dispatch().await?;
                }
                // A clean Shutdown or a dropped parent both end the relay.
                Some(ToAgent::Shutdown) | None => break,
                Some(other @ ToAgent::Hello { .. }) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unexpected message from parent: {other:?}"),
                    ));
                }
            },
            from_child = child_events_rx.recv() => match from_child {
                Some(ChildEvent::Message(_, FromAgent::Started { task_id })) => {
                    parent_tx.send(&FromAgent::Started { task_id }).await?;
                }
                Some(ChildEvent::Message(_, FromAgent::Completed { task_id, output })) => {
                    if let Some(child) = sched.on_terminal(task_id) {
                        parent_tx.send(&FromAgent::Completed { task_id, output }).await?;
                        sched.dispatch().await?;
                        if sched.load[child] == 0 {
                            sched.report(child, NodeState::Idle);
                        }
                    }
                }
                Some(ChildEvent::Message(_, FromAgent::Failed { task_id, error })) => {
                    if let Some(child) = sched.on_terminal(task_id) {
                        parent_tx.send(&FromAgent::Failed { task_id, error }).await?;
                        sched.dispatch().await?;
                        if sched.load[child] == 0 {
                            sched.report(child, NodeState::Idle);
                        }
                    }
                }
                // A grandchild's observability event: prefix its host with this
                // child's label so it carries a path, then pass it further up.
                Some(ChildEvent::Message(child, FromAgent::Observe(mut event))) => {
                    event.prefix_host(&sched.labels[child]);
                    parent_tx.send(&FromAgent::Observe(event)).await?;
                }
                // A child should ready only once, during `connect_agents`.
                Some(ChildEvent::Message(_, FromAgent::Ready { .. })) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "a child sent Ready more than once",
                    ));
                }
                // A dropped child: requeue its in-flight tasks onto the survivors.
                Some(ChildEvent::Lost(child)) => {
                    sched.on_lost(child);
                    sched.dispatch().await?;
                }
                // Every child reader has ended (each emits one `Lost` first), so
                // the subtree is gone: tear down and let the parent requeue.
                None => break,
            },
            // Forward this subtree's own observability up to the parent.
            Some(event) = uplink_rx.recv() => {
                parent_tx.send(&FromAgent::Observe(event)).await?;
            }
        }
    }

    sched.shutdown().await;
    flush_uplink(&mut uplink_rx, &mut parent_tx).await;
    for reader in readers {
        let _ = reader.await;
    }
    drop(guards);
    Ok(())
}

/// The error a relay returns when every child failed to launch, naming why each
/// dropped, so the parent treats the dead subtree as a lost host.
fn no_usable_children(failures: &[io::Error]) -> io::Error {
    let detail = failures
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("; ");
    io::Error::other(format!("rayonet: relay has no usable children: {detail}"))
}

/// Send any queued subtree events (the `Done` reports `shutdown` just emitted) up
/// to the parent before the relay returns, so the final tree view is complete.
async fn flush_uplink<P>(uplink_rx: &mut mpsc::UnboundedReceiver<Event>, parent_tx: &mut Sender<P>)
where
    P: AsyncRead + AsyncWrite + Unpin,
{
    while let Ok(event) = uplink_rx.try_recv() {
        let _ = parent_tx.send(&FromAgent::Observe(event)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::relay;
    use crate::agent::{handler, serve, Registry};
    use crate::coordinator::run_job;
    use crate::fleet::Launch;
    use crate::framing::Connection;
    use crate::observability::{EventSink, NoopSink};
    use crate::protocol::{FromAgent, ToAgent, PROTOCOL_VERSION};
    use crate::testing::{connection_pair, FaultInjector, LocalAgent};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::io::{duplex, DuplexStream};
    use tokio::task::JoinHandle;

    fn double(x: u32) -> u32 {
        x * 2
    }

    /// A child whose relay-facing reads are severed after `cut_after` bytes,
    /// simulating a leaf that drops mid-run. `usize::MAX` never cuts (healthy).
    struct FaultyChild {
        label: String,
        registry: Registry,
        cut_after: usize,
    }

    impl Launch for FaultyChild {
        type Stream = FaultInjector<DuplexStream>;
        type Guard = JoinHandle<std::io::Result<()>>;
        type Session = ();

        fn label(&self) -> String {
            self.label.clone()
        }

        async fn connect(&self) -> std::io::Result<()> {
            Ok(())
        }

        async fn activate(
            &self,
            _session: (),
            _events: &dyn EventSink,
        ) -> std::io::Result<(Connection<FaultInjector<DuplexStream>>, Self::Guard)> {
            let (client_raw, server_raw) = duplex(4096);
            let client =
                Connection::new(FaultInjector::cut_reads_after(client_raw, self.cut_after));
            let task = tokio::spawn(serve(Connection::new(server_raw), self.registry.clone()));
            Ok((client, task))
        }
    }

    /// A misbehaving child that readies twice, to exercise the relay's
    /// protocol-violation guard.
    struct DoubleReadyChild;

    impl Launch for DoubleReadyChild {
        type Stream = DuplexStream;
        type Guard = JoinHandle<()>;
        type Session = ();

        fn label(&self) -> String {
            "double-ready".to_string()
        }

        async fn connect(&self) -> std::io::Result<()> {
            Ok(())
        }

        async fn activate(
            &self,
            _session: (),
            _events: &dyn EventSink,
        ) -> std::io::Result<(Connection<DuplexStream>, Self::Guard)> {
            let (client, server) = connection_pair(256);
            let task = tokio::spawn(async move {
                let (mut tx, mut rx) = server.split();
                let _hello: ToAgent = rx.recv().await.unwrap().unwrap();
                tx.send(&FromAgent::Ready { slots: 1 }).await.unwrap();
                tx.send(&FromAgent::Ready { slots: 1 }).await.unwrap();
                let _ = rx.recv::<ToAgent>().await;
            });
            Ok((client, task))
        }
    }

    /// A child that is itself a relay over its own `leaves`, so a parent relay
    /// sees one of its children recurse into another relay (depth-3 and beyond).
    struct RelayAgent {
        leaves: Vec<(String, Registry)>,
    }

    impl Launch for RelayAgent {
        type Stream = DuplexStream;
        type Guard = JoinHandle<std::io::Result<()>>;
        type Session = ();

        fn label(&self) -> String {
            "sub-relay".to_string()
        }

        async fn connect(&self) -> std::io::Result<()> {
            Ok(())
        }

        async fn activate(
            &self,
            _session: (),
            _events: &dyn EventSink,
        ) -> std::io::Result<(Connection<DuplexStream>, Self::Guard)> {
            let (client, server) = connection_pair(256);
            let leaves: Vec<LocalAgent> = self
                .leaves
                .iter()
                .map(|(label, registry)| LocalAgent::new(label, registry.clone()))
                .collect();
            let task = tokio::spawn(async move { relay(server, leaves).await });
            Ok((client, task))
        }
    }

    /// Drive `inputs` through a top coordinator -> relay -> `children` and return
    /// the coordinator's results.
    async fn through_relay<L: Launch + Send + Sync>(
        children: Vec<L>,
        key: &str,
        inputs: Vec<u32>,
    ) -> std::io::Result<Vec<Result<u32, String>>> {
        let (coord_side, relay_side) = connection_pair(4096);
        let relay_fut = relay(relay_side, children);
        let coord_fut = run_job(
            vec![("relay".to_string(), coord_side)],
            key,
            inputs,
            &NoopSink,
        );
        let (relay_res, out) = tokio::join!(relay_fut, coord_fut);
        relay_res?;
        out
    }

    #[tokio::test]
    async fn coordinator_relay_leaves_returns_ordered_results() {
        let children = vec![
            LocalAgent::new("leaf-a", Registry::new().with("double", handler(double))),
            LocalAgent::new("leaf-b", Registry::new().with("double", handler(double))),
        ];
        let out = through_relay(children, "double", (0..20u32).collect())
            .await
            .unwrap();
        assert_eq!(out, (0..20u32).map(|x| Ok(x * 2)).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn the_coordinator_sees_the_whole_subtree() {
        use crate::capability::Role;
        use crate::observability::{NodeState, RunState};
        use crate::testing::EventRecorder;

        // A relay reports its children's role and lifecycle up, so the top
        // coordinator's run state contains the deep leaves at their full paths.
        let children = vec![
            LocalAgent::new("leaf-a", Registry::new().with("double", handler(double))),
            LocalAgent::new("leaf-b", Registry::new().with("double", handler(double))),
        ];
        let (coord_side, relay_side) = connection_pair(4096);
        let relay_fut = relay(relay_side, children);
        let recorder = EventRecorder::default();
        let coord_fut = run_job::<_, u32, u32>(
            vec![("relay".to_string(), coord_side)],
            "double",
            (0..12u32).collect(),
            &recorder,
        );
        let (relay_res, out) = tokio::join!(relay_fut, coord_fut);
        relay_res.unwrap();
        assert_eq!(out.unwrap().len(), 12);

        let mut state = RunState::default();
        for event in &recorder.events() {
            state.apply(event);
        }
        // The relay is the coordinator's direct child; its two leaves appear one
        // level deeper, profiled as Compute and finished.
        assert!(state.nodes.contains_key("relay"));
        for leaf in ["relay/leaf-a", "relay/leaf-b"] {
            assert_eq!(state.nodes[leaf].role, Some(Role::Compute), "{leaf}");
            assert_eq!(state.nodes[leaf].state, NodeState::Done, "{leaf}");
        }
    }

    #[tokio::test]
    async fn a_relay_whose_child_is_itself_a_relay_runs_to_depth_three() {
        // coordinator -> relay -> sub-relay -> two leaves. The middle relay sees
        // one child that is itself a relay; capacity and results pass through
        // both hops transparently.
        let children = vec![RelayAgent {
            leaves: vec![
                (
                    "leaf-a".to_string(),
                    Registry::new().with("double", handler(double)),
                ),
                (
                    "leaf-b".to_string(),
                    Registry::new().with("double", handler(double)),
                ),
            ],
        }];
        let out = through_relay(children, "double", (0..24u32).collect())
            .await
            .unwrap();
        assert_eq!(out, (0..24u32).map(|x| Ok(x * 2)).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn a_relay_runs_no_tasks_itself_leaves_run_them_all() {
        // Each leaf counts the tasks it ran; the relay has no handler of its own,
        // so the two leaf counts must account for every task.
        let count_a = Arc::new(AtomicUsize::new(0));
        let count_b = Arc::new(AtomicUsize::new(0));
        let (ca, cb) = (count_a.clone(), count_b.clone());
        let children = vec![
            LocalAgent::new(
                "leaf-a",
                Registry::new().with(
                    "id",
                    handler(move |x: u32| {
                        ca.fetch_add(1, Ordering::SeqCst);
                        x
                    }),
                ),
            ),
            LocalAgent::new(
                "leaf-b",
                Registry::new().with(
                    "id",
                    handler(move |x: u32| {
                        cb.fetch_add(1, Ordering::SeqCst);
                        x
                    }),
                ),
            ),
        ];
        let out = through_relay(children, "id", (0..30u32).collect())
            .await
            .unwrap();
        assert_eq!(out, (0..30u32).map(Ok).collect::<Vec<_>>());
        assert_eq!(
            count_a.load(Ordering::SeqCst) + count_b.load(Ordering::SeqCst),
            30
        );
    }

    #[tokio::test]
    async fn demand_pull_keeps_the_subtree_busy() {
        // Two leaves share a concurrency gauge; with the relay advertising both
        // slots the coordinator fills them, so both run at once (high-water 2).
        let current = Arc::new(AtomicUsize::new(0));
        let high_water = Arc::new(AtomicUsize::new(0));
        let gauge = |cur: Arc<AtomicUsize>, hw: Arc<AtomicUsize>| {
            handler(move |x: u32| {
                let now = cur.fetch_add(1, Ordering::SeqCst) + 1;
                hw.fetch_max(now, Ordering::SeqCst);
                std::thread::sleep(std::time::Duration::from_millis(5));
                cur.fetch_sub(1, Ordering::SeqCst);
                x
            })
        };
        let children = vec![
            LocalAgent::new(
                "leaf-a",
                Registry::new().with("id", gauge(current.clone(), high_water.clone())),
            ),
            LocalAgent::new(
                "leaf-b",
                Registry::new().with("id", gauge(current.clone(), high_water.clone())),
            ),
        ];
        let out = through_relay(children, "id", (0..16u32).collect())
            .await
            .unwrap();
        assert_eq!(out.len(), 16);
        assert!(
            high_water.load(Ordering::SeqCst) >= 2,
            "the relay should keep more than one leaf busy at once"
        );
    }

    #[tokio::test]
    async fn a_panicking_leaf_failure_is_forwarded_up() {
        fn boom(x: u32) -> u32 {
            assert!(x.is_multiple_of(2), "odd input");
            x
        }
        let children = vec![LocalAgent::new(
            "leaf",
            Registry::new().with("boom", handler(boom)),
        )];
        let out = through_relay(children, "boom", (0..4u32).collect())
            .await
            .unwrap();
        assert_eq!(out[0], Ok(0));
        assert!(out[1].as_ref().unwrap_err().contains("odd input"));
        assert_eq!(out[2], Ok(2));
        assert!(out[3].as_ref().unwrap_err().contains("odd input"));
    }

    #[tokio::test]
    async fn a_lost_child_has_its_work_absorbed_by_a_sibling() {
        // One child is severed mid-run, the other stays healthy: every task
        // still completes once, in order, the survivor absorbing the orphans.
        let children = vec![
            FaultyChild {
                label: "flaky".to_string(),
                registry: Registry::new().with("id", handler(|x: u32| x)),
                cut_after: 50,
            },
            FaultyChild {
                label: "healthy".to_string(),
                registry: Registry::new().with("id", handler(|x: u32| x)),
                cut_after: usize::MAX,
            },
        ];
        let out = through_relay(children, "id", (0..30u32).collect())
            .await
            .unwrap();
        assert_eq!(out, (0..30u32).map(Ok).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn a_relay_whose_only_child_dies_fails_its_subtree() {
        // With no surviving child the relay tears down, so the top coordinator
        // (which has only this relay) sees the subtree lost and the run errors.
        let children = vec![FaultyChild {
            label: "flaky".to_string(),
            registry: Registry::new().with("id", handler(|x: u32| x)),
            cut_after: 50,
        }];
        let err = through_relay(children, "id", (0..30u32).collect())
            .await
            .unwrap_err();
        assert!(!err.to_string().is_empty());
    }

    #[tokio::test]
    async fn a_relay_with_no_usable_children_errors() {
        let (coord_side, relay_side) = connection_pair(256);
        let relay_fut = relay::<_, LocalAgent>(relay_side, vec![]);
        let driver = async {
            let (mut tx, _rx) = coord_side.split();
            tx.send(&ToAgent::Hello {
                protocol_version: PROTOCOL_VERSION,
                fn_key: "id".to_string(),
            })
            .await
            .unwrap();
        };
        let (res, ()) = tokio::join!(relay_fut, driver);
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn a_second_hello_from_the_parent_is_rejected() {
        let children = vec![LocalAgent::new(
            "leaf",
            Registry::new().with("id", handler(|x: u32| x)),
        )];
        let (coord_side, relay_side) = connection_pair(256);
        let relay_fut = relay(relay_side, children);
        let driver = async {
            let (mut tx, mut rx) = coord_side.split();
            tx.send(&ToAgent::Hello {
                protocol_version: PROTOCOL_VERSION,
                fn_key: "id".to_string(),
            })
            .await
            .unwrap();
            // Wait for the relay to advertise, then send a second Hello mid-run.
            let ready: FromAgent = rx.recv().await.unwrap().unwrap();
            assert!(matches!(ready, FromAgent::Ready { .. }));
            tx.send(&ToAgent::Hello {
                protocol_version: PROTOCOL_VERSION,
                fn_key: "id".to_string(),
            })
            .await
            .unwrap();
            // Drain until the relay rejects the second Hello and closes, keeping
            // the parent alive so it does not end the relay cleanly first.
            while let Ok(Some(_)) = rx.recv::<FromAgent>().await {}
        };
        let (res, ()) = tokio::join!(relay_fut, driver);
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn on_terminal_dedups_a_repeated_task() {
        // A second terminal for the same task is unknown and not forwarded, so a
        // duplicate completion cannot double-count (mirrors the coordinator).
        let (client, _server) = connection_pair(64);
        let (tx, _rx) = client.split();
        let (uplink_tx, _uplink_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut sched = super::Relay::new(vec![tx], vec!["leaf".to_string()], vec![1], uplink_tx);
        sched.load[0] = 1;
        sched.inflight.insert(7, (0, vec![1, 2, 3]));
        assert!(sched.on_terminal(7).is_some(), "first terminal is known");
        assert!(sched.on_terminal(7).is_none(), "the duplicate is unknown");
        assert_eq!(sched.load[0], 0);
    }

    #[tokio::test]
    async fn a_child_that_readies_twice_is_rejected() {
        let (coord_side, relay_side) = connection_pair(256);
        let relay_fut = relay(relay_side, vec![DoubleReadyChild]);
        let driver = async {
            let (mut tx, mut rx) = coord_side.split();
            tx.send(&ToAgent::Hello {
                protocol_version: PROTOCOL_VERSION,
                fn_key: "id".to_string(),
            })
            .await
            .unwrap();
            // Keep the parent alive and drain (Ready, Observe, ...) until the
            // relay errors on the child's second Ready and closes, so the parent
            // does not drop first and end the relay cleanly.
            while let Ok(Some(_)) = rx.recv::<FromAgent>().await {}
        };
        let (res, ()) = tokio::join!(relay_fut, driver);
        assert!(res.is_err());
    }
}
