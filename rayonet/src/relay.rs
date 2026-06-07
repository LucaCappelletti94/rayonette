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
use crate::coordinator::{
    connect_agents, handshake_join, spawn_reader, ActivationPolicy, Agents, Event as ChildEvent,
};
use crate::fleet::{launch_all, Launch, Launched};
use crate::framing::{Connection, Receiver, Sender};
use crate::observability::{Event, EventSink, NodeState};
use crate::protocol::{ChildAd, FromAgent, TaskId, ToAgent};

/// How often a relay re-reads its children file to pick up newly listed children
/// (R6 elastic membership, one level below the coordinator's rejoin). Reading a
/// small local file is cheap, so a tight interval is fine.
const RESCAN_INTERVAL: std::time::Duration = std::time::Duration::from_millis(200);

/// A source of children a relay can gain after it started.
///
/// Re-reading its children file yields launchers for entries not yet present,
/// which the relay launches and splices in. The file-backed source lives in
/// `crate::node`; tests supply their own. A relay with a fixed child set uses
/// [`NoChildSource`].
pub(crate) trait ChildSource<L: Launch>: Send {
    /// Return launchers for children that should join now and are not among
    /// `present` (the labels already in the subtree). Cheap and synchronous: the
    /// relay does the slow launch and handshake.
    fn poll(&mut self, present: &[String]) -> Vec<L>;
}

/// The default source for a relay with a fixed child set: it never grows.
pub(crate) struct NoChildSource;

impl<L: Launch> ChildSource<L> for NoChildSource {
    fn poll(&mut self, _present: &[String]) -> Vec<L> {
        Vec::new()
    }
}

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
    /// Whether each child is in the active set. A standby child is built and
    /// connected but takes no tasks until the coordinator promotes it.
    active: Vec<bool>,
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
        active: Vec<bool>,
        uplink: mpsc::UnboundedSender<Event>,
    ) -> Self {
        let n = senders.len();
        Self {
            senders,
            labels,
            capacity,
            active,
            alive: vec![true; n],
            load: vec![0; n],
            pending: VecDeque::new(),
            inflight: HashMap::new(),
            uplink,
        }
    }

    /// The relay's advertised capacity: the slots of its active children only, so
    /// a standby child is not counted until it is promoted.
    fn active_capacity(&self) -> usize {
        (0..self.senders.len())
            .filter(|&c| self.active[c])
            .map(|c| self.capacity[c])
            .sum()
    }

    /// Bring the standby child labelled `child` into the active set on reroute,
    /// returning its index, or `None` if there is no such standby child.
    fn promote(&mut self, child: &str) -> Option<usize> {
        let index = self.labels.iter().position(|label| label == child)?;
        if self.active[index] {
            return None;
        }
        self.active[index] = true;
        Some(index)
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
            let Some(child) = (0..self.senders.len())
                .find(|&c| self.active[c] && self.alive[c] && self.load[c] < self.capacity[c])
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

    /// Whether any child is still up to take work. The relay holds its child-event
    /// sender open (to splice late children in), so it cannot rely on the channel
    /// closing to learn the subtree is gone; it counts live children instead.
    fn any_alive_child(&self) -> bool {
        self.alive.iter().any(|alive| *alive)
    }

    /// Handle one event from a child, forwarding its task lifecycle and
    /// observability up to `parent_tx` and keeping the schedule fed. Returns
    /// `false` when the subtree is gone and the relay should tear down.
    async fn handle_child<P>(
        &mut self,
        parent_tx: &mut Sender<P>,
        event: ChildEvent,
    ) -> io::Result<bool>
    where
        P: AsyncRead + AsyncWrite + Unpin,
    {
        match event {
            ChildEvent::Message(_, FromAgent::Started { task_id }) => {
                parent_tx.send(&FromAgent::Started { task_id }).await?;
            }
            ChildEvent::Message(_, FromAgent::Completed { task_id, output }) => {
                if let Some(child) = self.on_terminal(task_id) {
                    parent_tx
                        .send(&FromAgent::Completed { task_id, output })
                        .await?;
                    self.dispatch().await?;
                    if self.load[child] == 0 {
                        self.report(child, NodeState::Idle);
                    }
                }
            }
            ChildEvent::Message(_, FromAgent::Failed { task_id, error }) => {
                if let Some(child) = self.on_terminal(task_id) {
                    parent_tx
                        .send(&FromAgent::Failed { task_id, error })
                        .await?;
                    self.dispatch().await?;
                    if self.load[child] == 0 {
                        self.report(child, NodeState::Idle);
                    }
                }
            }
            // A grandchild's observability event: prefix its host with this
            // child's label so it carries a path, then pass it further up.
            ChildEvent::Message(child, FromAgent::Observe(mut event)) => {
                event.prefix_host(&self.labels[child]);
                parent_tx.send(&FromAgent::Observe(event)).await?;
            }
            // A grandchild relay promoted a standby of its own, so its capacity
            // grew: fold it into this child's and report the larger subtree up.
            ChildEvent::Message(child, FromAgent::Capacity { slots }) => {
                self.capacity[child] = slots;
                parent_tx
                    .send(&FromAgent::Capacity {
                        slots: self.active_capacity(),
                    })
                    .await?;
                self.dispatch().await?;
            }
            // A child readies or describes itself only during the handshake in
            // `connect_agents`, never again on the live channel.
            ChildEvent::Message(_, FromAgent::Ready { .. } | FromAgent::Discovered { .. }) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "a child sent a handshake message on the live channel",
                ));
            }
            // A dropped child: requeue its in-flight tasks onto the survivors. With
            // no child left the subtree is gone, so tear down and let the parent
            // requeue (unless a re-read could bring one back; that is the source's
            // job while a child is still alive, not after the subtree has died).
            ChildEvent::Lost(child) => {
                self.on_lost(child);
                self.dispatch().await?;
                if !self.any_alive_child() {
                    return Ok(false);
                }
            }
        }
        Ok(true)
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

/// Shut freshly-built children down and await their readers, for the path where
/// the parent leaves before naming an active set (so awaiting a reader whose
/// child is still serving cannot hang).
async fn discard_children<S, G>(
    senders: Vec<Sender<S>>,
    readers: Vec<tokio::task::JoinHandle<()>>,
    guards: Vec<G>,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
    for mut sender in senders {
        let _ = sender.send(&ToAgent::Shutdown).await;
    }
    for reader in readers {
        let _ = reader.await;
    }
    drop(guards);
}

/// Announce the relay's built children to the parent and await the active-set it
/// names, returning whether each child is active (parallel to `labels`), or
/// `None` if the parent left before naming one.
async fn announce_children<P>(
    parent_tx: &mut Sender<P>,
    parent_rx: &mut Receiver<P>,
    labels: &[String],
    ids: &[String],
    capacity: &[usize],
    latencies: &[u64],
) -> io::Result<Option<Vec<bool>>>
where
    P: AsyncRead + AsyncWrite + Unpin,
{
    let children: Vec<ChildAd> = (0..labels.len())
        .map(|child| ChildAd {
            label: labels[child].clone(),
            id: ids[child].clone(),
            slots: capacity[child],
            latency_us: latencies.get(child).copied().unwrap_or(0),
        })
        .collect();
    parent_tx.send(&FromAgent::Discovered { children }).await?;
    let active = match parent_rx.recv::<ToAgent>().await? {
        Some(ToAgent::Activate { active }) => active,
        Some(ToAgent::Shutdown) | None => return Ok(None),
        Some(other) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("expected Activate, got {other:?}"),
            ));
        }
    };
    Ok(Some(
        labels.iter().map(|label| active.contains(label)).collect(),
    ))
}

/// Run as a relay over `parent`, coordinating a fixed set of `children`.
///
/// A relay is an agent to its parent and a coordinator to its children: it
/// handshakes the parent (learning the job's `fn_key`), launches and handshakes
/// the children, reports them up by id, runs the active set the coordinator names
/// and holds the rest as standbys, then forwards work down and
/// `Started`/`Completed`/`Failed` straight back up (task ids pass through intact)
/// until the parent sends `Shutdown` or the connection closes. On reroute a
/// `Promote` brings a standby child into the active set. A child that drops
/// mid-run has its in-flight tasks requeued onto the survivors. With none left
/// the relay tears down so the parent requeues the whole subtree.
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
    relay_with_source(parent, children, NoChildSource).await
}

/// Run as a relay that can also gain children after it started (R6 elastic
/// membership).
///
/// On a backoff it polls `source` for children added to its file and splices each
/// new one in, advertising the larger capacity up. Otherwise it is [`relay`]. The
/// relay holds its child-event sender open so it can spawn a reader for a late
/// child, so it tears down on the count of live children reaching zero rather than
/// on that channel closing.
///
/// # Errors
/// Returns an error on a protocol violation or a transport failure on either side.
#[allow(clippy::too_many_lines)] // the splice is inlined to avoid an uncovered per-stream helper
pub(crate) async fn relay_with_source<P, L, C>(
    parent: Connection<P>,
    children: Vec<L>,
    mut source: C,
) -> io::Result<()>
where
    P: AsyncRead + AsyncWrite + Unpin + Send,
    L: Launch + Send + Sync,
    C: ChildSource<L>,
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

    // Launch and handshake the children, spawning one reader each. A relay has no
    // global view, so it activates all its own children (the coordinator dedups
    // across subtrees).
    let Launched {
        agents,
        ids,
        latencies,
        mut guards,
        failures,
    } = launch_all(&children, None, None, &uplink_sink).await;
    let (child_events_tx, mut child_events_rx) = mpsc::unbounded_channel();
    let Agents {
        senders,
        labels,
        capacity,
        mut readers,
        ..
    } = connect_agents(
        agents,
        &fn_key,
        &child_events_tx,
        &latencies,
        false,
        &ActivationPolicy::ApproveAll,
    )
    .await?;

    // A relay whose children all failed to launch has nothing to offer. It errors
    // (naming why each child dropped) rather than reporting an empty subtree, so
    // the parent treats the dead subtree as a lost host.
    if labels.is_empty() {
        return Err(no_usable_children(&failures));
    }

    // Report the built children up by id so the coordinator can dedup redundant
    // paths, then run the children it names and hold the rest as standbys. If the
    // parent leaves before naming an active set there is nothing to run.
    let Some(active) = announce_children(
        &mut parent_tx,
        &mut parent_rx,
        &labels,
        &ids,
        &capacity,
        &latencies,
    )
    .await?
    else {
        discard_children(senders, readers, guards).await;
        return Ok(());
    };

    // Splice the two sides until the parent is done or the subtree dies.
    let mut sched = Relay::new(senders, labels, capacity, active, uplink_tx);
    parent_tx
        .send(&FromAgent::Ready {
            slots: sched.active_capacity(),
        })
        .await?;
    let mut rescan = tokio::time::interval(RESCAN_INTERVAL);
    loop {
        tokio::select! {
            from_parent = parent_rx.recv::<ToAgent>() => match from_parent? {
                Some(ToAgent::Assign { task_id, payload }) => {
                    sched.pending.push_back((task_id, payload));
                    sched.dispatch().await?;
                }
                // Promote a standby child on reroute: bring it into the active
                // set, report the larger capacity up, and feed it pending work.
                Some(ToAgent::Promote { child }) => {
                    if let Some(child) = sched.promote(&child) {
                        sched.report(child, NodeState::Idle);
                        parent_tx
                            .send(&FromAgent::Capacity {
                                slots: sched.active_capacity(),
                            })
                            .await?;
                        sched.dispatch().await?;
                    }
                }
                // A clean Shutdown or a dropped parent both end the relay.
                Some(ToAgent::Shutdown) | None => break,
                Some(other @ (ToAgent::Hello { .. } | ToAgent::Activate { .. })) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unexpected message from parent: {other:?}"),
                    ));
                }
            },
            from_child = child_events_rx.recv() => {
                let event = from_child
                    .expect("the relay holds a child events sender for the loop's lifetime");
                if !sched.handle_child(&mut parent_tx, event).await? {
                    break;
                }
            },
            // Forward this subtree's own observability up to the parent.
            Some(event) = uplink_rx.recv() => {
                parent_tx.send(&FromAgent::Observe(event)).await?;
            }
            // Re-read the children file: launch, handshake, and splice in any child
            // that has been added (a new index appended to every per-child vector),
            // then advertise the larger capacity up and feed it pending work.
            _ = rescan.tick() => {
                for launcher in source.poll(&sched.labels) {
                    let Launched { agents, guards: new_guards, .. } =
                        launch_all(std::slice::from_ref(&launcher), None, None, &uplink_sink).await;
                    guards.extend(new_guards);
                    for (label, conn) in agents {
                        let Ok(joiner) = handshake_join(label, conn, &fn_key).await else {
                            continue;
                        };
                        let child = sched.senders.len();
                        sched.senders.push(joiner.tx);
                        sched.labels.push(joiner.label);
                        sched.capacity.push(joiner.capacity);
                        sched.active.push(true);
                        sched.alive.push(true);
                        sched.load.push(0);
                        readers.push(spawn_reader(joiner.rx, child, child_events_tx.clone()));
                        sched.report(child, NodeState::Idle);
                        parent_tx
                            .send(&FromAgent::Capacity {
                                slots: sched.active_capacity(),
                            })
                            .await?;
                        sched.dispatch().await?;
                    }
                }
            }
        }
    }

    // Release the held child-event sender so the reader joins below can complete,
    // then shut the live children down and finish the tree view.
    drop(child_events_tx);
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

    /// A child that is itself a relay (over its own leaves) whose parent-facing
    /// reads are severed after `cut_after` bytes, modelling an interior relay
    /// that is killed mid-run (line3: coordinator -> relay1 -> relay2 -> leaf,
    /// kill relay2). The parent of the severed relay is itself a relay.
    struct FaultyRelayChild {
        leaves: Vec<(String, Registry)>,
        cut_after: usize,
    }

    impl Launch for FaultyRelayChild {
        type Stream = FaultInjector<DuplexStream>;
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
        ) -> std::io::Result<(Connection<FaultInjector<DuplexStream>>, Self::Guard)> {
            let (client_raw, server_raw) = duplex(4096);
            let client =
                Connection::new(FaultInjector::cut_reads_after(client_raw, self.cut_after));
            let leaves: Vec<LocalAgent> = self
                .leaves
                .iter()
                .map(|(label, registry)| LocalAgent::new(label, registry.clone()))
                .collect();
            let task =
                tokio::spawn(async move { relay(Connection::new(server_raw), leaves).await });
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

    /// A leaf whose handler sleeps briefly, so tasks are in flight when a relay
    /// is severed and the reroute path is genuinely exercised.
    fn slow_id(x: u32) -> u32 {
        std::thread::sleep(std::time::Duration::from_millis(2));
        x
    }

    #[tokio::test]
    async fn a_diamond_reroutes_through_the_standby_when_the_primary_relay_dies() {
        // Two relays front a leaf with the same node id "shared", so the
        // coordinator runs it on the primary (A) and holds the standby (B) idle.
        // A's coordinator-facing reads are severed mid-run, and the standby is
        // then promoted and finishes every task once.
        let reg = || Registry::new().with("id", handler(slow_id));

        // The primary relay: its reads from the coordinator are cut after the
        // handshake, simulating the relay dropping mid-run.
        let (cut_here, primary_side) = duplex(4096);
        let primary_link = Connection::new(FaultInjector::cut_reads_after(cut_here, 120));
        let primary = tokio::spawn(relay(
            Connection::new(primary_side),
            vec![LocalAgent::new("shared", reg())],
        ));

        // The standby relay: healthy, ready to take over.
        let (whole, standby_side) = duplex(4096);
        let standby_link = Connection::new(FaultInjector::cut_reads_after(whole, usize::MAX));
        let standby = tokio::spawn(relay(
            Connection::new(standby_side),
            vec![LocalAgent::new("shared", reg())],
        ));

        let agents = vec![
            ("A".to_string(), primary_link),
            ("B".to_string(), standby_link),
        ];
        // A advertises the faster link, so the coordinator deterministically makes
        // A the primary path to the shared leaf and holds B as the standby. When
        // A's reads are cut mid-run it drops, and the coordinator promotes B, whose
        // relay then activates the shared leaf and finishes the run.
        let payloads =
            crate::coordinator::serialize_inputs(&(0..30u32).collect::<Vec<_>>()).unwrap();
        let raw = crate::coordinator::run_job_raw(
            agents,
            "id",
            payloads,
            &[0, 1000],
            crate::coordinator::RunOptions::default(),
            &NoopSink,
        )
        .await
        .unwrap();
        let out: Vec<Result<u32, String>> = raw
            .into_iter()
            .map(crate::coordinator::decode_output::<u32>)
            .collect();

        assert_eq!(out, (0..30u32).map(Ok).collect::<Vec<_>>());
        let _ = primary.await; // The primary errors once its coordinator read is cut.
        let _ = standby.await;
    }

    #[tokio::test]
    async fn an_interior_relay_killed_mid_run_tears_down_its_whole_chain() {
        // The line3 shape: coordinator -> relay1 -> relay2 -> leaf, then relay2
        // (an interior relay, whose parent is itself a relay) is severed mid-run.
        // relay1 must notice its only child is gone and tear down so the top
        // coordinator sees the subtree lost and the run errors, rather than the
        // chain hanging forever. tokio::time::timeout fails the test on a hang.
        let children = vec![FaultyRelayChild {
            leaves: vec![(
                "leaf".to_string(),
                Registry::new().with("id", handler(slow_id)),
            )],
            cut_after: 120,
        }];
        let run = through_relay(children, "id", (0..30u32).collect());
        let result = tokio::time::timeout(std::time::Duration::from_secs(10), run)
            .await
            .expect("the chain hung instead of tearing down after the interior relay died");
        assert!(
            result.is_err(),
            "stranding the leaf behind a dead interior relay must fail the run"
        );
    }

    #[tokio::test]
    async fn a_relay_absorbs_a_child_added_mid_run() {
        use super::{relay_with_source, ChildSource};
        use crate::observability::{Event, NodeState, RunState};
        use crate::testing::EventRecorder;

        fn slow(x: u32) -> u32 {
            std::thread::sleep(std::time::Duration::from_millis(10));
            x
        }

        // A source that yields a second child after the first poll, modelling a
        // children file that gains an entry while the relay is running.
        struct GrowOnce {
            polls: usize,
            extra: Option<(String, Registry)>,
        }
        impl ChildSource<LocalAgent> for GrowOnce {
            fn poll(&mut self, present: &[String]) -> Vec<LocalAgent> {
                self.polls += 1;
                if self.polls <= 1 {
                    return Vec::new(); // not listed at first
                }
                match self.extra.take() {
                    Some((label, registry)) if !present.contains(&label) => {
                        vec![LocalAgent::new(&label, registry)]
                    }
                    other => {
                        self.extra = other;
                        Vec::new()
                    }
                }
            }
        }

        let children = vec![LocalAgent::new(
            "leaf-a",
            Registry::new().with("id", handler(slow)),
        )];
        let source = GrowOnce {
            polls: 0,
            extra: Some((
                "leaf-b".to_string(),
                Registry::new().with("id", handler(slow)),
            )),
        };

        let (coord_side, relay_side) = connection_pair(4096);
        let relay_fut = relay_with_source(relay_side, children, source);
        let recorder = EventRecorder::default();
        let coord_fut = run_job::<_, u32, u32>(
            vec![("relay".to_string(), coord_side)],
            "id",
            (0..40u32).collect(),
            &recorder,
        );
        let (relay_res, out) = tokio::join!(relay_fut, coord_fut);
        relay_res.unwrap();
        assert_eq!(out.unwrap(), (0..40u32).map(Ok).collect::<Vec<_>>());

        let mut state = RunState::default();
        for event in &recorder.events() {
            state.apply(event);
        }
        assert!(
            state.nodes.contains_key("relay/leaf-b"),
            "the added child joined the subtree"
        );
        assert!(
            recorder.events().iter().any(|event| matches!(
                event,
                Event::Node { host, state } if host == "relay/leaf-b" && *state == NodeState::Working
            )),
            "the added child ran at least one task"
        );
    }

    #[tokio::test]
    async fn a_relay_whose_parent_leaves_during_the_handshake_exits_cleanly() {
        // The parent greets the relay, takes its Discovered, then shuts down
        // before naming an active set: the relay tears down without erroring.
        let children = vec![LocalAgent::new(
            "leaf",
            Registry::new().with("id", handler(|x: u32| x)),
        )];
        let (coord, relay_side) = connection_pair(256);
        let relay_fut = relay(relay_side, children);
        let driver = async {
            let (mut tx, mut rx) = coord.split();
            tx.send(&ToAgent::Hello {
                protocol_version: PROTOCOL_VERSION,
                fn_key: "id".to_string(),
            })
            .await
            .unwrap();
            let discovered: FromAgent = rx.recv().await.unwrap().unwrap();
            assert!(matches!(discovered, FromAgent::Discovered { .. }));
            tx.send(&ToAgent::Shutdown).await.unwrap();
        };
        let (res, ()) = tokio::join!(relay_fut, driver);
        res.unwrap();
    }

    #[tokio::test]
    async fn a_relay_rejects_a_non_activate_reply_to_discovery() {
        // After Discovered the relay expects Activate; anything else is rejected.
        let children = vec![LocalAgent::new(
            "leaf",
            Registry::new().with("id", handler(|x: u32| x)),
        )];
        let (coord, relay_side) = connection_pair(256);
        let relay_fut = relay(relay_side, children);
        let driver = async {
            let (mut tx, mut rx) = coord.split();
            tx.send(&ToAgent::Hello {
                protocol_version: PROTOCOL_VERSION,
                fn_key: "id".to_string(),
            })
            .await
            .unwrap();
            let _discovered: FromAgent = rx.recv().await.unwrap().unwrap();
            tx.send(&ToAgent::Assign {
                task_id: 0,
                payload: Vec::new(),
            })
            .await
            .unwrap();
            while let Ok(Some(_)) = rx.recv::<FromAgent>().await {}
        };
        let (res, ()) = tokio::join!(relay_fut, driver);
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn require_redundancy_refuses_compute_behind_a_lone_relay() {
        use crate::coordinator::{run_job_raw, serialize_inputs, RunOptions};
        // One relay with a single leaf has no redundant path, so a run that
        // requires redundancy refuses before any task is scheduled.
        let (coord, relay_side) = connection_pair(4096);
        let relay_task = tokio::spawn(relay(
            relay_side,
            vec![LocalAgent::new(
                "only",
                Registry::new().with("id", handler(|x: u32| x)),
            )],
        ));
        let payloads = serialize_inputs(&(0..4u32).collect::<Vec<_>>()).unwrap();
        let err = run_job_raw(
            vec![("A".to_string(), coord)],
            "id",
            payloads,
            &[],
            RunOptions {
                require_redundancy: true,
                speculative: false,
            },
            &NoopSink,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("require_redundancy"), "{err}");
        let _ = relay_task.await;
    }

    #[tokio::test]
    async fn require_redundancy_admits_a_node_reached_through_two_relays() {
        use crate::coordinator::{run_job_raw, serialize_inputs, RunOptions};
        // Two relays reach a leaf with the same id, so the leaf is redundant and a
        // redundancy-required run proceeds, scheduling on its primary path.
        let reg = || Registry::new().with("id", handler(|x: u32| x));
        let (coord_p, primary_side) = connection_pair(4096);
        let primary = tokio::spawn(relay(primary_side, vec![LocalAgent::new("shared", reg())]));
        let (coord_s, standby_side) = connection_pair(4096);
        let standby = tokio::spawn(relay(standby_side, vec![LocalAgent::new("shared", reg())]));

        let payloads = serialize_inputs(&(0..6u32).collect::<Vec<_>>()).unwrap();
        let raw = run_job_raw(
            vec![("A".to_string(), coord_p), ("B".to_string(), coord_s)],
            "id",
            payloads,
            &[],
            RunOptions {
                require_redundancy: true,
                speculative: false,
            },
            &NoopSink,
        )
        .await
        .unwrap();
        assert_eq!(raw.len(), 6);
        assert!(raw.iter().all(Result::is_ok));
        let _ = primary.await;
        let _ = standby.await;
    }

    #[tokio::test]
    async fn work_stranded_behind_a_dead_lone_relay_fails() {
        // A single relay with no redundant path is an articulation point: severing
        // it strands its leaf with no alternate, so the run surfaces an error.
        let (coord, relay_side) = duplex(4096);
        let client = Connection::new(FaultInjector::cut_reads_after(coord, 120));
        let relay_task = tokio::spawn(relay(
            Connection::new(relay_side),
            vec![LocalAgent::new(
                "only",
                Registry::new().with("id", handler(slow_id)),
            )],
        ));
        let agents = vec![("A".to_string(), client)];
        let err = run_job::<_, u32, u32>(agents, "id", (0..30u32).collect(), &NoopSink)
            .await
            .unwrap_err();
        assert!(!err.to_string().is_empty());
        let _ = relay_task.await;
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
            // The relay describes its children, so activate them all to ready it.
            let discovered: FromAgent = rx.recv().await.unwrap().unwrap();
            let FromAgent::Discovered { children } = discovered else {
                panic!("expected Discovered, got {discovered:?}");
            };
            tx.send(&ToAgent::Activate {
                active: children.into_iter().map(|child| child.label).collect(),
            })
            .await
            .unwrap();
            let ready: FromAgent = rx.recv().await.unwrap().unwrap();
            assert!(matches!(ready, FromAgent::Ready { .. }));
            // Now send a second Hello mid-run, which the relay must reject.
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
        let mut sched = super::Relay::new(
            vec![tx],
            vec!["leaf".to_string()],
            vec![1],
            vec![true],
            uplink_tx,
        );
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
            // Activate the child so the relay enters its loop and processes the
            // child's bogus second Ready, then drain until it errors and closes.
            if let Ok(Some(FromAgent::Discovered { children })) = rx.recv::<FromAgent>().await {
                tx.send(&ToAgent::Activate {
                    active: children.into_iter().map(|child| child.label).collect(),
                })
                .await
                .unwrap();
            }
            while let Ok(Some(_)) = rx.recv::<FromAgent>().await {}
        };
        let (res, ()) = tokio::join!(relay_fut, driver);
        assert!(res.is_err());
    }
}
