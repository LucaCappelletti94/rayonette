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
use crate::control::{ControlAction, KillMode};
use crate::coordinator::{
    connect_agents, handshake_join, spawn_reader, ActivationPolicy, Agents, Event as ChildEvent,
};
use crate::fleet::{launch_all, Launch, Launched};
use crate::framing::{Connection, Receiver, Sender};
use crate::observability::{join_label, Event, EventSink, NodeState};
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
    /// Whether each child is paused by an operator: connected, but handed no new
    /// work until resumed (its in-flight tasks finish).
    paused: Vec<bool>,
    /// Whether each child is draining toward a kill: handed no new work, and
    /// dropped once its in-flight tasks finish.
    draining: Vec<bool>,
    /// How many tasks each child is currently running.
    load: Vec<usize>,
    /// When each child was last heard from (any event, including a pong), on
    /// tokio's clock. The heartbeat reroutes a child silent past the timeout.
    last_activity: Vec<tokio::time::Instant>,
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
            paused: vec![false; n],
            draining: vec![false; n],
            load: vec![0; n],
            last_activity: vec![tokio::time::Instant::now(); n],
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
            let Some(child) = (0..self.senders.len()).find(|&c| {
                self.active[c]
                    && self.alive[c]
                    && !self.paused[c]
                    && !self.draining[c]
                    && self.load[c] < self.capacity[c]
            }) else {
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

    /// Kill a child: shut it down, then take the same lost path a dropped child
    /// would (requeue its work onto the survivors). Returns whether the subtree is
    /// still alive afterward (`false` means this relay should tear down).
    async fn kill_child(&mut self, child: usize) -> io::Result<bool> {
        if !self.alive[child] {
            return Ok(true);
        }
        let _ = self.senders[child].send(&ToAgent::Shutdown).await;
        self.on_lost(child);
        self.dispatch().await?;
        Ok(self.any_alive_child())
    }

    /// React to a child whose last in-flight task just finished: kill it if it was
    /// draining, leave it showing `Paused` if paused, else report it `Idle`.
    /// Returns whether the subtree is still alive (`false` means tear down).
    async fn on_child_idle(&mut self, child: usize) -> io::Result<bool> {
        if self.draining[child] {
            return self.kill_child(child).await;
        }
        if !self.paused[child] {
            self.report(child, NodeState::Idle);
        }
        Ok(true)
    }

    /// Apply an operator control routed from the parent. `target`'s first segment
    /// names a child; when the path ends there the action is applied here, else it
    /// is forwarded one hop further down. Returns whether the subtree is still
    /// alive (`false` means tear down).
    async fn control(&mut self, target: String, action: ControlAction) -> io::Result<bool> {
        let (head, rest) = match target.split_once('/') {
            Some((head, rest)) => (head, Some(rest)),
            None => (target.as_str(), None),
        };
        let Some(child) = self.labels.iter().position(|label| label == head) else {
            return Ok(true);
        };
        if !self.alive[child] {
            return Ok(true);
        }
        if let Some(rest) = rest {
            let _ = self.senders[child]
                .send(&ToAgent::Control {
                    target: rest.to_string(),
                    action,
                })
                .await;
            return Ok(true);
        }
        match action {
            ControlAction::Pause => {
                if !self.paused[child] && !self.draining[child] {
                    self.paused[child] = true;
                    self.report(child, NodeState::Paused);
                }
            }
            ControlAction::Resume => {
                if self.paused[child] {
                    self.paused[child] = false;
                    self.dispatch().await?;
                    let state = if self.load[child] > 0 {
                        NodeState::Working
                    } else {
                        NodeState::Idle
                    };
                    self.report(child, state);
                }
            }
            ControlAction::Kill { mode } => match mode {
                KillMode::Now => return self.kill_child(child).await,
                KillMode::AfterCurrent => {
                    if self.load[child] == 0 {
                        return self.kill_child(child).await;
                    } else if !self.draining[child] {
                        self.draining[child] = true;
                        self.report(child, NodeState::Draining);
                    }
                }
            },
        }
        Ok(true)
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
        // Any event is proof of life: refresh the child's activity so the heartbeat
        // does not reroute a busy or responsive child.
        if let ChildEvent::Message(child, _) = &event {
            self.last_activity[*child] = tokio::time::Instant::now();
        }
        match event {
            ChildEvent::Message(_, FromAgent::Started { task_id }) => {
                parent_tx.send(&FromAgent::Started { task_id }).await?;
            }
            ChildEvent::Message(
                _,
                FromAgent::Completed {
                    task_id,
                    output,
                    via,
                },
            ) => {
                if let Some(child) = self.on_terminal(task_id) {
                    // Prepend this child's label so the path reaches the deep leaf
                    // that ran the task, one hop longer per relay it passes.
                    let via = join_label(&self.labels[child], &via);
                    parent_tx
                        .send(&FromAgent::Completed {
                            task_id,
                            output,
                            via,
                        })
                        .await?;
                    self.dispatch().await?;
                    if self.load[child] == 0 && !self.on_child_idle(child).await? {
                        return Ok(false);
                    }
                }
            }
            ChildEvent::Message(
                _,
                FromAgent::Failed {
                    task_id,
                    error,
                    via,
                },
            ) => {
                if let Some(child) = self.on_terminal(task_id) {
                    let via = join_label(&self.labels[child], &via);
                    parent_tx
                        .send(&FromAgent::Failed {
                            task_id,
                            error,
                            via,
                        })
                        .await?;
                    self.dispatch().await?;
                    if self.load[child] == 0 && !self.on_child_idle(child).await? {
                        return Ok(false);
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
            // A child's pong: a pure liveness signal, already noted as activity.
            ChildEvent::Message(_, FromAgent::Pong) => {}
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
        .map(|child| {
            ChildAd::new(
                labels[child].clone(),
                ids[child].clone(),
                capacity[child],
                latencies.get(child).copied().unwrap_or(0),
            )
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
#[expect(
    clippy::too_many_lines,
    reason = "the splice is inlined to keep the per-stream monomorphization covered"
)]
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

    // Parent handshake: learn the job's function key and the run's heartbeat
    // cadence (a leaf does the same in `agent::serve`, sharing `recv_hello`). The
    // cadence is passed on to this relay's own children so the whole tree agrees.
    let (fn_key, heartbeat) = recv_hello(&mut parent_rx).await?;

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
        heartbeat,
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
    // The heartbeat: this relay is both child and parent. It pings its own children
    // each interval, and gives its parent up (tearing the subtree down) if it hears
    // nothing from it within the timeout, so a crashed coordinator does not strand
    // this subtree. The first tick is one interval out (a fast run sends none); a
    // disabled heartbeat uses an inert period and a guarded-off branch. The clock is
    // tokio's, so the timeout works under virtual time too.
    let heartbeat_on = heartbeat.is_enabled();
    let beat = heartbeat
        .interval()
        .max(std::time::Duration::from_millis(1));
    let mut heartbeat_tick = tokio::time::interval_at(tokio::time::Instant::now() + beat, beat);
    let mut last_parent = tokio::time::Instant::now();
    loop {
        tokio::select! {
            from_parent = parent_rx.recv::<ToAgent>() => {
                last_parent = tokio::time::Instant::now();
                match from_parent? {
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
                // An operator control: apply it to the named child or forward it
                // deeper. Tears down if killing the child empties the subtree.
                Some(ToAgent::Control { target, action }) => {
                    if !sched.control(target, action).await? {
                        break;
                    }
                }
                // A liveness probe from the parent: answer it so the parent knows
                // this relay is alive.
                Some(ToAgent::Ping) => parent_tx.send(&FromAgent::Pong).await?,
                // A clean Shutdown or a dropped parent both end the relay.
                Some(ToAgent::Shutdown) | None => break,
                Some(other @ (ToAgent::Hello { .. } | ToAgent::Activate { .. })) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unexpected message from parent: {other:?}"),
                    ));
                }
                }
            }
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
                        let Ok(joiner) = handshake_join(label, conn, &fn_key, heartbeat).await
                        else {
                            continue;
                        };
                        let child = sched.senders.len();
                        sched.senders.push(joiner.tx);
                        sched.labels.push(joiner.label);
                        sched.capacity.push(joiner.capacity);
                        sched.active.push(true);
                        sched.alive.push(true);
                        sched.paused.push(false);
                        sched.draining.push(false);
                        sched.load.push(0);
                        sched.last_activity.push(tokio::time::Instant::now());
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
            _ = heartbeat_tick.tick(), if heartbeat_on => {
                // Ping each live child so it knows this relay is alive...
                for child in 0..sched.senders.len() {
                    if sched.alive[child] {
                        let _ = sched.senders[child].send(&ToAgent::Ping).await;
                    }
                }
                // ...and give the parent up if it has gone silent, tearing this
                // subtree down (the shutdown below) rather than stranding it.
                if last_parent.elapsed() > heartbeat.timeout() {
                    break;
                }
                // Reroute any child silent past the timeout: its work moves to the
                // survivors, the same as a dropped child.
                let timeout = heartbeat.timeout();
                let stale: Vec<usize> = (0..sched.senders.len())
                    .filter(|&c| sched.alive[c] && sched.last_activity[c].elapsed() > timeout)
                    .collect();
                if !stale.is_empty() {
                    for child in stale {
                        sched.on_lost(child);
                        // A child lost to the heartbeat is half-open (no EOF), so end
                        // its reader; it would otherwise stay parked until teardown.
                        readers[child].abort();
                    }
                    sched.dispatch().await?;
                    if !sched.any_alive_child() {
                        break;
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

    /// A double slow enough that work stays in flight while a control lands.
    fn slow(x: u32) -> u32 {
        std::thread::sleep(std::time::Duration::from_millis(3));
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

    /// A leaf that is either healthy (`Some` registry, a normal `serve`) or, with
    /// `None`, completes the handshake and then goes silent: it reads and discards
    /// whatever the relay sends (so the relay's pings never block) but never
    /// answers, modelling a leaf that has hard-crashed without closing its socket.
    struct TestLeaf {
        label: String,
        registry: Option<Registry>,
    }

    impl Launch for TestLeaf {
        type Stream = DuplexStream;
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
        ) -> std::io::Result<(Connection<DuplexStream>, Self::Guard)> {
            let (client, server) = connection_pair(256);
            let task: JoinHandle<std::io::Result<()>> = match self.registry.clone() {
                Some(registry) => tokio::spawn(serve(server, registry)),
                None => tokio::spawn(async move {
                    let (mut tx, mut rx) = server.split();
                    if rx.recv::<ToAgent>().await?.is_some() {
                        tx.send(&FromAgent::Ready { slots: 1 }).await?;
                    }
                    while rx.recv::<ToAgent>().await?.is_some() {}
                    Ok(())
                }),
            };
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
        assert!(state.nodes().contains_key("relay"));
        for leaf in ["relay/leaf-a", "relay/leaf-b"] {
            assert_eq!(state.nodes()[leaf].role(), Some(Role::Compute), "{leaf}");
            assert_eq!(state.nodes()[leaf].state(), NodeState::Done, "{leaf}");
        }
        // Completions are credited to the deep leaf that ran them, not to the relay
        // the coordinator heard the result from.
        assert_eq!(
            state.nodes()["relay"].completed(),
            0,
            "the relay computes nothing"
        );
        let on_leaves =
            state.nodes()["relay/leaf-a"].completed() + state.nodes()["relay/leaf-b"].completed();
        assert_eq!(on_leaves, 12, "every completion landed on a leaf");
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
    async fn depth_three_credits_the_full_leaf_path() {
        use crate::observability::RunState;
        use crate::testing::EventRecorder;
        // coordinator -> relay -> sub-relay -> two leaves. A completion is credited
        // to the full path down to the leaf, with each relay prepending its label.
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
        let (coord_side, relay_side) = connection_pair(4096);
        let relay_fut = relay(relay_side, children);
        let recorder = EventRecorder::default();
        let coord_fut = run_job::<_, u32, u32>(
            vec![("relay".to_string(), coord_side)],
            "double",
            (0..16u32).collect(),
            &recorder,
        );
        let (relay_res, out) = tokio::join!(relay_fut, coord_fut);
        relay_res.unwrap();
        assert_eq!(out.unwrap().len(), 16);

        let mut state = RunState::default();
        for event in &recorder.events() {
            state.apply(event);
        }
        // Neither relay computes; the two deepest leaves account for every task.
        assert_eq!(state.nodes()["relay"].completed(), 0);
        assert_eq!(state.nodes()["relay/sub-relay"].completed(), 0);
        let on_leaves = state.nodes()["relay/sub-relay/leaf-a"].completed()
            + state.nodes()["relay/sub-relay/leaf-b"].completed();
        assert_eq!(on_leaves, 16, "every completion landed on a deep leaf");
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
            state.nodes().contains_key("relay/leaf-b"),
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
                heartbeat: crate::heartbeat::HeartbeatConfig::default(),
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
                heartbeat: crate::heartbeat::HeartbeatConfig::default(),
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
                heartbeat: crate::heartbeat::HeartbeatConfig::default(),
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
                heartbeat: crate::heartbeat::HeartbeatConfig::default(),
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
                heartbeat: crate::heartbeat::HeartbeatConfig::default(),
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
                heartbeat: crate::heartbeat::HeartbeatConfig::default(),
            })
            .await
            .unwrap();
            // The relay describes its children, so activate them all to ready it.
            let discovered: FromAgent = rx.recv().await.unwrap().unwrap();
            let FromAgent::Discovered { children } = discovered else {
                panic!("expected Discovered, got {discovered:?}");
            };
            tx.send(&ToAgent::Activate {
                active: children
                    .into_iter()
                    .map(|child| child.label().to_string())
                    .collect(),
            })
            .await
            .unwrap();
            let ready: FromAgent = rx.recv().await.unwrap().unwrap();
            assert!(matches!(ready, FromAgent::Ready { .. }));
            // Now send a second Hello mid-run, which the relay must reject.
            tx.send(&ToAgent::Hello {
                protocol_version: PROTOCOL_VERSION,
                fn_key: "id".to_string(),
                heartbeat: crate::heartbeat::HeartbeatConfig::default(),
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
    async fn resuming_an_idle_child_reports_it_idle_not_working() {
        // Resuming a paused child with no pending work reports it Idle (the empty
        // dispatch leaves its load at zero), not Working.
        use crate::control::ControlAction;
        use crate::observability::{Event, NodeState};
        let (client, _server) = connection_pair(64);
        let (tx, _rx) = client.split();
        let (uplink_tx, mut uplink_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut sched = super::Relay::new(
            vec![tx],
            vec!["leaf".to_string()],
            vec![1],
            vec![true],
            uplink_tx,
        );
        sched.paused[0] = true;
        let alive = sched
            .control("leaf".to_string(), ControlAction::Resume)
            .await
            .unwrap();
        assert!(alive, "resuming keeps the subtree alive");
        assert!(!sched.paused[0], "the child is no longer paused");
        let mut reported = None;
        while let Ok(Event::Node { state, .. }) = uplink_rx.try_recv() {
            reported = Some(state);
        }
        assert_eq!(reported, Some(NodeState::Idle));
    }

    #[tokio::test]
    async fn kill_after_current_on_an_idle_child_kills_it_immediately() {
        // An after-current kill on a child with nothing in flight takes effect at
        // once rather than waiting to drain.
        use crate::control::{ControlAction, KillMode};
        let (a_client, _a_server) = connection_pair(256);
        let (a_tx, _ar) = a_client.split();
        let (b_client, _b_server) = connection_pair(256);
        let (b_tx, _br) = b_client.split();
        let (uplink_tx, _uplink_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut sched = super::Relay::new(
            vec![a_tx, b_tx],
            vec!["leaf-a".to_string(), "leaf-b".to_string()],
            vec![1, 1],
            vec![true, true],
            uplink_tx,
        );
        let alive = sched
            .control(
                "leaf-a".to_string(),
                ControlAction::Kill {
                    mode: KillMode::AfterCurrent,
                },
            )
            .await
            .unwrap();
        assert!(alive, "leaf-b keeps the subtree alive");
        assert!(!sched.alive[0], "the idle leaf-a was killed immediately");
        assert!(sched.alive[1], "leaf-b is untouched");
    }

    #[tokio::test]
    async fn a_draining_childs_last_completion_tears_the_subtree_down() {
        // The last alive child, draining toward a kill, finishes its in-flight task:
        // the completion is forwarded up, then the child is dropped and, with no
        // sibling left, the relay tears down.
        let (child_client, _child_server) = connection_pair(256);
        let (child_tx, _cr) = child_client.split();
        let (parent_client, parent_server) = connection_pair(256);
        let (mut parent_tx, _pcr) = parent_client.split();
        let (_pst, mut parent_rx) = parent_server.split();
        let (uplink_tx, _uplink_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut sched = super::Relay::new(
            vec![child_tx],
            vec!["leaf".to_string()],
            vec![1],
            vec![true],
            uplink_tx,
        );
        sched.load[0] = 1;
        sched.draining[0] = true;
        sched.inflight.insert(1, (0, vec![]));
        let alive = sched
            .handle_child(
                &mut parent_tx,
                super::ChildEvent::Message(
                    0,
                    FromAgent::Completed {
                        task_id: 1,
                        output: vec![],
                        via: String::new(),
                    },
                ),
            )
            .await
            .unwrap();
        assert!(
            !alive,
            "the last draining child's completion tears the subtree down"
        );
        let forwarded = parent_rx.recv::<FromAgent>().await.unwrap();
        assert!(
            matches!(forwarded, Some(FromAgent::Completed { task_id: 1, .. })),
            "the completion is forwarded up first: {forwarded:?}"
        );
        assert!(!sched.alive[0], "the drained child is dropped");
    }

    #[tokio::test]
    async fn a_draining_childs_last_failure_tears_the_subtree_down() {
        // As above, but the draining child's final in-flight task fails rather than
        // completing: the failure is forwarded, then the subtree tears down.
        let (child_client, _child_server) = connection_pair(256);
        let (child_tx, _cr) = child_client.split();
        let (parent_client, parent_server) = connection_pair(256);
        let (mut parent_tx, _pcr) = parent_client.split();
        let (_pst, mut parent_rx) = parent_server.split();
        let (uplink_tx, _uplink_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut sched = super::Relay::new(
            vec![child_tx],
            vec!["leaf".to_string()],
            vec![1],
            vec![true],
            uplink_tx,
        );
        sched.load[0] = 1;
        sched.draining[0] = true;
        sched.inflight.insert(2, (0, vec![]));
        let alive = sched
            .handle_child(
                &mut parent_tx,
                super::ChildEvent::Message(
                    0,
                    FromAgent::Failed {
                        task_id: 2,
                        error: "boom".to_string(),
                        via: String::new(),
                    },
                ),
            )
            .await
            .unwrap();
        assert!(
            !alive,
            "the last draining child's failure tears the subtree down"
        );
        let forwarded = parent_rx.recv::<FromAgent>().await.unwrap();
        assert!(
            matches!(forwarded, Some(FromAgent::Failed { task_id: 2, .. })),
            "the failure is forwarded up first: {forwarded:?}"
        );
        assert!(!sched.alive[0], "the drained child is dropped");
    }

    #[tokio::test]
    async fn a_grandchild_capacity_growth_is_folded_and_reported_up() {
        // A child that is itself a relay grew its active capacity: the relay folds
        // the new figure into the child's slot count and reports its enlarged total
        // up to its own parent.
        let (child_client, _child_server) = connection_pair(256);
        let (child_tx, _cr) = child_client.split();
        let (parent_client, parent_server) = connection_pair(256);
        let (mut parent_tx, _pcr) = parent_client.split();
        let (_pst, mut parent_rx) = parent_server.split();
        let (uplink_tx, _uplink_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut sched = super::Relay::new(
            vec![child_tx],
            vec!["sub".to_string()],
            vec![1],
            vec![true],
            uplink_tx,
        );
        let alive = sched
            .handle_child(
                &mut parent_tx,
                super::ChildEvent::Message(0, FromAgent::Capacity { slots: 4 }),
            )
            .await
            .unwrap();
        assert!(alive, "a capacity report keeps the subtree alive");
        assert_eq!(sched.capacity[0], 4, "the child's capacity grew");
        let forwarded = parent_rx.recv::<FromAgent>().await.unwrap();
        assert!(
            matches!(forwarded, Some(FromAgent::Capacity { slots: 4 })),
            "the relay reports its enlarged active capacity up: {forwarded:?}"
        );
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
                heartbeat: crate::heartbeat::HeartbeatConfig::default(),
            })
            .await
            .unwrap();
            // Activate the child so the relay enters its loop and processes the
            // child's bogus second Ready, then drain until it errors and closes.
            if let Ok(Some(FromAgent::Discovered { children })) = rx.recv::<FromAgent>().await {
                tx.send(&ToAgent::Activate {
                    active: children
                        .into_iter()
                        .map(|child| child.label().to_string())
                        .collect(),
                })
                .await
                .unwrap();
            }
            while let Ok(Some(_)) = rx.recv::<FromAgent>().await {}
        };
        let (res, ()) = tokio::join!(relay_fut, driver);
        assert!(res.is_err());
    }

    /// Build a coordinator-side connection to a relay over two slow leaves, and the
    /// relay future, for the control tests below.
    fn coord_and_relay_over_two_leaves() -> (
        Connection<DuplexStream>,
        impl std::future::Future<Output = std::io::Result<()>>,
    ) {
        let children = vec![
            LocalAgent::new("leaf-a", Registry::new().with("slow", handler(slow))),
            LocalAgent::new("leaf-b", Registry::new().with("slow", handler(slow))),
        ];
        let (coord_side, relay_side) = connection_pair(4096);
        (coord_side, relay(relay_side, children))
    }

    #[tokio::test(start_paused = true)]
    async fn a_relay_tears_down_when_its_parent_goes_silent() {
        use crate::heartbeat::HeartbeatConfig;
        let children = vec![LocalAgent::new(
            "leaf",
            Registry::new().with("id", handler(double)),
        )];
        let (coord_side, relay_side) = connection_pair(4096);
        let relay_fut = relay(relay_side, children);
        let driver = async {
            let (mut tx, mut rx) = coord_side.split();
            tx.send(&ToAgent::Hello {
                protocol_version: PROTOCOL_VERSION,
                fn_key: "id".to_string(),
                heartbeat: HeartbeatConfig::new(
                    std::time::Duration::from_millis(50),
                    std::time::Duration::from_millis(200),
                ),
            })
            .await
            .unwrap();
            // Handshake: the relay announces its child, we activate it, it readies.
            loop {
                match rx.recv::<FromAgent>().await.ok().flatten() {
                    Some(FromAgent::Discovered { children }) => {
                        let active = children.iter().map(|c| c.label().to_string()).collect();
                        tx.send(&ToAgent::Activate { active }).await.unwrap();
                    }
                    Some(FromAgent::Ready { .. }) => break,
                    Some(_) => {}
                    None => return,
                }
            }
            // The relay answers a ping with a pong (its child-side liveness reply).
            tx.send(&ToAgent::Ping).await.unwrap();
            loop {
                match rx.recv::<FromAgent>().await.ok().flatten() {
                    Some(FromAgent::Pong) => break,
                    Some(_) => {}
                    None => return,
                }
            }
            // Now go silent (keep the connection open). The relay gives us up within
            // the timeout and tears its subtree down; drain until it closes.
            while rx.recv::<FromAgent>().await.ok().flatten().is_some() {}
        };
        let (relay_res, ()) = tokio::join!(relay_fut, driver);
        assert!(
            relay_res.is_ok(),
            "the relay tears down cleanly on parent silence: {relay_res:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_relays_healthy_leaf_absorbs_a_silent_sibling() {
        use crate::coordinator::{
            decode_output, run_job_raw_with_joins, serialize_inputs, RunOptions,
        };
        use crate::heartbeat::HeartbeatConfig;
        use crate::observability::{NodeState, RunState};
        use crate::testing::EventRecorder;
        use tokio::sync::mpsc;

        // A relay over a healthy leaf-a and a silent leaf-b. The heartbeat config
        // travels down in the Hello, so the relay pings both children; leaf-b never
        // pongs, so the relay reroutes its work to leaf-a and the run completes.
        let children = vec![
            TestLeaf {
                label: "leaf-a".to_string(),
                registry: Some(Registry::new().with("double", handler(double))),
            },
            TestLeaf {
                label: "leaf-b".to_string(),
                registry: None,
            },
        ];
        let (coord_side, relay_side) = connection_pair(4096);
        let relay_fut = relay(relay_side, children);
        let recorder = EventRecorder::default();
        let (joins_tx, joins_rx) = mpsc::unbounded_channel();
        drop(joins_tx);
        let (controls_tx, controls_rx) = mpsc::unbounded_channel();
        drop(controls_tx);
        let payloads = serialize_inputs(&(0..6u32).collect::<Vec<_>>()).unwrap();
        let options = RunOptions {
            require_redundancy: false,
            speculative: false,
            heartbeat: HeartbeatConfig::new(
                std::time::Duration::from_millis(50),
                std::time::Duration::from_millis(200),
            ),
        };
        let coord_fut = run_job_raw_with_joins(
            vec![("relay".to_string(), coord_side)],
            "double",
            payloads,
            &[],
            options,
            joins_rx,
            controls_rx,
            &recorder,
        );
        let (relay_res, raw) = tokio::join!(relay_fut, coord_fut);
        relay_res.unwrap();
        let outs: Vec<Result<u32, String>> =
            raw.unwrap().into_iter().map(decode_output::<u32>).collect();
        assert_eq!(outs, (0..6u32).map(|x| Ok(x * 2)).collect::<Vec<_>>());
        let mut state = RunState::default();
        for event in &recorder.events() {
            state.apply(event);
        }
        assert_eq!(state.nodes()["relay/leaf-b"].state(), NodeState::Lost);
        assert_eq!(state.nodes()["relay/leaf-a"].completed(), 6);
    }

    #[tokio::test]
    async fn killing_a_leaf_behind_a_relay_reroutes_and_reports_it_lost() {
        use crate::control::{Control, ControlAction, KillMode};
        use crate::coordinator::{
            decode_output, run_job_raw_with_joins, serialize_inputs, RunOptions,
        };
        use crate::observability::{NodeState, RunState};
        use crate::testing::EventRecorder;
        use tokio::sync::mpsc;

        let (coord_side, relay_fut) = coord_and_relay_over_two_leaves();
        let recorder = EventRecorder::default();
        let (joins_tx, joins_rx) = mpsc::unbounded_channel();
        drop(joins_tx);
        let (controls_tx, controls_rx) = mpsc::unbounded_channel::<Control>();
        let payloads = serialize_inputs(&(0..20u32).collect::<Vec<_>>()).unwrap();
        let coord_fut = run_job_raw_with_joins(
            vec![("relay".to_string(), coord_side)],
            "slow",
            payloads,
            &[],
            RunOptions::default(),
            joins_rx,
            controls_rx,
            &recorder,
        );
        let driver = async {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            controls_tx
                .send(Control::new(
                    "relay/leaf-a".to_string(),
                    ControlAction::Kill {
                        mode: KillMode::Now,
                    },
                ))
                .unwrap();
            // A follow-up to the now-dead leaf and an unknown child are no-ops.
            controls_tx
                .send(Control::new(
                    "relay/leaf-a".to_string(),
                    ControlAction::Resume,
                ))
                .unwrap();
            controls_tx
                .send(Control::new(
                    "relay/ghost".to_string(),
                    ControlAction::Pause,
                ))
                .unwrap();
            drop(controls_tx);
        };
        let (relay_res, raw, ()) = tokio::join!(relay_fut, coord_fut, driver);
        relay_res.unwrap();
        let outs: Vec<Result<u32, String>> =
            raw.unwrap().into_iter().map(decode_output::<u32>).collect();
        assert_eq!(outs, (0..20u32).map(|x| Ok(x * 2)).collect::<Vec<_>>());
        let mut state = RunState::default();
        for event in &recorder.events() {
            state.apply(event);
        }
        assert_eq!(state.nodes()["relay/leaf-a"].state(), NodeState::Lost);
    }

    #[tokio::test]
    async fn pausing_leaves_behind_a_relay_stalls_until_resumed() {
        use crate::control::{Control, ControlAction};
        use crate::coordinator::{run_job_raw_with_joins, serialize_inputs, RunOptions};
        use crate::observability::NodeState;
        use crate::testing::EventRecorder;
        use tokio::sync::mpsc;

        let (coord_side, relay_fut) = coord_and_relay_over_two_leaves();
        let relay_task = tokio::spawn(relay_fut);
        let recorder = EventRecorder::default();
        let (joins_tx, joins_rx) = mpsc::unbounded_channel();
        drop(joins_tx);
        let (controls_tx, controls_rx) = mpsc::unbounded_channel::<Control>();
        let payloads = serialize_inputs(&(0..10u32).collect::<Vec<_>>()).unwrap();
        let coord = run_job_raw_with_joins(
            vec![("relay".to_string(), coord_side)],
            "slow",
            payloads,
            &[],
            RunOptions::default(),
            joins_rx,
            controls_rx,
            &recorder,
        );
        tokio::pin!(coord);

        // Pause both leaves: no leaf can take work, so the run cannot finish.
        controls_tx
            .send(Control::new(
                "relay/leaf-a".to_string(),
                ControlAction::Pause,
            ))
            .unwrap();
        controls_tx
            .send(Control::new(
                "relay/leaf-b".to_string(),
                ControlAction::Pause,
            ))
            .unwrap();
        let early = tokio::time::timeout(std::time::Duration::from_millis(150), &mut coord).await;
        assert!(
            early.is_err(),
            "both leaves paused: the run cannot complete"
        );

        // Resume and let it drain.
        controls_tx
            .send(Control::new(
                "relay/leaf-a".to_string(),
                ControlAction::Resume,
            ))
            .unwrap();
        controls_tx
            .send(Control::new(
                "relay/leaf-b".to_string(),
                ControlAction::Resume,
            ))
            .unwrap();
        drop(controls_tx);
        let raw = (&mut coord).await.unwrap();
        assert_eq!(raw.len(), 10);
        relay_task.await.unwrap().unwrap();
        assert!(
            recorder.states().contains(&NodeState::Paused),
            "a leaf behind the relay was reported Paused"
        );
    }

    #[tokio::test]
    async fn kill_after_current_behind_a_relay_drains_then_loses_the_leaf() {
        use crate::control::{Control, ControlAction, KillMode};
        use crate::coordinator::{
            decode_output, run_job_raw_with_joins, serialize_inputs, RunOptions,
        };
        use crate::observability::{NodeState, RunState};
        use crate::testing::EventRecorder;
        use std::sync::atomic::AtomicBool;
        use std::sync::{Condvar, Mutex};
        use tokio::sync::mpsc;

        // leaf-a's first task blocks on this gate until the test releases it, so the
        // task is deterministically in flight when the after-current kill lands
        // (rather than racing a short task that may finish first under load).
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let entered = Arc::new(AtomicBool::new(false));
        let armed = Arc::new(AtomicBool::new(true));
        let leaf_a = {
            let gate = Arc::clone(&gate);
            let entered = Arc::clone(&entered);
            let armed = Arc::clone(&armed);
            move |x: u32| -> u32 {
                if armed.swap(false, Ordering::SeqCst) {
                    entered.store(true, Ordering::SeqCst);
                    let (lock, cvar) = &*gate;
                    let mut released = lock.lock().unwrap();
                    while !*released {
                        released = cvar.wait(released).unwrap();
                    }
                }
                x * 2
            }
        };
        let children = vec![
            LocalAgent::new("leaf-a", Registry::new().with("slow", handler(leaf_a))),
            LocalAgent::new("leaf-b", Registry::new().with("slow", handler(slow))),
        ];
        let (coord_side, relay_side) = connection_pair(4096);
        let relay_fut = relay(relay_side, children);

        let recorder = EventRecorder::default();
        let (joins_tx, joins_rx) = mpsc::unbounded_channel();
        drop(joins_tx);
        let (controls_tx, controls_rx) = mpsc::unbounded_channel::<Control>();
        let payloads = serialize_inputs(&(0..20u32).collect::<Vec<_>>()).unwrap();
        let coord_fut = run_job_raw_with_joins(
            vec![("relay".to_string(), coord_side)],
            "slow",
            payloads,
            &[],
            RunOptions::default(),
            joins_rx,
            controls_rx,
            &recorder,
        );
        let driver = async {
            // 1. Wait until leaf-a's first task is in flight (blocked on the gate).
            while !entered.load(Ordering::SeqCst) {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
            // 2. Kill it after-current: with a task in flight it must drain first.
            controls_tx
                .send(Control::new(
                    "relay/leaf-a".to_string(),
                    ControlAction::Kill {
                        mode: KillMode::AfterCurrent,
                    },
                ))
                .unwrap();
            // 3. Wait until the relay reports it Draining, proving the kill landed
            //    while the task was in flight, then release the task to finish.
            loop {
                let mut state = RunState::default();
                for event in &recorder.events() {
                    state.apply(event);
                }
                if state
                    .nodes()
                    .get("relay/leaf-a")
                    .is_some_and(|view| view.state() == NodeState::Draining)
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
            let (lock, cvar) = &*gate;
            *lock.lock().unwrap() = true;
            cvar.notify_all();
            drop(controls_tx);
        };
        let (relay_res, raw, ()) = tokio::join!(relay_fut, coord_fut, driver);
        relay_res.unwrap();
        let outs: Vec<Result<u32, String>> =
            raw.unwrap().into_iter().map(decode_output::<u32>).collect();
        assert_eq!(outs, (0..20u32).map(|x| Ok(x * 2)).collect::<Vec<_>>());
        let states = recorder.states();
        assert!(states.contains(&NodeState::Draining), "{states:?}");
        let mut state = RunState::default();
        for event in &recorder.events() {
            state.apply(event);
        }
        assert_eq!(state.nodes()["relay/leaf-a"].state(), NodeState::Lost);
    }

    #[tokio::test]
    async fn killing_a_leaf_two_relays_deep_routes_through_both() {
        use crate::control::{Control, ControlAction, KillMode};
        use crate::coordinator::{
            decode_output, run_job_raw_with_joins, serialize_inputs, RunOptions,
        };
        use crate::observability::{NodeState, RunState};
        use crate::testing::EventRecorder;
        use tokio::sync::mpsc;

        // coordinator -> relay -> sub-relay -> {leaf-x, leaf-y}. A control aimed at
        // the deep leaf is forwarded through both relays before it is applied.
        let children = vec![RelayAgent {
            leaves: vec![
                (
                    "leaf-x".to_string(),
                    Registry::new().with("slow", handler(slow)),
                ),
                (
                    "leaf-y".to_string(),
                    Registry::new().with("slow", handler(slow)),
                ),
            ],
        }];
        let (coord_side, relay_side) = connection_pair(4096);
        let relay_fut = relay(relay_side, children);
        let recorder = EventRecorder::default();
        let (joins_tx, joins_rx) = mpsc::unbounded_channel();
        drop(joins_tx);
        let (controls_tx, controls_rx) = mpsc::unbounded_channel::<Control>();
        let payloads = serialize_inputs(&(0..20u32).collect::<Vec<_>>()).unwrap();
        let coord_fut = run_job_raw_with_joins(
            vec![("relay".to_string(), coord_side)],
            "slow",
            payloads,
            &[],
            RunOptions::default(),
            joins_rx,
            controls_rx,
            &recorder,
        );
        let driver = async {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            controls_tx
                .send(Control::new(
                    "relay/sub-relay/leaf-x".to_string(),
                    ControlAction::Kill {
                        mode: KillMode::Now,
                    },
                ))
                .unwrap();
            drop(controls_tx);
        };
        let (relay_res, raw, ()) = tokio::join!(relay_fut, coord_fut, driver);
        relay_res.unwrap();
        let outs: Vec<Result<u32, String>> =
            raw.unwrap().into_iter().map(decode_output::<u32>).collect();
        assert_eq!(outs, (0..20u32).map(|x| Ok(x * 2)).collect::<Vec<_>>());
        let mut state = RunState::default();
        for event in &recorder.events() {
            state.apply(event);
        }
        assert_eq!(
            state.nodes()["relay/sub-relay/leaf-x"].state(),
            NodeState::Lost
        );
    }
}
