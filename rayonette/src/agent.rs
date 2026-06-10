//! The agent side: run a registry of task handlers against a connection,
//! turning `Assign` messages into `Completed`/`Failed`.
//!
//! A task fails only by panicking: each task runs
//! under `catch_unwind` so a panic becomes a `Failed` message rather than
//! tearing down the agent. The registry is generated from the consumer's
//! source (see `rayonette_build`); in tests it is built by hand.

use std::collections::HashMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{de::DeserializeOwned, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::framing::{Connection, Receiver};
use crate::observability::{Event, NodeTelemetry};
use crate::protocol::{FromAgent, ToAgent, PROTOCOL_VERSION};
use crate::telemetry::Sampler;

/// Minimum gap between an agent's self-reported telemetry samples. Sampling can
/// spawn `nvidia-smi`, so it is throttled rather than run on every task, which
/// would add latency to the task hot path and distort scheduling.
const TELEMETRY_INTERVAL: Duration = Duration::from_millis(250);

/// A fresh telemetry sample if at least [`TELEMETRY_INTERVAL`] has passed since
/// `last` (advancing it to `now`), else `None`. Keeping the clock a parameter
/// makes the throttle deterministic to test.
fn due_sample(
    sampler: &mut Sampler,
    last: &mut Instant,
    now: Instant,
    in_flight: usize,
) -> Option<NodeTelemetry> {
    if now.duration_since(*last) < TELEMETRY_INTERVAL {
        return None;
    }
    *last = now;
    Some(sampler.sample(in_flight))
}

/// A type-erased task: a postcard-encoded input in, a postcard-encoded output
/// out, or an error string (a decode failure or a captured panic message).
pub type TaskHandler = Arc<dyn Fn(Vec<u8>) -> Result<Vec<u8>, String> + Send + Sync>;

/// Wrap a task function as a [`TaskHandler`], adding (de)serialization and
/// `catch_unwind` so a panic becomes a `Failed` outcome.
///
/// Accepts any `Fn` for test ergonomics; the public API (Phase 3) restricts the
/// surface to a non-capturing `fn(Input) -> Output`.
pub fn handler<I, O, F>(f: F) -> TaskHandler
where
    I: DeserializeOwned,
    O: Serialize,
    F: Fn(I) -> O + Send + Sync + 'static,
{
    Arc::new(move |payload: Vec<u8>| {
        let input: I = postcard::from_bytes(&payload).map_err(|e| format!("decode input: {e}"))?;
        let output = catch_unwind(AssertUnwindSafe(|| f(input))).map_err(|p| panic_message(&*p))?;
        postcard::to_allocvec(&output).map_err(|e| format!("encode output: {e}"))
    })
}

/// The stable wire key for a task function: its `type_name`.
///
/// The same function on both coordinator and agent produces the same key
/// because it is the same compiled type. Pass the function *item* (not a
/// coerced `fn` pointer) so the key is unique per function.
#[must_use]
pub fn fn_key<F: ?Sized>(_f: &F) -> &'static str {
    std::any::type_name::<F>()
}

/// Read the opening `Hello` from a peer (a coordinator or a parent relay) and
/// return the job's `fn_key`, checking the protocol version. Shared by the leaf
/// [`serve`] and the relay (`crate::relay`), which begin a session identically.
///
/// # Errors
/// Returns an error on a protocol-version mismatch or any message other than
/// `Hello` (including a closed stream).
pub(crate) async fn recv_hello<S>(
    rx: &mut Receiver<S>,
) -> std::io::Result<(String, crate::heartbeat::HeartbeatConfig)>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    match rx.recv::<ToAgent>().await? {
        Some(ToAgent::Hello {
            protocol_version,
            fn_key,
            heartbeat,
        }) => {
            if protocol_version != PROTOCOL_VERSION {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("protocol mismatch: local {PROTOCOL_VERSION}, peer {protocol_version}"),
                ));
            }
            Ok((fn_key, heartbeat))
        }
        other => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("expected Hello, got {other:?}"),
        )),
    }
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|s| (*s).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "task panicked".to_string())
}

/// Maps `fn_key`s to handlers for one agent.
#[derive(Clone, Default)]
pub struct Registry {
    handlers: HashMap<String, TaskHandler>,
}

impl std::fmt::Debug for Registry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Registry")
            .field("keys", &self.handlers.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl Registry {
    /// An empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a registry from every [`TaskEntry`] submitted with `register_task!`
    /// across the program (the boot-time counterpart to a hand-built registry).
    ///
    /// This is how an agent populates itself: the `#[rayonette::tasks]` macro
    /// emits a `register_task!` per task, each submitting an entry to the
    /// inventory this iterates at startup.
    ///
    /// # Examples
    /// ```
    /// use rayonette::agent::Registry;
    ///
    /// // With no `#[rayonette::tasks]` scope compiled in, the gathered registry
    /// // is empty; a real agent's binary carries the macro's registrations.
    /// let registry = Registry::from_inventory();
    /// # let _ = registry;
    /// ```
    #[must_use]
    pub fn from_inventory() -> Self {
        let mut registry = Self::new();
        for entry in inventory::iter::<TaskEntry> {
            (entry.register)(&mut registry);
        }
        registry
    }

    /// Register `handler` under `key`, returning the registry for chaining.
    #[must_use]
    pub fn with(mut self, key: impl Into<String>, handler: TaskHandler) -> Self {
        self.handlers.insert(key.into(), handler);
        self
    }

    /// Register a task function under its `type_name` key (see [`fn_key`]), the
    /// same key the coordinator derives. This is what generated registry code
    /// will call for each task function in Phase 3.
    #[must_use]
    pub fn with_fn<I, O, F>(self, f: F) -> Self
    where
        I: DeserializeOwned,
        O: Serialize,
        F: Fn(I) -> O + Send + Sync + 'static,
    {
        self.with(std::any::type_name::<F>(), handler(f))
    }

    /// Register a task function under an explicit `key`, in place. The mutable,
    /// explicit-key sibling of [`Registry::with_fn`]: `register_task!` calls this
    /// with the macro-assigned key, recovering the input and output types
    /// generically so an annotated closure registers with no hand-written wrapper.
    pub fn add<I, O, F>(&mut self, key: impl Into<String>, f: F)
    where
        I: DeserializeOwned,
        O: Serialize,
        F: Fn(I) -> O + Send + Sync + 'static,
    {
        self.handlers.insert(key.into(), handler(f));
    }

    fn get(&self, key: &str) -> Option<&TaskHandler> {
        self.handlers.get(key)
    }

    /// The keys this registry can serve, for the unknown-key backstop message.
    fn keys(&self) -> impl Iterator<Item = &str> {
        self.handlers.keys().map(String::as_str)
    }
}

/// One task's registration, gathered by `inventory` so [`Registry::from_inventory`]
/// can build an agent's registry from every `register_task!` in the program.
///
/// It holds a function that inserts the task's handler under its macro-assigned
/// key (rather than the handler itself) so the input and output types stay
/// generic until the insert, where [`Registry::add`] recovers them.
#[derive(Debug)]
pub struct TaskEntry {
    register: fn(&mut Registry),
}

impl TaskEntry {
    /// Wrap a registration function. Called by `register_task!`, whose closure
    /// inserts one task into the registry it is handed.
    #[must_use]
    pub const fn new(register: fn(&mut Registry)) -> Self {
        Self { register }
    }
}

inventory::collect!(TaskEntry);

/// Serve a connection: handshake, then run each assigned task to completion and
/// report it, until `Shutdown` or end-of-stream.
///
/// One task runs at a time (the coordinator hands an agent its next task only
/// once the current one finishes), each on the blocking pool so a long-running
/// task does not stall the reactor.
///
/// # Errors
/// Returns an error on a protocol violation (a missing or unexpected handshake,
/// an unknown `fn_key`) or an underlying transport failure.
///
/// # Panics
/// Panics only if joining a task's blocking thread fails, which cannot happen:
/// the handler catches its own panics, so the blocking closure never unwinds.
pub async fn serve<S>(conn: Connection<S>, registry: Registry) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (mut tx, mut rx) = conn.split();

    let (fn_key, heartbeat) = recv_hello(&mut rx).await?;

    let handler = registry.get(&fn_key).cloned().ok_or_else(|| {
        // A self-explaining backstop for the bare-`net_map`-without-attribute
        // mistake: name the missing key, list what this agent did register, and
        // point at the fix. The two ends must agree on the task set.
        let mut registered: Vec<&str> = registry.keys().collect();
        registered.sort_unstable();
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "unknown task key `{fn_key}`. This agent registered [{}]. The coordinator \
                 derived a key no registered task matches: scope the `net_map` call sites with \
                 `#[rayonette::tasks]`, or pass a named function, so the task is registered on \
                 both sides.",
                registered.join(", ")
            ),
        )
    })?;

    // A leaf runs one task at a time, so it advertises a single slot.
    tx.send(&FromAgent::Ready { slots: 1 }).await?;

    let mut sampler = Sampler::new();
    let mut last_sample = Instant::now();

    // One task at a time: read an assignment, run it on the blocking pool, and
    // report its outcome before reading the next message. With the heartbeat on,
    // each read is bounded by the silence timeout: if the parent sends nothing
    // (not even a ping) within it, the parent is gone and the leaf exits rather
    // than blocking forever on a half-open connection.
    loop {
        let next = if heartbeat.is_enabled() {
            match tokio::time::timeout(heartbeat.timeout(), rx.recv::<ToAgent>()).await {
                Ok(received) => received?,
                Err(_elapsed) => break,
            }
        } else {
            rx.recv::<ToAgent>().await?
        };
        let Some(message) = next else { break };
        match message {
            ToAgent::Assign { task_id, payload } => {
                tx.send(&FromAgent::Started { task_id }).await?;
                // Report a throttled sample while the task runs and once it ends, so
                // the viewer sees the node busy and the CPU delta the task drove
                // without sampling (which can spawn nvidia-smi) on every task. The
                // send is inlined rather than a helper: a generic send wrapper would
                // monomorphize per stream and its unexercised instances would fail
                // the function-coverage gate.
                if let Some(telemetry) =
                    due_sample(&mut sampler, &mut last_sample, Instant::now(), 1)
                {
                    let host = String::new();
                    tx.send(&FromAgent::Observe(Event::Telemetry { host, telemetry }))
                        .await?;
                }
                let handler = handler.clone();
                // The handler catches panics internally, so the blocking task
                // never panics and its join cannot fail.
                let outcome = tokio::task::spawn_blocking(move || handler(payload))
                    .await
                    .expect("task handler cannot panic");
                match outcome {
                    // A leaf ran the task itself, so the path down to the runner is
                    // empty; relays prepend their child label as they forward this.
                    Ok(output) => {
                        tx.send(&FromAgent::Completed {
                            task_id,
                            output,
                            via: String::new(),
                        })
                        .await?;
                    }
                    Err(error) => {
                        tx.send(&FromAgent::Failed {
                            task_id,
                            error,
                            via: String::new(),
                        })
                        .await?;
                    }
                }
                if let Some(telemetry) =
                    due_sample(&mut sampler, &mut last_sample, Instant::now(), 0)
                {
                    let host = String::new();
                    tx.send(&FromAgent::Observe(Event::Telemetry { host, telemetry }))
                        .await?;
                }
            }
            // A liveness probe: answer it so the parent knows the leaf is alive.
            ToAgent::Ping => tx.send(&FromAgent::Pong).await?,
            ToAgent::Shutdown => break,
            // A leaf has no children, so it never readies via the relay handshake
            // and the coordinator never sends it an active-set, a promotion, or a
            // control to route (its relay applies pause/kill to it directly).
            other @ (ToAgent::Hello { .. }
            | ToAgent::Activate { .. }
            | ToAgent::Promote { .. }
            | ToAgent::Control { .. }) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("unexpected message: {other:?}"),
                ));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{due_sample, handler, serve, Registry, Sampler, TaskEntry, TELEMETRY_INTERVAL};
    use crate::framing::Receiver;
    use crate::observability::Event;
    use crate::protocol::{FromAgent, ToAgent, PROTOCOL_VERSION};
    use crate::testing::connection_pair;
    use std::time::{Duration, Instant};
    use tokio::io::{AsyncRead, AsyncWrite};

    /// Read the next message that is not a self-reported telemetry sample, which a
    /// serving agent now interleaves around each task. `None` at end of stream.
    async fn next_report<R>(rx: &mut Receiver<R>) -> Option<FromAgent>
    where
        R: AsyncRead + AsyncWrite + Unpin,
    {
        loop {
            match rx.recv::<FromAgent>().await.unwrap() {
                Some(FromAgent::Observe(_)) => {}
                other => return other,
            }
        }
    }

    #[test]
    fn telemetry_sampling_is_throttled() {
        let mut sampler = Sampler::new();
        let base = Instant::now();
        let mut last = base;
        // Just sampled: another sample at the same instant is throttled away.
        assert!(due_sample(&mut sampler, &mut last, base, 1).is_none());
        // Once the interval has elapsed a sample is due, and the clock advances.
        let later = base + TELEMETRY_INTERVAL;
        assert!(due_sample(&mut sampler, &mut last, later, 0).is_some());
        assert_eq!(last, later);
    }

    #[tokio::test]
    async fn recv_hello_returns_the_heartbeat_config() {
        use crate::heartbeat::HeartbeatConfig;
        let config = HeartbeatConfig::new(Duration::from_secs(2), Duration::from_secs(7));
        let (client, server) = connection_pair(64);
        let (mut tx, _crx) = client.split();
        tx.send(&ToAgent::Hello {
            protocol_version: PROTOCOL_VERSION,
            fn_key: "k".to_string(),
            heartbeat: config,
        })
        .await
        .unwrap();
        let (_stx, mut srx) = server.split();
        let (fn_key, got) = super::recv_hello(&mut srx).await.unwrap();
        assert_eq!(fn_key, "k");
        assert_eq!(got, config);
    }

    #[tokio::test]
    async fn a_leaf_answers_a_ping_with_a_pong() {
        let (client, server) = connection_pair(256);
        let agent = tokio::spawn(serve(
            server,
            Registry::new().with("id", handler(|x: u32| x)),
        ));
        let (mut tx, mut rx) = client.split();
        tx.send(&ToAgent::Hello {
            protocol_version: PROTOCOL_VERSION,
            fn_key: "id".to_string(),
            heartbeat: crate::heartbeat::HeartbeatConfig::default(),
        })
        .await
        .unwrap();
        assert_eq!(
            next_report(&mut rx).await,
            Some(FromAgent::Ready { slots: 1 })
        );
        tx.send(&ToAgent::Ping).await.unwrap();
        assert_eq!(next_report(&mut rx).await, Some(FromAgent::Pong));
        tx.send(&ToAgent::Shutdown).await.unwrap();
        agent.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_leaf_tears_down_when_its_parent_goes_silent() {
        use crate::heartbeat::HeartbeatConfig;
        let (client, server) = connection_pair(256);
        let agent = tokio::spawn(serve(
            server,
            Registry::new().with("id", handler(|x: u32| x)),
        ));
        let (mut tx, mut rx) = client.split();
        tx.send(&ToAgent::Hello {
            protocol_version: PROTOCOL_VERSION,
            fn_key: "id".to_string(),
            heartbeat: HeartbeatConfig::new(Duration::from_millis(100), Duration::from_millis(300)),
        })
        .await
        .unwrap();
        assert_eq!(
            next_report(&mut rx).await,
            Some(FromAgent::Ready { slots: 1 })
        );
        // Send nothing more, but keep the connection open (no EOF). With paused
        // time, awaiting the agent advances to the silence timeout, at which point
        // the leaf gives the silent parent up and exits cleanly rather than blocking.
        let result = agent.await.unwrap();
        assert!(
            result.is_ok(),
            "the leaf exits cleanly on silence: {result:?}"
        );
        drop(tx);
    }

    #[tokio::test]
    async fn serve_reports_throttled_telemetry_around_a_long_task() {
        let (client, server) = connection_pair(256);
        let registry = Registry::new().with(
            "slow",
            handler(|x: u32| {
                std::thread::sleep(Duration::from_millis(300));
                x
            }),
        );
        let agent = serve(server, registry);
        let driver = async {
            let (mut tx, mut rx) = client.split();
            tx.send(&ToAgent::Hello {
                protocol_version: PROTOCOL_VERSION,
                fn_key: "slow".to_string(),
                heartbeat: crate::heartbeat::HeartbeatConfig::default(),
            })
            .await
            .unwrap();
            let _ready: FromAgent = rx.recv().await.unwrap().unwrap();
            tx.send(&ToAgent::Assign {
                task_id: 0,
                payload: postcard::to_allocvec(&5u32).unwrap(),
            })
            .await
            .unwrap();
            // The task runs longer than the throttle interval, so a telemetry
            // sample is reported once it finishes.
            let mut saw_telemetry = false;
            for _ in 0..4 {
                let message = rx.recv::<FromAgent>().await.unwrap().unwrap();
                if matches!(message, FromAgent::Observe(Event::Telemetry { .. })) {
                    saw_telemetry = true;
                    break;
                }
            }
            assert!(saw_telemetry, "a long task triggers a telemetry sample");
            tx.send(&ToAgent::Shutdown).await.unwrap();
        };
        let (agent_res, ()) = tokio::join!(agent, driver);
        agent_res.unwrap();
    }

    #[tokio::test]
    async fn runs_a_task_and_reports_completion() {
        let (client, server) = connection_pair(256);
        let registry = Registry::new().with("doubler", handler(|x: u32| x * 2));

        let agent = serve(server, registry);
        let driver = async {
            let (mut tx, mut rx) = client.split();
            tx.send(&ToAgent::Hello {
                protocol_version: PROTOCOL_VERSION,
                fn_key: "doubler".to_string(),
                heartbeat: crate::heartbeat::HeartbeatConfig::default(),
            })
            .await
            .unwrap();

            let ready: FromAgent = rx.recv().await.unwrap().unwrap();
            assert_eq!(ready, FromAgent::Ready { slots: 1 });

            let payload = postcard::to_allocvec(&21u32).unwrap();
            tx.send(&ToAgent::Assign {
                task_id: 0,
                payload,
            })
            .await
            .unwrap();

            let started = next_report(&mut rx).await.unwrap();
            assert_eq!(started, FromAgent::Started { task_id: 0 });

            let completed = next_report(&mut rx).await.unwrap();
            assert_eq!(
                completed,
                FromAgent::Completed {
                    task_id: 0,
                    output: postcard::to_allocvec(&42u32).unwrap(),
                    via: String::new(),
                }
            );

            tx.send(&ToAgent::Shutdown).await.unwrap();
        };

        let (agent_res, ()) = tokio::join!(agent, driver);
        agent_res.unwrap();
    }

    #[tokio::test]
    async fn a_panicking_task_becomes_failed_not_a_crash() {
        let (client, server) = connection_pair(256);
        let registry = Registry::new().with(
            "boom",
            handler(|x: u32| -> u32 {
                assert!(x != 0, "zero is not allowed");
                x
            }),
        );

        let agent = serve(server, registry);
        let driver = async {
            let (mut tx, mut rx) = client.split();
            tx.send(&ToAgent::Hello {
                protocol_version: PROTOCOL_VERSION,
                fn_key: "boom".to_string(),
                heartbeat: crate::heartbeat::HeartbeatConfig::default(),
            })
            .await
            .unwrap();
            let _ready: FromAgent = rx.recv().await.unwrap().unwrap();

            tx.send(&ToAgent::Assign {
                task_id: 0,
                payload: postcard::to_allocvec(&0u32).unwrap(),
            })
            .await
            .unwrap();
            tx.send(&ToAgent::Assign {
                task_id: 1,
                payload: postcard::to_allocvec(&7u32).unwrap(),
            })
            .await
            .unwrap();

            let mut terminals: Vec<FromAgent> = Vec::new();
            while terminals.len() < 2 {
                let msg = rx.recv::<FromAgent>().await.unwrap().unwrap();
                if !matches!(msg, FromAgent::Started { .. } | FromAgent::Observe(_)) {
                    terminals.push(msg);
                }
            }
            assert!(terminals.contains(&FromAgent::Failed {
                task_id: 0,
                error: "zero is not allowed".to_string(),
                via: String::new(),
            }));
            assert!(terminals.contains(&FromAgent::Completed {
                task_id: 1,
                output: postcard::to_allocvec(&7u32).unwrap(),
                via: String::new(),
            }));

            tx.send(&ToAgent::Shutdown).await.unwrap();
        };

        let (agent_res, ()) = tokio::join!(agent, driver);
        agent_res.unwrap();
    }

    #[tokio::test]
    async fn shutdown_drains_in_flight_work() {
        let (client, server) = connection_pair(256);
        let registry = Registry::new().with(
            "slow",
            handler(|x: u32| {
                std::thread::sleep(Duration::from_millis(20));
                x + 1
            }),
        );

        let agent = serve(server, registry);
        let driver = async {
            let (mut tx, mut rx) = client.split();
            tx.send(&ToAgent::Hello {
                protocol_version: PROTOCOL_VERSION,
                fn_key: "slow".to_string(),
                heartbeat: crate::heartbeat::HeartbeatConfig::default(),
            })
            .await
            .unwrap();
            let _ready: FromAgent = rx.recv().await.unwrap().unwrap();

            // Assign, then shut down while the task is still running.
            tx.send(&ToAgent::Assign {
                task_id: 0,
                payload: postcard::to_allocvec(&41u32).unwrap(),
            })
            .await
            .unwrap();
            tx.send(&ToAgent::Shutdown).await.unwrap();

            // The in-flight task must still complete before the agent exits.
            let started = next_report(&mut rx).await.unwrap();
            assert_eq!(started, FromAgent::Started { task_id: 0 });
            let completed = next_report(&mut rx).await.unwrap();
            assert_eq!(
                completed,
                FromAgent::Completed {
                    task_id: 0,
                    output: postcard::to_allocvec(&42u32).unwrap(),
                    via: String::new(),
                }
            );
            // Then a clean end-of-stream, past the final telemetry sample.
            assert!(next_report(&mut rx).await.is_none());
        };

        let (agent_res, ()) = tokio::join!(agent, driver);
        agent_res.unwrap();
    }

    #[tokio::test]
    async fn rejects_protocol_mismatch() {
        let (client, server) = connection_pair(64);
        let agent = serve(server, Registry::new().with("k", handler(|x: u32| x)));
        let driver = async {
            let (mut tx, _rx) = client.split();
            tx.send(&ToAgent::Hello {
                protocol_version: PROTOCOL_VERSION + 1,
                fn_key: "k".to_string(),
                heartbeat: crate::heartbeat::HeartbeatConfig::default(),
            })
            .await
            .unwrap();
        };
        let (res, ()) = tokio::join!(agent, driver);
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn rejects_non_hello_handshake() {
        let (client, server) = connection_pair(64);
        let agent = serve(server, Registry::new());
        let driver = async {
            let (mut tx, _rx) = client.split();
            tx.send(&ToAgent::Shutdown).await.unwrap();
        };
        let (res, ()) = tokio::join!(agent, driver);
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn rejects_unknown_fn_key() {
        let (client, server) = connection_pair(64);
        let agent = serve(server, Registry::new().with("known", handler(|x: u32| x)));
        let driver = async {
            let (mut tx, _rx) = client.split();
            tx.send(&ToAgent::Hello {
                protocol_version: PROTOCOL_VERSION,
                fn_key: "unknown".to_string(),
                heartbeat: crate::heartbeat::HeartbeatConfig::default(),
            })
            .await
            .unwrap();
        };
        let (res, ()) = tokio::join!(agent, driver);
        let error = res.unwrap_err().to_string();
        // The backstop names the missing key, lists what the agent registered, and
        // points at the fix.
        assert!(error.contains("unknown task key `unknown`"), "{error}");
        assert!(error.contains("known"), "{error}");
        assert!(error.contains("#[rayonette::tasks]"), "{error}");
    }

    #[tokio::test]
    async fn rejects_unexpected_message_after_handshake() {
        let (client, server) = connection_pair(64);
        let agent = serve(server, Registry::new().with("k", handler(|x: u32| x)));
        let driver = async {
            let (mut tx, mut rx) = client.split();
            tx.send(&ToAgent::Hello {
                protocol_version: PROTOCOL_VERSION,
                fn_key: "k".to_string(),
                heartbeat: crate::heartbeat::HeartbeatConfig::default(),
            })
            .await
            .unwrap();
            let _ready: FromAgent = rx.recv().await.unwrap().unwrap();
            // A second Hello is unexpected after the handshake.
            tx.send(&ToAgent::Hello {
                protocol_version: PROTOCOL_VERSION,
                fn_key: "k".to_string(),
                heartbeat: crate::heartbeat::HeartbeatConfig::default(),
            })
            .await
            .unwrap();
        };
        let (res, ()) = tokio::join!(agent, driver);
        assert!(res.is_err());
    }

    #[test]
    fn panic_message_handles_str_string_and_other() {
        let s: Box<dyn std::any::Any + Send> = Box::new("boom");
        assert_eq!(super::panic_message(&*s), "boom");
        let s: Box<dyn std::any::Any + Send> = Box::new(String::from("kaboom"));
        assert_eq!(super::panic_message(&*s), "kaboom");
        let s: Box<dyn std::any::Any + Send> = Box::new(42i32);
        assert_eq!(super::panic_message(&*s), "task panicked");
    }

    #[test]
    fn registry_debug_lists_keys() {
        let r = Registry::new().with("alpha", handler(|x: u32| x));
        assert!(format!("{r:?}").contains("alpha"));
    }

    #[test]
    fn handler_reports_decode_and_encode_errors() {
        use crate::testing::FailsToSerialize;
        // An input that is not a valid u32 fails to decode.
        let decode = handler(|x: u32| x);
        assert!(decode(vec![0xFF; 6]).unwrap_err().contains("decode input"));
        // An output that cannot serialize is reported as an encode error.
        let encode = handler(|_: u32| FailsToSerialize);
        let payload = postcard::to_allocvec(&5u32).unwrap();
        assert!(encode(payload).unwrap_err().contains("encode output"));
    }

    #[test]
    fn registry_add_registers_under_explicit_key() {
        // `add` is the in-place, explicit-key sibling of `with_fn`: it inserts a
        // task under the key it is handed (here a macro-style string, not the
        // type_name), recovering the input/output types from the closure itself.
        let mut registry = Registry::new();
        registry.add("k", |x: u32| x * 3);
        let handler = registry.get("k").expect("the task is registered under k");
        let output = handler(postcard::to_allocvec(&7u32).unwrap()).unwrap();
        assert_eq!(postcard::from_bytes::<u32>(&output).unwrap(), 21);
    }

    #[test]
    fn task_entry_new_builds_a_runnable_entry() {
        // `register_task!` builds a `TaskEntry` from a registration closure via
        // `new`; applying that closure must register a runnable task. Exercised at
        // runtime here (the inventory submit path const-evaluates `new`), which
        // also renders the entry's `Debug`.
        let entry = TaskEntry::new(|registry| registry.add("te", |x: u32| x + 100));
        assert!(format!("{entry:?}").contains("TaskEntry"));
        let mut registry = Registry::new();
        (entry.register)(&mut registry);
        let handler = registry.get("te").expect("the task is registered under te");
        let output = handler(postcard::to_allocvec(&5u32).unwrap()).unwrap();
        assert_eq!(postcard::from_bytes::<u32>(&output).unwrap(), 105);
    }

    // A task submitted at module scope, the way `register_task!` does, so
    // `from_inventory` has at least one entry to gather (and its loop body runs).
    inventory::submit! {
        TaskEntry::new(|registry| {
            registry.add("phase1::dummy", |x: u32| x + 1);
        })
    }

    #[test]
    fn from_inventory_collects_submitted_entries() {
        // The boot path: gather every submitted `TaskEntry` into a registry, then
        // confirm the dummy task is present and actually runnable end to end.
        let registry = Registry::from_inventory();
        let handler = registry
            .get("phase1::dummy")
            .expect("the submitted entry is present");
        let output = handler(postcard::to_allocvec(&4u32).unwrap()).unwrap();
        assert_eq!(postcard::from_bytes::<u32>(&output).unwrap(), 5);
    }
}
