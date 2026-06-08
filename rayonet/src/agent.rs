//! The agent side: run a registry of task handlers against a connection,
//! turning `Assign` messages into `Completed`/`Failed` (PLAN.md Phase 1).
//!
//! A task fails only by panicking: each task runs
//! under `catch_unwind` so a panic becomes a `Failed` message rather than
//! tearing down the agent. The registry is generated from the consumer's
//! source (see `rayonet_build`); in tests it is built by hand.

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
pub(crate) async fn recv_hello<S>(rx: &mut Receiver<S>) -> std::io::Result<String>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    match rx.recv::<ToAgent>().await? {
        Some(ToAgent::Hello {
            protocol_version,
            fn_key,
        }) => {
            if protocol_version != PROTOCOL_VERSION {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("protocol mismatch: local {PROTOCOL_VERSION}, peer {protocol_version}"),
                ));
            }
            Ok(fn_key)
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

    fn get(&self, key: &str) -> Option<&TaskHandler> {
        self.handlers.get(key)
    }
}

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

    let fn_key = recv_hello(&mut rx).await?;

    let handler = registry.get(&fn_key).cloned().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("unknown fn_key: {fn_key}"),
        )
    })?;

    // A leaf runs one task at a time, so it advertises a single slot.
    tx.send(&FromAgent::Ready { slots: 1 }).await?;

    let mut sampler = Sampler::new();
    let mut last_sample = Instant::now();

    // One task at a time: read an assignment, run it on the blocking pool, and
    // report its outcome before reading the next message.
    while let Some(message) = rx.recv::<ToAgent>().await? {
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
            ToAgent::Shutdown => break,
            // A leaf has no children, so it never readies via the relay handshake
            // and the coordinator never sends it an active-set or a promotion.
            other
            @ (ToAgent::Hello { .. } | ToAgent::Activate { .. } | ToAgent::Promote { .. }) => {
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
    use super::{due_sample, handler, serve, Registry, Sampler, TELEMETRY_INTERVAL};
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
            })
            .await
            .unwrap();
        };
        let (res, ()) = tokio::join!(agent, driver);
        assert!(res.is_err());
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
            })
            .await
            .unwrap();
            let _ready: FromAgent = rx.recv().await.unwrap().unwrap();
            // A second Hello is unexpected after the handshake.
            tx.send(&ToAgent::Hello {
                protocol_version: PROTOCOL_VERSION,
                fn_key: "k".to_string(),
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
}
