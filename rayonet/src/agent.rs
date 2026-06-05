//! The agent side: run a registry of task handlers against a connection,
//! turning `Assign` messages into `Completed`/`Failed` (PLAN.md Phase 1).
//!
//! A task fails only by panicking: each task runs
//! under `catch_unwind` so a panic becomes a `Failed` message rather than
//! tearing down the agent or losing its in-flight siblings. Phase 3 will
//! generate the registry from the consumer's source; here it is built by hand.

use std::collections::HashMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

use serde::{de::DeserializeOwned, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;

use crate::framing::Connection;
use crate::protocol::{FromAgent, TaskId, ToAgent, PROTOCOL_VERSION};

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

/// Serve a connection: handshake, then run assigned tasks until `Shutdown` or
/// end-of-stream, draining any still-running tasks before returning.
///
/// Concurrency is bounded by the coordinator, which keeps at most `capacity`
/// tasks in flight per agent; the agent advertises `capacity` in `Ready` and
/// runs each assignment on the blocking pool.
///
/// # Errors
/// Returns an error on a protocol violation (a missing or unexpected handshake,
/// an unknown `fn_key`) or an underlying transport failure.
pub async fn serve<S>(conn: Connection<S>, registry: Registry, capacity: u32) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (mut tx, mut rx) = conn.split();

    let fn_key = match rx.recv::<ToAgent>().await? {
        Some(ToAgent::Hello {
            protocol_version,
            fn_key,
        }) => {
            if protocol_version != PROTOCOL_VERSION {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "protocol mismatch: agent {PROTOCOL_VERSION}, coordinator {protocol_version}"
                    ),
                ));
            }
            fn_key
        }
        other => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("expected Hello, got {other:?}"),
            ));
        }
    };

    let handler = registry.get(&fn_key).cloned().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("unknown fn_key: {fn_key}"),
        )
    })?;

    tx.send(&FromAgent::Ready { capacity }).await?;

    // Completed tasks report through a channel rather than a JoinSet, so there
    // is no JoinError to handle (a panic is already caught inside the handler).
    let (done_tx, mut done_rx) = mpsc::unbounded_channel::<(TaskId, Result<Vec<u8>, String>)>();
    let mut inflight = 0usize;
    let mut draining = false;

    loop {
        tokio::select! {
            // The completions branch has no guard, so a branch is always enabled
            // and the select never needs an `else`.
            Some((task_id, outcome)) = done_rx.recv() => {
                inflight -= 1;
                match outcome {
                    Ok(output) => tx.send(&FromAgent::Completed { task_id, output }).await?,
                    Err(error) => tx.send(&FromAgent::Failed { task_id, error }).await?,
                }
                if draining && inflight == 0 {
                    break;
                }
            }
            msg = rx.recv::<ToAgent>(), if !draining => {
                match msg? {
                    Some(ToAgent::Assign { task_id, payload }) => {
                        tx.send(&FromAgent::Started { task_id }).await?;
                        let handler = handler.clone();
                        let done = done_tx.clone();
                        inflight += 1;
                        tokio::task::spawn_blocking(move || {
                            let _ = done.send((task_id, handler(payload)));
                        });
                    }
                    Some(ToAgent::Shutdown) | None => {
                        draining = true;
                        if inflight == 0 {
                            break;
                        }
                    }
                    Some(other) => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("unexpected message: {other:?}"),
                        ));
                    }
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{handler, serve, Registry};
    use crate::protocol::{FromAgent, ToAgent, PROTOCOL_VERSION};
    use crate::testing::connection_pair;

    #[tokio::test]
    async fn runs_a_task_and_reports_completion() {
        let (client, server) = connection_pair(256);
        let registry = Registry::new().with("doubler", handler(|x: u32| x * 2));

        let agent = serve(server, registry, 1);
        let driver = async {
            let (mut tx, mut rx) = client.split();
            tx.send(&ToAgent::Hello {
                protocol_version: PROTOCOL_VERSION,
                fn_key: "doubler".to_string(),
            })
            .await
            .unwrap();

            let ready: FromAgent = rx.recv().await.unwrap().unwrap();
            assert_eq!(ready, FromAgent::Ready { capacity: 1 });

            let payload = postcard::to_allocvec(&21u32).unwrap();
            tx.send(&ToAgent::Assign {
                task_id: 0,
                payload,
            })
            .await
            .unwrap();

            let started: FromAgent = rx.recv().await.unwrap().unwrap();
            assert_eq!(started, FromAgent::Started { task_id: 0 });

            let completed: FromAgent = rx.recv().await.unwrap().unwrap();
            assert_eq!(
                completed,
                FromAgent::Completed {
                    task_id: 0,
                    output: postcard::to_allocvec(&42u32).unwrap(),
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

        let agent = serve(server, registry, 2);
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
                if !matches!(msg, FromAgent::Started { .. }) {
                    terminals.push(msg);
                }
            }
            assert!(terminals.contains(&FromAgent::Failed {
                task_id: 0,
                error: "zero is not allowed".to_string(),
            }));
            assert!(terminals.contains(&FromAgent::Completed {
                task_id: 1,
                output: postcard::to_allocvec(&7u32).unwrap(),
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
                std::thread::sleep(std::time::Duration::from_millis(20));
                x + 1
            }),
        );

        let agent = serve(server, registry, 4);
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
            let started: FromAgent = rx.recv().await.unwrap().unwrap();
            assert_eq!(started, FromAgent::Started { task_id: 0 });
            let completed: FromAgent = rx.recv().await.unwrap().unwrap();
            assert_eq!(
                completed,
                FromAgent::Completed {
                    task_id: 0,
                    output: postcard::to_allocvec(&42u32).unwrap(),
                }
            );
            // Then a clean end-of-stream.
            assert!(rx.recv::<FromAgent>().await.unwrap().is_none());
        };

        let (agent_res, ()) = tokio::join!(agent, driver);
        agent_res.unwrap();
    }

    #[tokio::test]
    async fn rejects_protocol_mismatch() {
        let (client, server) = connection_pair(64);
        let agent = serve(server, Registry::new().with("k", handler(|x: u32| x)), 1);
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
        let agent = serve(server, Registry::new(), 1);
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
        let agent = serve(
            server,
            Registry::new().with("known", handler(|x: u32| x)),
            1,
        );
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
        let agent = serve(server, Registry::new().with("k", handler(|x: u32| x)), 1);
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
