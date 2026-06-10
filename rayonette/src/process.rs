//! Real-process transport.
//!
//! Detect agent mode, build a connection over the current process's stdio (agent
//! side), and spawn an agent subprocess with a connection over its stdio plus
//! captured stderr.

use std::process::ExitStatus;
use std::sync::{Arc, Mutex};

use tokio::io::{join, AsyncReadExt, Join, Stdin, Stdout};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};
use tokio::task::JoinHandle;

use crate::framing::Connection;

/// Environment marker the coordinator sets when launching an agent.
pub(crate) const AGENT_ENV: &str = "RAYONETTE_AGENT";

/// True when this process was launched as a rayonette agent.
#[must_use]
pub fn is_agent() -> bool {
    std::env::var_os(AGENT_ENV).is_some()
}

/// A connection over this process's stdin (reads) and stdout (writes), for an
/// agent to serve on.
#[must_use]
pub(crate) fn agent_connection() -> Connection<Join<Stdin, Stdout>> {
    Connection::new(join(tokio::io::stdin(), tokio::io::stdout()))
}

/// A spawned agent subprocess: its handle and captured stderr.
#[doc(hidden)]
pub struct AgentProcess {
    /// The child process handle.
    child: Child,
    stderr: Arc<Mutex<Vec<u8>>>,
    task: JoinHandle<()>,
}

impl std::fmt::Debug for AgentProcess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AgentProcess").finish_non_exhaustive()
    }
}

impl AgentProcess {
    /// The agent's stderr captured so far (forwarded verbatim).
    #[must_use]
    pub fn stderr_text(&self) -> String {
        let captured = self
            .stderr
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        String::from_utf8_lossy(&captured).into_owned()
    }

    /// Kill the agent process, used to simulate a crashed agent.
    ///
    /// # Errors
    /// Returns an error if the kill signal cannot be delivered.
    pub async fn kill(&mut self) -> std::io::Result<()> {
        self.child.kill().await
    }

    /// Wait for the agent to exit, returning its status and full stderr.
    ///
    /// # Errors
    /// Returns an error if waiting on the child fails.
    pub async fn wait(mut self) -> std::io::Result<(ExitStatus, String)> {
        let status = self.child.wait().await?;
        self.task.abort();
        Ok((status, self.stderr_text()))
    }
}

/// Launch `command` as a rayonette agent: set the agent marker, pipe stdio, and
/// return a connection over the child plus its captured-stderr handle.
///
/// # Errors
/// Returns an error if the process fails to spawn.
///
/// # Panics
/// Panics if a piped stdio handle is missing, which cannot happen here because
/// all three streams are configured as pipes immediately above.
#[doc(hidden)]
pub fn spawn(
    mut command: Command,
) -> std::io::Result<(Connection<Join<ChildStdout, ChildStdin>>, AgentProcess)> {
    command
        .env(AGENT_ENV, "1")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn()?;
    let stdin = child.stdin.take().expect("piped stdin");
    let stdout = child.stdout.take().expect("piped stdout");
    let stderr = child.stderr.take().expect("piped stderr");

    let buf = Arc::new(Mutex::new(Vec::new()));
    let task = tokio::spawn(capture_stderr(stderr, Arc::clone(&buf)));

    let connection = Connection::new(join(stdout, stdin));
    Ok((
        connection,
        AgentProcess {
            child,
            stderr: buf,
            task,
        },
    ))
}

async fn capture_stderr(mut stderr: ChildStderr, buf: Arc<Mutex<Vec<u8>>>) {
    let mut chunk = [0u8; 1024];
    while let Ok(n) = stderr.read(&mut chunk).await {
        if n == 0 {
            break;
        }
        buf.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend_from_slice(&chunk[..n]);
    }
}
