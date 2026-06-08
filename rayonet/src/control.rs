//! Out-of-band control commands for a running job: pause, resume, and kill a node.
//!
//! A run can be steered from outside its task stream. A client (the terminal TUI
//! today, other tools later) sends a [`Control`] over a back-channel and the
//! coordinator applies it, pausing or killing a node mid-run. The command names a
//! node by its path (the same `/`-joined label the event stream uses), and the
//! resulting lifecycle change comes back through the normal observability events,
//! so a renderer needs no special path. The Unix-socket transport
//! ([`ControlListener`] and [`ControlClient`]) lands alongside these types; this
//! module defines what crosses it.

use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::framing::Connection;

/// When a kill takes effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum KillMode {
    /// Drop the node immediately, requeuing whatever it was running.
    Now,
    /// Stop giving the node new work and drop it once its in-flight tasks finish.
    AfterCurrent,
}

/// What to do to the targeted node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlAction {
    /// Stop assigning new tasks to the node; it stays connected and its in-flight
    /// work finishes. Reversed by [`ControlAction::Resume`].
    Pause,
    /// Re-enable a paused node.
    Resume,
    /// Remove the node from the run: shut it down, then requeue and reroute its
    /// work the way a lost node's is.
    Kill {
        /// Whether the kill is immediate or waits for the current tasks to drain.
        mode: KillMode,
    },
}

/// A control command targeting one node by its path (for example `"gw1/leafA"`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Control {
    target: String,
    action: ControlAction,
}

impl Control {
    /// Target the node at `target` (its `/`-joined path from the root) with
    /// `action`.
    #[must_use]
    pub const fn new(target: String, action: ControlAction) -> Self {
        Self { target, action }
    }

    /// The targeted node's path.
    #[must_use]
    pub fn target(&self) -> &str {
        &self.target
    }

    /// The action to apply.
    #[must_use]
    pub const fn action(&self) -> ControlAction {
        self.action
    }
}

/// The coordinator-side end of the control back-channel: a Unix-socket listener
/// that forwards every [`Control`] any client sends into the run loop's channel.
///
/// Bind it before the run and pass the returned receiver to the coordinator; hold
/// the listener for the run's duration. Dropping it stops accepting and removes
/// the socket file. Any client (the TUI, or another tool) can connect with a
/// [`ControlClient`] and steer the run.
#[derive(Debug)]
pub struct ControlListener {
    accept: JoinHandle<()>,
    path: PathBuf,
}

impl ControlListener {
    /// Bind a control socket at `path`, returning the listener and the receiver
    /// that yields every [`Control`] sent by any connected client. A stale socket
    /// file at `path` is removed first.
    ///
    /// # Errors
    /// Returns an error if the socket cannot be bound.
    ///
    /// # Panics
    /// Must be called from within a Tokio runtime (it spawns the accept loop).
    pub fn bind(path: impl AsRef<Path>) -> io::Result<(Self, mpsc::UnboundedReceiver<Control>)> {
        let path = path.as_ref().to_path_buf();
        // A leftover socket file from a prior run would make bind fail; clear it.
        let _ = std::fs::remove_file(&path);
        let listener = UnixListener::bind(&path)?;
        let (tx, rx) = mpsc::unbounded_channel();
        let accept = tokio::spawn(async move {
            // Each client gets a task that decodes framed Controls and forwards
            // them; the accept loop ends only on a listener error.
            while let Ok((stream, _addr)) = listener.accept().await {
                let tx = tx.clone();
                tokio::spawn(async move {
                    let mut conn = Connection::new(stream);
                    while let Ok(Some(control)) = conn.recv::<Control>().await {
                        if tx.send(control).is_err() {
                            break;
                        }
                    }
                });
            }
        });
        Ok((Self { accept, path }, rx))
    }
}

impl Drop for ControlListener {
    fn drop(&mut self) {
        self.accept.abort();
        let _ = std::fs::remove_file(&self.path);
    }
}

/// A client of the control back-channel: connects to a coordinator's control
/// socket and sends [`Control`] commands (the TUI uses this to pause/kill nodes).
pub struct ControlClient {
    conn: Connection<UnixStream>,
}

impl std::fmt::Debug for ControlClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlClient").finish_non_exhaustive()
    }
}

impl ControlClient {
    /// Connect to the control socket at `path`.
    ///
    /// # Errors
    /// Returns an error if the socket cannot be reached.
    pub async fn connect(path: impl AsRef<Path>) -> io::Result<Self> {
        let stream = UnixStream::connect(path).await?;
        Ok(Self {
            conn: Connection::new(stream),
        })
    }

    /// Send one control command to the coordinator.
    ///
    /// # Errors
    /// Returns an error if the command cannot be written.
    pub async fn send(&mut self, control: &Control) -> io::Result<()> {
        self.conn.send(control).await
    }
}

#[cfg(test)]
mod tests {
    use super::{Control, ControlAction, ControlClient, ControlListener, KillMode};

    #[tokio::test]
    async fn a_control_sent_over_the_socket_arrives_on_the_channel() {
        let dir = std::env::temp_dir();
        // A unique-enough path for this test process; the listener clears a stale one.
        let path = dir.join(format!("rayonet-control-test-{}.sock", std::process::id()));
        let (listener, mut rx) = ControlListener::bind(&path).unwrap();
        assert!(format!("{listener:?}").contains("ControlListener"));

        let mut client = ControlClient::connect(&path).await.unwrap();
        assert!(format!("{client:?}").contains("ControlClient"));
        client
            .send(&Control::new("gw1/leafA".to_string(), ControlAction::Pause))
            .await
            .unwrap();
        client
            .send(&Control::new(
                "gw1".to_string(),
                ControlAction::Kill {
                    mode: KillMode::AfterCurrent,
                },
            ))
            .await
            .unwrap();

        let first = rx.recv().await.unwrap();
        assert_eq!(
            first,
            Control::new("gw1/leafA".to_string(), ControlAction::Pause)
        );
        let second = rx.recv().await.unwrap();
        assert_eq!(second.target(), "gw1");
        assert_eq!(
            second.action(),
            ControlAction::Kill {
                mode: KillMode::AfterCurrent
            }
        );

        // Dropping the listener removes the socket file.
        drop(listener);
        assert!(!path.exists());
    }
}
