//! Real ssh transport and remote execution via the openssh crate (Phase 4b).
//!
//! [`SshRemote`] runs the provisioning ladder's commands over a live ssh
//! session (it is a [`Remote`]); [`Ssh`] is a [`Launch`] that starts the agent
//! on a host and bridges its stdio as the task transport. System ssh is driven
//! through openssh, so `~/.ssh/config`, connection
//! multiplexing, and `ProxyJump` apply unchanged, and no ports are opened.

use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use openssh::{ChildStdin, ChildStdout, KnownHosts, Session, SessionBuilder, Stdio};
use tokio::io::{join, AsyncWriteExt, Join};

use crate::capability::NodeProfile;
use crate::fleet::Launch;
use crate::framing::Connection;
use crate::observability::EventSink;
use crate::process::AGENT_ENV;
use crate::provisioning::{node_id, probe, provision, CommandOutput, Remote};

/// Map an openssh error into the crate's uniform `io::Error` result type.
fn to_io(error: openssh::Error) -> io::Error {
    io::Error::other(error)
}

/// How to reach a host over ssh: a destination plus an optional explicit key.
///
/// `destination` is anything ssh accepts: `user@host`, a bare `host`, or a
/// `~/.ssh/config` alias. With no key set, the ambient ssh agent and config are
/// used unchanged.
#[derive(Debug, Clone)]
pub struct SshConfig {
    destination: String,
    keyfile: Option<PathBuf>,
    config_file: Option<PathBuf>,
    port: Option<u16>,
}

impl SshConfig {
    /// Target `destination` using the ambient ssh configuration.
    #[must_use]
    pub fn new(destination: impl Into<String>) -> Self {
        Self {
            destination: destination.into(),
            keyfile: None,
            config_file: None,
            port: None,
        }
    }

    /// Authenticate with the private key at `path` rather than the ssh agent.
    #[must_use]
    pub fn keyfile(mut self, path: impl Into<PathBuf>) -> Self {
        self.keyfile = Some(path.into());
        self
    }

    /// Connect on `port` instead of 22 (for example a published container port).
    #[must_use]
    pub const fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    /// Read host aliases, `ProxyJump` chains, and the like from `path` instead
    /// of the default `~/.ssh/config`. Lets `destination` be an alias whose
    /// routing (including multi-hop jumps) lives in that file.
    #[must_use]
    pub fn config_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.config_file = Some(path.into());
        self
    }

    async fn connect(&self) -> io::Result<Session> {
        let mut builder = SessionBuilder::default();
        builder.known_hosts_check(KnownHosts::Add);
        if let Some(path) = &self.config_file {
            builder.config_file(path);
        }
        if let Some(path) = &self.keyfile {
            builder.keyfile(path);
        }
        if let Some(port) = self.port {
            builder.port(port);
        }
        builder.connect(&self.destination).await.map_err(to_io)
    }
}

/// Expand a leading `~/` to `$HOME` (ssh config does this; a plain path does not).
/// A path without the prefix, or one given when `$HOME` is unset, is returned
/// unchanged.
fn expand_tilde(path: &str) -> String {
    match (path.strip_prefix("~/"), std::env::var("HOME")) {
        (Some(rest), Ok(home)) => format!("{home}/{rest}"),
        _ => path.to_string(),
    }
}

/// Parse one `dest[=keyfile]` host spec into an [`SshConfig`].
///
/// A bare entry is a destination on the ambient ssh configuration; `dest=keyfile`
/// authenticates with an explicit private key (a leading `~/` is expanded). This
/// is the form used by both `RAYONETTE_HOSTS` and the children file.
#[must_use]
pub fn parse_host_spec(entry: &str) -> SshConfig {
    match entry.split_once('=') {
        Some((dest, keyfile)) => SshConfig::new(dest.trim()).keyfile(expand_tilde(keyfile.trim())),
        None => SshConfig::new(entry.trim()),
    }
}

/// Parse a host list: `dest[=keyfile]` entries separated by newlines, spaces, or
/// commas, with blank lines and `#` comments (whole-line or trailing) ignored.
///
/// Shared by the `RAYONETTE_HOSTS` env var and the children file (decentralized
/// per-node child list).
#[must_use]
pub fn parse_host_list(content: &str) -> Vec<SshConfig> {
    content
        .lines()
        .map(|line| line.split('#').next().unwrap_or("")) // drop comments
        .flat_map(|line| line.split([' ', ',']))
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(parse_host_spec)
        .collect()
}

/// A live ssh session that provisioning commands run over (a [`Remote`]).
#[derive(Debug)]
pub struct SshRemote {
    session: Arc<Session>,
}

impl SshRemote {
    /// Open a session to the host described by `config`.
    ///
    /// # Errors
    /// Returns an error if the ssh session cannot be established.
    pub async fn connect(config: &SshConfig) -> io::Result<Self> {
        Ok(Self {
            session: Arc::new(config.connect().await?),
        })
    }
}

impl Remote for SshRemote {
    async fn run(&self, command: &str) -> io::Result<CommandOutput> {
        let output = self
            .session
            .raw_command(command)
            .output()
            .await
            .map_err(to_io)?;
        Ok(CommandOutput::new(
            output.status.code().unwrap_or(-1),
            output.stdout,
            output.stderr,
        ))
    }

    async fn upload(&self, bytes: &[u8], dest: &str) -> io::Result<()> {
        let mut child = self
            .session
            .raw_command(format!("cat > {dest}"))
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .await
            .map_err(to_io)?;
        let mut stdin = child
            .stdin()
            .take()
            .expect("stdin was configured as a pipe");
        stdin.write_all(bytes).await?;
        stdin.shutdown().await?;
        drop(stdin);
        let output = child.wait_with_output().await.map_err(to_io)?;
        if output.status.success() {
            return Ok(());
        }
        Err(io::Error::other(format!(
            "rayonette: upload to {dest} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )))
    }
}

/// How the agent binary is obtained on the host before it is launched.
enum AgentSource {
    /// Spawn an already-built binary at this remote path (a cache hit, or a
    /// host provisioned out of band).
    Prebuilt(String),
    /// Run the provisioning ladder first, then spawn what it built.
    Build {
        source_tar: Vec<u8>,
        toolchain: String,
        binary_name: String,
    },
}

/// A [`Launch`] that starts the agent on a host over ssh.
///
/// Build it with [`Ssh::build`] to provision-then-spawn (the cold-host path),
/// or [`Ssh::prebuilt`] to spawn an already-built binary.
pub struct Ssh {
    config: SshConfig,
    source: AgentSource,
}

impl std::fmt::Debug for Ssh {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ssh")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl Ssh {
    /// Spawn the already-built agent binary at `binary_path` on the host.
    #[must_use]
    pub fn prebuilt(config: SshConfig, binary_path: impl Into<String>) -> Self {
        Self {
            config,
            source: AgentSource::Prebuilt(binary_path.into()),
        }
    }

    /// Provision the host from `source_tar` (the `extract()` bundle), building
    /// the `binary_name` agent with `toolchain`, then spawn it. Ladder
    /// transitions are emitted to the sink passed at launch.
    #[must_use]
    pub fn build(
        config: SshConfig,
        source_tar: Vec<u8>,
        toolchain: impl Into<String>,
        binary_name: impl Into<String>,
    ) -> Self {
        Self {
            config,
            source: AgentSource::Build {
                source_tar,
                toolchain: toolchain.into(),
                binary_name: binary_name.into(),
            },
        }
    }
}

impl Launch for Ssh {
    type Stream = Join<ChildStdout, ChildStdin>;
    type Guard = openssh::Child<Arc<Session>>;
    type Session = Arc<Session>;

    fn label(&self) -> String {
        self.config.port.map_or_else(
            || self.config.destination.clone(),
            |port| format!("{}:{port}", self.config.destination),
        )
    }

    async fn connect(&self) -> io::Result<Arc<Session>> {
        Ok(Arc::new(self.config.connect().await?))
    }

    async fn probe(&self, session: &Arc<Session>) -> io::Result<NodeProfile> {
        let remote = SshRemote {
            session: Arc::clone(session),
        };
        probe(&remote).await
    }

    async fn node_id(&self, session: &Arc<Session>) -> String {
        let remote = SshRemote {
            session: Arc::clone(session),
        };
        node_id(&remote).await
    }

    async fn activate(
        &self,
        session: Arc<Session>,
        events: &dyn EventSink,
    ) -> io::Result<(Connection<Self::Stream>, Self::Guard)> {
        let binary_path = match &self.source {
            AgentSource::Prebuilt(path) => path.clone(),
            AgentSource::Build {
                source_tar,
                toolchain,
                binary_name,
            } => {
                let remote = SshRemote {
                    session: Arc::clone(&session),
                };
                let provisioned = provision(
                    &remote,
                    source_tar,
                    toolchain,
                    binary_name,
                    &self.config.destination,
                    events,
                )
                .await?;
                provisioned.binary_path().to_string()
            }
        };
        // ssh does not forward the local environment, so the agent marker is
        // set inline in the remote shell command (the same `AGENT_ENV` the
        // subprocess launcher passes as a real env var).
        let command_line = format!("{AGENT_ENV}=1 {binary_path}");
        let mut child = session
            .arc_raw_command(command_line)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .await
            .map_err(to_io)?;
        let stdin = child
            .stdin()
            .take()
            .expect("stdin was configured as a pipe");
        let stdout = child
            .stdout()
            .take()
            .expect("stdout was configured as a pipe");
        Ok((Connection::new(join(stdout, stdin)), child))
    }
}

#[cfg(test)]
mod tests {
    use super::{expand_tilde, parse_host_list, parse_host_spec};

    #[test]
    fn expand_tilde_handles_home_and_plain_paths() {
        std::env::set_var("HOME", "/home/test");
        assert_eq!(expand_tilde("~/.ssh/key"), "/home/test/.ssh/key");
        // A path without the `~/` prefix is returned unchanged.
        assert_eq!(expand_tilde("/abs/key"), "/abs/key");
        assert_eq!(expand_tilde("relative"), "relative");
    }

    #[test]
    fn host_spec_parses_bare_and_keyed_destinations() {
        std::env::set_var("HOME", "/home/test");
        let bare = parse_host_spec("mac");
        assert!(format!("{bare:?}").contains("\"mac\""));
        assert!(format!("{bare:?}").contains("keyfile: None"));

        let keyed = parse_host_spec("user@host = ~/.ssh/id");
        assert!(format!("{keyed:?}").contains("\"user@host\""));
        assert!(format!("{keyed:?}").contains("/home/test/.ssh/id"));
    }

    #[test]
    fn host_list_ignores_comments_and_blanks_and_splits() {
        let body = "\
            # a comment\n\
            \n\
            mac\n\
            box-a, box-b   box-c  # trailing comment\n";
        let hosts = parse_host_list(body);
        let rendered: Vec<String> = hosts.iter().map(|h| format!("{h:?}")).collect();
        assert_eq!(hosts.len(), 4, "{rendered:?}");
        assert!(rendered[0].contains("\"mac\""));
        assert!(rendered[1].contains("\"box-a\""));
        assert!(rendered[3].contains("\"box-c\""));
    }
}
