//! Getting a cold ssh-only host to a running agent (PLAN.md Phase 4).
//!
//! [`provision`] drives the ladder: probe the host, install rust if missing,
//! ship and unpack the crate source, build it, and report the agent binary
//! path. It runs against any [`Remote`] (real ssh in Phase 4b, a mock at
//! level 1), emitting [`NodeState`] transitions to an [`EventSink`] as it goes.
//! A content-addressed build directory lets a second run skip the rebuild.

use std::future::Future;
use std::io;

use crate::observability::{Event, EventSink, NodeState};

/// The captured result of running a command to completion on a [`Remote`].
#[derive(Debug, Clone)]
pub struct CommandOutput {
    /// Process exit code (or a negative value if it was killed by a signal).
    pub status: i32,
    /// Captured standard output.
    pub stdout: Vec<u8>,
    /// Captured standard error.
    pub stderr: Vec<u8>,
}

/// A host the provisioner can run commands on and upload files to.
///
/// Abstracted so the ladder is proven in-process against a mock before any
/// real ssh is involved (the same testing seam as [`crate::fleet::Launch`]).
pub trait Remote {
    /// Run `command` (a shell line) to completion and capture its output.
    ///
    /// # Errors
    /// Returns an error if the command could not be started or the channel
    /// failed; a non-zero exit is reported in [`CommandOutput::status`], not as
    /// an error.
    fn run(&self, command: &str) -> impl Future<Output = io::Result<CommandOutput>> + Send;

    /// Upload `bytes` to `dest` on the remote, overwriting any existing file.
    ///
    /// # Errors
    /// Returns an error if the transfer fails.
    fn upload(&self, bytes: &[u8], dest: &str) -> impl Future<Output = io::Result<()>> + Send;
}

/// The outcome of [`provision`]: where the built agent binary lives on the host.
#[derive(Debug, Clone)]
pub struct Provisioned {
    /// Absolute (shell-expanded) path to the built agent binary on the remote.
    pub binary_path: String,
}

/// A non-cryptographic hash is not enough for a content cache; use blake3.
fn content_hash(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}

/// The content-addressed build directory for `source_tar` on a host.
///
/// Shell-unexpanded (`$HOME/...`) so it is valid in a remote command without
/// the coordinator knowing the remote home.
fn remote_cache_dir(source_tar: &[u8]) -> String {
    format!("$HOME/.cache/rayonet/{}", content_hash(source_tar))
}

/// Where [`provision`] places (and a cache hit finds) the built agent binary.
///
/// The path is content-addressed on `source_tar`, so the same source maps to
/// the same location on every host, which is what makes a repeat run a cache
/// hit.
#[must_use]
pub fn remote_binary_path(source_tar: &[u8], binary_name: &str) -> String {
    format!(
        "{}/target/release/{binary_name}",
        remote_cache_dir(source_tar)
    )
}

/// Turn a non-zero exit into a host-named error so the caller can requeue.
fn require_success(host: &str, step: &str, out: &CommandOutput) -> io::Result<()> {
    if out.status == 0 {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&out.stderr);
    Err(io::Error::other(format!(
        "rayonet: {host}: {step} failed (exit {}): {}",
        out.status,
        stderr.trim()
    )))
}

/// Provision `host` to a built agent and return where its binary lives.
///
/// Ships `source_tar` (the `extract()` bundle), installing the `toolchain` via
/// rustup if rust is absent, and builds the crate whose agent binary is named
/// `binary_name`. A content-addressed build dir makes a repeat run a cache hit
/// that skips straight to [`NodeState::Ready`].
///
/// # Errors
/// Returns an error if any provisioning step fails on the host; the message
/// names the host and the failed step so the caller can requeue its tasks.
pub async fn provision<R: Remote>(
    remote: &R,
    source_tar: &[u8],
    toolchain: &str,
    binary_name: &str,
    host: &str,
    events: &dyn EventSink,
) -> io::Result<Provisioned> {
    let dir = remote_cache_dir(source_tar);
    let binary_path = remote_binary_path(source_tar, binary_name);

    // Probe: cheapest possible round-trip, confirms the host answers at all.
    events.emit(Event::node(host, NodeState::Probing));
    require_success(host, "probe", &remote.run("uname -sm").await?)?;

    // Cache hit: a prior run already built this exact source here.
    if remote.run(&format!("test -x {binary_path}")).await?.status == 0 {
        events.emit(Event::node(host, NodeState::Ready));
        return Ok(Provisioned { binary_path });
    }

    // Install rust user-locally only when it is missing.
    let cargo = "command -v cargo >/dev/null 2>&1 || test -x \"$HOME/.cargo/bin/cargo\"";
    if remote.run(cargo).await?.status != 0 {
        events.emit(Event::node(host, NodeState::Installing));
        // Download then run as separate `&&`-chained commands: piping curl into
        // sh would mask a curl failure (the pipe's status is sh's), so a host
        // with no network would look like it installed and fail later.
        let install = format!(
            "curl --proto '=https' --tlsv1.2 --connect-timeout 20 -sSf \
             -o /tmp/rustup-init.sh https://sh.rustup.rs \
             && sh /tmp/rustup-init.sh -y --default-toolchain {toolchain}"
        );
        require_success(host, "rustup install", &remote.run(&install).await?)?;
    }

    // Ship and unpack the crate source.
    events.emit(Event::node(host, NodeState::Syncing));
    let mkdir = format!("mkdir -p {dir}");
    require_success(host, "mkdir", &remote.run(&mkdir).await?)?;
    let tarball = format!("{dir}/source.tar");
    remote.upload(source_tar, &tarball).await?;
    let extract = format!("tar -xf {dir}/source.tar -C {dir}");
    require_success(host, "extract", &remote.run(&extract).await?)?;

    // Build the agent on the host.
    events.emit(Event::node(host, NodeState::Building));
    let build = format!("cd {dir} && PATH=\"$HOME/.cargo/bin:$PATH\" cargo build --release");
    require_success(host, "build", &remote.run(&build).await?)?;

    events.emit(Event::node(host, NodeState::Ready));
    Ok(Provisioned { binary_path })
}

#[cfg(test)]
mod tests {
    use super::{content_hash, provision, CommandOutput, NodeState, Provisioned, Remote};
    use std::sync::Mutex;

    /// A scripted host: it answers the ladder's probes by configuration and
    /// records every command and upload so tests can assert the call sequence.
    struct MockRemote {
        cargo_present: bool,
        cached: bool,
        build_ok: bool,
        calls: Mutex<Vec<String>>,
        uploads: Mutex<Vec<String>>,
    }

    impl MockRemote {
        fn new(cargo_present: bool, cached: bool, build_ok: bool) -> Self {
            Self {
                cargo_present,
                cached,
                build_ok,
                calls: Mutex::new(Vec::new()),
                uploads: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
    }

    fn ok() -> CommandOutput {
        CommandOutput {
            status: 0,
            stdout: Vec::new(),
            stderr: Vec::new(),
        }
    }

    fn fail() -> CommandOutput {
        CommandOutput {
            status: 1,
            stdout: Vec::new(),
            stderr: b"boom".to_vec(),
        }
    }

    impl Remote for MockRemote {
        async fn run(&self, command: &str) -> std::io::Result<CommandOutput> {
            self.calls.lock().unwrap().push(command.to_string());
            let out = if command.contains("uname") {
                CommandOutput {
                    status: 0,
                    stdout: b"Linux x86_64\n".to_vec(),
                    stderr: Vec::new(),
                }
            } else if command.contains("command -v cargo") {
                if self.cargo_present {
                    ok()
                } else {
                    fail()
                }
            } else if command.contains("target/release") && command.contains("test -x") {
                if self.cached {
                    ok()
                } else {
                    fail()
                }
            } else if command.contains("cargo build") {
                if self.build_ok {
                    ok()
                } else {
                    fail()
                }
            } else {
                ok()
            };
            Ok(out)
        }

        async fn upload(&self, _bytes: &[u8], dest: &str) -> std::io::Result<()> {
            self.uploads.lock().unwrap().push(dest.to_string());
            Ok(())
        }
    }

    use crate::testing::EventRecorder as Recorder;

    #[tokio::test]
    async fn cold_host_runs_the_full_ladder() {
        let remote = MockRemote::new(false, false, true);
        let events = Recorder::default();

        let result = provision(&remote, b"tarbytes", "stable", "agent", "h1", &events)
            .await
            .unwrap();

        assert_eq!(
            events.states(),
            vec![
                NodeState::Probing,
                NodeState::Installing,
                NodeState::Syncing,
                NodeState::Building,
                NodeState::Ready,
            ]
        );
        assert_eq!(remote.uploads.lock().unwrap().len(), 1);
        assert!(remote.calls().iter().any(|c| c.contains("rustup")));
        assert!(remote.calls().iter().any(|c| c.contains("cargo build")));
        assert!(result.binary_path.ends_with("/target/release/agent"));
    }

    #[tokio::test]
    async fn existing_rust_skips_install() {
        let remote = MockRemote::new(true, false, true);
        let events = Recorder::default();

        provision(&remote, b"tarbytes", "stable", "agent", "h1", &events)
            .await
            .unwrap();

        assert_eq!(
            events.states(),
            vec![
                NodeState::Probing,
                NodeState::Syncing,
                NodeState::Building,
                NodeState::Ready,
            ]
        );
        assert!(!remote.calls().iter().any(|c| c.contains("rustup")));
    }

    #[tokio::test]
    async fn cache_hit_skips_build() {
        let remote = MockRemote::new(true, true, true);
        let events = Recorder::default();

        let result = provision(&remote, b"tarbytes", "stable", "agent", "h1", &events)
            .await
            .unwrap();

        assert_eq!(events.states(), vec![NodeState::Probing, NodeState::Ready]);
        assert!(remote.uploads.lock().unwrap().is_empty());
        assert!(!remote.calls().iter().any(|c| c.contains("cargo build")));
        assert!(result.binary_path.ends_with("/target/release/agent"));
    }

    #[tokio::test]
    async fn build_failure_errors_naming_the_host() {
        let remote = MockRemote::new(true, false, false);
        let events = Recorder::default();

        let err = provision(&remote, b"tarbytes", "stable", "agent", "h1", &events)
            .await
            .unwrap_err();

        let message = err.to_string();
        assert!(message.contains("h1"), "{message}");
        assert!(message.contains("build"), "{message}");
    }

    #[test]
    fn content_hash_is_deterministic_and_input_sensitive() {
        assert_eq!(content_hash(b"abc"), content_hash(b"abc"));
        assert_ne!(content_hash(b"abc"), content_hash(b"abd"));
    }

    #[test]
    fn provisioning_types_expose_debug_clone_and_eq() {
        let provisioned = Provisioned {
            binary_path: "/x".to_string(),
        };
        let provisioned_copy = provisioned.clone();
        assert_eq!(provisioned.binary_path, provisioned_copy.binary_path);
        assert!(format!("{provisioned:?}").contains("/x"));

        let output = CommandOutput {
            status: 0,
            stdout: Vec::new(),
            stderr: Vec::new(),
        };
        let output_copy = output.clone();
        assert_eq!(output.status, output_copy.status);
        assert!(format!("{output:?}").contains("status"));
    }
}
