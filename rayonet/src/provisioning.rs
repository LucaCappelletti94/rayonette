//! Getting a cold ssh-only host to a running agent (PLAN.md Phase 4).
//!
//! [`provision`] drives the ladder: probe the host, install rust if missing,
//! ship and unpack the crate source, build it, and report the agent binary
//! path. It runs against any [`Remote`] (real ssh in Phase 4b, a mock at
//! level 1), emitting [`NodeState`] transitions to an [`EventSink`] as it goes.
//! A content-addressed build directory lets a second run skip the rebuild.

use std::future::Future;
use std::io;

use crate::capability::{self, CpuArch, NodeProfile, Os};
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

/// The content-addressed build directory for `source_tar` on a host, keyed also
/// by `variant` (the architecture and target-cpu signature).
///
/// Shell-unexpanded (`$HOME/...`) so it is valid in a remote command without the
/// coordinator knowing the remote home. The `variant` keeps a `-C target-cpu`
/// build cached per microarchitecture, so a native binary is never reused on a
/// CPU that would fault on it (the build dir is otherwise identical on every host
/// with the same source).
fn remote_cache_dir(source_tar: &[u8], variant: &str) -> String {
    format!(
        "$HOME/.cache/rayonet/{}-{variant}",
        content_hash(source_tar)
    )
}

/// Where [`provision`] places (and a cache hit finds) the built agent binary, for
/// a given source and architecture `variant`.
fn remote_binary_path(source_tar: &[u8], variant: &str, binary_name: &str) -> String {
    format!(
        "{}/target/release/{binary_name}",
        remote_cache_dir(source_tar, variant)
    )
}

/// A short signature of the build variant: the host's CPU architecture plus the
/// `target_cpu` setting. Two builds share a cache entry only when both match, so
/// a native build (whose code depends on the exact microarchitecture) is keyed to
/// the CPU it was built for.
fn build_variant(arch: &CpuArch, target_cpu: &str) -> String {
    let signature = format!("{} {} {target_cpu}", arch.isa, arch.features.join(" "));
    content_hash(signature.as_bytes())[..16].to_string()
}

/// The agent build's `-C target-cpu` value: `native` by default (squeeze every
/// instruction the host offers), overridable with `RAYONET_TARGET_CPU` (for
/// example `x86-64-v2` for a portable build).
fn target_cpu() -> String {
    std::env::var("RAYONET_TARGET_CPU").unwrap_or_else(|_| "native".to_string())
}

/// Probe a host and return its build target: the cache `variant` (architecture
/// plus target-cpu signature) and the `target-cpu` value to compile with.
async fn build_target<R: Remote>(remote: &R) -> (String, String) {
    let os = capability::parse_os(&run_or_empty(remote, "uname -s").await);
    let arch = cpu_arch(remote, &os).await;
    let cpu = target_cpu();
    (build_variant(&arch, &cpu), cpu)
}

/// Where [`provision`] will place (or find cached) the agent binary on a host.
///
/// Resolved for the current source and that host's architecture, so a caller can
/// locate the same cache entry the provisioner uses (for example to seed or
/// inspect it).
///
/// # Errors
/// Never returns an error: an unreadable probe just yields a less specific path.
pub async fn remote_agent_path<R: Remote>(
    remote: &R,
    source_tar: &[u8],
    binary_name: &str,
) -> String {
    let (variant, _) = build_target(remote).await;
    remote_binary_path(source_tar, &variant, binary_name)
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
    // Probe: confirm the host answers and learn its CPU architecture, which keys
    // the build cache so a target-cpu=native binary is never reused on a CPU it
    // would fault on.
    events.emit(Event::node(host, NodeState::Probing));
    require_success(host, "probe", &remote.run("uname -sm").await?)?;
    let (variant, cpu) = build_target(remote).await;
    let dir = remote_cache_dir(source_tar, &variant);
    let binary_path = remote_binary_path(source_tar, &variant, binary_name);

    // Cache hit: a prior run already built this exact source for this CPU here.
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

    // Ship and unpack the crate source. The tarball is kept beside the build
    // dir, not inside it: the build dir is what gets re-tarred when a relay node
    // is itself built here, so a tarball inside it would nest into the bundle
    // that node re-ships to its own children (and macOS `tar` refuses to extract
    // an archive over itself).
    events.emit(Event::node(host, NodeState::Syncing));
    let mkdir = format!("mkdir -p {dir}");
    require_success(host, "mkdir", &remote.run(&mkdir).await?)?;
    let tarball = format!("{dir}.tar");
    remote.upload(source_tar, &tarball).await?;
    let extract = format!("tar -xf {tarball} -C {dir}");
    require_success(host, "extract", &remote.run(&extract).await?)?;

    // Build just the consumer's package (not every member of a shipped
    // workspace) on the host, tuned to this host's CPU so a distributed task runs
    // as fast as the hardware allows.
    events.emit(Event::node(host, NodeState::Building));
    let build = format!(
        "cd {dir} && PATH=\"$HOME/.cargo/bin:$PATH\" \
         RUSTFLAGS=\"-C target-cpu={cpu}\" cargo build --release -p {binary_name}"
    );
    require_success(host, "build", &remote.run(&build).await?)?;

    events.emit(Event::node(host, NodeState::Ready));
    Ok(Provisioned { binary_path })
}

/// Probe a host's [`NodeProfile`] by running detection commands over `remote`,
/// dispatching on the OS and feeding their output to the capability parsers.
///
/// Individual capability probes that fail or are missing (no `nvidia-smi`, say)
/// are treated as "absent", not fatal, so only a failure of the OS probe itself
/// errors.
///
/// # Errors
/// Returns an error if the `uname -s` probe cannot run.
pub async fn probe<R: Remote>(remote: &R) -> io::Result<NodeProfile> {
    let os = capability::parse_os(&run_stdout(remote, "uname -s").await?);
    let (cores, ram_mb, gpus) = if os == Os::MacOs {
        (
            capability::parse_cores(&run_or_empty(remote, "sysctl -n hw.ncpu").await),
            capability::parse_macos_ram_mb(&run_or_empty(remote, "sysctl -n hw.memsize").await),
            capability::parse_macos_gpus(
                &run_or_empty(remote, "system_profiler SPDisplaysDataType").await,
            ),
        )
    } else {
        let cores = capability::parse_cores(&run_or_empty(remote, "nproc").await);
        let ram_mb =
            capability::parse_linux_ram_mb(&run_or_empty(remote, "cat /proc/meminfo").await);
        let mut gpus = capability::parse_nvidia_smi(
            &run_or_empty(
                remote,
                "nvidia-smi --query-gpu=name,memory.total --format=csv,noheader,nounits",
            )
            .await,
        );
        gpus.extend(capability::parse_rocminfo(
            &run_or_empty(remote, "rocminfo").await,
        ));
        (cores, ram_mb, gpus)
    };
    let arch = cpu_arch(remote, &os).await;
    Ok(NodeProfile {
        os,
        arch,
        cores,
        ram_mb,
        gpus,
    })
}

/// Probe a host's CPU architecture over `remote`.
///
/// The instruction set comes from `uname -m` and the feature flags from
/// `/proc/cpuinfo` on Linux or `sysctl` on macOS. Best-effort, like the other
/// probes: a missing source just yields fewer features.
pub async fn cpu_arch<R: Remote>(remote: &R, os: &Os) -> CpuArch {
    let isa = run_or_empty(remote, "uname -m").await;
    let features = if *os == Os::MacOs {
        run_or_empty(
            remote,
            "sysctl -n machdep.cpu.features machdep.cpu.leaf7_features 2>/dev/null; \
             sysctl -a 2>/dev/null | sed -n 's/^hw\\.optional\\.\\([A-Za-z0-9_]*\\): 1$/\\1/p'",
        )
        .await
    } else {
        run_or_empty(
            remote,
            "grep -m1 -E '^(flags|Features)[[:space:]]*:' /proc/cpuinfo",
        )
        .await
    };
    capability::parse_cpu_arch(&isa, &features)
}

/// A shell command printing a stable machine id: the OS id source (Linux
/// `/etc/machine-id`, macOS `IOPlatformUUID`), falling back to a generated id
/// persisted under the cache so the same node yields the same id across paths and
/// runs. The id is the first non-empty line of its output.
const NODE_ID_COMMAND: &str = "\
    { cat /etc/machine-id 2>/dev/null \
      || ioreg -rd1 -c IOPlatformExpertDevice 2>/dev/null \
         | sed -n 's/.*\"IOPlatformUUID\" = \"\\(.*\\)\"/\\1/p'; } \
    | grep -m1 . \
    || { d=\"$HOME/.cache/rayonet\"; mkdir -p \"$d\"; f=\"$d/node-id\"; \
         cat \"$f\" 2>/dev/null \
         || { id=$(od -An -N16 -tx1 /dev/urandom | tr -d ' \\n'); echo \"$id\" >\"$f\"; echo \"$id\"; }; }";

/// Read a host's stable node id over `remote` (see [`NODE_ID_COMMAND`]).
///
/// Best-effort and infallible: a transport failure or an empty result yields
/// `"unknown"`, so a node always has some id rather than dropping out.
pub async fn node_id<R: Remote>(remote: &R) -> String {
    let out = run_or_empty(remote, NODE_ID_COMMAND).await;
    out.lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or("unknown")
        .to_string()
}

/// Run `command` and return its stdout (a non-zero exit still yields its
/// stdout); a transport failure propagates.
async fn run_stdout<R: Remote>(remote: &R, command: &str) -> io::Result<String> {
    let out = remote.run(command).await?;
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// Run `command`, returning its stdout only if it succeeded; an error or a
/// non-zero exit (a missing tool like `nvidia-smi`) yields an empty string.
async fn run_or_empty<R: Remote>(remote: &R, command: &str) -> String {
    match remote.run(command).await {
        Ok(out) if out.status == 0 => String::from_utf8_lossy(&out.stdout).into_owned(),
        _ => String::new(),
    }
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
        let upload_dest = {
            let uploads = remote.uploads.lock().unwrap();
            assert_eq!(uploads.len(), 1);
            uploads[0].clone()
        };
        // The source tarball must live beside the build dir, never inside it, so
        // re-tarring the build dir (the cascade) cannot nest the bundle.
        let dir = result.binary_path.trim_end_matches("/target/release/agent");
        assert!(
            !upload_dest.starts_with(&format!("{dir}/")),
            "tarball {upload_dest} must be outside the build dir {dir}"
        );
        assert!(remote.calls().iter().any(|c| c.contains("rustup")));
        assert!(remote.calls().iter().any(|c| c.contains("cargo build")));
        // The agent is built tuned to the host's CPU for maximum task throughput.
        assert!(
            remote
                .calls()
                .iter()
                .any(|c| c.contains("target-cpu=native")),
            "the build should tune to the native CPU"
        );
        assert!(result.binary_path.ends_with("/target/release/agent"));
    }

    #[test]
    fn build_variant_separates_architectures_and_target_cpus() {
        use super::{build_variant, CpuArch};
        let avx2 = CpuArch {
            isa: "x86_64".to_string(),
            features: vec!["avx2".to_string(), "sse2".to_string()],
        };
        let avx512 = CpuArch {
            isa: "x86_64".to_string(),
            features: vec![
                "avx2".to_string(),
                "avx512f".to_string(),
                "sse2".to_string(),
            ],
        };
        // A native binary's cache key changes with the microarchitecture, so it is
        // never reused on a CPU missing a feature (which would fault), even when
        // the source is identical.
        assert_ne!(
            build_variant(&avx2, "native"),
            build_variant(&avx512, "native")
        );
        // The same arch with a different target-cpu is also a distinct entry.
        assert_ne!(
            build_variant(&avx2, "native"),
            build_variant(&avx2, "x86-64-v2")
        );
        // The same arch and target-cpu reuse the same entry.
        assert_eq!(
            build_variant(&avx2, "native"),
            build_variant(&avx2, "native")
        );
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

    // A scripted host for probe tests: `uname -s` returns `os`, other commands
    // match by substring, and a miss is a non-zero exit (like a missing tool).
    struct ProbeHost {
        os: &'static str,
        replies: Vec<(&'static str, &'static str)>,
    }

    fn out(status: i32, stdout: &str) -> CommandOutput {
        CommandOutput {
            status,
            stdout: stdout.as_bytes().to_vec(),
            stderr: Vec::new(),
        }
    }

    impl Remote for ProbeHost {
        async fn run(&self, command: &str) -> std::io::Result<CommandOutput> {
            if command.contains("uname -s") {
                return Ok(out(0, self.os));
            }
            for (needle, stdout) in &self.replies {
                if command.contains(needle) {
                    return Ok(out(0, stdout));
                }
            }
            Ok(out(1, ""))
        }
        async fn upload(&self, _bytes: &[u8], _dest: &str) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn node_id_reads_the_machine_id() {
        // The id command's output is parsed as its first non-empty line.
        let host = ProbeHost {
            os: "Linux",
            replies: vec![("machine-id", "  abcdef0123456789\n")],
        };
        assert_eq!(super::node_id(&host).await, "abcdef0123456789");
    }

    #[tokio::test]
    async fn node_id_falls_back_to_unknown_when_empty() {
        let host = ProbeHost {
            os: "Linux",
            replies: Vec::new(),
        };
        assert_eq!(super::node_id(&host).await, "unknown");
    }

    #[tokio::test]
    async fn probe_linux_with_nvidia() {
        use crate::capability::{GpuRuntime, Os};
        let host = ProbeHost {
            os: "Linux",
            replies: vec![
                ("nproc", "64\n"),
                ("/proc/meminfo", "MemTotal:      131923148 kB\n"),
                ("nvidia-smi", "NVIDIA GeForce RTX 4090, 24564\n"),
            ],
        };
        let p = super::probe(&host).await.unwrap();
        assert_eq!(p.os, Os::Linux);
        assert_eq!(p.cores, 64);
        assert_eq!(p.ram_mb, 131_923_148 / 1024);
        assert_eq!(p.gpus.len(), 1);
        assert_eq!(p.gpus[0].runtime, Some(GpuRuntime::Cuda));

        // Probing is read-only; the scripted host still satisfies the full
        // `Remote` contract, whose upload is a no-op here.
        host.upload(b"", "/dev/null").await.unwrap();
    }

    #[tokio::test]
    async fn probe_macos_with_metal() {
        use crate::capability::{GpuRuntime, Os};
        let host = ProbeHost {
            os: "Darwin",
            replies: vec![
                ("hw.ncpu", "12\n"),
                ("hw.memsize", "137438953472\n"),
                ("system_profiler", "      Chipset Model: Apple M2 Pro\n"),
            ],
        };
        let p = super::probe(&host).await.unwrap();
        assert_eq!(p.os, Os::MacOs);
        assert_eq!(p.cores, 12);
        assert_eq!(p.ram_mb, 131_072);
        assert_eq!(p.gpus.len(), 1);
        assert_eq!(p.gpus[0].runtime, Some(GpuRuntime::Metal));
    }

    #[tokio::test]
    async fn probe_treats_missing_gpu_tools_as_no_gpu() {
        use crate::capability::Os;
        let host = ProbeHost {
            os: "Linux",
            replies: vec![
                ("nproc", "8\n"),
                ("/proc/meminfo", "MemTotal: 8000000 kB\n"),
            ],
        };
        let p = super::probe(&host).await.unwrap();
        assert_eq!(p.os, Os::Linux);
        assert!(p.gpus.is_empty());
    }
}
