//! Level-3 ssh-to-localhost smoke tests (PLAN.md Phase 4).
//!
//! These exercise the real openssh path against `localhost`. They are
//! `#[ignore]`d so the default `cargo test` stays ssh-free; run them with
//! `cargo test -- --include-ignored` on a host where this user can ssh into
//! itself with the `rayonet_localhost_ed25519` key (CI sets that up).

use rayonet::capability::Os;
use rayonet::coordinator::run_job;
use rayonet::fleet::Launch;
use rayonet::observability::{NodeState, NoopSink};
use rayonet::provisioning::{remote_agent_path, Remote};
use rayonet::ssh::{Ssh, SshConfig, SshRemote};
use rayonet::testing::EventRecorder as Recorder;

/// The dedicated test key's path.
fn keyfile() -> String {
    let home = std::env::var("HOME").expect("HOME is set");
    format!("{home}/.ssh/rayonet_localhost_ed25519")
}

/// ssh into this same machine, authenticating with the dedicated test key.
fn localhost() -> SshConfig {
    SshConfig::new("localhost").keyfile(keyfile())
}

#[tokio::test]
#[ignore = "needs ssh localhost self-login; run with --include-ignored"]
async fn ssh_remote_runs_a_command() {
    let remote = SshRemote::connect(&localhost()).await.unwrap();
    let out = remote.run("echo hello-rayonet").await.unwrap();
    assert_eq!(out.status, 0);
    assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "hello-rayonet");
}

#[tokio::test]
#[ignore = "needs ssh localhost self-login; run with --include-ignored"]
async fn ssh_remote_uploads_and_rejects_bad_paths() {
    let remote = SshRemote::connect(&localhost()).await.unwrap();
    let dest = "/tmp/rayonet-ssh-upload-test.bin";
    remote.upload(b"payload-bytes", dest).await.unwrap();
    let out = remote.run(&format!("cat {dest}")).await.unwrap();
    assert_eq!(out.stdout, b"payload-bytes");
    remote.run(&format!("rm -f {dest}")).await.unwrap();

    // A write into a non-existent directory fails, reported as an error.
    let bad = remote.upload(b"x", "/no/such/dir/file").await;
    assert!(bad.is_err(), "{bad:?}");
}

#[tokio::test]
#[ignore = "needs ssh localhost self-login; run with --include-ignored"]
async fn ssh_launch_runs_a_task_end_to_end() {
    let agent = env!("CARGO_BIN_EXE_rayonet-test-agent");
    let ssh = Ssh::prebuilt(localhost(), agent);
    assert!(format!("{ssh:?}").contains("Ssh"));
    assert_eq!(ssh.label(), "localhost");
    assert_eq!(
        Ssh::prebuilt(localhost().port(2222), agent).label(),
        "localhost:2222"
    );

    let session = ssh.connect().await.unwrap();

    // Probing the real localhost reports a usable profile (this host runs the
    // CI, so it is Linux with at least one core).
    let profile = ssh.probe(&session).await.unwrap();
    assert_eq!(profile.os, Os::Linux);
    assert!(profile.cores >= 1, "{profile:?}");

    // The real machine id is read over ssh and is stable (non-empty, repeatable).
    let id = ssh.node_id(&session).await;
    assert!(!id.is_empty(), "node id should be non-empty");
    assert_eq!(id, ssh.node_id(&session).await, "node id is stable");

    let (connection, guard) = ssh.activate(session, &NoopSink).await.unwrap();
    let out: Vec<Result<u32, String>> = run_job(
        vec![("localhost".to_string(), connection)],
        "double",
        vec![1u32, 2, 3],
        &NoopSink,
    )
    .await
    .unwrap();

    assert_eq!(out, vec![Ok(2), Ok(4), Ok(6)]);
    drop(guard);
}

#[tokio::test]
#[ignore = "needs ssh localhost self-login; run with --include-ignored"]
async fn ssh_remote_honors_an_explicit_port() {
    let remote = SshRemote::connect(&localhost().port(22)).await.unwrap();
    assert_eq!(remote.run("true").await.unwrap().status, 0);
}

#[tokio::test]
#[ignore = "needs ssh localhost self-login; run with --include-ignored"]
async fn ssh_connect_to_unknown_host_errors() {
    // `.invalid` never resolves (RFC 6761), so the session fails to open.
    let config = SshConfig::new("rayonet.invalid").keyfile("/nonexistent");
    assert!(SshRemote::connect(&config).await.is_err());
}

/// `Ssh::build` over a warm cache: provision takes the cache-hit path (no slow
/// compile), then spawns the seeded binary and runs a task. Also drives the
/// `config_file` route (an alias whose key and host live in an ssh config) and
/// asserts the emitted ladder transitions. The cold compile-and-build path is
/// covered by the docker harness.
#[tokio::test]
#[ignore = "needs ssh localhost self-login; run with --include-ignored"]
async fn ssh_build_with_warm_cache_provisions_and_runs() {
    let home = std::env::var("HOME").expect("HOME is set");
    let user = std::env::var("USER").expect("USER is set");

    // Reach localhost via an alias defined in a config file (exercises the
    // `ProxyJump`/config-file route used by the docker harness).
    let config_path = std::env::temp_dir().join("rayonet-build-test-ssh-config");
    std::fs::write(
        &config_path,
        format!(
            "Host rayonet-local\n  HostName localhost\n  User {user}\n  \
             IdentityFile {key}\n  IdentitiesOnly yes\n  StrictHostKeyChecking no\n  \
             UserKnownHostsFile /dev/null\n",
            key = keyfile(),
        ),
    )
    .unwrap();
    let config = SshConfig::new("rayonet-local").config_file(&config_path);

    // Seed the content-addressed cache with the test agent so provision hits it.
    // The cache path is keyed by this host's architecture, so resolve it by
    // probing the host the same way the provisioner does.
    let tar = b"warm-cache-seed".to_vec();
    let probe = SshRemote::connect(&config).await.unwrap();
    let remote_path = remote_agent_path(&probe, &tar, "rayonet-test-agent").await;
    let local_path = remote_path.replace("$HOME", &home);
    let dir = std::path::Path::new(&local_path).parent().unwrap();
    std::fs::create_dir_all(dir).unwrap();
    std::fs::copy(env!("CARGO_BIN_EXE_rayonet-test-agent"), &local_path).unwrap();

    let events = Recorder::default();
    let ssh = Ssh::build(config, tar, "stable", "rayonet-test-agent");

    let session = ssh.connect().await.unwrap();
    let (connection, guard) = ssh.activate(session, &events).await.unwrap();
    let out: Vec<Result<u32, String>> = run_job(
        vec![("rayonet-local".to_string(), connection)],
        "double",
        vec![5u32],
        &NoopSink,
    )
    .await
    .unwrap();
    drop(guard);

    assert_eq!(out, vec![Ok(10)]);
    assert_eq!(events.states(), vec![NodeState::Probing, NodeState::Ready]);

    let cache_root = format!("{home}/.cache/rayonet");
    let _ = std::fs::remove_dir_all(&cache_root);
}
