//! Phase 2: the protocol running over a real spawned process's stdio.

use rayonet::coordinator::run_job;
use rayonet::framing::Connection;
use rayonet::observability::NoopSink;
use rayonet::process;
use tokio::process::Command;

fn agent_command() -> Command {
    Command::new(env!("CARGO_BIN_EXE_rayonet-test-agent"))
}

/// One labeled agent, for the single-agent subprocess jobs.
fn solo<S>(connection: Connection<S>) -> Vec<(String, Connection<S>)> {
    vec![("agent".to_string(), connection)]
}

#[tokio::test]
async fn runs_a_task_through_a_real_subprocess() {
    let (connection, agent) = process::spawn(agent_command()).expect("spawn agent");

    let inputs: Vec<u32> = (0..8).collect();
    let out: Vec<Result<u32, String>> =
        run_job(solo(connection), "double", inputs.clone(), &NoopSink)
            .await
            .unwrap();

    let expected: Vec<Result<u32, String>> = inputs.iter().map(|x| Ok(x * 2)).collect();
    assert_eq!(out, expected);
    assert!(format!("{agent:?}").contains("AgentProcess"));
}

#[tokio::test]
async fn agent_stderr_is_captured_including_panics() {
    let (connection, agent) = process::spawn(agent_command()).expect("spawn agent");

    let out: Vec<Result<u32, String>> = run_job(solo(connection), "boom", vec![5u32], &NoopSink)
        .await
        .unwrap();
    assert!(out[0].as_ref().unwrap_err().contains("boom"));

    let (_status, stderr) = agent.wait().await.unwrap();
    assert!(stderr.contains("about to panic"), "stderr was: {stderr:?}");
}

#[tokio::test]
async fn a_killed_agent_is_observed_as_a_failure() {
    let (connection, mut agent) = process::spawn(agent_command()).expect("spawn agent");
    agent.kill().await.expect("kill agent");

    let res = run_job::<_, u32, u32>(solo(connection), "double", vec![1, 2, 3], &NoopSink).await;
    assert!(res.is_err());
}

#[tokio::test]
async fn subprocess_launcher_connects_and_runs() {
    use rayonet::fleet::{Launch, Subprocess};

    let launcher = Subprocess::command(env!("CARGO_BIN_EXE_rayonet-test-agent"));
    assert!(format!("{launcher:?}").contains("Subprocess"));

    let () = launcher.connect().await.expect("connect");
    let (connection, _guard) = launcher.activate((), &NoopSink).await.expect("activate");
    let out: Vec<Result<u32, String>> =
        run_job(solo(connection), "double", (0..5u32).collect(), &NoopSink)
            .await
            .unwrap();
    assert_eq!(out, (0..5u32).map(|x| Ok(x * 2)).collect::<Vec<_>>());

    // `current_exe` constructs without spawning.
    let _ = Subprocess::current_exe().expect("current exe");
}

#[tokio::test]
async fn the_binary_exits_when_not_in_agent_mode() {
    // Launched directly, without the agent marker the coordinator would set.
    let status = Command::new(env!("CARGO_BIN_EXE_rayonet-test-agent"))
        .status()
        .await
        .expect("run binary");
    assert_eq!(status.code(), Some(2));
}
