//! The docker harness consumer: one binary, two roles.
//!
//! Built and shipped to the leaf containers, where it runs as an agent. Run on
//! the host (coordinator role) it provisions the leaves named in `RAYONET_LEAVES`
//! over ssh, ships the whole-workspace source tar at `RAYONET_SOURCE_TAR`,
//! builds itself on each leaf, then runs a distributed `.netmap`. It prints each
//! node-state transition (so the harness can assert the ladder) and, at the end,
//! a per-host completed-task count (so the harness can assert work-share).

use std::sync::{Arc, Mutex};

use rayonet::fleet::Fleet;
use rayonet::observability::{Event, EventSink, RunState};
use rayonet::process;
use rayonet::ssh::{Ssh, SshConfig};

fn double(x: u32) -> u32 {
    x * 2
}

/// A CPU-bound task heavy enough that a throttled host visibly drains fewer of
/// them (tens of milliseconds of pure compute each).
fn crunch(x: u32) -> u32 {
    let mut acc = x;
    for i in 0..200_000_000u32 {
        acc = acc.wrapping_mul(31).wrapping_add(i);
    }
    acc
}

rayonet::embed_microcrates!();

/// Prints each node-state transition and reduces the stream so the run's
/// per-host work-share can be reported when it finishes.
#[derive(Default)]
struct ConsoleSink {
    state: Mutex<RunState>,
}

impl EventSink for ConsoleSink {
    fn emit(&self, event: Event) {
        self.state.lock().unwrap().apply(&event);
        if let Event::Node { host, state } = &event {
            println!("state {host} {state:?}");
        }
    }
}

fn env(key: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| panic!("{key} must be set"))
}

#[tokio::main]
async fn main() {
    if process::is_agent() {
        process::run_agent(__rayonet_registry())
            .await
            .expect("agent failed");
        return;
    }

    let config_path = env("RAYONET_SSH_CONFIG");
    let leaves = env("RAYONET_LEAVES");
    let tar = std::fs::read(env("RAYONET_SOURCE_TAR")).expect("read source tar");
    let toolchain = std::env::var("RAYONET_TOOLCHAIN").unwrap_or_else(|_| "stable".to_string());
    let task = std::env::var("RAYONET_TASK").unwrap_or_else(|_| "double".to_string());
    let count: u32 = std::env::var("RAYONET_COUNT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);

    let sink = Arc::new(ConsoleSink::default());
    let launchers: Vec<Ssh> = leaves
        .split(',')
        .map(str::trim)
        .filter(|leaf| !leaf.is_empty())
        .map(|leaf| {
            Ssh::build(
                SshConfig::new(leaf).config_file(&config_path),
                tar.clone(),
                toolchain.clone(),
                "rayonet-docker-consumer",
            )
        })
        .collect();
    let fleet = Fleet::observed(launchers, sink.clone());

    let inputs: Vec<u32> = (0..count).collect();
    let out = if task == "crunch" {
        fleet.netmap(crunch, inputs.clone()).await
    } else {
        fleet.netmap(double, inputs.clone()).await
    }
    .expect("netmap failed");

    assert_eq!(out.len(), inputs.len());
    assert!(out.iter().all(Result::is_ok), "some task failed: {out:?}");
    if task == "double" {
        let expected: Vec<Result<u32, String>> = inputs.iter().map(|x| Ok(x * 2)).collect();
        assert_eq!(out, expected);
    }
    println!("ok: {} results", out.len());

    let state = sink.state.lock().unwrap();
    for (host, view) in &state.nodes {
        println!("share {host} {}", view.completed);
    }
}
