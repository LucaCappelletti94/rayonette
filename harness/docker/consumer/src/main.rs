//! The docker harness consumer: one binary, two roles.
//!
//! Built and shipped to the leaf containers, where it runs as an agent. Run on
//! the host (coordinator role) it provisions the leaves named in `RAYONET_LEAVES`
//! over ssh, ships the workspace source bundle that `build.rs` embedded (via
//! `__rayonet_source`), builds itself on each leaf, then runs a distributed
//! `.net_map`. It prints each
//! node-state transition (so the harness can assert the ladder) and, at the end,
//! a per-host completed-task count (so the harness can assert work-share).

use std::sync::{Arc, Mutex};

use rayonet::fleet::{Fleet, NetMapExt};
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

/// A wall-clock task: it sleeps a fixed span rather than burning CPU, so a run's
/// duration is predictable regardless of how fast the host is. The kill and join
/// scenarios use this in CI (with a modest count) so the event reliably lands
/// mid-run on a slow shared runner, where a CPU-bound `crunch` would either crawl
/// or, on a fast runner, drain before a joiner could provision.
fn dawdle(x: u32) -> u32 {
    std::thread::sleep(std::time::Duration::from_millis(25));
    x
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
        // Relay-capable agent: with a children file it relays to its own subtree,
        // without one it serves as a leaf. This is what lets the harness build
        // real relay trees, not just a flat star. agent_main serves, then exits
        // the process (an agent must not linger on its parent's stdin).
        rayonet::node::agent_main(rayonet::node::NodeConfig {
            registry: __rayonet_registry(),
            source: __rayonet_source(),
            binary_name: "rayonet-docker-consumer".to_string(),
            toolchain: std::env::var("RAYONET_TOOLCHAIN").unwrap_or_else(|_| "stable".to_string()),
        })
        .await;
    }

    let config_path = env("RAYONET_SSH_CONFIG");
    let leaves = env("RAYONET_LEAVES");
    let tar = __rayonet_source();
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
    // Task functions must appear literally in `net_map_with_fleet(<fn>, ...)` so
    // the build-time extractor registers them, hence the duplicated branches.
    let require_redundancy = std::env::var("RAYONET_REQUIRE_REDUNDANCY").is_ok();
    let result = if task == "crunch" {
        let job = inputs.clone().net_map_with_fleet(crunch, &fleet);
        if require_redundancy {
            job.require_redundancy().collect().await
        } else {
            job.collect().await
        }
    } else if task == "dawdle" {
        let job = inputs.clone().net_map_with_fleet(dawdle, &fleet);
        if require_redundancy {
            job.require_redundancy().collect().await
        } else {
            job.collect().await
        }
    } else {
        let job = inputs.clone().net_map_with_fleet(double, &fleet);
        if require_redundancy {
            job.require_redundancy().collect().await
        } else {
            job.collect().await
        }
    };
    // A run can fail legibly (every relay lost, or redundancy required but not
    // met): print the error so the harness can assert on it, rather than panic.
    let out = match result {
        Ok(out) => out,
        Err(error) => {
            println!("error: {error}");
            std::process::exit(1);
        }
    };

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
