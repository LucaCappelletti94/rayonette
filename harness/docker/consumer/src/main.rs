//! The docker harness consumer: one binary, two roles (DECISIONS.md decision 4).
//!
//! Built and shipped to the leaf containers, where it runs as an agent. Run on
//! the host (coordinator role) it provisions the leaves named in `RAYONET_LEAVES`
//! over ssh, ships the whole-workspace source tar at `RAYONET_SOURCE_TAR`,
//! builds itself on each leaf, then runs a distributed `.netmap` and checks the
//! result against the local map. Ladder transitions are printed so the harness
//! script can assert the state sequence.

use std::sync::Arc;

use rayonet::fleet::Fleet;
use rayonet::process;
use rayonet::provisioning::{Event, EventSink};
use rayonet::ssh::{Ssh, SshConfig};

fn double(x: u32) -> u32 {
    x * 2
}

rayonet::embed_microcrates!();

/// Prints each node-state transition so the harness can grep the event stream.
struct PrintSink;

impl EventSink for PrintSink {
    fn emit(&self, event: Event) {
        println!("state {} {:?}", event.host, event.state);
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
    let events: Arc<dyn EventSink> = Arc::new(PrintSink);

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
                events.clone(),
            )
        })
        .collect();
    let fleet = Fleet::new(launchers);

    let inputs: Vec<u32> = (0..10).collect();
    let out = fleet
        .netmap(double, inputs.clone())
        .await
        .expect("netmap failed");

    let expected: Vec<Result<u32, String>> = inputs.iter().map(|x| Ok(x * 2)).collect();
    assert_eq!(out, expected);
    println!("ok: {} results", out.len());
}
