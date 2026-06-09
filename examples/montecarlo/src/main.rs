//! Estimate pi by Monte Carlo, distributed with rayonet across a local docker
//! "swarm" of blank hosts. The flagship example.
//!
//! The whole rayonet contract in one small program:
//!
//! - **One binary, two roles.** Run normally it is the coordinator; run with the
//!   agent marker (which rayonet sets when it launches a worker) it serves the
//!   task. The very same binary runs on every worker.
//! - **One line of build glue.** `build.rs` calls `rayonet_build::extract()`,
//!   which finds the `.net_map(sample)` call below and generates the agent's task
//!   registry; `rayonet::embed_microcrates!()` pulls in both that registry and
//!   the source bundle to ship (so this program never tars its own source).
//! - **Point it at blank hosts; it ships and builds your code there.** The
//!   workers are bare ssh containers with no rust. rayonet provisions each:
//!   install rust, ship the source, compile the agent, launch it, then fans
//!   `sample` across them and the results come back to be summed.
//!
//! Bring up the swarm and run it (see this crate's README):
//!
//! ```text
//! examples/montecarlo/cluster/up.sh     # start the blank workers
//! cargo run -p montecarlo               # provision + distribute
//! examples/montecarlo/cluster/down.sh   # tear down
//! ```

use std::path::{Path, PathBuf};
use std::sync::Arc;

use rayonet::fleet::{Fleet, NetMapExt};
use rayonet::observability::{Event, EventSink};
use rayonet::process;
use rayonet::ssh::{Ssh, SshConfig};

/// Samples each task draws. Large enough that the compute dwarfs the transport.
const SAMPLES_PER_TASK: u64 = 5_000_000;
/// How many independent chunks to fan out across the fleet.
const TASKS: u32 = 32;

/// Draw `SAMPLES_PER_TASK` points in the unit square and count those inside the
/// quarter circle. Seeded only by `chunk`, so it is deterministic and
/// idempotent: re-running it yields the identical count, which is what lets
/// rayonet replay a lost host's work on a survivor without changing the answer.
#[expect(
    clippy::cast_precision_loss,
    reason = "both operands are at most 2^53, which an f64 mantissa holds exactly"
)]
fn sample(chunk: u32) -> u64 {
    // xorshift64: a tiny, fast, dependency-free PRNG.
    let mut state = u64::from(chunk)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(0xD1B5_4A32_D192_ED03);
    let mut unit = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        // Top 53 bits as a float in [0, 1).
        (state >> 11) as f64 / (1u64 << 53) as f64
    };

    let mut inside = 0u64;
    for _ in 0..SAMPLES_PER_TASK {
        let (x, y) = (unit(), unit());
        if y.mul_add(y, x * x) <= 1.0 {
            inside += 1;
        }
    }
    inside
}

rayonet::embed_microcrates!();

/// Prints each node's provisioning ladder so the deployment is visible.
struct Progress;

impl EventSink for Progress {
    fn emit(&self, event: Event) {
        if let Event::Node { host, state } = event {
            println!("  {host}: {state:?}");
        }
    }
}

fn cluster_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("cluster")
}

#[tokio::main]
async fn main() {
    if process::is_agent() {
        process::run_agent(__rayonet_registry())
            .await
            .expect("agent failed");
        return;
    }

    // `cluster/up.sh` writes the key and one `host port` line per worker.
    let cluster = cluster_dir();
    let key = cluster.join("secrets/id");
    let fleet_spec = std::fs::read_to_string(cluster.join("fleet"))
        .expect("no fleet found; run examples/montecarlo/cluster/up.sh first");

    // rayonet bundled our source at build time; just hand it the bytes.
    let source = __rayonet_source();
    let launchers: Vec<Ssh> = fleet_spec
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let mut parts = line.split_whitespace();
            let host = parts.next().expect("host");
            let port: u16 = parts.next().expect("port").parse().expect("port number");
            let config = SshConfig::new(format!("root@{host}"))
                .port(port)
                .keyfile(&key);
            Ssh::build(config, source.clone(), "stable", "montecarlo")
        })
        .collect();
    let workers = launchers.len();

    let fleet = Fleet::observed(launchers, Arc::new(Progress));
    println!("provisioning {workers} workers and distributing {TASKS} tasks...");
    // Map `sample` across the fleet, then sum the per-task hit counts: the
    // reduce folds on the coordinator as the results come back.
    let hits: u64 = (0..TASKS)
        .net_map_with_fleet(sample, &fleet)
        .net_reduce(|a, b| a + b)
        .await
        .expect("distributed run failed")
        .unwrap_or(0);

    let total = u64::from(TASKS) * SAMPLES_PER_TASK;
    #[expect(
        clippy::cast_precision_loss,
        reason = "the sample counts are far below f64's exact-integer range"
    )]
    let pi = 4.0 * hits as f64 / total as f64;
    println!("pi ~= {pi:.5} (from {total} samples across {TASKS} tasks on {workers} workers)");
}
