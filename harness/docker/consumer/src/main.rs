//! The docker harness consumer: one binary, two roles.
//!
//! Built and shipped to the leaf containers, where it runs as an agent. Run on
//! the host (coordinator role) it provisions the leaves named in `RAYONETTE_LEAVES`
//! over ssh, ships the workspace source bundle that `build.rs` embedded (via
//! `__rayonette_source`), builds itself on each leaf, then runs a distributed
//! `.net_map`. It prints each
//! node-state transition (so the harness can assert the ladder) and, at the end,
//! a per-host completed-task count (so the harness can assert work-share).

use std::io::Write;
use std::sync::{Arc, Mutex};

use rayonette::fleet::{Fleet, NetMapExt};
use rayonette::node::{serve_if_agent, NodeConfig, Toolchain};
use rayonette::observability::{Event, EventSink, RecordedEvent, RunState};
use rayonette::ssh::{Ssh, SshConfig};

const fn double(x: u32) -> u32 {
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

/// A generic doubler, registered through an explicit turbofish (`twice::<u32>`).
/// The `task-forms` scenario runs it to prove a monomorphized generic instance
/// survives the ship-and-build-remotely round trip, where the old scanner dropped
/// the turbofish and miscompiled.
fn twice<T: std::ops::Add<Output = T> + Copy>(x: T) -> T {
    x + x
}

rayonette::embed_microcrates!();

/// Appends each event to the `RAYONETTE_EVENT_LOG` file as JSONL, timestamped from
/// the run's start, so a run can be replayed into the TUI (see examples/tui-replay).
struct Recorder {
    file: std::fs::File,
    start: std::time::Instant,
}

impl Recorder {
    fn record(&mut self, event: &Event) {
        let elapsed_ms = u64::try_from(self.start.elapsed().as_millis()).unwrap_or(u64::MAX);
        let record = RecordedEvent::new(elapsed_ms, event.clone());
        if let Ok(line) = serde_json::to_string(&record) {
            let _ = writeln!(self.file, "{line}");
        }
    }
}

/// Prints each node-state transition, reduces the stream so the run's per-host
/// work-share can be reported when it finishes, and (when `RAYONETTE_EVENT_LOG` is
/// set) records the full event stream for TUI replay.
struct ConsoleSink {
    state: Mutex<RunState>,
    recorder: Option<Mutex<Recorder>>,
}

impl ConsoleSink {
    fn new() -> Self {
        let recorder = std::env::var_os("RAYONETTE_EVENT_LOG").map(|path| {
            let file =
                std::fs::File::create(&path).expect("cannot create RAYONETTE_EVENT_LOG file");
            Mutex::new(Recorder {
                file,
                start: std::time::Instant::now(),
            })
        });
        Self {
            state: Mutex::new(RunState::default()),
            recorder,
        }
    }
}

impl EventSink for ConsoleSink {
    fn emit(&self, event: Event) {
        self.state.lock().unwrap().apply(&event);
        if let Event::Node { host, state } = &event {
            println!("state {host} {state:?}");
        }
        if let Some(recorder) = &self.recorder {
            recorder.lock().unwrap().record(&event);
        }
    }
}

fn env(key: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| panic!("{key} must be set"))
}

#[rayonette::tasks]
#[tokio::main]
async fn main() {
    // Relay-capable agent: with a children file it relays to its own subtree,
    // without one it serves as a leaf. serve_if_agent serves then exits (an agent
    // must not linger on its parent's stdin); it carries the env-selected
    // toolchain a relay builds its children with. The registry is built from the
    // inventory of `#[rayonette::tasks]` registrations.
    serve_if_agent(
        NodeConfig::new(
            rayonette::agent::Registry::from_inventory(),
            __rayonette_source(),
        )
        .toolchain(Toolchain::named(
            std::env::var("RAYONETTE_TOOLCHAIN").unwrap_or_else(|_| "stable".to_string()),
        )),
    )
    .await;

    let config_path = env("RAYONETTE_SSH_CONFIG");
    let leaves = env("RAYONETTE_LEAVES");
    let tar = __rayonette_source();
    let toolchain = Toolchain::named(
        std::env::var("RAYONETTE_TOOLCHAIN").unwrap_or_else(|_| "stable".to_string()),
    );
    let task = std::env::var("RAYONETTE_TASK").unwrap_or_else(|_| "double".to_string());
    let count: u32 = std::env::var("RAYONETTE_COUNT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);

    let sink = Arc::new(ConsoleSink::new());
    let launchers: Vec<Ssh> = leaves
        .split(',')
        .map(str::trim)
        .filter(|leaf| !leaf.is_empty())
        .map(|leaf| {
            Ssh::build(
                SshConfig::new(leaf).config_file(&config_path),
                tar.clone(),
                toolchain.clone(),
                "rayonette-docker-consumer",
            )
        })
        .collect();
    let fleet = Fleet::observed(launchers, sink.clone());

    let inputs: Vec<u32> = (0..count).collect();
    // Each task call site must name its task literally so `#[rayonette::tasks]`
    // can rewrite it, hence the duplicated branches. The first three are the
    // topology tasks (named functions, with the require-redundancy variants the
    // kill scenarios use); the last three are the `task-forms` corner cases that
    // prove every macro-supported task shape ships and runs: an annotated closure,
    // an unannotated closure recovered from a typed binding, and a turbofished
    // generic instance.
    let require_redundancy = std::env::var("RAYONETTE_REQUIRE_REDUNDANCY").is_ok();
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
    } else if task == "closure" {
        inputs
            .clone()
            .net_map_with_fleet(|x: u32| x * 2, &fleet)
            .collect()
            .await
    } else if task == "inferred" {
        // A bare typed binding receiver, so the macro recovers the input type
        // (`u32`) with no annotation (Tier B).
        let nums: Vec<u32> = inputs.clone();
        nums.net_map_with_fleet(|x| x * 2, &fleet).collect().await
    } else if task == "generic" {
        inputs
            .clone()
            .net_map_with_fleet(twice::<u32>, &fleet)
            .collect()
            .await
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
    // Every doubling task form (the named fn and the three corner cases) must
    // produce the same answer, which is the point of the `task-forms` scenario.
    if matches!(task.as_str(), "double" | "closure" | "inferred" | "generic") {
        let expected: Vec<Result<u32, String>> = inputs.iter().map(|x| Ok(x * 2)).collect();
        assert_eq!(out, expected);
    }
    println!("ok: {} results", out.len());

    // Completions are credited to the deep leaf that ran them, so a relay rolls its
    // subtree up: its share line is the work done beneath it (a leaf reports its
    // own), which is what the topology assertions read. Snapshot under the lock,
    // then print without holding it.
    let shares: Vec<(String, usize)> = {
        let state = sink.state.lock().unwrap();
        state
            .nodes()
            .keys()
            .map(|host| (host.clone(), state.subtree_completed(host)))
            .collect()
    };
    for (host, completed) in shares {
        println!("share {host} {completed}");
    }
}
