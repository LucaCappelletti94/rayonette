//! Run a trivial task across the ssh hosts named in `RAYONETTE_HOSTS`.
//!
//! `RAYONETTE_HOSTS` is a space- or comma-separated list of ssh destinations, each
//! an alias from `~/.ssh/config` (for example `mac`) or a `user@host`, optionally
//! suffixed `=<keyfile>` to authenticate with an explicit private key. The hosts
//! populate the process-global fleet (`install_fleet`), so the task is a bare
//! `inputs.net_map(double)`. A host that fails to connect or build is dropped and
//! the run proceeds on whoever is left.
//!
//! ```text
//! RAYONETTE_HOSTS="mac localhost=~/.ssh/rayonette_localhost_ed25519" cargo run -p ssh-run
//! ```
//!
//! Set `RAYONETTE_FILTER=no-macos` to apply a fleet role filter that excludes
//! macOS hosts (they are profiled, then dropped before provisioning); any other
//! value, or unset, keeps every host as compute.

use std::sync::Arc;

use rayonette::capability::{pred, Filter, Os, Role};
use rayonette::fleet::{Fleet, NetMapExt};
use rayonette::node::{agent_main, NodeConfig};
use rayonette::observability::{depth, leaf_of, Event, EventSink};
use rayonette::process;
use rayonette::ssh::{parse_host_spec, Ssh};

/// The distributed task: doubles its input.
const fn double(x: u32) -> u32 {
    x * 2
}

rayonette::embed_microcrates!();

/// Prints each node's provisioning ladder and capability/role so the run is
/// visible, indented by tree depth so a relay's subtree is shown beneath it.
struct Progress;

impl EventSink for Progress {
    fn emit(&self, event: Event) {
        match event {
            Event::Node { host, state } => {
                println!(
                    "  {}{}: {state:?}",
                    "  ".repeat(depth(&host)),
                    leaf_of(&host)
                );
            }
            Event::Profiled {
                host,
                profile,
                role,
                ..
            } => println!(
                "  {}{}: {role:?} ({:?}, {} cores, {} MB RAM, {} GPUs)",
                "  ".repeat(depth(&host)),
                leaf_of(&host),
                profile.os(),
                profile.cores(),
                profile.ram_mb(),
                profile.gpus().len()
            ),
            _ => {}
        }
    }
}

/// The fleet role filter selected by `RAYONETTE_FILTER`, if any.
fn filter_from_env() -> Option<Filter> {
    match std::env::var("RAYONETTE_FILTER").ok()?.as_str() {
        "no-macos" => Some(
            Filter::new()
                .exclude(pred::os_is(Os::MacOs))
                .otherwise(Role::Compute),
        ),
        _ => None,
    }
}

#[tokio::main]
async fn main() {
    if process::is_agent() {
        // As an agent: a leaf, or a relay if this host has a children file. A
        // relay re-ships this same source bundle down to its own children.
        let config = NodeConfig::new(
            __rayonette_registry(),
            __rayonette_source(),
            "ssh-run".to_string(),
            "stable".to_string(),
        );
        // `agent_main` runs the node then exits the process; it never returns.
        agent_main(config).await;
    }

    let spec = std::env::var("RAYONETTE_HOSTS")
        .expect("set RAYONETTE_HOSTS to a space/comma list of ssh destinations");
    let source = __rayonette_source();
    let launchers: Vec<Ssh> = spec
        .split([' ', ','])
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(|entry| Ssh::build(parse_host_spec(entry), source.clone(), "stable", "ssh-run"))
        .collect();
    assert!(!launchers.is_empty(), "RAYONETTE_HOSTS named no hosts");
    let hosts = launchers.len();

    let mut fleet = Fleet::observed(launchers, Arc::new(Progress));
    if let Some(filter) = filter_from_env() {
        println!("applying RAYONETTE_FILTER role policy");
        fleet = fleet.with_filter(filter);
    }
    rayonette::install_fleet(fleet);

    println!("running across up to {hosts} host(s)...");
    match (0..8u32).net_map(double).collect::<u32>().await {
        Ok(out) => {
            println!("results: {out:?}");
            let ok = out.iter().filter(|result| result.is_ok()).count();
            println!("{ok}/{} tasks succeeded", out.len());
        }
        Err(error) => println!("run produced no results: {error}"),
    }
}
