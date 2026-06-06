//! Run a trivial task across the ssh hosts named in `RAYONET_HOSTS`.
//!
//! `RAYONET_HOSTS` is a space- or comma-separated list of ssh destinations, each
//! an alias from `~/.ssh/config` (for example `mac`) or a `user@host`, optionally
//! suffixed `=<keyfile>` to authenticate with an explicit private key. The hosts
//! populate the process-global fleet (`install_fleet`), so the task is a bare
//! `inputs.net_map(double)`. A host that fails to connect or build is dropped and
//! the run proceeds on whoever is left.
//!
//! ```text
//! RAYONET_HOSTS="mac localhost=~/.ssh/rayonet_localhost_ed25519" cargo run -p ssh-run
//! ```

use std::sync::Arc;

use rayonet::fleet::{Fleet, NetMapExt};
use rayonet::observability::{Event, EventSink};
use rayonet::process;
use rayonet::ssh::{Ssh, SshConfig};

/// The distributed task: doubles its input.
fn double(x: u32) -> u32 {
    x * 2
}

rayonet::embed_microcrates!();

/// Prints each node's provisioning ladder so the run is visible.
struct Progress;

impl EventSink for Progress {
    fn emit(&self, event: Event) {
        if let Event::Node { host, state } = event {
            println!("  {host}: {state:?}");
        }
    }
}

/// Parse one `dest[=keyfile]` entry into an ssh config.
fn parse_host(entry: &str) -> SshConfig {
    match entry.split_once('=') {
        Some((dest, keyfile)) => SshConfig::new(dest).keyfile(expand_tilde(keyfile)),
        None => SshConfig::new(entry),
    }
}

/// Expand a leading `~/` to `$HOME` (ssh config does this, a plain path does not).
fn expand_tilde(path: &str) -> String {
    match path.strip_prefix("~/") {
        Some(rest) => {
            std::env::var("HOME").map_or_else(|_| path.to_string(), |home| format!("{home}/{rest}"))
        }
        None => path.to_string(),
    }
}

#[tokio::main]
async fn main() {
    if process::is_agent() {
        process::run_agent(__rayonet_registry())
            .await
            .expect("agent failed");
        return;
    }

    let spec = std::env::var("RAYONET_HOSTS")
        .expect("set RAYONET_HOSTS to a space/comma list of ssh destinations");
    let source = __rayonet_source();
    let launchers: Vec<Ssh> = spec
        .split([' ', ','])
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(|entry| Ssh::build(parse_host(entry), source.clone(), "stable", "ssh-run"))
        .collect();
    assert!(!launchers.is_empty(), "RAYONET_HOSTS named no hosts");
    let hosts = launchers.len();

    rayonet::install_fleet(Fleet::observed(launchers, Arc::new(Progress)));

    println!("running across up to {hosts} host(s)...");
    let out: Vec<Result<u32, String>> = (0..8u32)
        .net_map(double)
        .collect()
        .await
        .expect("every host failed to launch");

    println!("results: {out:?}");
    let ok = out.iter().filter(|result| result.is_ok()).count();
    println!("{ok}/{} tasks succeeded", out.len());
}
