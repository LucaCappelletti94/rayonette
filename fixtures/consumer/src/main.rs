//! A fixture consumer for the Phase 3 end-to-end test. The same binary is both
//! coordinator and agent: as the agent it serves the registry generated from
//! this file's `.net_map` call sites; as the coordinator it runs a real
//! `.net_map` over subprocess agents (copies of itself).

use rayonette::fleet::{Fleet, NetMapExt, Subprocess};
use rayonette::node::{serve_if_agent, NodeConfig};

const fn double(x: u32) -> u32 {
    x * 2
}

rayonette::embed_microcrates!();

#[rayonette::tasks]
#[tokio::main]
async fn main() {
    // Serve as an agent and exit if launched as one; else run as coordinator. The
    // registry is built from the `#[rayonette::tasks]` registrations gathered by
    // inventory at boot, not the build-time scan.
    serve_if_agent(NodeConfig::new(
        rayonette::agent::Registry::from_inventory(),
        __rayonette_source(),
    ))
    .await;

    // The source bundle rayonette would ship to a remote worker (unused here, since
    // subprocess agents are copies of this exe, but it must be embedded and valid).
    assert!(!__rayonette_source().is_empty(), "empty source bundle");

    let fleet = Fleet::new(
        (0..2)
            .map(|_| Subprocess::current_exe().expect("current exe"))
            .collect(),
    );
    let inputs: Vec<u32> = (0..10).collect();
    let out = inputs
        .clone()
        .net_map_with_fleet(double, &fleet)
        .collect()
        .await
        .expect("net_map failed");

    let expected: Vec<Result<u32, String>> = inputs.iter().map(|x| Ok(x * 2)).collect();
    assert_eq!(out, expected);
    println!("ok: {} results", out.len());
}
