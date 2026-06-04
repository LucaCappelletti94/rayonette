//! A fixture consumer for the Phase 3 end-to-end test. The same binary is both
//! coordinator and agent: as the agent it serves the registry generated from
//! this file's `.netmap` call sites; as the coordinator it runs a real
//! `.netmap` over subprocess agents (copies of itself).

use rayonet::fleet::{Fleet, Subprocess};
use rayonet::process;

fn double(x: u32) -> u32 {
    x * 2
}

rayonet::embed_microcrates!();

#[tokio::main]
async fn main() {
    if process::is_agent() {
        process::run_agent(__rayonet_registry())
            .await
            .expect("agent failed");
        return;
    }

    let fleet = Fleet::new(
        (0..2)
            .map(|_| Subprocess::current_exe().expect("current exe"))
            .collect(),
    );
    let inputs: Vec<u32> = (0..10).collect();
    let out = fleet
        .netmap(double, inputs.clone())
        .await
        .expect("netmap failed");

    let expected: Vec<Result<u32, String>> = inputs.iter().map(|x| Ok(x * 2)).collect();
    assert_eq!(out, expected);
    println!("ok: {} results", out.len());
}
