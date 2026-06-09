//! A stand-in consumer binary used by the Phase 2 subprocess tests: in agent
//! mode it serves a small registry over stdio; otherwise it exits with code 2.
//! (Test scaffolding; Phase 7 will feature-gate or relocate it.)

use rayonette::agent::{handler, Registry};
use rayonette::node::{agent_main, NodeConfig};
use rayonette::process;

#[tokio::main]
async fn main() {
    if !process::is_agent() {
        eprintln!("rayonette-test-agent: not launched as an agent");
        std::process::exit(2);
    }

    let registry = Registry::new()
        .with("double", handler(|x: u32| x * 2))
        .with(
            "boom",
            handler(|x: u32| -> u32 {
                eprintln!("about to panic on {x}");
                panic!("boom {x}");
            }),
        );

    // Runs as a leaf (no children file in the test environment); the relay path
    // is exercised by the node/relay unit tests and the real R2 verification.
    let config = NodeConfig::new(registry, Vec::new());
    // Serve, then exit the process (an agent must not linger on its parent's
    // stdin; see rayonette::node::agent_main).
    agent_main(config).await;
}
