//! A stand-in consumer binary used by the Phase 2 subprocess tests: in agent
//! mode it serves a small registry over stdio; otherwise it exits with code 2.
//! (Test scaffolding; Phase 7 will feature-gate or relocate it.)

use rayonet::agent::{handler, serve, Registry};
use rayonet::process;

#[tokio::main]
async fn main() {
    if !process::is_agent() {
        eprintln!("rayonet-test-agent: not launched as an agent");
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

    if let Err(e) = serve(process::agent_connection(), registry).await {
        eprintln!("rayonet-test-agent: serve error: {e}");
        std::process::exit(1);
    }
}
