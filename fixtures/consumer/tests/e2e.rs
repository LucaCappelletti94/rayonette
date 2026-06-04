//! End-to-end: build.rs -> extract -> embed -> run a real `.netmap`.
//!
//! Runs the consumer binary in coordinator mode; it spawns subprocess agents
//! (copies of itself), runs `.netmap`, and asserts the results, exiting 0.

#[test]
fn netmap_runs_end_to_end_over_subprocess_agents() {
    let status = std::process::Command::new(env!("CARGO_BIN_EXE_consumer"))
        .status()
        .expect("run consumer binary");
    assert!(status.success(), "consumer exited with {status}");
}
