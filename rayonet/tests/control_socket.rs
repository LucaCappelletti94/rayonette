//! End-to-end test of the control back-channel.
//!
//! A client connects to the coordinator's Unix control socket and kills a node
//! mid-run; the run survives it (the survivor finishes the work) and the killed
//! node is reported `Lost` through the normal event stream. This runs in its own
//! test process, so setting `RAYONET_CONTROL_SOCKET` cannot affect other tests.

use std::sync::Arc;
use std::time::Duration;

use rayonet::agent::Registry;
use rayonet::control::{Control, ControlAction, ControlClient, KillMode};
use rayonet::fleet::{Fleet, NetMapExt};
use rayonet::observability::{NodeState, RunState};
use rayonet::testing::{EventRecorder, LocalAgent};

/// A task slow enough that a control sent over the socket lands mid-run.
fn slow(x: u32) -> u32 {
    std::thread::sleep(Duration::from_millis(6));
    x * 2
}

#[tokio::test]
async fn a_client_kills_a_node_over_the_control_socket() {
    let path =
        std::env::temp_dir().join(format!("rayonet-control-e2e-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&path);
    std::env::set_var("RAYONET_CONTROL_SOCKET", &path);

    let sink = Arc::new(EventRecorder::default());
    let fleet = Fleet::observed(
        vec![
            LocalAgent::new("a", Registry::new().with_fn(slow)),
            LocalAgent::new("b", Registry::new().with_fn(slow)),
        ],
        sink.clone(),
    );

    let run = (0..30u32).net_map_with_fleet(slow, &fleet).collect();
    let driver = async {
        // The listener binds inside the run, after discovery; connect once it is up.
        let mut client = loop {
            match ControlClient::connect(&path).await {
                Ok(client) => break client,
                Err(_) => tokio::time::sleep(Duration::from_millis(2)).await,
            }
        };
        client
            .send(&Control::new(
                "a".to_string(),
                ControlAction::Kill {
                    mode: KillMode::Now,
                },
            ))
            .await
            .unwrap();
    };
    let (out, ()) = tokio::join!(run, driver);
    let out: Vec<Result<u32, String>> = out.unwrap();
    assert_eq!(out, (0..30u32).map(|x| Ok(x * 2)).collect::<Vec<_>>());

    let mut state = RunState::default();
    for event in &sink.events() {
        state.apply(event);
    }
    assert_eq!(
        state.nodes()["a"].state(),
        NodeState::Lost,
        "the node killed over the socket is reported Lost"
    );

    let _ = std::fs::remove_file(&path);
}
