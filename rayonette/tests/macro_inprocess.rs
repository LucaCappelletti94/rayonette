//! End-to-end proof of the new capability: `#[rayonette::tasks]` turns an
//! annotated closure into a runnable distributed task with no hand-written
//! registry.
//!
//! The macro keys the closure and submits it to the inventory, an agent builds
//! its registry from that inventory with [`Registry::from_inventory`], and an
//! in-process [`LocalAgent`] runs it. This is the structural twin of
//! `agent::tests::runs_a_task_and_reports_completion`, except the registry comes
//! from the inventory and the wire key comes from the macro rather than being
//! hand-wired.

use rayonette::agent::Registry;
use rayonette::prelude::*;
use rayonette::testing::LocalAgent;

#[rayonette::tasks]
async fn doubled_over(fleet: &Fleet<LocalAgent>) -> std::io::Result<Vec<Result<u32, String>>> {
    (0..5u32)
        .net_map_with_fleet(|x: u32| x * 2, fleet)
        .collect()
        .await
}

#[tokio::test]
async fn an_annotated_closure_runs_as_a_distributed_task() {
    // The agent's registry is built purely from the macro's `register_task!`
    // submission; nothing here names a key or builds a handler by hand.
    let fleet = Fleet::new(vec![LocalAgent::new("leaf", Registry::from_inventory())]);
    let out = doubled_over(&fleet).await.unwrap();
    assert_eq!(out, (0..5u32).map(|x| Ok(x * 2)).collect::<Vec<_>>());
}

// A Tier C closure with NO annotation: the macro recovers `u32` from the range
// receiver, so this bare closure becomes a runnable task. Uses the process-global
// fleet (bare `net_map`), the only test here that does.
#[rayonette::tasks]
async fn ranged() -> std::io::Result<Vec<Result<u32, String>>> {
    (0..5u32).net_map(|x| x * 2).collect().await
}

#[tokio::test]
async fn unannotated_closure_over_literal_range_runs() {
    install_fleet(Fleet::new(vec![LocalAgent::new(
        "leaf",
        Registry::from_inventory(),
    )]));
    let out = ranged().await.unwrap();
    assert_eq!(out, (0..5u32).map(|x| Ok(x * 2)).collect::<Vec<_>>());
}

// A Tier B closure with NO annotation: the macro recovers `u32` from the typed
// `let` binding in scope, so a bare `|x| ...` over `values` becomes a task.
#[rayonette::tasks]
async fn over_a_binding(fleet: &Fleet<LocalAgent>) -> std::io::Result<Vec<Result<u32, String>>> {
    let values: Vec<u32> = (0..5u32).collect();
    values.net_map_with_fleet(|x| x * 2, fleet).collect().await
}

#[tokio::test]
async fn binding_inferred_closure_runs_as_a_distributed_task() {
    let fleet = Fleet::new(vec![LocalAgent::new("leaf", Registry::from_inventory())]);
    let out = over_a_binding(&fleet).await.unwrap();
    assert_eq!(out, (0..5u32).map(|x| Ok(x * 2)).collect::<Vec<_>>());
}

// A doubler reached as a turbofished generic instance: the macro passes the
// turbofish through verbatim and keys the concrete instantiation, so the agent
// registers and runs exactly `twice::<u32>`.
fn twice<T: std::ops::Add<Output = T> + Copy>(x: T) -> T {
    x + x
}

#[rayonette::tasks]
async fn over_a_generic(fleet: &Fleet<LocalAgent>) -> std::io::Result<Vec<Result<u32, String>>> {
    (0..5u32)
        .net_map_with_fleet(twice::<u32>, fleet)
        .collect()
        .await
}

#[tokio::test]
async fn turbofished_generic_runs_as_a_distributed_task() {
    let fleet = Fleet::new(vec![LocalAgent::new("leaf", Registry::from_inventory())]);
    let out = over_a_generic(&fleet).await.unwrap();
    assert_eq!(out, (0..5u32).map(|x| Ok(x * 2)).collect::<Vec<_>>());
}
