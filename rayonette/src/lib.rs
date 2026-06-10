//! rayonette: SSH-transport task distribution.
//!
//! Where rayon fans data-parallel work across the cores of one machine, rayonette
//! fans task-parallel work across machines on a network. See `DECISIONS.md` and
//! `PLAN.md` at the repo root for the design and the phased build.

// In non-test code the only sanctioned panic surface is a documented `expect()`,
// so these bans keep `unwrap`, `panic!`, `unreachable!`, and a message-less
// assert out. Test code is exempt (it unwraps and asserts freely), and the
// integration tests and binaries are separate crates this attribute never
// reaches.
#![cfg_attr(
    not(test),
    deny(
        clippy::unwrap_used,
        clippy::panic,
        clippy::unreachable,
        clippy::unwrap_in_result,
        clippy::panic_in_result_fn,
        clippy::get_unwrap,
        clippy::missing_assert_message
    )
)]

// The user-facing API.
pub mod capability;
pub mod control;
pub mod fleet;
pub mod node;
pub mod observability;
pub mod process;
pub mod ssh;
#[cfg(feature = "tui")]
pub mod tui;

// Engine internals, private to the crate.
pub(crate) mod graph;
pub(crate) mod heartbeat;
#[cfg(feature = "tui")]
pub(crate) mod layout;
pub(crate) mod protocol;
pub(crate) mod relay;
pub(crate) mod telemetry;

// Reachable but not a hand-use API: the build-time-generated registry references
// `agent`, and the integration tests drive `coordinator`, `framing`,
// `provisioning`, and `testing` directly. Hidden from the docs.
#[doc(hidden)]
pub mod agent;
#[doc(hidden)]
pub mod coordinator;
#[doc(hidden)]
pub mod framing;
#[doc(hidden)]
pub mod provisioning;
#[doc(hidden)]
pub mod testing;

/// Install the process-global fleet that bare `net_map(map)` runs against.
pub use fleet::install_fleet;

/// Re-exported so [`register_task!`] can name `inventory::submit!` through
/// `$crate` in a consumer crate that does not depend on `inventory` directly.
#[doc(hidden)]
pub use inventory;

/// Scope a function's `net_map` call sites so each becomes a registered task.
///
/// Put `#[rayonette::tasks]` on the function containing the `net_map` /
/// `net_map_with_fleet` calls: each annotated closure or named function is keyed
/// and registered automatically (gathered by [`agent::Registry::from_inventory`]),
/// with no hand-written registry. An unannotated closure whose input type cannot
/// be recovered is a compile error at the call site, never a silent runtime miss.
pub use rayonette_macros::tasks;

/// Register a task under an explicit wire `key`, submitting it to the inventory
/// that [`agent::Registry::from_inventory`] gathers at agent boot.
///
/// The `#[rayonette::tasks]` macro emits one of these per task call site, using
/// the same `key` literal it puts in the rewritten `net_map_task` call, so the
/// coordinator and the agent agree on the key by construction. `task` must be a
/// named function or a non-capturing closure (the same contract `net_map`
/// enforces); its input and output types are recovered generically, so no
/// hand-written decode/encode wrapper is needed.
#[macro_export]
macro_rules! register_task {
    ($key:expr, $task:expr $(,)?) => {
        $crate::inventory::submit! {
            $crate::agent::TaskEntry::new(|registry| {
                registry.add($key, $task);
            })
        }
    };
}

/// The common API in one import: `use rayonette::prelude::*;`.
pub mod prelude {
    pub use crate::capability::{pred, Filter, Os, Role};
    pub use crate::control::{Control, ControlClient};
    #[cfg(feature = "rayon")]
    pub use crate::fleet::RayonNetMapExt;
    pub use crate::fleet::{Fleet, Launch, NetMapExt, Subprocess};
    pub use crate::install_fleet;
    pub use crate::node::{agent_main, serve_if_agent, NodeConfig, Toolchain};
    pub use crate::observability::{Event, EventSink, RunState};
    pub use crate::process::is_agent;
    pub use crate::ssh::{parse_host_spec, Ssh, SshConfig};
    #[cfg(feature = "tui")]
    pub use crate::tui::{Action, App, Input};
}

/// Embed the crate source bundle that `rayonette_build::extract()` produced.
///
/// Invoke once at the consumer's crate root, after its `build.rs` has called
/// `rayonette_build::extract()`. Expands to `__rayonette_source()`, returning the
/// source bundle to ship to workers (so a consumer never tars its own source).
/// The agent's task registry is no longer generated here: it is built at boot
/// from the `#[rayonette::tasks]` registrations via [`agent::Registry::from_inventory`].
#[macro_export]
macro_rules! embed_microcrates {
    () => {
        #[allow(dead_code)]
        fn __rayonette_source() -> ::std::vec::Vec<u8> {
            include_bytes!(concat!(env!("OUT_DIR"), "/rayonette_source.tar")).to_vec()
        }
    };
}
