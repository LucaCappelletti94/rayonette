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

/// Pull in what `rayonette_build::extract()` generated.
///
/// Invoke once at the consumer's crate root, after its `build.rs` has called
/// `rayonette_build::extract()`. Expands to `__rayonette_registry()`, returning the
/// agent [`agent::Registry`], and `__rayonette_source()`, returning the source
/// bundle to ship to workers (so a consumer never tars its own source).
#[macro_export]
macro_rules! embed_microcrates {
    () => {
        #[allow(dead_code)]
        fn __rayonette_registry() -> $crate::agent::Registry {
            include!(concat!(env!("OUT_DIR"), "/rayonette_registry.rs"))
        }

        #[allow(dead_code)]
        fn __rayonette_source() -> ::std::vec::Vec<u8> {
            include_bytes!(concat!(env!("OUT_DIR"), "/rayonette_source.tar")).to_vec()
        }
    };
}
