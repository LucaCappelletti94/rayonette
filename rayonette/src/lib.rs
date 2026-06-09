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

pub mod agent;
pub mod capability;
pub mod control;
pub mod coordinator;
pub mod fleet;
pub mod framing;
pub mod graph;
pub mod heartbeat;
#[cfg(feature = "tui")]
pub mod layout;
pub mod node;
pub mod observability;
pub mod process;
pub mod protocol;
pub mod provisioning;
pub mod relay;
pub mod ssh;
pub mod telemetry;
pub mod testing;
#[cfg(feature = "tui")]
pub mod tui;

/// Install the process-global fleet that bare `net_map(map)` runs against.
pub use fleet::install_fleet;

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
