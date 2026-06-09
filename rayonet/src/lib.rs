//! rayonet: SSH-transport task distribution.
//!
//! Where rayon fans data-parallel work across the cores of one machine, rayonet
//! fans task-parallel work across machines on a network. See `DECISIONS.md` and
//! `PLAN.md` at the repo root for the design and the phased build.

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

/// Pull in what `rayonet_build::extract()` generated.
///
/// Invoke once at the consumer's crate root, after its `build.rs` has called
/// `rayonet_build::extract()`. Expands to `__rayonet_registry()`, returning the
/// agent [`agent::Registry`], and `__rayonet_source()`, returning the source
/// bundle to ship to workers (so a consumer never tars its own source).
#[macro_export]
macro_rules! embed_microcrates {
    () => {
        #[allow(dead_code)]
        fn __rayonet_registry() -> $crate::agent::Registry {
            include!(concat!(env!("OUT_DIR"), "/rayonet_registry.rs"))
        }

        #[allow(dead_code)]
        fn __rayonet_source() -> ::std::vec::Vec<u8> {
            include_bytes!(concat!(env!("OUT_DIR"), "/rayonet_source.tar")).to_vec()
        }
    };
}
