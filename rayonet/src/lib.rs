//! rayonet: SSH-transport task distribution.
//!
//! Where rayon fans data-parallel work across the cores of one machine, rayonet
//! fans task-parallel work across machines on a network. See `DECISIONS.md` and
//! `PLAN.md` at the repo root for the design and the phased build.

pub mod agent;
pub mod coordinator;
pub mod framing;
pub mod process;
pub mod protocol;
pub mod testing;
