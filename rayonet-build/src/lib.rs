//! Build-time extraction for rayonet, called from a consumer crate's `build.rs`
//! (DECISIONS.md decision 9). Parses the consumer crate, bundles whole-crate
//! source for shipping, and generates the task registry.
//!
//! Implemented in PLAN.md Phase 3; this is a placeholder so the workspace builds.

/// Placeholder entry point. Real implementation lands in Phase 3.
///
/// # Errors
/// Will return an error if source extraction or codegen fails. The current
/// placeholder never errors.
pub const fn extract() -> std::io::Result<()> {
    Ok(())
}
