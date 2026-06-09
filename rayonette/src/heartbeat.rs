//! Liveness heartbeat configuration.
//!
//! A parent (the coordinator, or a relay) pings each of its children on an
//! interval, and each child replies with a pong. A child that hears nothing from
//! its parent within the timeout tears itself down, so a crashed parent does not
//! leave it a zombie blocked on a half-open connection; a parent that hears no pong
//! (nor any other message) from a child within the timeout treats it as lost and
//! reroutes its work. The cadence is chosen on the run (the `net_map` builder) and
//! carried to every node in the `Hello` handshake, so the whole tree agrees on it.

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// How a run probes node liveness: whether it is on, the ping interval, and the
/// silence timeout after which a peer is given up on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeartbeatConfig {
    enabled: bool,
    interval_ms: u64,
    timeout_ms: u64,
}

impl Default for HeartbeatConfig {
    /// Enabled, pinging every 5s and giving up after 20s of silence (about four
    /// missed beats): fast enough to reap a dead peer in seconds, slack enough to
    /// tolerate load spikes and slow links.
    fn default() -> Self {
        Self {
            enabled: true,
            interval_ms: 5_000,
            timeout_ms: 20_000,
        }
    }
}

impl HeartbeatConfig {
    /// A heartbeat that pings every `interval` and gives up after `timeout` of
    /// silence.
    #[must_use]
    pub fn new(interval: Duration, timeout: Duration) -> Self {
        Self {
            enabled: true,
            interval_ms: u64::try_from(interval.as_millis()).unwrap_or(u64::MAX),
            timeout_ms: u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
        }
    }

    /// A disabled heartbeat: no pings and no liveness teardown (the behaviour
    /// before the heartbeat existed).
    #[must_use]
    pub const fn disabled() -> Self {
        Self {
            enabled: false,
            interval_ms: 0,
            timeout_ms: 0,
        }
    }

    /// Whether the heartbeat is on.
    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// How often a parent pings its children.
    #[must_use]
    pub const fn interval(&self) -> Duration {
        Duration::from_millis(self.interval_ms)
    }

    /// How long a peer may be silent before it is given up on.
    #[must_use]
    pub const fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::HeartbeatConfig;
    use std::time::Duration;

    #[test]
    fn new_and_accessors_round_trip() {
        let config = HeartbeatConfig::new(Duration::from_secs(3), Duration::from_secs(9));
        assert!(config.is_enabled());
        assert_eq!(config.interval(), Duration::from_secs(3));
        assert_eq!(config.timeout(), Duration::from_secs(9));
    }

    #[test]
    fn the_default_is_enabled_with_balanced_timing() {
        let config = HeartbeatConfig::default();
        assert!(config.is_enabled());
        assert_eq!(config.interval(), Duration::from_secs(5));
        assert_eq!(config.timeout(), Duration::from_secs(20));
    }

    #[test]
    fn disabled_is_off() {
        assert!(!HeartbeatConfig::disabled().is_enabled());
    }
}
