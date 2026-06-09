//! Local resource sampling for an agent's self-reported telemetry.
//!
//! An agent samples its own CPU, memory, and GPU use and reports it up the tree
//! alongside its task transitions, so a viewer can show what each node is doing. A
//! [`Sampler`] holds the previous CPU snapshot, since CPU use is a busy-time delta
//! between two reads. The parsing is pure and unit tested; the reads themselves are
//! thin and platform specific. Live sampling is implemented for Linux (`/proc` and
//! `nvidia-smi`); other platforms report a zero baseline until their sampling
//! lands, which the detail panel still labels with the live task count.

use crate::observability::NodeTelemetry;

/// Samples local resource utilisation, remembering the previous CPU reading so
/// each sample reports the busy fraction since the one before it.
#[derive(Debug, Default)]
pub(crate) struct Sampler {
    /// The previous `(busy, total)` CPU jiffies, for the next delta.
    prev_cpu: Option<(u64, u64)>,
}

impl Sampler {
    /// A fresh sampler with no prior CPU reading.
    #[must_use]
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Sample current utilisation, recording `in_flight` tasks running right now.
    pub(crate) fn sample(&mut self, in_flight: usize) -> NodeTelemetry {
        let (cpu_pct, mem_pct, gpu) = read_local(&mut self.prev_cpu);
        telemetry_from_reading(cpu_pct, mem_pct, gpu, in_flight, local_interfaces())
    }
}

/// Assemble a [`NodeTelemetry`] from a raw reading, splitting the combined GPU
/// `(compute, memory)` pair into its two optional fields. Kept separate from the
/// `read_local` sampling so the GPU split is exercised by a unit test on any
/// machine, with or without a GPU present.
fn telemetry_from_reading(
    cpu_pct: u8,
    mem_pct: u8,
    gpu: Option<(u8, u8)>,
    in_flight: usize,
    interfaces: Vec<String>,
) -> NodeTelemetry {
    NodeTelemetry::new(
        cpu_pct,
        mem_pct,
        gpu.map(|(compute, _)| compute),
        gpu.map(|(_, memory)| memory),
        in_flight,
        interfaces,
    )
}

/// `part` as a whole-number percentage of `whole`, clamped to `0..=100`. A zero
/// whole is reported as zero rather than dividing.
fn percent(part: u64, whole: u64) -> u8 {
    if whole == 0 {
        return 0;
    }
    u8::try_from((part.saturating_mul(100) / whole).min(100)).unwrap_or(100)
}

/// The `(busy, total)` CPU jiffies from the aggregate `cpu` line of `/proc/stat`.
fn cpu_busy_total(proc_stat: &str) -> Option<(u64, u64)> {
    let mut fields = proc_stat.lines().next()?.split_whitespace();
    if fields.next()? != "cpu" {
        return None;
    }
    let values: Vec<u64> = fields.filter_map(|field| field.parse().ok()).collect();
    if values.len() < 4 {
        return None;
    }
    let total: u64 = values.iter().sum();
    // Fields 3 and 4 are idle and iowait; the rest is busy time.
    let idle = values[3] + values.get(4).copied().unwrap_or(0);
    Some((total.saturating_sub(idle), total))
}

/// CPU utilisation between two `/proc/stat` readings, as a percentage.
fn cpu_percent(prev: (u64, u64), curr: (u64, u64)) -> u8 {
    let busy = curr.0.saturating_sub(prev.0);
    let total = curr.1.saturating_sub(prev.1);
    percent(busy, total)
}

/// A named `kB` value from `/proc/meminfo`, for example `MemTotal:`.
fn meminfo_value(meminfo: &str, key: &str) -> Option<u64> {
    meminfo.lines().find_map(|line| {
        line.strip_prefix(key)?
            .split_whitespace()
            .next()?
            .parse()
            .ok()
    })
}

/// Memory in use as a percentage of total, from `/proc/meminfo`.
fn mem_percent(meminfo: &str) -> Option<u8> {
    let total = meminfo_value(meminfo, "MemTotal:")?;
    let available = meminfo_value(meminfo, "MemAvailable:")?;
    Some(percent(total.saturating_sub(available), total))
}

/// The `(compute, memory)` GPU utilisation percentages from an `nvidia-smi`
/// `utilization.gpu,utilization.memory` CSV row.
fn parse_gpu_util(csv: &str) -> Option<(u8, u8)> {
    let row = csv.lines().find(|line| !line.trim().is_empty())?;
    let mut columns = row.split(',');
    let compute: u64 = columns.next()?.trim().parse().ok()?;
    let memory: u64 = columns.next()?.trim().parse().ok()?;
    Some((percent(compute, 100), percent(memory, 100)))
}

/// Read local CPU, memory, and GPU utilisation on Linux.
#[cfg(target_os = "linux")]
fn read_local(prev: &mut Option<(u64, u64)>) -> (u8, u8, Option<(u8, u8)>) {
    let curr = std::fs::read_to_string("/proc/stat")
        .ok()
        .and_then(|stat| cpu_busy_total(&stat));
    let cpu = match (prev.take(), curr) {
        // With a prior reading the delta is meaningful; the first reading just
        // establishes the baseline and reports zero.
        (Some(previous), Some(current)) => {
            *prev = Some(current);
            cpu_percent(previous, current)
        }
        (_, Some(current)) => {
            *prev = Some(current);
            0
        }
        (_, None) => 0,
    };
    let mem = std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|info| mem_percent(&info))
        .unwrap_or(0);
    (cpu, mem, nvidia_gpu_util())
}

/// A zero baseline on platforms without a sampler yet.
#[cfg(not(target_os = "linux"))]
fn read_local(_prev: &mut Option<(u64, u64)>) -> (u8, u8, Option<(u8, u8)>) {
    (0, 0, None)
}

/// Whether an IPv4 address is worth reporting as a node's own: not loopback and not
/// link-local. Container and overlay addresses (172.x, 100.x, ...) are kept, since
/// in a container the docker-network address is the node's real one.
fn is_reportable_ip(ip: &str) -> bool {
    !ip.is_empty() && !ip.starts_with("127.") && !ip.starts_with("169.254.")
}

/// The reportable IPv4 addresses from whitespace-separated `hostname -I` output.
fn parse_interfaces(hostnames_output: &str) -> Vec<String> {
    hostnames_output
        .split_whitespace()
        .filter(|token| token.contains('.') && is_reportable_ip(token))
        .map(ToString::to_string)
        .collect()
}

/// The node's own non-loopback IPv4 interface addresses on Linux, via `hostname
/// -I`. Empty when the command is absent or reports nothing.
#[cfg(target_os = "linux")]
fn local_interfaces() -> Vec<String> {
    std::process::Command::new("hostname")
        .arg("-I")
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| parse_interfaces(&String::from_utf8_lossy(&output.stdout)))
        .unwrap_or_default()
}

/// No interface sampler on platforms other than Linux yet.
#[cfg(not(target_os = "linux"))]
fn local_interfaces() -> Vec<String> {
    Vec::new()
}

/// GPU utilisation via `nvidia-smi`, or `None` when it is absent or has no GPU.
#[cfg(target_os = "linux")]
fn nvidia_gpu_util() -> Option<(u8, u8)> {
    let output = std::process::Command::new("nvidia-smi")
        .args([
            "--query-gpu=utilization.gpu,utilization.memory",
            "--format=csv,noheader,nounits",
        ])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    parse_gpu_util(&String::from_utf8_lossy(&output.stdout))
}

#[cfg(test)]
mod tests {
    use super::{
        cpu_busy_total, cpu_percent, mem_percent, parse_gpu_util, percent, telemetry_from_reading,
        Sampler,
    };

    #[test]
    fn parse_interfaces_keeps_real_ips_and_drops_loopback() {
        use super::parse_interfaces;
        // hostname -I lists every IPv4 space-separated. Keep routable addresses
        // (LAN, Tailscale overlay, a container's docker-network IP); drop loopback
        // and link-local.
        let out = "192.168.1.40 100.64.0.7 172.18.0.3 127.0.0.1 169.254.1.2";
        assert_eq!(
            parse_interfaces(out),
            vec![
                "192.168.1.40".to_string(),
                "100.64.0.7".to_string(),
                "172.18.0.3".to_string()
            ]
        );
        assert!(parse_interfaces("").is_empty());
    }

    #[test]
    fn percent_clamps_and_guards_zero() {
        assert_eq!(percent(1, 4), 25);
        assert_eq!(percent(5, 0), 0);
        // A part larger than the whole is clamped, never overflowing a u8.
        assert_eq!(percent(300, 100), 100);
    }

    #[test]
    fn cpu_busy_total_sums_the_aggregate_line() {
        // cpu user nice system idle iowait irq softirq
        let stat = "cpu  100 0 50 800 50 0 0\ncpu0 ...\n";
        let (busy, total) = cpu_busy_total(stat).expect("the aggregate line parses");
        assert_eq!(total, 1000);
        // idle (800) + iowait (50) = 850 idle, so 150 busy.
        assert_eq!(busy, 150);
        // A line that does not start with `cpu`, or is too short, is rejected.
        assert_eq!(cpu_busy_total("intr 1 2 3 4 5"), None);
        assert_eq!(cpu_busy_total("cpu 1 2"), None);
    }

    #[test]
    fn cpu_percent_is_the_busy_delta() {
        // From 150/1000 busy to 650/1500: 500 busy of 500 total elapsed = 100%.
        assert_eq!(cpu_percent((150, 1000), (650, 1500)), 100);
        // Half busy.
        assert_eq!(cpu_percent((0, 0), (50, 100)), 50);
        // No time elapsed reports zero rather than dividing.
        assert_eq!(cpu_percent((10, 20), (10, 20)), 0);
    }

    #[test]
    fn mem_percent_uses_available() {
        let info = "MemTotal:      1000 kB\nMemFree: 100 kB\nMemAvailable:   250 kB\n";
        // 1000 total, 250 available, so 750 used = 75%.
        assert_eq!(mem_percent(info), Some(75));
        // Missing a required field yields nothing.
        assert_eq!(mem_percent("MemTotal: 1000 kB"), None);
    }

    #[test]
    fn parse_gpu_util_reads_the_first_row() {
        assert_eq!(parse_gpu_util("85, 40\n"), Some((85, 40)));
        assert_eq!(parse_gpu_util("\n  \n"), None);
        assert_eq!(parse_gpu_util("oops"), None);
    }

    #[test]
    fn the_sampler_reports_plausible_values() {
        // Sampling twice on the host exercises the real read path; the first call
        // sets the baseline and the second yields a delta. Values stay in range.
        let mut sampler = Sampler::new();
        let first = sampler.sample(1);
        assert_eq!(first.in_flight(), 1);
        let second = sampler.sample(0);
        assert!(second.cpu_pct() <= 100);
        assert!(second.mem_pct() <= 100);
        assert_eq!(second.in_flight(), 0);
        // The GPU split is asserted with explicit values in the test below; not
        // here, because `is_none_or` would not invoke its closure on a GPU-less
        // host (gpu_pct is None there), leaving it an uncovered function.
    }

    #[test]
    fn a_gpu_reading_is_split_into_its_two_fields() {
        // Assembling from a Some reading runs both gpu.map splits on any machine,
        // with or without a GPU, so their coverage does not depend on the host
        // having one. A None reading leaves the GPU fields empty.
        let with_gpu = telemetry_from_reading(10, 20, Some((85, 40)), 2, Vec::new());
        assert_eq!(with_gpu.gpu_pct(), Some(85));
        assert_eq!(with_gpu.in_flight(), 2);

        let without_gpu = telemetry_from_reading(10, 20, None, 0, Vec::new());
        assert_eq!(without_gpu.gpu_pct(), None);
    }
}
