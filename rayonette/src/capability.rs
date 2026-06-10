//! Node capability profiles and the scheduling role filter.
//!
//! A [`NodeProfile`] is what probing a host yields: a stable core (OS, cores,
//! RAM) plus extensible capability lists, today the GPUs. The consumer maps a
//! profile to a [`Role`] with a filter (`Fn(&NodeProfile) -> Role`), composing
//! the predicate helpers here. Probing runs commands on a host and feeds their
//! output to the pure parsers below, which are unit-tested against fixture
//! output so the brittle part (text parsing) is covered without ssh. A
//! capability that cannot be parsed reads as absent, so the filter never falsely
//! claims one.

use serde::{Deserialize, Serialize};

/// A node's operating system, from `uname -s`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Os {
    /// Linux.
    Linux,
    /// macOS (`Darwin`).
    MacOs,
    /// Anything else, kept verbatim.
    Other(String),
}

/// A GPU's software runtime, which is what a task actually targets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GpuRuntime {
    /// NVIDIA CUDA.
    Cuda,
    /// AMD `ROCm`.
    Rocm,
    /// Apple Metal.
    Metal,
    /// Anything else, kept verbatim.
    Other(String),
}

/// A GPU vendor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GpuVendor {
    /// NVIDIA.
    Nvidia,
    /// AMD.
    Amd,
    /// Apple.
    Apple,
    /// Intel.
    Intel,
    /// Anything else, kept verbatim.
    Other(String),
}

/// A single GPU on a node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Gpu {
    /// The hardware vendor.
    vendor: GpuVendor,
    /// The runtime it exposes, if known.
    runtime: Option<GpuRuntime>,
    /// The marketing/model name, verbatim.
    model: String,
    /// Total video memory in MB, if known.
    vram_mb: Option<u64>,
}

impl Gpu {
    /// Describe one GPU: its `vendor`, the `runtime` it exposes (if known), its
    /// `model` name, and its `vram_mb` (if known).
    #[must_use]
    pub const fn new(
        vendor: GpuVendor,
        runtime: Option<GpuRuntime>,
        model: String,
        vram_mb: Option<u64>,
    ) -> Self {
        Self {
            vendor,
            runtime,
            model,
            vram_mb,
        }
    }

    /// The runtime this GPU exposes, if known.
    #[must_use]
    pub const fn runtime(&self) -> Option<&GpuRuntime> {
        self.runtime.as_ref()
    }
}

/// A node's CPU architecture: the instruction set plus the enabled feature flags.
///
/// Two nodes are interchangeable for a `-C target-cpu=native` binary iff these are
/// equal (same instruction set and the same instruction-set extensions), so this
/// is both the "same architecture" test and what keeps a native build cached per
/// microarchitecture rather than reused on a CPU that would fault on it.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct CpuArch {
    /// The instruction set from `uname -m`, for example `x86_64` or `aarch64`.
    isa: String,
    /// The CPU instruction-set feature flags, sorted and deduplicated.
    features: Vec<String>,
}

impl CpuArch {
    /// A CPU architecture: its `isa` (instruction set) and `features` (the
    /// instruction-set extension flags).
    #[must_use]
    pub const fn new(isa: String, features: Vec<String>) -> Self {
        Self { isa, features }
    }

    /// An unknown architecture, for a host that has not been probed.
    #[must_use]
    pub fn unknown() -> Self {
        Self {
            isa: "unknown".to_string(),
            features: Vec::new(),
        }
    }

    /// The instruction set, for example `x86_64` or `aarch64`.
    #[must_use]
    pub fn isa(&self) -> &str {
        &self.isa
    }

    /// The CPU instruction-set feature flags, sorted and deduplicated.
    #[must_use]
    pub fn features(&self) -> &[String] {
        &self.features
    }
}

/// A node's probed capabilities. Extensible: new capabilities are new fields,
/// added without breaking existing filters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeProfile {
    /// The operating system.
    os: Os,
    /// The host's own name (`uname -n`), the human-friendly handle for the machine,
    /// distinct from its ssh-dest label and its stable machine id. Empty if unknown.
    /// Defaulted on decode so traces recorded before this field still load.
    #[serde(default)]
    hostname: String,
    /// The CPU architecture (instruction set and feature flags).
    arch: CpuArch,
    /// Logical CPU count (0 if it could not be determined).
    cores: u32,
    /// Total RAM in MB (0 if it could not be determined).
    ram_mb: u64,
    /// The GPUs found, empty if none or none could be parsed.
    gpus: Vec<Gpu>,
}

impl NodeProfile {
    /// Assemble a probed profile from its `os`, `hostname`, CPU `arch`, logical
    /// `cores`, `ram_mb`, and discovered `gpus`.
    #[must_use]
    pub const fn new(
        os: Os,
        hostname: String,
        arch: CpuArch,
        cores: u32,
        ram_mb: u64,
        gpus: Vec<Gpu>,
    ) -> Self {
        Self {
            os,
            hostname,
            arch,
            cores,
            ram_mb,
            gpus,
        }
    }

    /// The operating system.
    #[must_use]
    pub const fn os(&self) -> &Os {
        &self.os
    }

    /// The host's own name (`uname -n`), empty when unknown.
    #[must_use]
    pub fn hostname(&self) -> &str {
        &self.hostname
    }

    /// The CPU architecture (instruction set and feature flags).
    #[must_use]
    pub const fn arch(&self) -> &CpuArch {
        &self.arch
    }

    /// Logical CPU count (0 if it could not be determined).
    #[must_use]
    pub const fn cores(&self) -> u32 {
        self.cores
    }

    /// Total RAM in MB (0 if it could not be determined).
    #[must_use]
    pub const fn ram_mb(&self) -> u64 {
        self.ram_mb
    }

    /// The GPUs found, empty if none or none could be parsed.
    #[must_use]
    pub fn gpus(&self) -> &[Gpu] {
        &self.gpus
    }

    /// A placeholder profile for a launcher with no real host to probe (a local
    /// subprocess or an in-process test agent): no known capabilities.
    #[must_use]
    pub fn unknown() -> Self {
        Self {
            os: Os::Other("unknown".to_string()),
            hostname: String::new(),
            arch: CpuArch::unknown(),
            cores: 0,
            ram_mb: 0,
            gpus: Vec::new(),
        }
    }

    /// Whether this node and `other` have the same CPU architecture, so a
    /// `-C target-cpu=native` binary built on one runs correctly on the other.
    #[must_use]
    pub fn same_arch(&self, other: &Self) -> bool {
        self.arch == other.arch
    }

    /// Whether the node has any GPU at all.
    #[must_use]
    pub const fn has_gpu(&self) -> bool {
        !self.gpus.is_empty()
    }

    /// Whether any of the node's GPUs exposes `runtime` (for example `Rocm`).
    #[must_use]
    pub fn has_gpu_runtime(&self, runtime: &GpuRuntime) -> bool {
        self.gpus
            .iter()
            .any(|gpu| gpu.runtime.as_ref() == Some(runtime))
    }

    /// The largest known VRAM across the node's GPUs in MB, 0 if none is known.
    #[must_use]
    pub fn max_vram_mb(&self) -> u64 {
        self.gpus
            .iter()
            .filter_map(|gpu| gpu.vram_mb)
            .max()
            .unwrap_or(0)
    }
}

/// The scheduling role the filter assigns a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    /// Runs tasks (and relays, if it has children).
    Compute,
    /// Forwards to its children but runs no tasks of its own.
    RelayOnly,
    /// Used for nothing.
    Excluded,
}

/// A first-match-wins rule list mapping a [`NodeProfile`] to a [`Role`], built
/// from [`pred::Predicate`]s.
///
/// Rules are tried in order, the first whose predicate matches wins, and an
/// unmatched profile takes the default (`Excluded` unless changed with
/// [`Filter::otherwise`]). Example:
/// `Filter::new().relay_only(os_is(MacOs)).compute(rocm().and(ram_at_least_gb(16)))`.
#[derive(Debug, Clone)]
pub struct Filter {
    rules: Vec<(pred::Predicate, Role)>,
    default: Role,
}

impl Default for Filter {
    fn default() -> Self {
        Self::new()
    }
}

impl Filter {
    /// An empty filter whose default is [`Role::Excluded`].
    #[must_use]
    pub const fn new() -> Self {
        Self {
            rules: Vec::new(),
            default: Role::Excluded,
        }
    }

    /// Append a rule assigning [`Role::Compute`] to profiles matching `predicate`.
    #[must_use]
    pub fn compute(mut self, predicate: pred::Predicate) -> Self {
        self.rules.push((predicate, Role::Compute));
        self
    }

    /// Append a rule assigning [`Role::RelayOnly`] to profiles matching `predicate`.
    #[must_use]
    pub fn relay_only(mut self, predicate: pred::Predicate) -> Self {
        self.rules.push((predicate, Role::RelayOnly));
        self
    }

    /// Append a rule assigning [`Role::Excluded`] to profiles matching `predicate`.
    #[must_use]
    pub fn exclude(mut self, predicate: pred::Predicate) -> Self {
        self.rules.push((predicate, Role::Excluded));
        self
    }

    /// Set the role for a profile that matches no rule (defaults to `Excluded`).
    #[must_use]
    pub const fn otherwise(mut self, role: Role) -> Self {
        self.default = role;
        self
    }

    /// The role for `profile`: the first matching rule, or the default.
    #[must_use]
    pub fn role_of(&self, profile: &NodeProfile) -> Role {
        for (predicate, role) in &self.rules {
            if predicate.eval(profile) {
                return *role;
            }
        }
        self.default
    }
}

/// Parse `uname -s` output into an [`Os`].
#[must_use]
pub fn parse_os(uname_s: &str) -> Os {
    match uname_s.trim() {
        "Linux" => Os::Linux,
        "Darwin" => Os::MacOs,
        other => Os::Other(other.to_string()),
    }
}

/// Parse a logical-core count (for example `nproc` or `sysctl -n hw.ncpu`),
/// returning 0 if it is not a number.
#[must_use]
pub fn parse_cores(s: &str) -> u32 {
    s.trim().parse().unwrap_or(0)
}

/// Build a [`CpuArch`] from the instruction set (`uname -m`) and raw feature text.
///
/// The text is whitespace- or colon-separated tokens: the Linux `/proc/cpuinfo`
/// `flags`/`Features` line, or macOS `sysctl` feature output. Tokens are
/// lowercased, sorted, and deduplicated so the same CPU always yields the same
/// architecture regardless of probe ordering.
#[must_use]
pub fn parse_cpu_arch(uname_m: &str, features_raw: &str) -> CpuArch {
    // A `flags : a b c` cpuinfo line keeps only the part after the colon; sysctl
    // output has no colon and is taken whole.
    let after_colon = features_raw.rsplit(':').next().unwrap_or(features_raw);
    let mut features: Vec<String> = after_colon
        .split_whitespace()
        .map(str::to_lowercase)
        .collect();
    features.sort();
    features.dedup();
    CpuArch {
        isa: uname_m.trim().to_string(),
        features,
    }
}

/// Parse total RAM in MB from Linux `/proc/meminfo` (`MemTotal: N kB`).
#[must_use]
pub fn parse_linux_ram_mb(meminfo: &str) -> u64 {
    for line in meminfo.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            if let Some(kb) = rest.split_whitespace().next() {
                if let Ok(kb) = kb.parse::<u64>() {
                    return kb / 1024;
                }
            }
        }
    }
    0
}

/// Parse total RAM in MB from macOS `sysctl -n hw.memsize` (a byte count).
#[must_use]
pub fn parse_macos_ram_mb(memsize: &str) -> u64 {
    memsize
        .trim()
        .parse::<u64>()
        .map_or(0, |bytes| bytes / (1024 * 1024))
}

/// Parse `nvidia-smi --query-gpu=name,memory.total --format=csv,noheader,nounits`.
#[must_use]
pub fn parse_nvidia_smi(csv: &str) -> Vec<Gpu> {
    csv.lines()
        .filter_map(|line| {
            let mut parts = line.splitn(2, ',');
            let model = parts.next()?.trim().to_string();
            if model.is_empty() {
                return None;
            }
            let vram_mb = parts.next().and_then(|m| m.trim().parse::<u64>().ok());
            Some(Gpu {
                vendor: GpuVendor::Nvidia,
                runtime: Some(GpuRuntime::Cuda),
                model,
                vram_mb,
            })
        })
        .collect()
}

/// Parse `rocminfo`, extracting AMD/ROCm GPU agents by marketing name.
#[must_use]
pub fn parse_rocminfo(out: &str) -> Vec<Gpu> {
    let mut gpus = Vec::new();
    let mut name: Option<String> = None;
    let mut is_gpu = false;
    for line in out.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("Agent ") {
            if is_gpu {
                if let Some(model) = name.take() {
                    gpus.push(amd_rocm_gpu(model));
                }
            }
            name = None;
            is_gpu = false;
        } else if let Some(rest) = trimmed.strip_prefix("Marketing Name:") {
            name = Some(rest.trim().to_string());
        } else if let Some(rest) = trimmed.strip_prefix("Device Type:") {
            if rest.trim() == "GPU" {
                is_gpu = true;
            }
        }
    }
    if is_gpu {
        if let Some(model) = name {
            gpus.push(amd_rocm_gpu(model));
        }
    }
    gpus
}

/// An AMD GPU exposing `ROCm`, with VRAM left unknown.
const fn amd_rocm_gpu(model: String) -> Gpu {
    Gpu {
        vendor: GpuVendor::Amd,
        runtime: Some(GpuRuntime::Rocm),
        model,
        vram_mb: None,
    }
}

/// Parse macOS `system_profiler SPDisplaysDataType` for Metal GPUs.
#[must_use]
pub fn parse_macos_gpus(out: &str) -> Vec<Gpu> {
    out.lines()
        .filter_map(|line| {
            let model = line
                .trim()
                .strip_prefix("Chipset Model:")?
                .trim()
                .to_string();
            if model.is_empty() {
                return None;
            }
            let vendor = if model.contains("Apple") {
                GpuVendor::Apple
            } else if model.contains("AMD") || model.contains("Radeon") {
                GpuVendor::Amd
            } else if model.contains("NVIDIA") {
                GpuVendor::Nvidia
            } else if model.contains("Intel") {
                GpuVendor::Intel
            } else {
                GpuVendor::Other(model.clone())
            };
            Some(Gpu {
                vendor,
                runtime: Some(GpuRuntime::Metal),
                model,
                vram_mb: None,
            })
        })
        .collect()
}

/// Composable capability predicates for building filters.
///
/// A [`Predicate`] is a reusable test over a [`NodeProfile`], built from the
/// constructors here and combined with `and` / `or` / `not`. The same
/// predicates drive both the fleet's role filter and a job's `.requires(..)`.
/// They run only on the coordinator, so they may capture and are never shipped.
pub mod pred {
    use super::{GpuRuntime, NodeProfile, Os};
    use std::sync::Arc;

    /// A reusable, composable test over a [`NodeProfile`].
    #[derive(Clone)]
    pub struct Predicate(Arc<dyn Fn(&NodeProfile) -> bool + Send + Sync>);

    impl std::fmt::Debug for Predicate {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("Predicate").finish_non_exhaustive()
        }
    }

    impl Predicate {
        /// Wrap a custom test (an escape hatch for anything the constructors do
        /// not cover).
        #[must_use]
        pub fn new(test: impl Fn(&NodeProfile) -> bool + Send + Sync + 'static) -> Self {
            Self(Arc::new(test))
        }

        /// Evaluate the predicate against `profile`.
        #[must_use]
        pub fn eval(&self, profile: &NodeProfile) -> bool {
            (self.0)(profile)
        }

        /// Both this and `other` hold.
        #[must_use]
        pub fn and(self, other: Self) -> Self {
            Self::new(move |p| self.eval(p) && other.eval(p))
        }

        /// Either this or `other` holds.
        #[must_use]
        pub fn or(self, other: Self) -> Self {
            Self::new(move |p| self.eval(p) || other.eval(p))
        }

        /// This does not hold.
        #[must_use]
        pub fn negate(self) -> Self {
            Self::new(move |p| !self.eval(p))
        }
    }

    /// The node runs `os`.
    #[must_use]
    pub fn os_is(os: Os) -> Predicate {
        Predicate::new(move |p| p.os == os)
    }

    /// The node has a GPU exposing `runtime`.
    #[must_use]
    pub fn gpu_runtime(runtime: GpuRuntime) -> Predicate {
        Predicate::new(move |p| p.has_gpu_runtime(&runtime))
    }

    /// The node has a CUDA GPU.
    #[must_use]
    pub fn cuda() -> Predicate {
        gpu_runtime(GpuRuntime::Cuda)
    }

    /// The node has a `ROCm` GPU.
    #[must_use]
    pub fn rocm() -> Predicate {
        gpu_runtime(GpuRuntime::Rocm)
    }

    /// The node has a Metal GPU.
    #[must_use]
    pub fn metal() -> Predicate {
        gpu_runtime(GpuRuntime::Metal)
    }

    /// The node has any GPU.
    #[must_use]
    pub fn gpu() -> Predicate {
        Predicate::new(NodeProfile::has_gpu)
    }

    /// The node has at least `gb` GB of RAM.
    #[must_use]
    pub fn ram_at_least_gb(gb: u64) -> Predicate {
        Predicate::new(move |p| p.ram_mb >= gb * 1024)
    }

    /// Some GPU has at least `gb` GB of VRAM.
    #[must_use]
    pub fn vram_at_least_gb(gb: u64) -> Predicate {
        Predicate::new(move |p| p.max_vram_mb() >= gb * 1024)
    }

    /// At least `n` logical cores.
    #[must_use]
    pub fn cores_at_least(n: u32) -> Predicate {
        Predicate::new(move |p| p.cores >= n)
    }

    #[cfg(test)]
    mod tests {
        use super::super::{Gpu, GpuRuntime, GpuVendor, NodeProfile, Os};
        use super::{
            cores_at_least, cuda, gpu, metal, os_is, ram_at_least_gb, rocm, vram_at_least_gb,
        };

        fn rocm_box() -> NodeProfile {
            NodeProfile {
                os: Os::Linux,
                hostname: String::new(),
                arch: crate::capability::CpuArch::unknown(),
                cores: 64,
                ram_mb: 131_072,
                gpus: vec![Gpu {
                    vendor: GpuVendor::Amd,
                    runtime: Some(GpuRuntime::Rocm),
                    model: "RX 7900 XTX".to_string(),
                    vram_mb: Some(24_576),
                }],
            }
        }

        fn mac() -> NodeProfile {
            NodeProfile {
                os: Os::MacOs,
                hostname: String::new(),
                arch: crate::capability::CpuArch::unknown(),
                cores: 8,
                ram_mb: 16_384,
                gpus: vec![],
            }
        }

        #[test]
        fn constructors() {
            let n = rocm_box();
            assert!(rocm().eval(&n));
            assert!(!cuda().eval(&n));
            assert!(gpu().eval(&n));
            assert!(!gpu().eval(&mac()));
            assert!(os_is(Os::Linux).eval(&n));
            assert!(!os_is(Os::MacOs).eval(&n));
            assert!(ram_at_least_gb(64).eval(&n));
            assert!(!ram_at_least_gb(256).eval(&n));
            assert!(vram_at_least_gb(24).eval(&n));
            assert!(!vram_at_least_gb(48).eval(&n));
            assert!(cores_at_least(32).eval(&n));
            assert!(!metal().eval(&n));
            assert!(format!("{:?}", rocm()).contains("Predicate"));
        }

        #[test]
        fn combinators() {
            let n = rocm_box();
            assert!(rocm().and(ram_at_least_gb(64)).eval(&n));
            assert!(!rocm().and(ram_at_least_gb(256)).eval(&n));
            assert!(cuda().or(rocm()).eval(&n));
            assert!(!cuda().or(os_is(Os::MacOs)).eval(&n));
            assert!(cuda().negate().eval(&n));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        parse_cores, parse_linux_ram_mb, parse_macos_gpus, parse_macos_ram_mb, parse_nvidia_smi,
        parse_os, parse_rocminfo, Gpu, GpuRuntime, GpuVendor, Os,
    };

    #[test]
    fn os_from_uname() {
        assert_eq!(parse_os("Linux\n"), Os::Linux);
        assert_eq!(parse_os("Darwin\n"), Os::MacOs);
        assert_eq!(parse_os("FreeBSD"), Os::Other("FreeBSD".to_string()));
    }

    #[test]
    fn cores_are_lenient() {
        assert_eq!(parse_cores("64\n"), 64);
        assert_eq!(parse_cores("  8 "), 8);
        assert_eq!(parse_cores(""), 0);
        assert_eq!(parse_cores("garbage"), 0);
    }

    #[test]
    fn linux_ram_from_meminfo() {
        let meminfo = "MemTotal:      131923148 kB\nMemFree:  1000 kB\n";
        assert_eq!(parse_linux_ram_mb(meminfo), 131_923_148 / 1024);
        assert_eq!(parse_linux_ram_mb("no memtotal here"), 0);
        assert_eq!(parse_linux_ram_mb("MemTotal:   notanumber kB"), 0);
    }

    #[test]
    fn macos_ram_from_memsize() {
        // 128 GiB in bytes.
        assert_eq!(parse_macos_ram_mb("137438953472\n"), 131_072);
        assert_eq!(parse_macos_ram_mb("nope"), 0);
    }

    #[test]
    fn nvidia_smi_gpus() {
        // A blank line is skipped, and a line with no memory column has unknown VRAM.
        let csv = "NVIDIA GeForce RTX 4090, 24564\n\nQuadro K2200\n";
        let gpus = parse_nvidia_smi(csv);
        assert_eq!(gpus.len(), 2);
        assert_eq!(
            gpus[0],
            Gpu {
                vendor: GpuVendor::Nvidia,
                runtime: Some(GpuRuntime::Cuda),
                model: "NVIDIA GeForce RTX 4090".to_string(),
                vram_mb: Some(24564),
            }
        );
        assert_eq!(gpus[1].model, "Quadro K2200");
        assert_eq!(gpus[1].vram_mb, None);
    }

    #[test]
    fn rocminfo_picks_gpu_agents_only() {
        let out = "\
*******
Agent 1
*******
  Marketing Name:          AMD EPYC 7551
  Device Type:             CPU
*******
Agent 2
*******
  Marketing Name:          AMD Radeon RX 7900 XTX
  Device Type:             GPU
*******
Agent 3
*******
  Marketing Name:          AMD Radeon RX 7800 XT
  Device Type:             GPU
";
        // Agent 2 flushes at the Agent 3 boundary; Agent 3 flushes at the end.
        let gpus = parse_rocminfo(out);
        assert_eq!(gpus.len(), 2);
        assert_eq!(gpus[0].vendor, GpuVendor::Amd);
        assert_eq!(gpus[0].runtime, Some(GpuRuntime::Rocm));
        assert_eq!(gpus[0].model, "AMD Radeon RX 7900 XTX");
        assert_eq!(gpus[1].model, "AMD Radeon RX 7800 XT");
    }

    #[test]
    fn node_profile_unknown_is_empty() {
        use super::{NodeProfile, Os};
        let u = NodeProfile::unknown();
        assert_eq!(u.cores, 0);
        assert_eq!(u.ram_mb, 0);
        assert!(u.gpus.is_empty());
        assert!(matches!(u.os, Os::Other(_)));
        assert_eq!(u.arch.isa, "unknown");
        assert!(u.arch.features.is_empty());
    }

    #[test]
    fn parse_cpu_arch_normalizes_features() {
        use super::parse_cpu_arch;
        // A Linux cpuinfo `flags : ...` line: only the part after the colon is
        // features, and they come out lowercased, sorted, and deduplicated.
        let arch = parse_cpu_arch("x86_64\n", "flags\t\t: SSE2 avx2 sse2 AVX2 fma");
        assert_eq!(arch.isa, "x86_64");
        assert_eq!(arch.features, vec!["avx2", "fma", "sse2"]);

        // No colon (a sysctl-style list) is taken whole.
        let mac = parse_cpu_arch("arm64", "neon fp asimd");
        assert_eq!(mac.isa, "arm64");
        assert_eq!(mac.features, vec!["asimd", "fp", "neon"]);
    }

    #[test]
    fn same_arch_compares_isa_and_features() {
        use super::{parse_cpu_arch, NodeProfile};
        let mut a = NodeProfile::unknown();
        let mut b = NodeProfile::unknown();
        a.arch = parse_cpu_arch("x86_64", "sse2 avx2");
        b.arch = parse_cpu_arch("x86_64", "avx2 sse2"); // same set, different order
        assert!(
            a.same_arch(&b),
            "same isa and feature set are the same arch"
        );

        b.arch = parse_cpu_arch("x86_64", "sse2 avx2 avx512f"); // extra feature
        assert!(
            !a.same_arch(&b),
            "a different feature set is a different arch"
        );

        b.arch = parse_cpu_arch("aarch64", "sse2 avx2"); // different isa
        assert!(!a.same_arch(&b), "a different isa is a different arch");
    }

    #[test]
    fn filter_first_match_wins() {
        use super::pred::{os_is, ram_at_least_gb, rocm};
        use super::{Filter, Gpu, GpuRuntime, GpuVendor, NodeProfile, Os, Role};

        let filter = Filter::new()
            .relay_only(os_is(Os::MacOs))
            .compute(rocm().and(ram_at_least_gb(16)))
            .otherwise(Role::Excluded);

        let mac = NodeProfile {
            os: Os::MacOs,
            hostname: String::new(),
            arch: crate::capability::CpuArch::unknown(),
            cores: 8,
            ram_mb: 16_384,
            gpus: vec![],
        };
        assert_eq!(filter.role_of(&mac), Role::RelayOnly);

        let rocm_box = NodeProfile {
            os: Os::Linux,
            hostname: String::new(),
            arch: crate::capability::CpuArch::unknown(),
            cores: 64,
            ram_mb: 131_072,
            gpus: vec![Gpu {
                vendor: GpuVendor::Amd,
                runtime: Some(GpuRuntime::Rocm),
                model: "RX 7900 XTX".to_string(),
                vram_mb: Some(24_576),
            }],
        };
        assert_eq!(filter.role_of(&rocm_box), Role::Compute);

        let plain = NodeProfile {
            os: Os::Linux,
            hostname: String::new(),
            arch: crate::capability::CpuArch::unknown(),
            cores: 4,
            ram_mb: 8_000,
            gpus: vec![],
        };
        assert_eq!(filter.role_of(&plain), Role::Excluded);
    }

    #[test]
    fn filter_exclude_rule_beats_default() {
        use super::pred::cuda;
        use super::{Filter, Gpu, GpuRuntime, GpuVendor, NodeProfile, Os, Role};

        // CUDA boxes are barred even though the default is Compute.
        let filter = Filter::new().exclude(cuda()).otherwise(Role::Compute);
        let cuda_box = NodeProfile {
            os: Os::Linux,
            hostname: String::new(),
            arch: crate::capability::CpuArch::unknown(),
            cores: 32,
            ram_mb: 65_536,
            gpus: vec![Gpu {
                vendor: GpuVendor::Nvidia,
                runtime: Some(GpuRuntime::Cuda),
                model: "RTX 4090".to_string(),
                vram_mb: Some(24_564),
            }],
        };
        assert_eq!(filter.role_of(&cuda_box), Role::Excluded);

        let cpu_only = NodeProfile {
            os: Os::Linux,
            hostname: String::new(),
            arch: crate::capability::CpuArch::unknown(),
            cores: 32,
            ram_mb: 65_536,
            gpus: vec![],
        };
        assert_eq!(filter.role_of(&cpu_only), Role::Compute);

        // The empty default filter excludes everything.
        assert_eq!(Filter::default().role_of(&cpu_only), Role::Excluded);
    }

    #[test]
    fn profile_query_helpers() {
        use super::{Gpu, GpuRuntime, GpuVendor, NodeProfile, Os};
        let rocm = NodeProfile {
            os: Os::Linux,
            hostname: String::new(),
            arch: crate::capability::CpuArch::unknown(),
            cores: 64,
            ram_mb: 131_072,
            gpus: vec![Gpu {
                vendor: GpuVendor::Amd,
                runtime: Some(GpuRuntime::Rocm),
                model: "RX 7900 XTX".to_string(),
                vram_mb: Some(24_576),
            }],
        };
        assert!(rocm.has_gpu());
        assert!(rocm.has_gpu_runtime(&GpuRuntime::Rocm));
        assert!(!rocm.has_gpu_runtime(&GpuRuntime::Cuda));
        assert_eq!(rocm.max_vram_mb(), 24_576);

        let bare = NodeProfile {
            os: Os::MacOs,
            hostname: String::new(),
            arch: crate::capability::CpuArch::unknown(),
            cores: 8,
            ram_mb: 16_384,
            gpus: vec![],
        };
        assert!(!bare.has_gpu());
        assert!(!bare.has_gpu_runtime(&GpuRuntime::Metal));
        assert_eq!(bare.max_vram_mb(), 0);
    }

    #[test]
    fn macos_metal_gpus_with_vendors() {
        let out = "      Chipset Model: Apple M2 Pro\n      Chipset Model: AMD Radeon Pro 5500M\n      Chipset Model: NVIDIA GeForce GT 750M\n      Chipset Model: Intel UHD Graphics 630\n      Chipset Model: SomeOther Card\n      Chipset Model:   \n";
        let gpus = parse_macos_gpus(out);
        assert_eq!(gpus.len(), 5); // the empty chipset line is skipped
        assert_eq!(gpus[0].vendor, GpuVendor::Apple);
        assert_eq!(gpus[1].vendor, GpuVendor::Amd);
        assert_eq!(gpus[2].vendor, GpuVendor::Nvidia);
        assert_eq!(gpus[3].vendor, GpuVendor::Intel);
        assert_eq!(
            gpus[4].vendor,
            GpuVendor::Other("SomeOther Card".to_string())
        );
        assert!(gpus.iter().all(|g| g.runtime == Some(GpuRuntime::Metal)));
    }
}
