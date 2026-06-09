//! The coordinator-side topology graph (PLAN.md R5).
//!
//! Discovery reports each node's stable physical id and its parent up to the
//! coordinator. This module folds those facts into one directed graph keyed by
//! physical id: a machine reached through two relays is deduped into a single
//! vertex with two parent edges, so the relay tree becomes a DAG. The graph then
//! answers the policy questions redundancy needs, using `geometric-traits`
//! algorithms over its CSR structures: is the children configuration acyclic
//! ([`Kahn`](geometric_traits::traits::algorithms::kahn::Kahn)), which relays are
//! single points of failure (articulation points of the undirected projection via
//! [`BiconnectedComponents`](geometric_traits::traits::algorithms::biconnected_components::BiconnectedComponents)),
//! and is a given compute node reachable by more than one independent path so no
//! single relay's death can strand it
//! ([`ConnectedComponents`](geometric_traits::traits::algorithms::connected_components::ConnectedComponents)).
//!
//! When a node is reachable by more than one path, the coordinator must pick one
//! to run it on (the primary) and keep the others as standbys. Each parent link
//! carries a measured [`LinkMetric`], and [`Topology::select_primaries`] ranks a
//! node's parents by a per-run [`Metric`]: the widest path (the largest
//! bottleneck bandwidth, best for big payloads) or the shortest path (the least
//! total latency, best for small ones). Shortest path runs on
//! [`PairwiseDijkstra`](geometric_traits::traits::algorithms::pairwise_dijkstra::PairwiseDijkstra).
//! Widest path is a maximum-bottleneck relaxation implemented here, since
//! `geometric-traits` does not expose it.

use std::collections::{BTreeMap, BTreeSet};

use geometric_traits::{
    impls::{SortedVec, SymmetricCSR2D, ValuedCSR2D, CSR2D},
    prelude::*,
    traits::{
        algorithms::connected_components::ConnectedComponentsResult, EdgesBuilder,
        VocabularyBuilder,
    },
};
// Only the test-only `directed` acyclicity check uses the square matrix form.
#[cfg(test)]
use geometric_traits::impls::SquareCSR2D;

use crate::capability::{NodeProfile, Role};
use crate::observability::{parent_of, Event, NodeView, RunState};
use crate::protocol::ChildAd;

/// The synthetic root vertex standing for the coordinator, the parent of every
/// top-level node. The leading NUL keeps it from colliding with any real machine
/// id.
const ROOT: &str = "\0coordinator";

/// Convert a microsecond latency to milliseconds for the path metric. A realistic
/// latency is far below f64's exact-integer range, so the cast does not lose
/// precision.
#[expect(
    clippy::cast_precision_loss,
    reason = "a realistic latency is far below f64's exact-integer range"
)]
fn microseconds_to_millis(microseconds: u64) -> f64 {
    microseconds as f64 / 1000.0
}

/// A measured parent-to-child link, the weight a [`Metric`] ranks paths by.
///
/// Latency is the round-trip time of the discovery handshake, always known.
/// Bandwidth comes from an opt-in calibrated transfer, so it is absent until that
/// probe runs and the widest-path metric is meaningless without it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct LinkMetric {
    /// Link round-trip latency in milliseconds (lower is better).
    latency_ms: f64,
    /// Link bandwidth in bytes per second, if probed (higher is better).
    bandwidth: Option<f64>,
}

impl Default for LinkMetric {
    /// An unmeasured link: zero latency and no bandwidth, the neutral weight a
    /// freshly assembled topology carries until discovery fills it in.
    fn default() -> Self {
        Self {
            latency_ms: 0.0,
            bandwidth: None,
        }
    }
}

/// How the coordinator ranks the candidate paths to a multiply-reachable node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum Metric {
    /// Maximize the path's bottleneck bandwidth (the default: the slowest hop
    /// throttles a large transfer, so the widest bottleneck wins).
    #[default]
    WidestBandwidth,
    /// Minimize the path's total latency (better for small, latency-bound work).
    ShortestLatency,
}

/// One relay as the coordinator sees it at the handshake: its label, the latency
/// of the coordinator's link to it, and the children it built and is offering.
#[derive(Debug)]
pub(crate) struct RelayReport {
    /// The relay's label under the coordinator (its path segment).
    label: String,
    /// Measured latency (microseconds) of the coordinator's link to this relay.
    latency_us: u64,
    /// The children this relay discovered, by id and link latency.
    children: Vec<ChildAd>,
}

impl RelayReport {
    /// Describe one relay at the handshake: its `label` under the coordinator,
    /// the `latency_us` of the coordinator's link to it, and the `children` it
    /// built and is offering.
    #[must_use]
    pub(crate) const fn new(label: String, latency_us: u64, children: Vec<ChildAd>) -> Self {
        Self {
            label,
            latency_us,
            children,
        }
    }
}

/// Choose, by `metric`, which children each relay activates now.
///
/// A node reachable through several relays runs on its primary path, and every
/// uniquely reached child runs too. Returns each relay's active child labels,
/// parallel to `relays`. The coordinator runs this over the relays' discovery
/// reports to dedup redundant paths before any task flows, so a shared node runs
/// on its best path and the others hold it standby.
#[must_use]
pub(crate) fn choose_active(relays: &[RelayReport], metric: Metric) -> Vec<Vec<String>> {
    let primaries = Topology::from_run_state(&relay_topology(relays)).select_primaries(metric);
    relays
        .iter()
        .map(|relay| {
            relay
                .children
                .iter()
                .filter(|child| {
                    // A uniquely reached child (absent from the primary map) is
                    // always active. A shared one runs only on its primary relay.
                    primaries.get(child.id()).is_none_or(|order| {
                        order.first().map(String::as_str) == Some(relay.label.as_str())
                    })
                })
                .map(|child| child.label().to_string())
                .collect()
        })
        .collect()
}

/// The synthesized two-level run state (root -> relay -> child) the path metric is
/// built from, weighting each edge with its measured latency. A relay's own label
/// doubles as its vertex id (relays are distinct), so only shared child ids dedup.
fn relay_topology(relays: &[RelayReport]) -> RunState {
    let mut state = RunState::default();
    for relay in relays {
        state.apply(&Event::profiled(
            &relay.label,
            &relay.label,
            NodeProfile::unknown(),
            Role::Compute,
            relay.latency_us,
        ));
        for child in &relay.children {
            state.apply(&Event::profiled(
                &format!("{}/{}", relay.label, child.label()),
                child.id(),
                NodeProfile::unknown(),
                Role::Compute,
                child.latency_us(),
            ));
        }
    }
    state
}

/// The physical ids of the compute children reachable through only one relay, so
/// no alternate path survives that relay's death. A run that requires redundancy
/// refuses to start when this is non-empty.
#[must_use]
pub(crate) fn redundancy_gaps(relays: &[RelayReport]) -> Vec<String> {
    let topology = Topology::from_run_state(&relay_topology(relays));
    let mut gaps = Vec::new();
    let mut seen = BTreeSet::new();
    for child in relays.iter().flat_map(|relay| &relay.children) {
        if seen.insert(child.id()) && !topology.is_redundant(child.id()) {
            gaps.push(child.id().to_string());
        }
    }
    gaps
}

/// The discovered relay tree as a DAG over physical nodes, keyed by stable id.
#[derive(Debug)]
pub(crate) struct Topology {
    /// Physical node ids in vertex-index order. Index 0 is always [`ROOT`].
    ids: Vec<String>,
    /// The vertex index of each physical id (and of [`ROOT`]).
    index: BTreeMap<String, usize>,
    /// Directed parent -> child edges (a node reached by two relays contributes
    /// two distinct parent edges), each with its measured link weight.
    edges: BTreeMap<(usize, usize), LinkMetric>,
}

impl Topology {
    /// Assemble the topology from a run's discovered nodes. Each profiled node
    /// contributes its physical id as a vertex and an edge from its parent's
    /// physical id (or [`ROOT`] for a top-level node), so a node seen on two
    /// paths becomes one vertex with two parent edges.
    #[must_use]
    pub(crate) fn from_run_state(state: &RunState) -> Self {
        // Vertex 0 is the coordinator root, then each distinct physical id in
        // sorted order for a deterministic vertex numbering.
        let physical: BTreeSet<&str> = state.nodes().values().filter_map(NodeView::id).collect();
        let mut ids = Vec::with_capacity(physical.len() + 1);
        let mut index = BTreeMap::new();
        ids.push(ROOT.to_string());
        index.insert(ROOT.to_string(), 0);
        for id in physical {
            index.insert(id.to_string(), ids.len());
            ids.push(id.to_string());
        }

        // One edge per discovered node, from its parent path's physical id (or
        // the root) to its own. A node on two paths yields two parent edges into
        // its single vertex, and identical edges collapse in the map. Self-loops
        // (a node reaching itself) are dropped, since the articulation algorithm
        // rejects them and they carry no topology. Link weights start neutral and
        // are filled in as discovery measures them.
        let mut edges = BTreeMap::new();
        for (path, view) in state.nodes() {
            let Some(child) = view.id() else {
                continue;
            };
            let parent = parent_of(path).map_or(ROOT, |parent_path| {
                state
                    .nodes()
                    .get(parent_path)
                    .and_then(NodeView::id)
                    .unwrap_or(ROOT)
            });
            let (parent, child) = (index[parent], index[child]);
            if parent != child {
                // This path's last-hop link latency weights its edge. A node on
                // two paths gets two edges, each with its own latency.
                let latency_ms = view.latency_us().map_or(0.0, microseconds_to_millis);
                edges.insert(
                    (parent, child),
                    LinkMetric {
                        latency_ms,
                        bandwidth: None,
                    },
                );
            }
        }

        Self { ids, index, edges }
    }

    /// Whether the discovered children configuration is acyclic (a sane tree or
    /// DAG). A cycle means a node is reachable as its own ancestor.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn is_acyclic(&self) -> bool {
        self.directed().kahn().is_ok()
    }

    /// The physical ids of the relays that are single points of failure: the
    /// articulation points of the undirected projection, excluding the
    /// coordinator root (whose loss is inherent, not a relay fault).
    #[must_use]
    pub(crate) fn single_points_of_failure(&self) -> BTreeSet<String> {
        self.articulation_points()
            .into_iter()
            .filter_map(|v| self.ids.get(v).cloned())
            .filter(|id| id != ROOT)
            .collect()
    }

    /// The physical ids of the relays that are a parent of `id` (the coordinator
    /// root is never listed). More than one means the node is multiply reachable.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn parents_of(&self, id: &str) -> BTreeSet<String> {
        let Some(&child) = self.index.get(id) else {
            return BTreeSet::new();
        };
        self.edges
            .keys()
            .filter(|&&(_, c)| c == child)
            .filter_map(|&(parent, _)| self.ids.get(parent).cloned())
            .filter(|parent| parent != ROOT)
            .collect()
    }

    /// Whether `id` stays connected to the coordinator after any single relay
    /// dies: no articulation point lies on every path from the root to it. A node
    /// reachable through only one relay is not redundant.
    #[must_use]
    pub(crate) fn is_redundant(&self, id: &str) -> bool {
        let Some(&node) = self.index.get(id) else {
            return false;
        };
        // Only an articulation point can separate the node from the root, so it
        // is redundant unless removing one of them disconnects it.
        for cut in self.articulation_points() {
            if cut == node || cut == 0 {
                continue;
            }
            if !self.root_reaches(node, cut) {
                return false;
            }
        }
        true
    }

    /// Order each multiply-reachable node's parents best-first under `metric`: the
    /// first is the primary path to run the node on, the rest are standbys. Nodes
    /// with a single parent are omitted, since they offer no choice.
    #[must_use]
    pub(crate) fn select_primaries(&self, metric: Metric) -> BTreeMap<String, Vec<String>> {
        // The cost-to-root of every vertex, in the units the chosen metric ranks
        // by (summed latency, or bottleneck bandwidth).
        let cost = match metric {
            Metric::ShortestLatency => self.shortest_from_root(),
            Metric::WidestBandwidth => self.widest_from_root(),
        };

        // Gather each child's parents, then keep only the multiply-reachable ones.
        let mut parents: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
        for &(parent, child) in self.edges.keys() {
            parents.entry(child).or_default().push(parent);
        }

        let mut chosen = BTreeMap::new();
        for (child, mut candidates) in parents {
            if candidates.len() < 2 {
                continue;
            }
            // Rank ascending: a smaller key is the better path. Ties break on the
            // parent id so the ordering is deterministic.
            candidates.sort_by(|&a, &b| {
                self.rank_key(metric, &cost, a, child)
                    .total_cmp(&self.rank_key(metric, &cost, b, child))
                    .then_with(|| self.ids.get(a).cmp(&self.ids.get(b)))
            });
            let ranked = candidates.iter().map(|&parent| self.name(parent)).collect();
            chosen.insert(self.name(child), ranked);
        }
        chosen
    }

    /// The physical node ids in vertex-index order, index 0 being the synthetic
    /// coordinator root. A graph layout positions one point per entry.
    #[must_use]
    pub(crate) fn vertices(&self) -> &[String] {
        &self.ids
    }

    /// The directed parent -> child edges as vertex-index pairs into
    /// [`vertices`](Self::vertices). A node reached through two relays contributes
    /// two edges into its single vertex.
    #[must_use]
    pub(crate) fn edge_indices(&self) -> Vec<(usize, usize)> {
        self.edges.keys().copied().collect()
    }

    /// Set a parent link's measured weight. Discovery fills these in for real in a
    /// later step. Tests use it to weight a topology directly.
    #[cfg(test)]
    fn set_link(&mut self, parent: &str, child: &str, metric: LinkMetric) {
        if let (Some(&parent), Some(&child)) = (self.index.get(parent), self.index.get(child)) {
            self.edges.insert((parent, child), metric);
        }
    }

    /// The directed CSR matrix of the DAG, the form [`Kahn`](geometric_traits::traits::algorithms::kahn::Kahn)
    /// consumes.
    #[cfg(test)]
    fn directed(&self) -> SquareCSR2D<CSR2D<usize, usize, usize>> {
        DiEdgesBuilder::default()
            .expected_number_of_edges(self.edges.len())
            .expected_shape(self.ids.len())
            .edges(self.edges.keys().copied())
            .build()
            .expect("the edges are vertex-index pairs within the vertex count")
    }

    /// The undirected projection over `edges` (a `SymmetricCSR2D` mirrors each
    /// directed edge), the form the connectivity algorithms consume.
    fn undirected(&self, edges: &[(usize, usize)]) -> UndiGraph<usize> {
        let order = self.ids.len();
        let nodes: SortedVec<usize> = GenericVocabularyBuilder::default()
            .expected_number_of_symbols(order)
            .symbols((0..order).enumerate())
            .build()
            .expect("a contiguous 0..order vocabulary is always valid");
        // The undirected builder is upper triangular and wants its coordinates in
        // sorted order, so each edge is normalized to (min, max) and the whole set
        // re-sorted: flipping a directed parent -> child edge can move it earlier
        // than an edge already emitted, which the builder rejects as unordered.
        // Deduplicate too, since two directed edges can normalize to the same pair.
        let mut undirected: Vec<(usize, usize)> =
            edges.iter().map(|&(a, b)| (a.min(b), a.max(b))).collect();
        undirected.sort_unstable();
        undirected.dedup();
        let matrix: SymmetricCSR2D<CSR2D<usize, usize, usize>> = UndiEdgesBuilder::default()
            .expected_number_of_edges(undirected.len())
            .expected_shape(order)
            .edges(undirected.into_iter())
            .build()
            .expect("the edges are vertex-index pairs within the vertex count");
        UndiGraph::from((nodes, matrix))
    }

    /// The cut vertices of the undirected projection, as vertex indices.
    fn articulation_points(&self) -> Vec<usize> {
        let edges: Vec<(usize, usize)> = self.edges.keys().copied().collect();
        self.undirected(&edges)
            .biconnected_components()
            .map(|decomposition| decomposition.articulation_points().collect())
            .unwrap_or_default()
    }

    /// The ranking key of routing `child` through `parent` under `metric`, given
    /// the precomputed cost-to-root of every vertex. Smaller is better, so the
    /// widest-path bottleneck is negated (a larger bottleneck sorts first).
    fn rank_key(&self, metric: Metric, cost: &[f64], parent: usize, child: usize) -> f64 {
        let link = self
            .edges
            .get(&(parent, child))
            .copied()
            .unwrap_or_default();
        let parent_cost = cost.get(parent).copied().unwrap_or(f64::INFINITY);
        match metric {
            Metric::ShortestLatency => parent_cost + link.latency_ms,
            Metric::WidestBandwidth => -parent_cost.min(link.bandwidth.unwrap_or(f64::INFINITY)),
        }
    }

    /// The least summed latency from [`ROOT`] to every vertex, by
    /// [`PairwiseDijkstra`](geometric_traits::traits::algorithms::pairwise_dijkstra::PairwiseDijkstra)
    /// over a latency-valued matrix. An unreachable vertex is infinitely far.
    fn shortest_from_root(&self) -> Vec<f64> {
        let order = self.ids.len();
        let valued: ValuedCSR2D<usize, usize, usize, f64> =
            GenericEdgesBuilder::<_, ValuedCSR2D<usize, usize, usize, f64>>::default()
                .expected_number_of_edges(self.edges.len())
                .expected_shape((order, order))
                .edges(
                    self.edges
                        .iter()
                        .map(|(&(p, c), link)| (p, c, link.latency_ms)),
                )
                .build()
                .expect("the edges are vertex-index pairs within the vertex count");
        let distances = valued
            .pairwise_dijkstra()
            .expect("latencies are finite and non-negative");
        (0..order)
            .map(|v| distances.value((0, v)).unwrap_or(f64::INFINITY))
            .collect()
    }

    /// Whether the root still reaches `node` once every edge touching `cut` is
    /// removed (the test for whether losing the relay `cut` strands `node`).
    fn root_reaches(&self, node: usize, cut: usize) -> bool {
        let survivors: Vec<(usize, usize)> = self
            .edges
            .keys()
            .copied()
            .filter(|&(parent, child)| parent != cut && child != cut)
            .collect();
        let graph = self.undirected(&survivors);
        let components: ConnectedComponentsResult<'_, _, usize> = graph
            .connected_components()
            .expect("a usize marker holds any component count");
        components.component_of_node(node) == components.component_of_node(0)
    }

    /// The physical id at `vertex` (empty for an out-of-range vertex, which the
    /// callers never pass).
    fn name(&self, vertex: usize) -> String {
        self.ids.get(vertex).cloned().unwrap_or_default()
    }

    /// The widest bottleneck bandwidth from [`ROOT`] to every vertex (a maximum
    /// bottleneck relaxation, since `geometric-traits` has no widest-path). An
    /// unmeasured link imposes no bottleneck, and an unreachable vertex is
    /// infinitely narrow.
    fn widest_from_root(&self) -> Vec<f64> {
        let mut best = vec![f64::NEG_INFINITY; self.ids.len()];
        if let Some(root) = best.first_mut() {
            *root = f64::INFINITY;
        }
        loop {
            let mut changed = false;
            for (&(parent, child), link) in &self.edges {
                let bandwidth = link.bandwidth.unwrap_or(f64::INFINITY);
                let through = best
                    .get(parent)
                    .copied()
                    .unwrap_or(f64::NEG_INFINITY)
                    .min(bandwidth);
                if through > best.get(child).copied().unwrap_or(f64::NEG_INFINITY) {
                    if let Some(slot) = best.get_mut(child) {
                        *slot = through;
                        changed = true;
                    }
                }
            }
            if !changed {
                break;
            }
        }
        best
    }
}

#[cfg(test)]
mod tests {
    use super::Topology;
    use crate::capability::{NodeProfile, Os, Role};
    use crate::observability::{Event, NodeState, RunState};
    use std::collections::BTreeSet;

    fn profile() -> NodeProfile {
        NodeProfile::new(
            Os::Linux,
            String::new(),
            crate::capability::CpuArch::unknown(),
            4,
            8_000,
            Vec::new(),
        )
    }

    /// Build a run state from `(path_id, physical_id)` discovery facts.
    fn discovered(facts: &[(&str, &str)]) -> RunState {
        let mut state = RunState::default();
        for (path, id) in facts {
            state.apply(&Event::profiled(path, id, profile(), Role::Compute, 0));
        }
        state
    }

    fn ids(items: &[&str]) -> BTreeSet<String> {
        items.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn analysis_survives_edges_that_unsort_when_normalized() {
        // Vertices are numbered by sorted id (ROOT=0, a=1, b=2, c=3, d=4, e=5). The
        // edge e -> b is (5, 2), which flips to (2, 5) and so must sort before the
        // already-emitted c -> d (3, 4). The undirected builder rejects an unsorted
        // sequence, so this used to panic; the analysis must just run.
        let state = discovered(&[
            ("a", "a"),
            ("c", "c"),
            ("c/d", "d"),
            ("e", "e"),
            ("e/b", "b"),
        ]);
        let topo = Topology::from_run_state(&state);
        // c and e each gate their lone child, so both are single points of failure;
        // the top-level leaf a gates nothing.
        assert_eq!(topo.single_points_of_failure(), ids(&["c", "e"]));
        assert!(topo.is_acyclic());
        assert!(!topo.is_redundant("d"));
    }

    #[test]
    fn a_diamond_dedups_the_shared_leaf_into_one_two_parent_vertex() {
        // coordinator -> relay A and relay B, both naming the same leaf L.
        let state = discovered(&[("A", "idA"), ("B", "idB"), ("A/L", "idL"), ("B/L", "idL")]);
        let topo = Topology::from_run_state(&state);

        // The shared leaf is a single vertex reached from both relays, and a
        // top-level relay's only parent is the coordinator, so it lists none.
        assert_eq!(topo.parents_of("idL"), ids(&["idA", "idB"]));
        assert!(topo.parents_of("idA").is_empty());

        // A diamond is acyclic, the shared leaf is not a cut vertex, and it stays
        // reachable if either relay dies.
        assert!(topo.is_acyclic());
        assert!(!topo.single_points_of_failure().contains("idL"));
        assert!(topo.is_redundant("idL"));
    }

    #[test]
    fn a_single_parent_relay_is_a_flagged_point_of_failure() {
        // coordinator -> relay A -> leaf L: the only path to L runs through A.
        let state = discovered(&[("A", "idA"), ("A/L", "idL")]);
        let topo = Topology::from_run_state(&state);

        assert_eq!(topo.parents_of("idL"), ids(&["idA"]));
        // A is an articulation point, so its leaf is reachable through only one
        // relay and is therefore not redundant.
        assert!(topo.single_points_of_failure().contains("idA"));
        assert!(!topo.is_redundant("idL"));
    }

    #[test]
    fn the_coordinator_root_is_not_reported_as_a_point_of_failure() {
        // Two independent top-level relays: the coordinator is the only cut vertex
        // of the graph, but it is the root, not a relay, so it is not flagged.
        let state = discovered(&[("A", "idA"), ("B", "idB")]);
        let topo = Topology::from_run_state(&state);
        assert!(topo.single_points_of_failure().is_empty());
    }

    #[test]
    fn a_cyclic_children_config_is_detected() {
        // idA is reached under idB and idB under idA: a children-file cycle.
        let state = discovered(&[("A", "idA"), ("A/B", "idB"), ("A/B/A2", "idA")]);
        let topo = Topology::from_run_state(&state);
        assert!(!topo.is_acyclic());
    }

    #[test]
    fn an_unknown_node_has_no_parents_and_is_not_redundant() {
        let state = discovered(&[("A", "idA"), ("A/L", "idL")]);
        let topo = Topology::from_run_state(&state);
        assert!(topo.parents_of("missing").is_empty());
        assert!(!topo.is_redundant("missing"));
    }

    #[test]
    fn a_node_seen_without_a_profile_contributes_nothing() {
        // A node observed only through a lifecycle event has no profile and so no
        // physical id, so it adds no vertex or edge to the topology.
        let mut state = discovered(&[("A", "idA"), ("A/L", "idL")]);
        state.apply(&Event::node("A/ghost", NodeState::Working));
        let topo = Topology::from_run_state(&state);
        assert_eq!(topo.parents_of("idL"), ids(&["idA"]));
        assert!(!topo.is_redundant("idL"));
    }

    #[test]
    fn the_metric_picks_the_expected_primary_on_a_weighted_diamond() {
        use super::{LinkMetric, Metric, ROOT};
        // Diamond: leaf L reachable via relay A and relay B. The route through A
        // is low latency but narrow, the route through B is high latency but wide.
        let state = discovered(&[("A", "idA"), ("B", "idB"), ("A/L", "idL"), ("B/L", "idL")]);
        let mut topo = Topology::from_run_state(&state);
        let link = |latency_ms, bandwidth| LinkMetric {
            latency_ms,
            bandwidth: Some(bandwidth),
        };
        topo.set_link(ROOT, "idA", link(1.0, 10.0));
        topo.set_link("idA", "idL", link(1.0, 10.0));
        topo.set_link(ROOT, "idB", link(5.0, 100.0));
        topo.set_link("idB", "idL", link(1.0, 100.0));

        // Widest path takes the wide route through B, shortest path the quick one
        // through A. The other parent is the standby in each case.
        let widest = topo.select_primaries(Metric::WidestBandwidth);
        assert_eq!(widest["idL"], vec!["idB".to_string(), "idA".to_string()]);
        let shortest = topo.select_primaries(Metric::ShortestLatency);
        assert_eq!(shortest["idL"], vec!["idA".to_string(), "idB".to_string()]);
    }

    #[test]
    fn metric_and_link_types_expose_their_derives() {
        use super::{LinkMetric, Metric};
        let link = LinkMetric {
            latency_ms: 1.0,
            bandwidth: Some(2.0),
        };
        let same = link;
        assert_eq!(link, same);
        assert_ne!(link, LinkMetric::default());
        assert!(format!("{link:?}").contains("LinkMetric"));

        assert_eq!(Metric::default(), Metric::WidestBandwidth);
        assert_ne!(Metric::WidestBandwidth, Metric::ShortestLatency);
        assert!(format!("{:?}", Metric::ShortestLatency).contains("Shortest"));
    }

    #[test]
    fn equally_good_paths_break_ties_by_id() {
        use super::{LinkMetric, Metric, ROOT};
        // Both routes to L are identical, so neither metric prefers one. The tie
        // breaks on the parent id, putting idA before idB deterministically.
        let state = discovered(&[("A", "idA"), ("B", "idB"), ("A/L", "idL"), ("B/L", "idL")]);
        let mut topo = Topology::from_run_state(&state);
        let link = LinkMetric {
            latency_ms: 1.0,
            bandwidth: Some(50.0),
        };
        for (parent, child) in [(ROOT, "idA"), ("idA", "idL"), (ROOT, "idB"), ("idB", "idL")] {
            topo.set_link(parent, child, link);
        }
        assert_eq!(
            topo.select_primaries(Metric::WidestBandwidth)["idL"],
            vec!["idA".to_string(), "idB".to_string()]
        );
    }

    #[test]
    fn from_run_state_weights_edges_by_measured_latency() {
        use super::Metric;
        // A diamond whose discovery measured a faster last hop through A than B.
        // The latencies ride the profiled events, so shortest-latency selection
        // works straight off the run state with no hand-set weights.
        let p = profile();
        let mut state = RunState::default();
        state.apply(&Event::profiled(
            "A",
            "idA",
            p.clone(),
            Role::Compute,
            1_000,
        ));
        state.apply(&Event::profiled(
            "B",
            "idB",
            p.clone(),
            Role::Compute,
            1_000,
        ));
        state.apply(&Event::profiled(
            "A/L",
            "idL",
            p.clone(),
            Role::Compute,
            1_000,
        ));
        state.apply(&Event::profiled("B/L", "idL", p, Role::Compute, 9_000));
        let topo = Topology::from_run_state(&state);
        assert_eq!(
            topo.select_primaries(Metric::ShortestLatency)["idL"],
            vec!["idA".to_string(), "idB".to_string()]
        );
    }

    #[test]
    fn choose_active_runs_a_shared_child_on_its_lower_latency_relay() {
        use super::{choose_active, Metric, RelayReport};
        use crate::protocol::ChildAd;
        let shared = |latency_us| ChildAd::new("L".to_string(), "idL".to_string(), 1, latency_us);
        // Both relays reach leaf L, but "fast" has the lower-latency path. "fast"
        // also has its own leaf U, reached only through it.
        let relays = vec![
            RelayReport {
                label: "fast".to_string(),
                latency_us: 100,
                children: vec![
                    shared(100),
                    ChildAd::new("U".to_string(), "idU".to_string(), 1, 100),
                ],
            },
            RelayReport {
                label: "slow".to_string(),
                latency_us: 5_000,
                children: vec![shared(5_000)],
            },
        ];
        let active = choose_active(&relays, Metric::ShortestLatency);
        // The fast relay runs the shared leaf and its own; the slow relay holds
        // the shared leaf as a standby.
        assert_eq!(active[0], vec!["L".to_string(), "U".to_string()]);
        assert!(active[1].is_empty());
    }

    #[test]
    fn redundancy_gaps_flags_compute_behind_a_single_relay() {
        use super::{redundancy_gaps, RelayReport};
        use crate::protocol::ChildAd;
        let child = |label: &str, id: &str| ChildAd::new(label.to_string(), id.to_string(), 1, 0);
        // L is shared by both relays (redundant). X hangs off relay A alone.
        let relays = vec![
            RelayReport {
                label: "A".to_string(),
                latency_us: 0,
                children: vec![child("L", "idL"), child("X", "idX")],
            },
            RelayReport {
                label: "B".to_string(),
                latency_us: 0,
                children: vec![child("L", "idL")],
            },
        ];
        assert_eq!(redundancy_gaps(&relays), vec!["idX".to_string()]);
    }

    #[test]
    fn a_single_parent_node_offers_no_path_choice() {
        use super::Metric;
        // Nothing is multiply reachable, so the default (widest) selection, run
        // over links with no measured bandwidth, is empty.
        let state = discovered(&[("A", "idA"), ("A/L", "idL")]);
        let topo = Topology::from_run_state(&state);
        assert!(topo.select_primaries(Metric::default()).is_empty());
    }

    #[test]
    fn a_redundant_node_survives_when_an_unrelated_relay_dies() {
        // A diamond over L (reached via relays A and B), plus a leaf X hanging
        // only off A. A is a cut vertex (its death strands X), but L stays
        // reachable through B, so L is redundant while X is not. A itself is
        // reached directly from the coordinator, so it counts as redundant.
        let state = discovered(&[
            ("A", "idA"),
            ("B", "idB"),
            ("A/L", "idL"),
            ("B/L", "idL"),
            ("A/X", "idX"),
        ]);
        let topo = Topology::from_run_state(&state);
        assert!(topo.single_points_of_failure().contains("idA"));
        assert!(topo.is_redundant("idL"));
        assert!(!topo.is_redundant("idX"));
        assert!(topo.is_redundant("idA"));
    }
}
