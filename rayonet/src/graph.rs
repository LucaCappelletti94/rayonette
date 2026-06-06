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

use std::collections::{BTreeMap, BTreeSet};

use geometric_traits::{
    impls::{SortedVec, SquareCSR2D, SymmetricCSR2D, CSR2D},
    prelude::*,
    traits::{
        algorithms::connected_components::ConnectedComponentsResult, EdgesBuilder,
        VocabularyBuilder,
    },
};

use crate::observability::{parent_of, RunState};

/// The synthetic root vertex standing for the coordinator, the parent of every
/// top-level node. The leading NUL keeps it from colliding with any real machine
/// id.
const ROOT: &str = "\0coordinator";

/// The discovered relay tree as a DAG over physical nodes, keyed by stable id.
#[derive(Debug)]
pub struct Topology {
    /// Physical node ids in vertex-index order. Index 0 is always [`ROOT`].
    ids: Vec<String>,
    /// The vertex index of each physical id (and of [`ROOT`]).
    index: BTreeMap<String, usize>,
    /// Directed parent -> child edges as vertex-index pairs, deduplicated (a node
    /// reached by two relays contributes two distinct parent edges).
    edges: BTreeSet<(usize, usize)>,
}

impl Topology {
    /// Assemble the topology from a run's discovered nodes. Each profiled node
    /// contributes its physical id as a vertex and an edge from its parent's
    /// physical id (or [`ROOT`] for a top-level node), so a node seen on two
    /// paths becomes one vertex with two parent edges.
    #[must_use]
    pub fn from_run_state(state: &RunState) -> Self {
        // Vertex 0 is the coordinator root, then each distinct physical id in
        // sorted order for a deterministic vertex numbering.
        let physical: BTreeSet<&str> = state
            .nodes
            .values()
            .filter_map(|v| v.id.as_deref())
            .collect();
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
        // its single vertex, and identical edges collapse in the set. Self-loops
        // (a node reaching itself) are dropped, since the articulation algorithm
        // rejects them and they carry no topology.
        let mut edges = BTreeSet::new();
        for (path, view) in &state.nodes {
            let Some(child) = view.id.as_deref() else {
                continue;
            };
            let parent = parent_of(path).map_or(ROOT, |parent_path| {
                state
                    .nodes
                    .get(parent_path)
                    .and_then(|v| v.id.as_deref())
                    .unwrap_or(ROOT)
            });
            let (parent, child) = (index[parent], index[child]);
            if parent != child {
                edges.insert((parent, child));
            }
        }

        Self { ids, index, edges }
    }

    /// Whether the discovered children configuration is acyclic (a sane tree or
    /// DAG). A cycle means a node is reachable as its own ancestor.
    #[must_use]
    pub fn is_acyclic(&self) -> bool {
        self.directed().kahn().is_ok()
    }

    /// The physical ids of the relays that are single points of failure: the
    /// articulation points of the undirected projection, excluding the
    /// coordinator root (whose loss is inherent, not a relay fault).
    #[must_use]
    pub fn single_points_of_failure(&self) -> BTreeSet<String> {
        self.articulation_points()
            .into_iter()
            .filter_map(|v| self.ids.get(v).cloned())
            .filter(|id| id != ROOT)
            .collect()
    }

    /// The physical ids of the relays that are a parent of `id` (the coordinator
    /// root is never listed). More than one means the node is multiply reachable.
    #[must_use]
    pub fn parents_of(&self, id: &str) -> BTreeSet<String> {
        let Some(&child) = self.index.get(id) else {
            return BTreeSet::new();
        };
        self.edges
            .iter()
            .filter(|&&(_, c)| c == child)
            .filter_map(|&(parent, _)| self.ids.get(parent).cloned())
            .filter(|parent| parent != ROOT)
            .collect()
    }

    /// Whether `id` stays connected to the coordinator after any single relay
    /// dies: no articulation point lies on every path from the root to it. A node
    /// reachable through only one relay is not redundant.
    #[must_use]
    pub fn is_redundant(&self, id: &str) -> bool {
        let Some(&node) = self.index.get(id) else {
            return false;
        };
        // Only an articulation point can separate the node from the root, so it
        // is redundant unless removing one of them disconnects it. Removing a
        // vertex means dropping the edges incident to it.
        for cut in self.articulation_points() {
            if cut == node || cut == 0 {
                continue;
            }
            let survivors: Vec<(usize, usize)> = self
                .edges
                .iter()
                .copied()
                .filter(|&(parent, child)| parent != cut && child != cut)
                .collect();
            let graph = self.undirected(&survivors);
            let components: Result<ConnectedComponentsResult<'_, _, usize>, _> =
                graph.connected_components();
            if let Ok(components) = components {
                if components.component_of_node(node) != components.component_of_node(0) {
                    return false;
                }
            }
        }
        true
    }

    /// The directed CSR matrix of the DAG, the form [`Kahn`](geometric_traits::traits::algorithms::kahn::Kahn)
    /// consumes.
    fn directed(&self) -> SquareCSR2D<CSR2D<usize, usize, usize>> {
        DiEdgesBuilder::default()
            .expected_number_of_edges(self.edges.len())
            .expected_shape(self.ids.len())
            .edges(self.edges.iter().copied())
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
        let matrix: SymmetricCSR2D<CSR2D<usize, usize, usize>> = UndiEdgesBuilder::default()
            .expected_number_of_edges(edges.len())
            .expected_shape(order)
            .edges(edges.iter().copied())
            .build()
            .expect("the edges are vertex-index pairs within the vertex count");
        UndiGraph::from((nodes, matrix))
    }

    /// The cut vertices of the undirected projection, as vertex indices.
    fn articulation_points(&self) -> Vec<usize> {
        let edges: Vec<(usize, usize)> = self.edges.iter().copied().collect();
        self.undirected(&edges)
            .biconnected_components()
            .map(|decomposition| decomposition.articulation_points().collect())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::Topology;
    use crate::capability::{NodeProfile, Os, Role};
    use crate::observability::{Event, NodeState, RunState};
    use std::collections::BTreeSet;

    fn profile() -> NodeProfile {
        NodeProfile {
            os: Os::Linux,
            cores: 4,
            ram_mb: 8_000,
            gpus: Vec::new(),
        }
    }

    /// Build a run state from `(path_id, physical_id)` discovery facts.
    fn discovered(facts: &[(&str, &str)]) -> RunState {
        let mut state = RunState::default();
        for (path, id) in facts {
            state.apply(&Event::profiled(path, id, profile(), Role::Compute));
        }
        state
    }

    fn ids(items: &[&str]) -> BTreeSet<String> {
        items.iter().map(|s| (*s).to_string()).collect()
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
