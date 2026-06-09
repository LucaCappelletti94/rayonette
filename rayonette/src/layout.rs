//! Deterministic 2D positions for the topology graph panel, behind the `tui`
//! feature.
//!
//! The graph panel draws the relay tree as a node-link diagram. This module turns
//! a [`Topology`] into one position per vertex using the `ForceAtlas2`
//! force-directed layout from `geometric-traits`. The layout is made fully
//! deterministic (fixed initial positions on a circle and a fixed iteration count,
//! never the random seed) so the rendered graph is stable and stays in the golden
//! snapshot. Positions are normalized into the unit square, and the panel maps
//! them onto its canvas. The panel does not know `ForceAtlas2` produced them,
//! leaving room for another backend behind this same seam.

use std::collections::BTreeMap;

use geometric_traits::{impls::ValuedCSR2D, prelude::*, traits::ForceAtlas2Config};

use crate::graph::Topology;

/// A weighted adjacency matrix indexed by vertex, the form `ForceAtlas2` consumes.
type WeightedMatrix = ValuedCSR2D<usize, usize, usize, f64>;

/// Iterations of the force simulation. Fixed for determinism; our graphs are
/// small (tens of nodes) so this settles well within a frame budget.
const ITERATIONS: usize = 500;

/// Compute a normalized 2D position for every vertex of `topology`.
///
/// Returns a map from physical node id (including the synthetic coordinator root)
/// to an `(x, y)` in the unit square `[0, 1]^2`. Deterministic: the same topology
/// always yields the same positions.
#[must_use]
pub fn positions(topology: &Topology) -> BTreeMap<String, (f64, f64)> {
    let vertices = topology.vertices();

    // A lone coordinator (no nodes discovered yet) has nothing to spread out: it
    // sits at the centre.
    if vertices.len() <= 1 {
        return vertices.iter().map(|id| (id.clone(), (0.5, 0.5))).collect();
    }

    let coords = force_atlas2_coords(vertices.len(), &topology.edge_indices());
    normalize(vertices, &coords)
}

/// Run `ForceAtlas2` over the `n`-vertex graph with the given directed `edges`,
/// returning each vertex's raw `(x, y)`.
fn force_atlas2_coords(n: usize, edges: &[(usize, usize)]) -> Vec<(f64, f64)> {
    // ForceAtlas2 needs a symmetric matrix, so mirror each directed parent -> child
    // edge. A uniform weight lays out the tree's shape rather than its latencies.
    let mut directed: Vec<(usize, usize, f64)> = Vec::with_capacity(edges.len() * 2);
    for &(a, b) in edges {
        directed.push((a, b, 1.0));
        directed.push((b, a, 1.0));
    }
    directed.sort_unstable_by_key(|&(a, b, _)| (a, b));

    let matrix: WeightedMatrix = GenericEdgesBuilder::<_, WeightedMatrix>::default()
        .expected_number_of_edges(directed.len())
        .expected_shape((n, n))
        .edges(directed.into_iter())
        .build()
        .expect("edges are vertex-index pairs within the vertex count");

    let config = ForceAtlas2Config {
        iterations: ITERATIONS,
        initial_positions: Some(circle_positions(n)),
        ..ForceAtlas2Config::default()
    };
    let result = matrix
        .force_atlas2(&config)
        .expect("a connected, uniformly weighted graph always lays out");
    (0..n)
        .map(|i| {
            let point = result.point(i);
            (point[0], point[1])
        })
        .collect()
}

/// Seed positions evenly around the unit circle, so the deterministic layout
/// starts from a fixed, well-spread configuration instead of a random one.
#[expect(
    clippy::cast_precision_loss,
    reason = "a node count is far below f64's exact-integer range"
)]
fn circle_positions(n: usize) -> Vec<[f64; 2]> {
    (0..n)
        .map(|i| {
            let theta = std::f64::consts::TAU * i as f64 / n as f64;
            [theta.cos(), theta.sin()]
        })
        .collect()
}

/// Rescale raw coordinates into the unit square, keyed by vertex id.
fn normalize(vertices: &[String], coords: &[(f64, f64)]) -> BTreeMap<String, (f64, f64)> {
    let mut min = (f64::INFINITY, f64::INFINITY);
    let mut max = (f64::NEG_INFINITY, f64::NEG_INFINITY);
    for &(x, y) in coords {
        min = (min.0.min(x), min.1.min(y));
        max = (max.0.max(x), max.1.max(y));
    }
    vertices
        .iter()
        .zip(coords)
        .map(|(id, &(x, y))| {
            (
                id.clone(),
                (rescale(x, min.0, max.0), rescale(y, min.1, max.1)),
            )
        })
        .collect()
}

/// Map `value` from `[lo, hi]` onto `[0, 1]`. A degenerate (zero-width) range maps
/// to the centre, avoiding a divide-by-zero.
fn rescale(value: f64, lo: f64, hi: f64) -> f64 {
    let range = hi - lo;
    if range < f64::EPSILON {
        0.5
    } else {
        (value - lo) / range
    }
}

#[cfg(test)]
mod tests {
    use super::{positions, rescale};
    use crate::capability::{NodeProfile, Os, Role};
    use crate::graph::Topology;
    use crate::observability::{Event, RunState};

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

    /// A topology from a chain of profiled nodes: each `path` is one physical id
    /// reached from the one before it.
    fn chain(paths: &[(&str, &str)]) -> Topology {
        let mut state = RunState::default();
        for &(path, id) in paths {
            state.apply(&Event::profiled(path, id, profile(), Role::Compute, 0));
        }
        Topology::from_run_state(&state)
    }

    #[test]
    fn positions_are_deterministic() {
        let topology = chain(&[("a", "a"), ("a/b", "b"), ("a/c", "c")]);
        assert_eq!(positions(&topology), positions(&topology));
    }

    #[test]
    fn every_vertex_lands_in_the_unit_square() {
        let topology = chain(&[("a", "a"), ("a/b", "b"), ("a/c", "c")]);
        let pos = positions(&topology);
        // The synthetic root plus the three nodes.
        assert_eq!(pos.len(), 4);
        for (x, y) in pos.values() {
            assert!((0.0..=1.0).contains(x), "x out of range: {x}");
            assert!((0.0..=1.0).contains(y), "y out of range: {y}");
        }
    }

    #[test]
    fn a_lone_coordinator_sits_at_the_centre() {
        let topology = Topology::from_run_state(&RunState::default());
        let pos = positions(&topology);
        assert_eq!(pos.len(), 1);
        assert_eq!(pos.values().next(), Some(&(0.5, 0.5)));
    }

    #[test]
    fn adjacent_nodes_lie_closer_than_distant_ones() {
        // A path root -> a -> b -> c -> d stretches out under the forces, so a step
        // along the chain is shorter than spanning it.
        let topology = chain(&[("a", "a"), ("a/b", "b"), ("a/b/c", "c"), ("a/b/c/d", "d")]);
        let pos = positions(&topology);
        let dist = |p: &str, q: &str| {
            let (px, py) = pos[p];
            let (qx, qy) = pos[q];
            (px - qx).hypot(py - qy)
        };
        assert!(dist("a", "b") < dist("a", "d"));
    }

    #[test]
    fn rescale_centres_a_degenerate_range() {
        // Every coordinate equal on an axis collapses the range: map to the centre
        // rather than dividing by zero.
        assert!((rescale(3.0, 3.0, 3.0) - 0.5).abs() < f64::EPSILON);
        assert!((rescale(5.0, 0.0, 10.0) - 0.5).abs() < f64::EPSILON);
        assert!(rescale(0.0, 0.0, 10.0).abs() < f64::EPSILON);
    }
}
