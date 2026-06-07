//! The terminal TUI renderer (PLAN.md Phase 5), behind the `tui` feature.
//!
//! [`App`] is the live dashboard state: the reduced [`RunState`], a rolling event
//! log, and the elapsed time the driver feeds it. [`draw`] turns an [`App`] into a
//! framed dashboard: a header with a progress gauge, the topology graph (the
//! centrepiece, a node-link diagram of the relay tree), a per-node table, and an
//! event log. It is one of the pluggable views over the event stream.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::Duration;

use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Cell, Gauge, Paragraph, Row, Table};
use ratatui::Frame;

use crate::graph::{Metric, Topology};
use crate::layout::positions;
use crate::observability::{leaf_of, Event, NodeState, RunState};

/// The most recent event-log lines [`App`] keeps for the log panel.
const LOG_CAPACITY: usize = 256;

/// The live dashboard state a renderer draws.
///
/// Folds the event stream into a [`RunState`] and a rolling human-readable log.
/// `elapsed` is supplied by the driver (wall clock for a live run, the recorded
/// timestamp for a replay) so rendering stays a pure function of this state.
#[derive(Debug, Clone, Default)]
pub struct App {
    /// The reduced per-node and per-task picture.
    pub state: RunState,
    /// The most recent event-log lines, oldest first, capped at [`LOG_CAPACITY`].
    pub log: VecDeque<String>,
    /// Time since the run started, set by the driver.
    pub elapsed: Duration,
}

impl App {
    /// A fresh dashboard with empty state.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold one event into the dashboard: update the run state and, if the event
    /// is worth surfacing, append a line to the rolling log.
    pub fn apply(&mut self, event: &Event) {
        self.state.apply(event);
        if let Some(line) = log_line(event, &self.state) {
            if self.log.len() == LOG_CAPACITY {
                self.log.pop_front();
            }
            self.log.push_back(line);
        }
    }
}

/// Microseconds as milliseconds for display. A real latency is far below f64's
/// exact-integer range, so the cast does not lose precision.
#[allow(clippy::cast_precision_loss)]
fn microseconds_to_millis(microseconds: u64) -> f64 {
    microseconds as f64 / 1000.0
}

/// The log line an event contributes, or `None` for events not worth a line.
fn log_line(event: &Event, state: &RunState) -> Option<String> {
    match event {
        Event::RunStarted { tasks } => Some(format!("run started: {tasks} tasks")),
        Event::Node { host, state } => Some(format!("{}: {state:?}", leaf_of(host))),
        Event::Profiled { host, role, .. } => Some(format!("{}: {role:?}", leaf_of(host))),
        Event::TaskStarted { .. } => None,
        Event::TaskFinished { .. } => Some(format!(
            "progress {}/{}",
            state.completed + state.failed,
            state.total_tasks
        )),
    }
}

/// Draw the dashboard for `app` into `frame`.
///
/// A vertical stack: a header with the progress gauge, the topology panel, the
/// per-node table, and the event log.
pub fn draw(frame: &mut Frame<'_>, app: &App) {
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(6),
            Constraint::Length(12),
            Constraint::Length(8),
        ])
        .split(frame.area());

    render_header(frame, rows[0], app);
    render_topology(frame, rows[1], app);
    render_table(frame, rows[2], app);
    render_log(frame, rows[3], app);
}

/// The header: a run summary and a completion gauge side by side.
fn render_header(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let cells = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Min(0), Constraint::Length(28)])
        .split(area);

    let state = &app.state;
    let summary = format!(
        "{}/{} done   {} failed   {} nodes   {:.1}s",
        state.completed,
        state.total_tasks,
        state.failed,
        state.nodes.len(),
        app.elapsed.as_secs_f64(),
    );
    frame.render_widget(
        Paragraph::new(summary).block(Block::default().borders(Borders::ALL).title(" rayonet ")),
        cells[0],
    );

    let done = state.completed + state.failed;
    let ratio = if state.total_tasks == 0 {
        0.0
    } else {
        f64::from(u32::try_from(done).unwrap_or(u32::MAX))
            / f64::from(u32::try_from(state.total_tasks).unwrap_or(u32::MAX))
    };
    frame.render_widget(
        Gauge::default()
            .block(Block::default().borders(Borders::ALL).title(" progress "))
            .gauge_style(Style::default().fg(Color::Green))
            .ratio(ratio.clamp(0.0, 1.0))
            .label(format!("{done}/{}", state.total_tasks)),
        cells[1],
    );
}

/// The topology panel: the relay tree as a node-link graph.
///
/// Vertices are physical nodes (a machine reached through two relays is one
/// vertex), positioned by the deterministic [`positions`] layout and coloured by
/// state. Parent links are drawn as lines, the active (primary) path bright and a
/// deduped standby path dim. A relay that is a single point of failure is marked.
fn render_topology(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let block = Block::default().borders(Borders::ALL).title(" topology ");
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if inner.width < 2 || inner.height < 2 {
        return;
    }

    let geometry = graph_geometry(inner, app);
    let occupied: BTreeSet<(u16, u16)> = geometry.nodes.iter().map(|node| node.cell).collect();
    let buffer = frame.buffer_mut();

    // Edges first, so the node labels drawn next sit on top of them.
    for edge in &geometry.edges {
        let style = if edge.active {
            Style::default().fg(Color::Green)
        } else {
            Style::default().fg(Color::DarkGray)
        };
        let glyph = edge_char(edge.from, edge.to);
        for cell in line_cells(edge.from, edge.to) {
            if occupied.contains(&cell) {
                continue;
            }
            if let Some(slot) = buffer.cell_mut(cell) {
                slot.set_char(glyph).set_style(style);
            }
        }
    }

    for node in &geometry.nodes {
        let style = node_style(node.state).add_modifier(Modifier::BOLD);
        // A single point of failure is flagged with a leading marker so the graph
        // shows it even without colour.
        let label = if node.spof {
            format!("!{}", node.label)
        } else {
            node.label.clone()
        };
        draw_label(buffer, inner, node.cell, &label, style);
    }
}

/// One positioned vertex of the topology graph.
struct GraphNode {
    /// The vertex's display label (its node's local name, or `coordinator`).
    label: String,
    /// The screen cell the vertex is centred on.
    cell: (u16, u16),
    /// The vertex's representative state, or `None` for the coordinator root.
    state: Option<NodeState>,
    /// Whether the vertex is a relay that is a single point of failure.
    spof: bool,
}

/// One drawn parent -> child link of the topology graph.
struct GraphEdge {
    /// The parent vertex's cell.
    from: (u16, u16),
    /// The child vertex's cell.
    to: (u16, u16),
    /// Whether this is the active (primary) path, as opposed to a deduped standby.
    active: bool,
}

/// The projected vertices and edges of the topology graph within `inner`.
struct GraphGeometry {
    /// Every vertex, including the synthetic coordinator root.
    nodes: Vec<GraphNode>,
    /// Every parent -> child link.
    edges: Vec<GraphEdge>,
}

/// Project the deterministic layout of `app`'s topology onto the cells of `inner`.
fn graph_geometry(inner: Rect, app: &App) -> GraphGeometry {
    let topology = Topology::from_run_state(&app.state);
    let coords = positions(&topology);
    let spofs = topology.single_points_of_failure();
    let primaries = topology.select_primaries(Metric::ShortestLatency);
    let labels = vertex_labels(&app.state);

    let cell_of = |id: &str| {
        let &(x, y) = coords.get(id).unwrap_or(&(0.5, 0.5));
        project(inner, x, y)
    };

    let vertices = topology.vertices();
    let nodes = vertices
        .iter()
        .map(|id| GraphNode {
            label: labels
                .get(id)
                .cloned()
                .unwrap_or_else(|| "coordinator".to_string()),
            cell: cell_of(id),
            state: vertex_state(&app.state, id),
            spof: spofs.contains(id),
        })
        .collect();

    let edges = topology
        .edge_indices()
        .into_iter()
        .filter_map(|(parent, child)| {
            let parent_id = vertices.get(parent)?;
            let child_id = vertices.get(child)?;
            // A multiply-reachable child names its parents primary-first; any
            // other parent is a standby. A uniquely reached child is always active.
            let active = primaries
                .get(child_id)
                .is_none_or(|ranked| ranked.first().map(String::as_str) == Some(parent_id));
            Some(GraphEdge {
                from: cell_of(parent_id),
                to: cell_of(child_id),
                active,
            })
        })
        .collect();

    GraphGeometry { nodes, edges }
}

/// Each physical node id's display label: the local name of any path that reaches
/// it (all paths to one node share that last segment).
fn vertex_labels(state: &RunState) -> BTreeMap<String, String> {
    state
        .paths_by_id()
        .into_iter()
        .filter_map(|(id, paths)| paths.first().map(|path| (id, leaf_of(path).to_string())))
        .collect()
}

/// A physical node's representative state across all the paths that reach it: the
/// most advanced one, so a node that completed on its surviving path reads Done
/// even if a dead path still strands a copy. `None` for the coordinator root.
fn vertex_state(state: &RunState, id: &str) -> Option<NodeState> {
    state
        .paths_by_id()
        .get(id)?
        .iter()
        .map(|path| state.effective_state(path))
        .max_by_key(|&s| state_rank(s))
}

/// How advanced a state is, for picking a node's representative state. A larger
/// rank wins, so Done beats Working beats a Lost copy.
const fn state_rank(state: NodeState) -> u8 {
    match state {
        NodeState::Lost => 0,
        NodeState::Probing => 1,
        NodeState::Installing => 2,
        NodeState::Syncing => 3,
        NodeState::Building => 4,
        NodeState::Idle => 5,
        NodeState::Ready => 6,
        NodeState::Working => 7,
        NodeState::Done => 8,
    }
}

/// The colour a vertex is drawn in: its state colour, or magenta for the
/// coordinator root, which has no state of its own.
fn node_style(state: Option<NodeState>) -> Style {
    state.map_or_else(|| Style::default().fg(Color::Magenta), state_style)
}

/// Project a unit-square position onto a cell of `area`, with y flipped so the top
/// of the area is `y = 1`.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn project(area: Rect, x: f64, y: f64) -> (u16, u16) {
    let col = area.x + (x * f64::from(area.width.saturating_sub(1))).round() as u16;
    let row = area.y + ((1.0 - y) * f64::from(area.height.saturating_sub(1))).round() as u16;
    (col, row)
}

/// The line glyph for an edge, chosen from its dominant direction on screen.
fn edge_char(from: (u16, u16), to: (u16, u16)) -> char {
    let dcol = i32::from(to.0) - i32::from(from.0);
    let drow = i32::from(to.1) - i32::from(from.1);
    if dcol.abs() >= drow.abs() * 2 {
        '─'
    } else if drow.abs() >= dcol.abs() * 2 {
        '│'
    } else if (dcol > 0) == (drow > 0) {
        '╲'
    } else {
        '╱'
    }
}

/// The cells a straight line from `from` to `to` passes through (Bresenham).
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn line_cells(from: (u16, u16), to: (u16, u16)) -> Vec<(u16, u16)> {
    let (mut x, mut y) = (i32::from(from.0), i32::from(from.1));
    let (x1, y1) = (i32::from(to.0), i32::from(to.1));
    let dx = (x1 - x).abs();
    let dy = -(y1 - y).abs();
    let step_x = if x < x1 { 1 } else { -1 };
    let step_y = if y < y1 { 1 } else { -1 };
    let mut error = dx + dy;
    let mut cells = Vec::new();
    loop {
        cells.push((x as u16, y as u16));
        if x == x1 && y == y1 {
            break;
        }
        let double = 2 * error;
        if double >= dy {
            error += dy;
            x += step_x;
        }
        if double <= dx {
            error += dx;
            y += step_y;
        }
    }
    cells
}

/// Draw `label` centred on `cell`, clamped within `inner`.
fn draw_label(buffer: &mut Buffer, inner: Rect, cell: (u16, u16), label: &str, style: Style) {
    let len = u16::try_from(label.chars().count()).unwrap_or(u16::MAX);
    let half = len / 2;
    let start = cell
        .0
        .saturating_sub(half)
        .clamp(inner.x, inner.right().saturating_sub(len).max(inner.x));
    let room = inner.right().saturating_sub(start) as usize;
    buffer.set_stringn(start, cell.1, label, room, style);
}

/// The per-node table: full path (so two paths to one node stay distinct), role,
/// effective state, finished count, link latency, architecture, and a SPOF flag.
fn render_table(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let state = &app.state;
    let spofs = Topology::from_run_state(state).single_points_of_failure();

    let header = Row::new(["node", "role", "state", "done", "lat ms", "arch", "flag"])
        .style(Style::default().add_modifier(Modifier::BOLD));

    let rows = state.nodes.iter().map(|(path, view)| {
        let effective = state.effective_state(path);
        let role = view
            .role
            .map_or_else(String::new, |role| format!("{role:?}"));
        let latency = view.latency_us.map_or_else(String::new, |us| {
            format!("{:.1}", microseconds_to_millis(us))
        });
        let arch = view
            .profile
            .as_ref()
            .map_or_else(String::new, |profile| profile.arch.isa.clone());
        let flag = if view.id.as_deref().is_some_and(|id| spofs.contains(id)) {
            "SPOF"
        } else {
            ""
        };
        Row::new([
            Cell::from(path.clone()),
            Cell::from(role),
            Cell::from(format!("{effective:?}")).style(state_style(effective)),
            Cell::from(view.completed.to_string()),
            Cell::from(latency),
            Cell::from(arch),
            Cell::from(flag).style(Style::default().fg(Color::Red)),
        ])
    });

    let widths = [
        Constraint::Min(16),
        Constraint::Length(10),
        Constraint::Length(10),
        Constraint::Length(5),
        Constraint::Length(7),
        Constraint::Length(14),
        Constraint::Length(5),
    ];
    frame.render_widget(
        Table::new(rows, widths)
            .header(header)
            .block(Block::default().borders(Borders::ALL).title(" nodes ")),
        area,
    );
}

/// The event log: the most recent lines that fit, newest at the bottom.
fn render_log(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let block = Block::default().borders(Borders::ALL).title(" events ");
    let capacity = block.inner(area).height as usize;
    let lines: Vec<Line<'_>> = app
        .log
        .iter()
        .rev()
        .take(capacity)
        .rev()
        .map(|line| Line::from(line.as_str()))
        .collect();
    frame.render_widget(Paragraph::new(lines).block(block), area);
}

/// The colour a node state is drawn in: green working, blue done, red lost,
/// cyan idle-and-ready, yellow while it is still being provisioned.
fn state_style(state: NodeState) -> Style {
    let color = match state {
        NodeState::Working => Color::Green,
        NodeState::Done => Color::Blue,
        NodeState::Lost => Color::Red,
        NodeState::Ready | NodeState::Idle => Color::Cyan,
        NodeState::Probing | NodeState::Installing | NodeState::Syncing | NodeState::Building => {
            Color::Yellow
        }
    };
    Style::default().fg(color)
}

#[cfg(test)]
mod tests {
    use super::{draw, App};
    use crate::capability::{CpuArch, NodeProfile, Os, Role};
    use crate::observability::{Event, NodeState};
    use ratatui::backend::TestBackend;
    use ratatui::buffer::Buffer;
    use ratatui::layout::Rect;
    use ratatui::style::Color;
    use ratatui::Terminal;

    /// A simple Linux profile with the given instruction set, for table tests.
    fn profile(isa: &str) -> NodeProfile {
        NodeProfile {
            os: Os::Linux,
            arch: CpuArch {
                isa: isa.to_string(),
                features: Vec::new(),
            },
            cores: 8,
            ram_mb: 16_000,
            gpus: Vec::new(),
        }
    }

    /// Each buffer row as a trailing-space-trimmed string.
    fn rows(buffer: &Buffer) -> Vec<String> {
        let area = buffer.area();
        (0..area.height)
            .map(|y| {
                (0..area.width)
                    .map(|x| buffer.cell((x, y)).unwrap().symbol())
                    .collect::<String>()
                    .trim_end()
                    .to_string()
            })
            .collect()
    }

    /// Render `app` to trimmed text rows on a fixed backend.
    fn render(app: &App) -> Vec<String> {
        rows(&render_buffer(app))
    }

    /// Render `app` to a buffer on a fixed backend, keeping cell styles for tests
    /// that inspect colour.
    fn render_buffer(app: &App) -> Buffer {
        let mut terminal = Terminal::new(TestBackend::new(120, 40)).unwrap();
        terminal.draw(|frame| draw(frame, app)).unwrap();
        terminal.backend().buffer().clone()
    }

    /// A profiled diamond: two gateways front one shared leaf, the link through
    /// `gatewayA` being faster so it is the primary path.
    fn diamond() -> App {
        let mut app = App::new();
        app.apply(&Event::profiled(
            "gatewayA",
            "idA",
            profile("x86_64"),
            Role::Compute,
            1_000,
        ));
        app.apply(&Event::profiled(
            "gatewayB",
            "idB",
            profile("x86_64"),
            Role::Compute,
            1_000,
        ));
        app.apply(&Event::profiled(
            "gatewayA/shared",
            "idS",
            profile("x86_64"),
            Role::Compute,
            100,
        ));
        app.apply(&Event::profiled(
            "gatewayB/shared",
            "idS",
            profile("x86_64"),
            Role::Compute,
            200,
        ));
        for host in ["gatewayA", "gatewayB", "gatewayA/shared", "gatewayB/shared"] {
            app.apply(&Event::node(host, NodeState::Working));
        }
        app
    }

    #[test]
    fn the_table_keeps_two_paths_to_one_node_distinct() {
        // Both gateways front the same physical leaf, so each path appears. The
        // bare leaf label ("shared") is identical, so the table must show the
        // full path to tell them apart.
        let mut app = App::new();
        for event in [
            Event::node("gatewayA", NodeState::Working),
            Event::node("gatewayA/shared", NodeState::Working),
            Event::node("gatewayB", NodeState::Working),
            Event::node("gatewayB/shared", NodeState::Working),
        ] {
            app.apply(&event);
        }
        let text = render(&app).join("\n");
        assert!(text.contains("gatewayA/shared"), "{text}");
        assert!(text.contains("gatewayB/shared"), "{text}");
    }

    #[test]
    fn a_child_of_a_lost_relay_is_drawn_lost() {
        // gatewayA dies; its shared child never gets a closing event, so its own
        // state is a stale Working. The table must show it Lost (stranded).
        let mut app = App::new();
        for event in [
            Event::node("gatewayA", NodeState::Lost),
            Event::node("gatewayA/shared", NodeState::Working),
        ] {
            app.apply(&event);
        }
        let stranded = render(&app)
            .into_iter()
            .find(|row| row.contains("gatewayA/shared"))
            .expect("the stranded child row is present");
        assert!(stranded.contains("Lost"), "{stranded}");
        assert!(!stranded.contains("Working"), "{stranded}");
    }

    #[test]
    fn the_header_shows_progress_and_a_gauge() {
        let mut app = App::new();
        app.apply(&Event::RunStarted { tasks: 4 });
        for task in 0..3 {
            app.apply(&Event::TaskFinished {
                host: "leaf".to_string(),
                task,
                ok: true,
            });
        }
        let text = render(&app).join("\n");
        assert!(text.contains("3/4 done"), "{text}");
        // The gauge labels its filled fraction.
        assert!(text.contains("3/4"), "{text}");
    }

    #[test]
    fn the_log_panel_shows_recent_events() {
        let mut app = App::new();
        app.apply(&Event::RunStarted { tasks: 1 });
        app.apply(&Event::node("leaf", NodeState::Building));
        app.apply(&Event::profiled(
            "leaf",
            "id",
            profile("x86_64"),
            Role::Compute,
            0,
        ));
        // A task start contributes no log line; a finish does.
        app.apply(&Event::TaskStarted {
            host: "leaf".to_string(),
            task: 0,
        });
        app.apply(&Event::TaskFinished {
            host: "leaf".to_string(),
            task: 0,
            ok: true,
        });
        let text = render(&app).join("\n");
        assert!(text.contains("run started: 1 tasks"), "{text}");
        assert!(text.contains("leaf: Building"), "{text}");
        assert!(text.contains("leaf: Compute"), "{text}");
        assert!(text.contains("progress 1/1"), "{text}");
    }

    #[test]
    fn the_table_flags_a_single_point_of_failure() {
        // A relay whose single leaf has no redundant path is a SPOF.
        let mut app = App::new();
        app.apply(&Event::profiled(
            "relay",
            "idR",
            profile("x86_64"),
            Role::Compute,
            0,
        ));
        app.apply(&Event::profiled(
            "relay/leaf",
            "idL",
            profile("x86_64"),
            Role::Compute,
            0,
        ));
        app.apply(&Event::node("relay", NodeState::Working));
        app.apply(&Event::node("relay/leaf", NodeState::Working));

        let rows = render(&app);
        assert!(
            rows.iter()
                .any(|r| r.contains("relay") && !r.contains("relay/leaf") && r.contains("SPOF")),
            "{rows:?}"
        );
        assert!(
            rows.iter()
                .any(|r| r.contains("relay/leaf") && !r.contains("SPOF")),
            "{rows:?}"
        );
    }

    #[test]
    fn every_node_state_renders_with_its_colour() {
        // Drive one node through each lifecycle state so the colour map and the
        // topology placeholder are all exercised.
        let mut app = App::new();
        app.elapsed = std::time::Duration::from_millis(1500);
        for (index, state) in [
            NodeState::Probing,
            NodeState::Installing,
            NodeState::Syncing,
            NodeState::Building,
            NodeState::Ready,
            NodeState::Working,
            NodeState::Idle,
            NodeState::Done,
            NodeState::Lost,
        ]
        .into_iter()
        .enumerate()
        {
            app.apply(&Event::node(&format!("n{index}"), state));
        }
        let text = render(&app).join("\n");
        assert!(text.contains("topology"), "{text}");
        assert!(text.contains("1.5s"), "{text}");
    }

    #[test]
    fn graph_geometry_places_every_vertex_inside_the_panel() {
        let app = diamond();
        let inner = Rect::new(1, 1, 40, 18);
        let geometry = super::graph_geometry(inner, &app);
        // The synthetic coordinator plus the two gateways plus the shared leaf.
        assert_eq!(geometry.nodes.len(), 4);
        assert!(
            geometry
                .nodes
                .iter()
                .any(|node| node.label == "coordinator"),
            "the coordinator root is drawn"
        );
        for node in &geometry.nodes {
            let (col, row) = node.cell;
            assert!(col >= inner.x && col < inner.right(), "col {col} off panel");
            assert!(
                row >= inner.y && row < inner.bottom(),
                "row {row} off panel"
            );
        }
        // The shared leaf is reached by both gateways, so two edges end on it.
        assert_eq!(geometry.edges.len(), 4);
    }

    #[test]
    fn the_graph_draws_active_and_standby_links_distinctly() {
        // The shared leaf's primary link (through the faster gatewayA) is active and
        // green; the deduped standby link is dim. Look for a line glyph of each
        // colour in the topology panel.
        let buffer = render_buffer(&diamond());
        let line_glyphs = ['\u{2500}', '\u{2502}', '\u{2572}', '\u{2571}'];
        let mut active = false;
        let mut standby = false;
        let area = buffer.area();
        for y in 0..area.height {
            for x in 0..area.width {
                let cell = buffer.cell((x, y)).unwrap();
                let symbol = cell.symbol().chars().next().unwrap_or(' ');
                if !line_glyphs.contains(&symbol) {
                    continue;
                }
                match cell.style().fg {
                    Some(Color::Green) => active = true,
                    Some(Color::DarkGray) => standby = true,
                    _ => {}
                }
            }
        }
        assert!(active, "an active link is drawn green");
        assert!(standby, "a standby link is drawn dim");
    }

    #[test]
    fn the_graph_marks_a_single_point_of_failure() {
        // A relay whose single leaf has no redundant path is a SPOF, flagged with a
        // leading marker in the graph.
        let mut app = App::new();
        app.apply(&Event::profiled(
            "relay",
            "idR",
            profile("x86_64"),
            Role::Compute,
            0,
        ));
        app.apply(&Event::profiled(
            "relay/leaf",
            "idL",
            profile("x86_64"),
            Role::Compute,
            0,
        ));
        app.apply(&Event::node("relay", NodeState::Working));
        app.apply(&Event::node("relay/leaf", NodeState::Working));

        let geometry = super::graph_geometry(Rect::new(0, 0, 40, 18), &app);
        let relay = geometry
            .nodes
            .iter()
            .find(|node| node.label == "relay")
            .expect("the relay vertex is present");
        assert!(relay.spof, "the lone relay is a single point of failure");
        let leaf = geometry
            .nodes
            .iter()
            .find(|node| node.label == "leaf")
            .expect("the leaf vertex is present");
        assert!(!leaf.spof, "the leaf is not a point of failure");
        // The marker reaches the rendered frame.
        assert!(render(&app).iter().any(|row| row.contains("!relay")));
    }

    #[test]
    fn state_rank_orders_lifecycle_progress() {
        use super::state_rank;
        // Done is the most advanced, Lost the least, and the order is strict, so a
        // node's representative state is the furthest any of its paths reached.
        let order = [
            NodeState::Lost,
            NodeState::Probing,
            NodeState::Installing,
            NodeState::Syncing,
            NodeState::Building,
            NodeState::Idle,
            NodeState::Ready,
            NodeState::Working,
            NodeState::Done,
        ];
        for pair in order.windows(2) {
            assert!(state_rank(pair[0]) < state_rank(pair[1]), "{pair:?}");
        }
    }

    #[test]
    fn draw_survives_a_terminal_too_small_for_the_graph() {
        // The topology panel shrinks below a drawable size on a tiny terminal; it
        // must skip the graph rather than panic.
        let app = diamond();
        // Three columns leave the topology panel only one cell inside its border.
        let mut terminal = Terminal::new(TestBackend::new(3, 40)).unwrap();
        terminal.draw(|frame| draw(frame, &app)).unwrap();
    }

    #[test]
    fn edge_glyphs_and_cells_follow_direction() {
        use super::{edge_char, line_cells};
        // A horizontal, vertical, and both diagonal runs pick distinct glyphs.
        assert_eq!(edge_char((0, 5), (10, 5)), '\u{2500}');
        assert_eq!(edge_char((5, 0), (5, 10)), '\u{2502}');
        assert_eq!(edge_char((0, 0), (10, 10)), '\u{2572}');
        assert_eq!(edge_char((10, 0), (0, 10)), '\u{2571}');
        // Bresenham includes both endpoints and is contiguous.
        let cells = line_cells((2, 2), (5, 4));
        assert_eq!(cells.first(), Some(&(2, 2)));
        assert_eq!(cells.last(), Some(&(5, 4)));
        // A single-cell line is just that cell.
        assert_eq!(line_cells((3, 3), (3, 3)), vec![(3, 3)]);
    }
}
