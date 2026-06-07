//! The terminal TUI renderer (PLAN.md Phase 5), behind the `tui` feature.
//!
//! [`App`] is the live dashboard state: the reduced [`RunState`], a rolling event
//! log, and the elapsed time the driver feeds it. [`draw`] turns an [`App`] into a
//! framed dashboard: a header with a progress gauge, a topology panel (the graph
//! lands here in a later phase), a per-node table, and an event log. It is one of
//! the pluggable views over the event stream.

use std::collections::VecDeque;
use std::time::Duration;

use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Cell, Gauge, Paragraph, Row, Table};
use ratatui::Frame;

use crate::graph::Topology;
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
    render_topology(frame, rows[1]);
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

/// The topology panel. The node-link graph lands here in a later phase; for now
/// it is a titled placeholder so the layout is already in its final shape.
fn render_topology(frame: &mut Frame<'_>, area: Rect) {
    let block = Block::default().borders(Borders::ALL).title(" topology ");
    let inner = block.inner(area);
    frame.render_widget(block, area);
    frame.render_widget(
        Paragraph::new("graph view lands here").style(Style::default().fg(Color::DarkGray)),
        inner,
    );
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
        let mut terminal = Terminal::new(TestBackend::new(120, 40)).unwrap();
        terminal.draw(|frame| draw(frame, app)).unwrap();
        rows(terminal.backend().buffer())
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
}
