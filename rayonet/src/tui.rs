//! The terminal TUI renderer (PLAN.md Phase 5), behind the `tui` feature.
//!
//! A pure [`draw`] turns a [`RunState`] snapshot into a frame: a summary line
//! plus one row per node with its state and finished-task count. It is one of
//! the pluggable views over the event stream.

use ratatui::text::Line;
use ratatui::widgets::Paragraph;
use ratatui::Frame;

use crate::graph::Topology;
use crate::observability::{depth, leaf_of, RunState};

/// Draw the current run state into `frame`.
///
/// A summary header and one row per node, indented by its depth so the tree's
/// shape is visible. A relay that is a single point of failure (no redundant path
/// past it) is flagged `SPOF`.
pub fn draw(frame: &mut Frame<'_>, state: &RunState) {
    let spofs = Topology::from_run_state(state).single_points_of_failure();
    let mut lines = vec![Line::from(format!(
        "rayonet  {}/{} done  {} failed",
        state.completed, state.total_tasks, state.failed
    ))];
    for (host, view) in &state.nodes {
        let role = view
            .role
            .map_or_else(String::new, |role| format!("  {role:?}"));
        let spof = if view.id.as_deref().is_some_and(|id| spofs.contains(id)) {
            "  SPOF"
        } else {
            ""
        };
        lines.push(Line::from(format!(
            "{}{}  {:?}{role}{spof}  {}",
            "  ".repeat(depth(host)),
            leaf_of(host),
            view.state,
            view.completed
        )));
    }
    frame.render_widget(Paragraph::new(lines), frame.area());
}

#[cfg(test)]
mod tests {
    use crate::observability::{Event, NodeState, RunState};
    use ratatui::backend::TestBackend;
    use ratatui::buffer::Buffer;
    use ratatui::Terminal;

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

    #[test]
    fn tui_renders_node_rows_states_and_counts() {
        let mut state = RunState::default();
        for event in [
            Event::RunStarted { tasks: 3 },
            Event::node("leaf-a", NodeState::Done),
            Event::node("leaf-b", NodeState::Working),
            Event::TaskFinished {
                host: "leaf-a".to_string(),
                task: 0,
                ok: true,
            },
            Event::TaskFinished {
                host: "leaf-a".to_string(),
                task: 1,
                ok: true,
            },
            Event::TaskFinished {
                host: "leaf-b".to_string(),
                task: 2,
                ok: false,
            },
        ] {
            state.apply(&event);
        }

        let mut terminal = Terminal::new(TestBackend::new(40, 3)).unwrap();
        terminal.draw(|frame| super::draw(frame, &state)).unwrap();

        assert_eq!(
            rows(terminal.backend().buffer()),
            vec![
                "rayonet  2/3 done  1 failed",
                "leaf-a  Done  2",
                "leaf-b  Working  1",
            ]
        );
    }

    #[test]
    fn tui_indents_a_multi_level_tree() {
        let mut state = RunState::default();
        for event in [
            Event::RunStarted { tasks: 4 },
            Event::node("relay", NodeState::Working),
            Event::node("relay/leaf-a", NodeState::Working),
            Event::node("relay/leaf-b", NodeState::Done),
        ] {
            state.apply(&event);
        }

        let mut terminal = Terminal::new(TestBackend::new(40, 4)).unwrap();
        terminal.draw(|frame| super::draw(frame, &state)).unwrap();

        assert_eq!(
            rows(terminal.backend().buffer()),
            vec![
                "rayonet  0/4 done  0 failed",
                "relay  Working  0",
                "  leaf-a  Working  0",
                "  leaf-b  Done  0",
            ],
        );
    }

    #[test]
    fn tui_flags_a_single_point_of_failure() {
        use crate::capability::{NodeProfile, Os, Role};
        // A relay with a single leaf has no redundant path, so the relay is a
        // single point of failure. Its leaf, reached only through it, is not one.
        let profile = NodeProfile {
            os: Os::Linux,
            cores: 8,
            ram_mb: 16_000,
            gpus: Vec::new(),
        };
        let mut state = RunState::default();
        state.apply(&Event::profiled(
            "relay",
            "idR",
            profile.clone(),
            Role::Compute,
            0,
        ));
        state.apply(&Event::profiled(
            "relay/leaf",
            "idL",
            profile,
            Role::Compute,
            0,
        ));
        state.apply(&Event::node("relay", NodeState::Working));
        state.apply(&Event::node("relay/leaf", NodeState::Working));

        let mut terminal = Terminal::new(TestBackend::new(48, 3)).unwrap();
        terminal.draw(|frame| super::draw(frame, &state)).unwrap();

        let rows = rows(terminal.backend().buffer());
        assert!(
            rows.iter()
                .any(|r| r.starts_with("relay") && r.contains("SPOF")),
            "{rows:?}"
        );
        assert!(
            rows.iter()
                .any(|r| r.contains("leaf") && !r.contains("SPOF")),
            "{rows:?}"
        );
    }

    #[test]
    fn tui_shows_the_role_for_a_profiled_node() {
        use crate::capability::{NodeProfile, Os, Role};

        let mut state = RunState::default();
        let profile = NodeProfile {
            os: Os::Linux,
            cores: 8,
            ram_mb: 16_000,
            gpus: Vec::new(),
        };
        state.apply(&Event::RunStarted { tasks: 1 });
        state.apply(&Event::profiled(
            "leaf-a",
            "id-a",
            profile,
            Role::Compute,
            0,
        ));
        state.apply(&Event::node("leaf-a", NodeState::Working));

        let mut terminal = Terminal::new(TestBackend::new(40, 2)).unwrap();
        terminal.draw(|frame| super::draw(frame, &state)).unwrap();

        assert_eq!(
            rows(terminal.backend().buffer()),
            vec!["rayonet  0/1 done  0 failed", "leaf-a  Working  Compute  0"],
        );
    }
}
