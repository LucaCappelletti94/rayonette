//! The terminal TUI renderer (PLAN.md Phase 5), behind the `tui` feature.
//!
//! A pure [`draw`] turns a [`RunState`] snapshot into a frame: a summary line
//! plus one row per node with its state and finished-task count. It is one of
//! the pluggable views over the event stream (DECISIONS.md decision 19).

use ratatui::text::Line;
use ratatui::widgets::Paragraph;
use ratatui::Frame;

use crate::observability::RunState;

/// Draw the current run state into `frame`: a summary header and a row per node.
pub fn draw(frame: &mut Frame<'_>, state: &RunState) {
    let mut lines = vec![Line::from(format!(
        "rayonet  {}/{} done  {} failed",
        state.completed, state.total_tasks, state.failed
    ))];
    for (host, view) in &state.nodes {
        lines.push(Line::from(format!(
            "{host}  {:?}  {}",
            view.state, view.completed
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
}
