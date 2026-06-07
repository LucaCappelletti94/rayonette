//! Golden snapshot of the TUI rendered over a real captured run.
//!
//! Replays the committed capstone event trace (`tests/fixtures/capstone.jsonl`,
//! recorded from the docker capstone scenario) into a [`RunState`] and renders
//! the TUI at several points across the run, diffing the result against a
//! committed text golden. This is the loop for refining the TUI: edit
//! `tui::draw`, run this test, read the visual diff, and regenerate the golden
//! with `RAYONET_TUI_BLESS=1` once the change is intended.
#![cfg(feature = "tui")]

use std::fmt::Write as _;
use std::path::PathBuf;

use ratatui::backend::TestBackend;
use ratatui::buffer::Buffer;
use ratatui::Terminal;
use rayonet::observability::{RecordedEvent, RunState};

/// A fixed frame size, so the rendered text is stable across machines.
const WIDTH: u16 = 64;
const HEIGHT: u16 = 8;

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

/// Each buffer row, trailing space trimmed.
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

/// Render the TUI for `state` to text (one line per row).
fn render(state: &RunState) -> String {
    let mut terminal = Terminal::new(TestBackend::new(WIDTH, HEIGHT)).unwrap();
    terminal
        .draw(|frame| rayonet::tui::draw(frame, state))
        .unwrap();
    rows(terminal.backend().buffer()).join("\n")
}

#[test]
fn tui_matches_the_capstone_golden() {
    let trace = std::fs::read_to_string(fixture("capstone.jsonl")).unwrap();
    let events: Vec<RecordedEvent> = trace
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert!(!events.is_empty(), "the capstone trace fixture is empty");

    // Render the tree at four points across the run, so the golden shows its
    // stages: provisioning, mid-run, after the join and reroute, and the end.
    let mut snapshot = String::new();
    for pct in [25_usize, 50, 75, 100] {
        let count = (events.len() * pct / 100).max(1);
        let mut state = RunState::default();
        for record in &events[..count] {
            state.apply(&record.event);
        }
        writeln!(snapshot, "=== after {pct}% of the run ===").unwrap();
        snapshot.push_str(&render(&state));
        snapshot.push_str("\n\n");
    }

    let golden = fixture("capstone-tui.golden");
    if std::env::var_os("RAYONET_TUI_BLESS").is_some() {
        std::fs::write(&golden, &snapshot).unwrap();
        return;
    }
    let expected = std::fs::read_to_string(&golden)
        .expect("golden missing; run the test with RAYONET_TUI_BLESS=1 to create it");
    assert_eq!(
        snapshot, expected,
        "the TUI render drifted from the golden; review the diff and, if intended, re-bless with RAYONET_TUI_BLESS=1"
    );
}
