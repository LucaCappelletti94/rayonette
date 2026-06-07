//! Replay a recorded rayonet event log into the live terminal TUI.
//!
//! A run records its event stream when `RAYONET_EVENT_LOG` is set (the docker
//! consumer does, and the topology harness forwards it). This renders that trace
//! through the same `rayonet::tui::draw` the live run would use, so you can watch
//! the run unfold and refine the view against a real, evolving topology.
//!
//! ```text
//! tui-replay <trace.jsonl> [speed]   replay a finished trace (speed x, default 1)
//! tui-replay --follow <trace.jsonl>  watch a trace as it is written (live)
//! ```
//!
//! Press `q` (or Esc) to quit; the terminal is restored on exit.

use std::fs::File;
use std::io::{self, BufRead, BufReader, Stdout};
use std::time::{Duration, Instant};

use ratatui::backend::CrosstermBackend;
use ratatui::crossterm::event::{self, Event as CtEvent, KeyCode};
use ratatui::crossterm::execute;
use ratatui::crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::Terminal;
use rayonet::observability::{RecordedEvent, RunState};

type Term = Terminal<CrosstermBackend<Stdout>>;

fn main() -> io::Result<()> {
    let mut follow = false;
    let mut path: Option<String> = None;
    let mut speed = 1.0_f64;
    for arg in std::env::args().skip(1) {
        if arg == "--follow" {
            follow = true;
        } else if path.is_none() {
            path = Some(arg);
        } else {
            speed = arg.parse().unwrap_or(1.0);
        }
    }
    let Some(path) = path else {
        eprintln!("usage: tui-replay [--follow] <trace.jsonl> [speed]");
        std::process::exit(2);
    };

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let mut terminal = Terminal::new(CrosstermBackend::new(stdout))?;
    let outcome = replay(&mut terminal, &path, speed, follow);
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    outcome
}

/// Whether the viewer pressed `q` or Esc since the last check.
fn quit_requested() -> io::Result<bool> {
    if event::poll(Duration::from_millis(0))? {
        if let CtEvent::Key(key) = event::read()? {
            return Ok(matches!(key.code, KeyCode::Char('q') | KeyCode::Esc));
        }
    }
    Ok(false)
}

/// Open the trace (waiting for it in follow mode), then apply and draw each event
/// until the trace ends or the viewer quits.
fn replay(terminal: &mut Term, path: &str, speed: f64, follow: bool) -> io::Result<()> {
    let mut reader = loop {
        match File::open(path) {
            Ok(file) => break BufReader::new(file),
            Err(_) if follow => {
                if quit_requested()? {
                    return Ok(());
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(error) => return Err(error),
        }
    };

    let mut state = RunState::default();
    let start = Instant::now();
    let mut line = String::new();
    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            // End of the trace. A finished trace holds its final frame; a live
            // one waits for more to be appended.
            if quit_requested()? {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(if follow { 100 } else { 50 }));
            continue;
        }
        let Ok(record) = serde_json::from_str::<RecordedEvent>(line.trim()) else {
            continue; // a blank or partially-written line; the next event redraws
        };
        // Pace a finished trace by its own timestamps; show a live one as it lands.
        if !follow {
            let target = Duration::from_secs_f64(record.elapsed_ms as f64 / 1000.0 / speed);
            while start.elapsed() < target {
                if quit_requested()? {
                    return Ok(());
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        }
        state.apply(&record.event);
        terminal.draw(|frame| rayonet::tui::draw(frame, &state))?;
        if quit_requested()? {
            return Ok(());
        }
    }
}
