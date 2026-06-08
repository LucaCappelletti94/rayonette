//! Replay a recorded rayonet event log into the live, interactive terminal TUI.
//!
//! A run records its event stream when `RAYONET_EVENT_LOG` is set (the docker
//! consumer does, and the topology harness forwards it). This renders that trace
//! through the same `rayonet::tui` dashboard the live run would use, so you can
//! watch the run unfold and refine the view against a real, evolving topology.
//!
//! ```text
//! tui-replay <trace.jsonl> [speed]   replay a finished trace (speed x, default 1)
//! tui-replay --follow <trace.jsonl>  watch a trace as it is written (live)
//! ```
//!
//! Controls: Tab / Shift-Tab (or the arrow keys) select a node, the mouse selects
//! a node or hovers a link, Esc clears the selection, `p` pauses playback, and `q`
//! quits. The terminal is restored on exit.

use std::fs::File;
use std::io::{self, BufRead, BufReader, Stdout};
use std::time::{Duration, Instant};

use ratatui::backend::CrosstermBackend;
use ratatui::crossterm::event::{
    self, DisableMouseCapture, EnableMouseCapture, Event as CtEvent, KeyCode, MouseButton,
    MouseEventKind,
};
use ratatui::crossterm::execute;
use ratatui::crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::Terminal;
use rayonet::observability::RecordedEvent;
use rayonet::tui::{Action, App, Input};

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
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let mut terminal = Terminal::new(CrosstermBackend::new(stdout))?;
    let outcome = replay(&mut terminal, &path, speed, follow);
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;
    outcome
}

/// Translate a terminal event into a dashboard [`Input`], or `None` for events the
/// dashboard ignores.
fn to_input(event: CtEvent) -> Option<Input> {
    match event {
        CtEvent::Key(key) => match key.code {
            KeyCode::Char('q') => Some(Input::Quit),
            KeyCode::Char('p') => Some(Input::TogglePause),
            KeyCode::Tab | KeyCode::Down => Some(Input::SelectNext),
            KeyCode::BackTab | KeyCode::Up => Some(Input::SelectPrev),
            KeyCode::Esc => Some(Input::Clear),
            _ => None,
        },
        CtEvent::Mouse(mouse) => match mouse.kind {
            MouseEventKind::Moved => Some(Input::PointerMoved {
                col: mouse.column,
                row: mouse.row,
            }),
            MouseEventKind::Down(MouseButton::Left) => Some(Input::Click {
                col: mouse.column,
                row: mouse.row,
            }),
            _ => None,
        },
        CtEvent::Resize(..) | CtEvent::FocusGained | CtEvent::FocusLost | CtEvent::Paste(_) => None,
    }
}

/// Drain pending terminal events into `app`, returning [`Action::Quit`] if any of
/// them asked to quit.
fn pump_input(app: &mut App) -> io::Result<Action> {
    while event::poll(Duration::from_millis(0))? {
        if let Some(input) = to_input(event::read()?) {
            if app.on_input(input) == Action::Quit {
                return Ok(Action::Quit);
            }
        }
    }
    Ok(Action::Continue)
}

/// Open the trace (waiting for it in follow mode), then apply and draw each event,
/// pacing playback and handling input, until the trace ends or the viewer quits.
fn replay(terminal: &mut Term, path: &str, speed: f64, follow: bool) -> io::Result<()> {
    let mut app = App::new();
    let mut reader = loop {
        match File::open(path) {
            Ok(file) => break BufReader::new(file),
            Err(_) if follow => {
                if pump_input(&mut app)? == Action::Quit {
                    return Ok(());
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(error) => return Err(error),
        }
    };

    // Hold each event on screen for at least this long, so a burst of events
    // recorded in the same instant does not flash by faster than the eye can
    // follow. Override with RAYONET_REPLAY_MIN_DWELL_MS.
    let min_dwell = Duration::from_millis(
        std::env::var("RAYONET_REPLAY_MIN_DWELL_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(120),
    );

    let start = Instant::now();
    let mut last_applied = Instant::now();
    let mut line = String::new();
    loop {
        if pump_input(&mut app)? == Action::Quit {
            return Ok(());
        }
        terminal.draw(|frame| rayonet::tui::draw(frame, &mut app))?;

        // While paused, keep the view interactive but hold the trace position.
        if app.paused {
            std::thread::sleep(Duration::from_millis(30));
            continue;
        }

        line.clear();
        if reader.read_line(&mut line)? == 0 {
            // End of the trace. A finished trace holds its final frame; a live one
            // waits for more to be appended.
            std::thread::sleep(Duration::from_millis(if follow { 100 } else { 50 }));
            continue;
        }
        let Ok(record) = serde_json::from_str::<RecordedEvent>(line.trim()) else {
            continue; // a blank or partially-written line; the next event redraws
        };
        // Pace a finished trace by its own timestamps; show a live one as it lands.
        let target = if follow {
            last_applied + min_dwell
        } else {
            (start + Duration::from_secs_f64(record.elapsed_ms as f64 / 1000.0 / speed))
                .max(last_applied + min_dwell)
        };
        while Instant::now() < target {
            if pump_input(&mut app)? == Action::Quit {
                return Ok(());
            }
            terminal.draw(|frame| rayonet::tui::draw(frame, &mut app))?;
            std::thread::sleep(Duration::from_millis(10));
        }
        app.apply(&record.event);
        app.elapsed = start.elapsed();
        last_applied = Instant::now();
    }
}
