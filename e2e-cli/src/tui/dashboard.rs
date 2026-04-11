//! Full-screen ratatui TUI dashboard for the quickstart command.
//!
//! Displays pipeline status, throughput sparkline, table sync counts,
//! and keybinding footer. Fetches data from the dbmazz HTTP API on a
//! timer tick and renders using ratatui + crossterm.

use std::io;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use crossterm::event::{self, Event, KeyCode, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::prelude::*;
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Sparkline, Table};

use crate::clients::dbmazz::{DaemonStatus, DbmazzClient};
use crate::clients::targets::TargetBackend;
use crate::compose::runner;
use crate::load::TrafficGenerator;

// ── Constants ──────────────────────────────────────────────────────────────

const HISTORY_LEN: usize = 60;
const POLL_MS: u64 = 250;
const TICK_MS: u64 = 500;

// ── Data model ─────────────────────────────────────────────────────────────

struct TableCount {
    name: String,
    source: i64,
    target: i64,
}

// ── Dashboard ──────────────────────────────────────────────────────────────

pub struct QuickstartDashboard {
    name: String,
    dbmazz: DbmazzClient,
    target: Box<dyn TargetBackend>,
    tables: Vec<String>,
    source_dsn: String,
    compose_file: Option<PathBuf>,
    // state
    throughput_history: Vec<u64>,
    last_status: Option<DaemonStatus>,
    table_counts: Vec<TableCount>,
    traffic_running: bool,
    traffic_generator: Option<TrafficGenerator>,
    status_message: Option<(String, Instant)>,
    last_events_total: u64,
    last_tick: Instant,
}

impl QuickstartDashboard {
    pub fn new(
        name: String,
        dbmazz: DbmazzClient,
        target: Box<dyn TargetBackend>,
        tables: Vec<String>,
        source_dsn: String,
        compose_file: Option<PathBuf>,
    ) -> Self {
        Self {
            name,
            dbmazz,
            target,
            tables,
            source_dsn,
            compose_file,
            throughput_history: Vec::with_capacity(HISTORY_LEN),
            last_status: None,
            table_counts: Vec::new(),
            traffic_running: false,
            traffic_generator: None,
            status_message: None,
            last_events_total: 0,
            last_tick: Instant::now(),
        }
    }

    /// Run the dashboard event loop. Blocks until the user quits.
    pub async fn run(&mut self) -> anyhow::Result<()> {
        // Install panic hook that restores terminal.
        let original_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let _ = disable_raw_mode();
            let _ = execute!(io::stdout(), LeaveAlternateScreen);
            original_hook(info);
        }));

        enable_raw_mode()?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen)?;

        let backend = CrosstermBackend::new(stdout);
        let mut terminal = Terminal::new(backend)?;

        let result = self.event_loop(&mut terminal).await;

        // Restore terminal.
        disable_raw_mode()?;
        execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
        terminal.show_cursor()?;

        result
    }

    async fn event_loop(
        &mut self,
        terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    ) -> anyhow::Result<()> {
        let mut last_tick = Instant::now();
        let tick_rate = Duration::from_millis(TICK_MS);

        // Initial fetch.
        self.fetch_data().await;

        loop {
            terminal.draw(|f| self.render(f))?;

            let timeout = tick_rate.saturating_sub(last_tick.elapsed());
            let poll_timeout = timeout.min(Duration::from_millis(POLL_MS));

            if event::poll(poll_timeout)? {
                if let Event::Key(key) = event::read()? {
                    match key.code {
                        KeyCode::Char('q') => break,
                        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                            break
                        }
                        KeyCode::Char('t') => self.toggle_traffic().await,
                        KeyCode::Char('p') => self.toggle_pause().await,
                        KeyCode::Char('l') => {
                            self.show_logs(terminal).await?;
                        }
                        KeyCode::Char('s') => {
                            self.set_status_message("Snapshot trigger not yet implemented");
                        }
                        _ => {}
                    }
                }
            }

            if last_tick.elapsed() >= tick_rate {
                self.fetch_data().await;
                last_tick = Instant::now();
            }
        }

        // Stop traffic generator if running.
        if let Some(ref mut gen) = self.traffic_generator {
            gen.stop().await;
        }

        Ok(())
    }

    // ── Data fetching ──────────────────────────────────────────────────────

    async fn fetch_data(&mut self) {
        // Fetch dbmazz status.
        match self.dbmazz.status().await {
            Ok(status) => {
                // Compute throughput delta.
                let elapsed = self.last_tick.elapsed().as_secs_f64().max(0.001);
                let delta = status.events_total.saturating_sub(self.last_events_total);
                let eps = (delta as f64 / elapsed) as u64;

                self.throughput_history.push(eps);
                if self.throughput_history.len() > HISTORY_LEN {
                    self.throughput_history.remove(0);
                }

                self.last_events_total = status.events_total;
                self.last_status = Some(status);
                self.last_tick = Instant::now();
            }
            Err(_) => {
                // Push zero throughput on error.
                self.throughput_history.push(0);
                if self.throughput_history.len() > HISTORY_LEN {
                    self.throughput_history.remove(0);
                }
            }
        }

        // Fetch table counts from target.
        let mut counts = Vec::new();
        for table in &self.tables {
            let source_count = self.fetch_source_count(table).await.unwrap_or(-1);
            let target_count = self
                .target
                .count_rows(table, true)
                .await
                .unwrap_or(-1);
            counts.push(TableCount {
                name: table.clone(),
                source: source_count,
                target: target_count,
            });
        }
        self.table_counts = counts;
    }

    async fn fetch_source_count(&self, _table: &str) -> anyhow::Result<i64> {
        // We report source counts from the daemon status or skip if unavailable.
        // Since we don't hold a persistent source connection, use -1 as placeholder.
        // The dashboard shows target counts which is the main value.
        Ok(-1)
    }

    // ── Actions ────────────────────────────────────────────────────────────

    async fn toggle_traffic(&mut self) {
        if let Some(ref mut gen) = self.traffic_generator {
            if gen.is_running() {
                gen.stop().await;
                self.traffic_generator = None;
                self.traffic_running = false;
                self.set_status_message("Traffic stopped");
                return;
            }
        }

        let mut gen = TrafficGenerator::new(&self.source_dsn, 10.0);
        gen.start();
        self.traffic_generator = Some(gen);
        self.traffic_running = true;
        self.set_status_message("Traffic started (~10 ops/s)");
    }

    async fn toggle_pause(&mut self) {
        match self.last_status.as_ref().map(|s| s.stage.as_str()) {
            Some("paused") => {
                if self.dbmazz.resume().await.is_ok() {
                    self.set_status_message("Pipeline resumed");
                } else {
                    self.set_status_message("Failed to resume pipeline");
                }
            }
            Some(_) => {
                if self.dbmazz.pause().await.is_ok() {
                    self.set_status_message("Pipeline paused");
                } else {
                    self.set_status_message("Failed to pause pipeline");
                }
            }
            None => {
                self.set_status_message("Pipeline not connected");
            }
        }
    }

    async fn show_logs(
        &mut self,
        terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    ) -> anyhow::Result<()> {
        // Leave alternate screen and raw mode for logs.
        disable_raw_mode()?;
        execute!(terminal.backend_mut(), LeaveAlternateScreen)?;

        if let Some(ref compose_file) = self.compose_file {
            let _ = runner::logs(compose_file, None, true, 100);
        } else {
            println!("No compose file available for logs.");
            println!("Press Enter to return...");
            let mut input = String::new();
            let _ = std::io::stdin().read_line(&mut input);
        }

        // Re-enter alternate screen.
        execute!(terminal.backend_mut(), EnterAlternateScreen)?;
        enable_raw_mode()?;
        terminal.clear()?;

        Ok(())
    }

    fn set_status_message(&mut self, msg: &str) {
        self.status_message = Some((msg.to_string(), Instant::now()));
    }

    // ── Rendering ──────────────────────────────────────────────────────────

    fn render(&self, frame: &mut Frame) {
        let area = frame.area();

        // Main layout: header | body | footer.
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),  // header
                Constraint::Min(8),    // body
                Constraint::Length(5), // tables
                Constraint::Length(3),  // footer
            ])
            .split(area);

        self.render_header(frame, chunks[0]);
        self.render_body(frame, chunks[1]);
        self.render_tables(frame, chunks[2]);
        self.render_footer(frame, chunks[3]);
    }

    fn render_header(&self, frame: &mut Frame, area: Rect) {
        let (stage, stage_style) = match &self.last_status {
            Some(status) => {
                let (label, style) = match status.stage.as_str() {
                    "cdc" => ("\u{25CF} CDC running", Style::default().fg(Color::Green)),
                    "snapshot" => ("\u{25CF} Snapshot", Style::default().fg(Color::Yellow)),
                    "setup" => ("\u{25CB} Setting up", Style::default().fg(Color::Yellow)),
                    "paused" => ("\u{23F8} Paused", Style::default().fg(Color::Yellow)),
                    "stopped" => ("\u{25A0} Stopped", Style::default().fg(Color::Red)),
                    other => (other, Style::default().fg(Color::Gray)),
                };
                // stage.as_str() borrows self.last_status which we can't move out of,
                // so we use a &'static str for the known cases above.
                (label.to_string(), style)
            }
            None => (
                "Connecting...".to_string(),
                Style::default().fg(Color::DarkGray),
            ),
        };

        let header_text = Line::from(vec![
            Span::styled("  ez-cdc ", Style::default().fg(Color::Cyan).bold()),
            Span::styled("\u{00B7} ", Style::default().fg(Color::DarkGray)),
            Span::styled(&self.name, Style::default().fg(Color::White)),
            Span::raw("  "),
            // Right-align: fill with spaces, then status.
            // For simplicity, just append the status with spacing.
            Span::styled(
                format!(
                    "{:>width$}",
                    &stage,
                    width = area.width.saturating_sub(self.name.len() as u16 + 16) as usize,
                ),
                stage_style,
            ),
        ]);

        let header = Paragraph::new(header_text).block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::DarkGray)),
        );
        frame.render_widget(header, area);
    }

    fn render_body(&self, frame: &mut Frame, area: Rect) {
        // Split body into left (pipeline info) and right (sparkline).
        let body_chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(40), Constraint::Percentage(60)])
            .split(area);

        self.render_pipeline_info(frame, body_chunks[0]);
        self.render_sparkline(frame, body_chunks[1]);
    }

    fn render_pipeline_info(&self, frame: &mut Frame, area: Rect) {
        let info = match &self.last_status {
            Some(s) => {
                let uptime = format_duration(s.uptime_secs);
                let events_formatted = format_number(s.events_total);
                let rate = format!("~{:.0}/s", s.events_per_sec);
                let lag = format!("{}ms", s.replication_lag_ms);
                let cpu_mem = format!("{}mc   RSS {:.1}MB", s.cpu_millicores, s.memory_rss_mb);

                let traffic_status = if self.traffic_running {
                    let stats = self
                        .traffic_generator
                        .as_ref()
                        .map(|g| g.stats())
                        .unwrap_or(crate::load::generator::StatsSnapshot {
                            inserts: 0,
                            updates: 0,
                            deletes: 0,
                            errors: 0,
                        });
                    format!(
                        "ON (I:{} U:{} D:{})",
                        stats.inserts, stats.updates, stats.deletes
                    )
                } else {
                    "OFF".to_string()
                };

                vec![
                    Line::from(vec![
                        Span::styled("  Stage     ", Style::default().fg(Color::DarkGray)),
                        Span::styled(
                            format!("\u{25CF} {}", s.stage.to_uppercase()),
                            Style::default().fg(Color::Green),
                        ),
                    ]),
                    Line::from(vec![
                        Span::styled("  Uptime    ", Style::default().fg(Color::DarkGray)),
                        Span::styled(uptime, Style::default().fg(Color::White)),
                    ]),
                    Line::from(vec![
                        Span::styled("  LSN       ", Style::default().fg(Color::DarkGray)),
                        Span::styled(&s.confirmed_lsn, Style::default().fg(Color::White)),
                    ]),
                    Line::from(vec![
                        Span::styled("  Events    ", Style::default().fg(Color::DarkGray)),
                        Span::styled(events_formatted, Style::default().fg(Color::White)),
                    ]),
                    Line::from(vec![
                        Span::styled("  Rate      ", Style::default().fg(Color::DarkGray)),
                        Span::styled(rate, Style::default().fg(Color::White)),
                    ]),
                    Line::from(vec![
                        Span::styled("  Lag       ", Style::default().fg(Color::DarkGray)),
                        Span::styled(lag, Style::default().fg(Color::White)),
                    ]),
                    Line::from(vec![
                        Span::styled("  CPU/RSS   ", Style::default().fg(Color::DarkGray)),
                        Span::styled(cpu_mem, Style::default().fg(Color::White)),
                    ]),
                    Line::from(vec![
                        Span::styled("  Traffic   ", Style::default().fg(Color::DarkGray)),
                        Span::styled(
                            traffic_status,
                            if self.traffic_running {
                                Style::default().fg(Color::Green)
                            } else {
                                Style::default().fg(Color::DarkGray)
                            },
                        ),
                    ]),
                ]
            }
            None => {
                vec![
                    Line::from(""),
                    Line::from(Span::styled(
                        "  Connecting to dbmazz...",
                        Style::default().fg(Color::DarkGray),
                    )),
                ]
            }
        };

        let block = Block::default()
            .title(" Pipeline ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray));

        let paragraph = Paragraph::new(info).block(block);
        frame.render_widget(paragraph, area);
    }

    fn render_sparkline(&self, frame: &mut Frame, area: Rect) {
        let block = Block::default()
            .title(" Throughput (events/sec) ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray));

        let data: Vec<u64> = if self.throughput_history.is_empty() {
            vec![0]
        } else {
            self.throughput_history.clone()
        };

        let sparkline = Sparkline::default()
            .block(block)
            .data(&data)
            .style(Style::default().fg(Color::Cyan));

        frame.render_widget(sparkline, area);
    }

    fn render_tables(&self, frame: &mut Frame, area: Rect) {
        let header = Row::new(vec![
            Cell::from("  Table").style(Style::default().fg(Color::DarkGray)),
            Cell::from("Target").style(Style::default().fg(Color::DarkGray)),
            Cell::from("Status").style(Style::default().fg(Color::DarkGray)),
        ]);

        let rows: Vec<Row> = self
            .table_counts
            .iter()
            .map(|tc| {
                let target_str = if tc.target >= 0 {
                    format_number(tc.target as u64)
                } else {
                    "-".to_string()
                };

                let (status, style) = if tc.target < 0 {
                    ("...", Style::default().fg(Color::DarkGray))
                } else if tc.target > 0 {
                    ("\u{2713} synced", Style::default().fg(Color::Green))
                } else {
                    ("\u{25C6} waiting", Style::default().fg(Color::Yellow))
                };

                Row::new(vec![
                    Cell::from(format!("  {}", tc.name)).style(Style::default().fg(Color::White)),
                    Cell::from(target_str).style(Style::default().fg(Color::White)),
                    Cell::from(status).style(style),
                ])
            })
            .collect();

        let widths = [
            Constraint::Percentage(40),
            Constraint::Percentage(30),
            Constraint::Percentage(30),
        ];

        let table = Table::new(rows, widths)
            .header(header)
            .block(
                Block::default()
                    .title(" Tables ")
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(Color::DarkGray)),
            );

        frame.render_widget(table, area);
    }

    fn render_footer(&self, frame: &mut Frame, area: Rect) {
        // Status message (auto-dismiss after 3 seconds).
        let status_line = match &self.status_message {
            Some((msg, ts)) if ts.elapsed() < Duration::from_secs(3) => {
                Line::from(Span::styled(
                    format!("  {msg}"),
                    Style::default().fg(Color::Yellow),
                ))
            }
            _ => Line::from(""),
        };

        let traffic_label = if self.traffic_running { "stop" } else { "start" };

        let keybindings = Line::from(vec![
            Span::styled("  [t]", Style::default().fg(Color::Cyan).bold()),
            Span::styled(
                format!(" {traffic_label} traffic  "),
                Style::default().fg(Color::DarkGray),
            ),
            Span::styled("[p]", Style::default().fg(Color::Cyan).bold()),
            Span::styled(
                " pause/resume  ",
                Style::default().fg(Color::DarkGray),
            ),
            Span::styled("[l]", Style::default().fg(Color::Cyan).bold()),
            Span::styled(" logs  ", Style::default().fg(Color::DarkGray)),
            Span::styled("[s]", Style::default().fg(Color::Cyan).bold()),
            Span::styled(" snapshot  ", Style::default().fg(Color::DarkGray)),
            Span::styled("[q]", Style::default().fg(Color::Cyan).bold()),
            Span::styled(" quit", Style::default().fg(Color::DarkGray)),
        ]);

        let footer = Paragraph::new(vec![status_line, keybindings]).block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::DarkGray)),
        );
        frame.render_widget(footer, area);
    }
}

// ── Helpers ────────────────────────────────────────────────────────────────

fn format_duration(secs: u64) -> String {
    let hours = secs / 3600;
    let minutes = (secs % 3600) / 60;
    let seconds = secs % 60;
    if hours > 0 {
        format!("{hours}h {minutes:02}m {seconds:02}s")
    } else if minutes > 0 {
        format!("{minutes}m {seconds:02}s")
    } else {
        format!("{seconds}s")
    }
}

fn format_number(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_duration_seconds() {
        assert_eq!(format_duration(45), "45s");
    }

    #[test]
    fn format_duration_minutes() {
        assert_eq!(format_duration(125), "2m 05s");
    }

    #[test]
    fn format_duration_hours() {
        assert_eq!(format_duration(9252), "2h 34m 12s");
    }

    #[test]
    fn format_number_small() {
        assert_eq!(format_number(0), "0");
        assert_eq!(format_number(42), "42");
        assert_eq!(format_number(999), "999");
    }

    #[test]
    fn format_number_thousands() {
        assert_eq!(format_number(1000), "1,000");
        assert_eq!(format_number(1234567), "1,234,567");
    }

    #[test]
    fn format_number_millions() {
        assert_eq!(format_number(10000000), "10,000,000");
    }
}
