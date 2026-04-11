//! Full-screen ratatui TUI dashboard for the quickstart command.
//!
//! Layout inspired by btop/lazydocker: hero metrics up top, full-width
//! sparkline, clean table, borderless keybind bar at the very bottom.

use std::io;
use std::path::PathBuf;
use std::time::{Duration, Instant};

// Maximum time a single DB query (source count or target count) may take
// before it is abandoned. Keeps the TUI responsive even when a backend is slow.
const QUERY_TIMEOUT: Duration = Duration::from_secs(3);

// Minimum interval between consecutive source-PG reconnect attempts.
// Prevents spamming the database with connection attempts every 500 ms tick.
const RECONNECT_COOLDOWN: Duration = Duration::from_secs(5);

use crossterm::event::{self, Event, KeyCode, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::prelude::*;
use ratatui::widgets::{Block, Borders, Paragraph, Sparkline};

use crate::clients::dbmazz::{DaemonStatus, DbmazzClient};
use crate::clients::targets::TargetBackend;
use crate::compose::runner;
use crate::load::TrafficGenerator;

// ── Constants ──────────────────────────────────────────────────────────────

const HISTORY_LEN: usize = 60;
const POLL_MS: u64 = 250;
const TICK_MS: u64 = 500;

// ── Brand colors ───────────────────────────────────────────────────────────

const PRIMARY: Color = Color::Rgb(96, 165, 250); // #60A5FA
const INFO: Color = Color::Rgb(59, 130, 246); // #3B82F6
const SUCCESS: Color = Color::Rgb(16, 185, 129); // #10B981
const ERROR: Color = Color::Rgb(239, 68, 68); // #EF4444
const WARNING: Color = Color::Rgb(245, 158, 11); // #F59E0B
const MUTED: Color = Color::Rgb(148, 163, 184); // #94A3B8
const DIM: Color = Color::Rgb(100, 116, 139); // #64748B
const BORDER: Color = Color::Rgb(71, 85, 105); // #475569

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
    // lazy source PG connection
    source_client: Option<tokio_postgres::Client>,
    /// Timestamp of the last connection attempt for the source PG client.
    /// Used to enforce `RECONNECT_COOLDOWN` between retries.
    last_connect_attempt: Option<Instant>,
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
            source_client: None,
            last_connect_attempt: None,
        }
    }

    /// Run the dashboard event loop. Blocks until the user quits.
    pub async fn run(&mut self) -> anyhow::Result<()> {
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

        if let Some(ref mut gen) = self.traffic_generator {
            gen.stop().await;
        }

        Ok(())
    }

    // ── Data fetching ──────────────────────────────────────────────────────

    async fn fetch_data(&mut self) {
        // Bounded by QUERY_TIMEOUT so a slow/unreachable daemon can't freeze the TUI.
        match tokio::time::timeout(QUERY_TIMEOUT, self.dbmazz.status()).await {
            Ok(Ok(status)) => {
                // Use the daemon's reported events_per_second as the authoritative
                // throughput value rather than computing a local delta.
                let eps = status.events_per_sec as u64;

                self.throughput_history.push(eps);
                if self.throughput_history.len() > HISTORY_LEN {
                    self.throughput_history.remove(0);
                }

                self.last_status = Some(status);
            }
            Ok(Err(_)) | Err(_) => {
                self.throughput_history.push(0);
                if self.throughput_history.len() > HISTORY_LEN {
                    self.throughput_history.remove(0);
                }
            }
        }

        self.ensure_source_client().await;

        let mut counts = Vec::new();
        for table in &self.tables.clone() {
            // Both queries are bounded by QUERY_TIMEOUT so that a slow source or
            // target cannot stall the 500 ms event loop tick.
            let source_count = tokio::time::timeout(
                QUERY_TIMEOUT,
                self.fetch_source_count(table),
            )
            .await
            .unwrap_or(Ok(-1))
            .unwrap_or(-1);

            let target_count = tokio::time::timeout(
                QUERY_TIMEOUT,
                self.target.count_rows(table, true),
            )
            .await
            .unwrap_or(Ok(-1))
            .unwrap_or(-1);

            counts.push(TableCount {
                name: table.clone(),
                source: source_count,
                target: target_count,
            });
        }
        self.table_counts = counts;
    }

    async fn ensure_source_client(&mut self) {
        if self.source_client.is_some() {
            return;
        }

        // Enforce a cooldown between reconnect attempts so a repeated failure
        // does not hammer the database once every 500 ms tick.
        if let Some(last) = self.last_connect_attempt {
            if last.elapsed() < RECONNECT_COOLDOWN {
                return;
            }
        }
        self.last_connect_attempt = Some(Instant::now());

        let dsn = self.source_dsn.clone();
        // Bound the connection attempt itself so a network timeout can't freeze
        // the TUI for the OS default TCP timeout (often 2 minutes).
        match tokio::time::timeout(
            QUERY_TIMEOUT,
            tokio_postgres::connect(&dsn, tokio_postgres::NoTls),
        )
        .await
        {
            Ok(Ok((client, connection))) => {
                tokio::spawn(async move {
                    let _ = connection.await;
                });
                self.source_client = Some(client);
            }
            Ok(Err(_)) | Err(_) => {
                // Connection failed or timed out — will retry after RECONNECT_COOLDOWN.
            }
        }
    }

    async fn fetch_source_count(&mut self, table: &str) -> anyhow::Result<i64> {
        let client = match self.source_client.as_ref() {
            Some(c) => c,
            None => return Ok(-1),
        };

        if table.contains('"') {
            return Ok(-1);
        }

        let query = format!("SELECT COUNT(*) FROM \"{}\"", table);
        match client.query_one(&query, &[]).await {
            Ok(row) => {
                let count: i64 = row.get(0);
                Ok(count)
            }
            Err(_) => {
                self.source_client = None;
                Ok(-1)
            }
        }
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

        let mut gen = TrafficGenerator::new(&self.source_dsn, 50.0);
        gen.start();
        self.traffic_generator = Some(gen);
        self.traffic_running = true;
        self.set_status_message("Traffic started (~50 ops/s) — watch the counts update");
    }

    async fn toggle_pause(&mut self) {
        match self.last_status.as_ref().map(|s| s.effective_stage()) {
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
        // The TUI is fully suspended (raw mode off, alternate screen left) before
        // invoking runner::logs(), which uses a blocking std::process::Command with
        // inherited stdio. This is intentional: the blocking call runs while the
        // event loop is paused, so it does not stall the tokio runtime.
        disable_raw_mode()?;
        execute!(terminal.backend_mut(), LeaveAlternateScreen)?;

        if let Some(ref compose_file) = self.compose_file {
            let _ = runner::logs(compose_file, Some("dbmazz"), true, 100);
        } else {
            println!("No compose file available for logs.");
            println!("Press Enter to return...");
            let mut input = String::new();
            let _ = std::io::stdin().read_line(&mut input);
        }

        execute!(terminal.backend_mut(), EnterAlternateScreen)?;
        enable_raw_mode()?;
        terminal.clear()?;

        Ok(())
    }

    fn set_status_message(&mut self, msg: &str) {
        self.status_message = Some((msg.to_string(), Instant::now()));
    }

    // ── Rendering ──────────────────────────────────────────────────────────
    //
    //   1. Header          (1)  — brand + status + uptime
    //   2. Separator        (1)
    //   3. Metrics          (1)  — ev/s, lag, events, batches
    //   4. Blank            (1)
    //   5. Table header     (1)  — column labels
    //   6. Table rows       (N)  — one per table: name, source, target, delta
    //   7. Blank            (1)
    //   8. Sparkline        (flex) — throughput history
    //   9. Traffic bar      (1)
    //  10. Status msg       (1)
    //  11. Keybind bar      (1)

    fn render(&self, frame: &mut Frame) {
        let area = frame.area();
        let n_tables = self.table_counts.len().max(1) as u16;

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1),          // header
                Constraint::Length(1),          // separator
                Constraint::Length(1),          // metrics
                Constraint::Length(1),          // blank
                Constraint::Length(1),          // table header
                Constraint::Min(n_tables),     // table rows (flex — gets extra space)
                Constraint::Length(1),          // blank
                Constraint::Length(5),          // sparkline (compact, fixed)
                Constraint::Length(1),          // traffic bar
                Constraint::Length(1),          // status msg
                Constraint::Length(1),          // keybind bar
            ])
            .split(area);

        self.render_header(frame, chunks[0]);
        self.render_separator(frame, chunks[1]);
        self.render_metrics(frame, chunks[2]);
        // chunks[3] = blank
        self.render_table_header(frame, chunks[4]);
        self.render_table_rows(frame, chunks[5]);
        // chunks[6] = blank
        self.render_sparkline(frame, chunks[7]);
        self.render_traffic(frame, chunks[8]);
        self.render_status_msg(frame, chunks[9]);
        self.render_keybinds(frame, chunks[10]);
    }

    fn render_header(&self, frame: &mut Frame, area: Rect) {
        let (status_text, status_style) = match &self.last_status {
            Some(s) => stage_display(s.effective_stage()),
            None => ("Connecting...".to_string(), Style::default().fg(DIM)),
        };

        let uptime = self
            .last_status
            .as_ref()
            .map(|s| format_duration(s.uptime_secs))
            .unwrap_or_default();

        let error_span = match self.last_status.as_ref().and_then(|s| s.error_detail.as_ref()) {
            Some(err) => {
                let t = if err.len() > 30 { format!("{}...", &err[..30]) } else { err.clone() };
                Span::styled(format!("  {t}"), Style::default().fg(ERROR))
            }
            None => Span::raw(""),
        };

        let right = format!("{status_text}  {uptime}");
        let pad = area.width.saturating_sub(self.name.len() as u16 + right.len() as u16 + 12) as usize;

        let line = Line::from(vec![
            Span::styled(" EZ-CDC ", Style::default().fg(PRIMARY).bold()),
            Span::styled(" ", Style::default().fg(BORDER)),
            Span::styled(&self.name, Style::default().fg(Color::White)),
            error_span,
            Span::raw(" ".repeat(pad)),
            Span::styled(status_text, status_style),
            Span::styled(format!("  {uptime} "), Style::default().fg(MUTED)),
        ]);
        frame.render_widget(Paragraph::new(line), area);
    }

    fn render_separator(&self, frame: &mut Frame, area: Rect) {
        let rule = "\u{2500}".repeat(area.width as usize);
        frame.render_widget(
            Paragraph::new(Line::from(Span::styled(rule, Style::default().fg(BORDER)))),
            area,
        );
    }

    /// One-line metrics: "  42 ev/s   12ms lag   5,234 events   18 batches   LSN 0/1A3F"
    fn render_metrics(&self, frame: &mut Frame, area: Rect) {
        let mut spans: Vec<Span> = vec![Span::raw("  ")];
        if let Some(s) = &self.last_status {
            spans.extend([
                Span::styled(format!("{:.0}", s.events_per_sec), Style::default().fg(PRIMARY).bold()),
                Span::styled(" ev/s", Style::default().fg(DIM)),
                Span::raw("   "),
                Span::styled(format!("{}ms", s.replication_lag_ms), Style::default().fg(lag_color(&self.last_status))),
                Span::styled(" lag", Style::default().fg(DIM)),
                Span::raw("   "),
                Span::styled(format_number(s.events_total), Style::default().fg(Color::White)),
                Span::styled(" events", Style::default().fg(DIM)),
                Span::raw("   "),
                Span::styled(format_number(s.batches_sent), Style::default().fg(Color::White)),
                Span::styled(" batches", Style::default().fg(DIM)),
                Span::raw("   "),
                Span::styled("LSN ", Style::default().fg(DIM)),
                Span::styled(&s.confirmed_lsn, Style::default().fg(MUTED)),
            ]);
        } else {
            spans.push(Span::styled("Connecting...", Style::default().fg(DIM)));
        }
        frame.render_widget(Paragraph::new(Line::from(spans)), area);
    }

    /// Column header: "  TABLE              SOURCE       TARGET       DELTA"
    fn render_table_header(&self, frame: &mut Frame, area: Rect) {
        let s = Style::default().fg(DIM).bold();
        let line = Line::from(vec![
            Span::styled(format!("  {:<20}", "TABLE"), s),
            Span::styled(format!("{:>12}", "SOURCE"), s),
            Span::styled(format!("{:>12}", "TARGET"), s),
            Span::styled(format!("{:>10}", "DELTA"), s),
        ]);
        frame.render_widget(Paragraph::new(line), area);
    }

    /// One row per table with live counts and delta.
    fn render_table_rows(&self, frame: &mut Frame, area: Rect) {
        if self.table_counts.is_empty() {
            let line = Line::from(Span::styled("  Waiting for data...", Style::default().fg(DIM)));
            frame.render_widget(Paragraph::new(line), area);
            return;
        }

        let constraints: Vec<Constraint> = self.table_counts.iter()
            .map(|_| Constraint::Length(1))
            .collect();

        let rows = Layout::default()
            .direction(Direction::Vertical)
            .constraints(constraints)
            .split(area);

        for (i, tc) in self.table_counts.iter().enumerate() {
            if i >= rows.len() { break; }

            let src = if tc.source >= 0 { format_number(tc.source as u64) } else { "-".into() };
            let tgt = if tc.target >= 0 { format_number(tc.target as u64) } else { "-".into() };

            // Delta: source - target. Positive means target is behind.
            let (delta_str, delta_style) = if tc.source >= 0 && tc.target >= 0 {
                let diff = tc.source - tc.target;
                if diff == 0 {
                    ("\u{2713}".to_string(), Style::default().fg(SUCCESS).bold()) // ✓
                } else if diff > 0 {
                    (format!("+{}", diff), Style::default().fg(WARNING).bold()) // target behind
                } else {
                    // target > source (deletes not yet reflected, or race)
                    ("\u{2713}".to_string(), Style::default().fg(SUCCESS).bold())
                }
            } else {
                ("...".to_string(), Style::default().fg(DIM))
            };

            let line = Line::from(vec![
                Span::styled(format!("  {:<20}", tc.name), Style::default().fg(Color::White)),
                Span::styled(format!("{:>12}", src), Style::default().fg(MUTED)),
                Span::styled(format!("{:>12}", tgt), Style::default().fg(Color::White).bold()),
                Span::styled(format!("{:>10}", delta_str), delta_style),
            ]);
            frame.render_widget(Paragraph::new(line), rows[i]);
        }
    }

    fn render_sparkline(&self, frame: &mut Frame, area: Rect) {
        let current = self.throughput_history.last().copied().unwrap_or(0);
        let peak = self.throughput_history.iter().copied().max().unwrap_or(0);
        let title = if current > 0 || peak > 0 {
            format!(" Events/sec: {current} (peak {peak}) ")
        } else {
            " Events/sec (no activity) ".to_string()
        };

        let block = Block::default()
            .title(Span::styled(title, Style::default().fg(MUTED)))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(BORDER));

        let data: Vec<u64> = if self.throughput_history.is_empty() {
            vec![0]
        } else {
            self.throughput_history.clone()
        };

        let sparkline = Sparkline::default()
            .block(block)
            .data(&data)
            .style(Style::default().fg(PRIMARY));

        frame.render_widget(sparkline, area);
    }

    fn render_traffic(&self, frame: &mut Frame, area: Rect) {
        let line = if self.traffic_running {
            let stats = self.traffic_generator.as_ref()
                .map(|g| g.stats())
                .unwrap_or(crate::load::generator::StatsSnapshot {
                    inserts: 0, updates: 0, deletes: 0, errors: 0,
                });
            Line::from(vec![
                Span::styled(
                    format!("  Traffic ON   inserts:{} updates:{} deletes:{}   RSS {:.1}MB",
                        stats.inserts, stats.updates, stats.deletes,
                        self.last_status.as_ref().map(|s| s.memory_mb()).unwrap_or(0.0),
                    ),
                    Style::default().fg(SUCCESS),
                ),
            ])
        } else {
            Line::from(vec![
                Span::styled("  Traffic OFF", Style::default().fg(DIM)),
                Span::styled(
                    format!("   RSS {:.1}MB",
                        self.last_status.as_ref().map(|s| s.memory_mb()).unwrap_or(0.0),
                    ),
                    Style::default().fg(DIM),
                ),
            ])
        };
        frame.render_widget(Paragraph::new(line), area);
    }

    fn render_status_msg(&self, frame: &mut Frame, area: Rect) {
        let line = match &self.status_message {
            Some((msg, ts)) if ts.elapsed() < Duration::from_secs(3) => {
                Line::from(Span::styled(format!("  {msg}"), Style::default().fg(WARNING)))
            }
            _ => Line::from(""),
        };
        frame.render_widget(Paragraph::new(line), area);
    }

    fn render_keybinds(&self, frame: &mut Frame, area: Rect) {
        let key = Style::default().fg(Color::Black).bg(PRIMARY).bold();
        let lbl = Style::default().fg(DIM);
        let line = Line::from(vec![
            Span::raw(" "),
            Span::styled(" t ", key), Span::styled(" traffic  ", lbl),
            Span::styled(" p ", key), Span::styled(" pause  ", lbl),
            Span::styled(" l ", key), Span::styled(" logs  ", lbl),
            Span::styled(" s ", key), Span::styled(" snapshot  ", lbl),
            Span::styled(" q ", key), Span::styled(" quit", lbl),
        ]);
        frame.render_widget(Paragraph::new(line), area);
    }
}

// ── Helpers ────────────────────────────────────────────────────────────────

/// Stage display text and color.
fn stage_display(stage: &str) -> (String, Style) {
    match stage {
        "cdc" => (
            "\u{25CF} REPLICATING".to_string(),
            Style::default().fg(SUCCESS).bold(),
        ),
        "snapshot" => (
            "\u{25CF} SNAPSHOT".to_string(),
            Style::default().fg(WARNING).bold(),
        ),
        "setup" => (
            "\u{25CB} SETTING UP".to_string(),
            Style::default().fg(INFO).bold(),
        ),
        "paused" => (
            "\u{23F8} PAUSED".to_string(),
            Style::default().fg(MUTED).bold(),
        ),
        "stopped" => (
            "\u{25A0} STOPPED".to_string(),
            Style::default().fg(ERROR).bold(),
        ),
        other => (other.to_uppercase(), Style::default().fg(MUTED)),
    }
}

/// Color for the lag metric — green if <100ms, yellow if <1000ms, red if >=1000ms.
fn lag_color(status: &Option<DaemonStatus>) -> Color {
    match status {
        Some(s) if s.replication_lag_ms < 100 => SUCCESS,
        Some(s) if s.replication_lag_ms < 1000 => WARNING,
        Some(_) => ERROR,
        None => DIM,
    }
}

/// Compute sync percentage and color for a table row.
///
/// Returns (percentage 0-100, color). -1 means unknown.
#[cfg(test)]
fn sync_pct(source: i64, target: i64) -> (i64, Color) {
    if source < 0 || target < 0 {
        return (-1, DIM);
    }
    if source == 0 && target == 0 {
        return (100, SUCCESS);
    }
    if source == 0 {
        return (100, SUCCESS); // target has data, source empty (edge case)
    }
    let pct = ((target as f64 / source as f64) * 100.0).min(100.0) as i64;
    let color = if pct >= 100 {
        SUCCESS
    } else if pct > 0 {
        WARNING
    } else {
        ERROR
    };
    (pct, color)
}

/// Sync status label and style for a table row (used by tests).
#[cfg(test)]
fn sync_status(source: i64, target: i64) -> (String, Style) {
    if source < 0 {
        return if target < 0 {
            ("...".to_string(), Style::default().fg(DIM))
        } else if target == 0 {
            (
                "\u{25C6} waiting".to_string(),
                Style::default().fg(WARNING),
            )
        } else {
            (
                "\u{2713} synced".to_string(),
                Style::default().fg(SUCCESS),
            )
        };
    }

    if target == 0 {
        return (
            "\u{25C6} waiting".to_string(),
            Style::default().fg(WARNING),
        );
    }

    if target < source {
        let pct = if source > 0 {
            (target * 100) / source
        } else {
            0
        };
        return (
            format!("\u{25C6} syncing {pct}%"),
            Style::default().fg(WARNING),
        );
    }

    (
        "\u{2713} synced".to_string(),
        Style::default().fg(SUCCESS),
    )
}

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

    // ── sync_status tests ──────────────────────────────────────────────────

    #[test]
    fn sync_status_source_unknown_target_zero() {
        let (label, _) = sync_status(-1, 0);
        assert_eq!(label, "\u{25C6} waiting");
    }

    #[test]
    fn sync_status_source_unknown_target_positive() {
        let (label, _) = sync_status(-1, 1000);
        assert_eq!(label, "\u{2713} synced");
    }

    #[test]
    fn sync_status_source_unknown_both_unknown() {
        let (label, _) = sync_status(-1, -1);
        assert_eq!(label, "...");
    }

    #[test]
    fn sync_status_waiting() {
        let (label, _) = sync_status(1000, 0);
        assert_eq!(label, "\u{25C6} waiting");
    }

    #[test]
    fn sync_status_syncing_pct() {
        let (label, _) = sync_status(1000, 500);
        assert_eq!(label, "\u{25C6} syncing 50%");
    }

    #[test]
    fn sync_status_synced() {
        let (label, _) = sync_status(1000, 1000);
        assert_eq!(label, "\u{2713} synced");
    }

    #[test]
    fn sync_status_target_exceeds_source() {
        let (label, _) = sync_status(1000, 1001);
        assert_eq!(label, "\u{2713} synced");
    }

    // ── stage_display tests ────────────────────────────────────────────────

    #[test]
    fn stage_cdc() {
        let (text, _) = stage_display("cdc");
        assert!(text.contains("REPLICATING"));
    }

    #[test]
    fn stage_snapshot_progress() {
        let (text, _) = stage_display("snapshot");
        assert!(text.contains("SNAPSHOT"));
    }

    #[test]
    fn stage_stopped() {
        let (text, _) = stage_display("stopped");
        assert!(text.contains("STOPPED"));
    }

    // ── lag_color tests ────────────────────────────────────────────────────

    #[test]
    fn lag_green_under_100() {
        let status = Some(DaemonStatus {
            stage: "cdc".into(),
            replication_lag_ms: 42,
            ..default_status()
        });
        assert_eq!(lag_color(&status), SUCCESS);
    }

    #[test]
    fn lag_yellow_under_1000() {
        let status = Some(DaemonStatus {
            stage: "cdc".into(),
            replication_lag_ms: 500,
            ..default_status()
        });
        assert_eq!(lag_color(&status), WARNING);
    }

    #[test]
    fn lag_red_over_1000() {
        let status = Some(DaemonStatus {
            stage: "cdc".into(),
            replication_lag_ms: 2000,
            ..default_status()
        });
        assert_eq!(lag_color(&status), ERROR);
    }

    #[test]
    fn lag_none() {
        assert_eq!(lag_color(&None), DIM);
    }

    /// Helper to build a DaemonStatus with sane defaults for tests.
    fn default_status() -> DaemonStatus {
        DaemonStatus {
            stage: "cdc".into(),
            uptime_secs: 0,
            confirmed_lsn: "0/0".into(),
            current_lsn: "0/0".into(),
            events_total: 0,
            events_per_sec: 0.0,
            batches_sent: 0,
            replication_lag_ms: 0,
            memory_bytes: 0,
            state: "running".into(),
            error_detail: None,
        }
    }
}
