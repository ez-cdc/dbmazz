use clap::{Parser, Subcommand};

mod clients;
mod commands;
mod compose;
mod config;
mod daemon;
mod instantiate;
mod load;
mod paths;
mod preflight;
mod tui;
mod verify;

#[derive(Parser)]
#[command(
    name = "ez-cdc",
    about = "EZ-CDC end-to-end test harness",
    version,
    propagate_version = true
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    /// Path to config file
    #[arg(long, global = true, default_value = "ez-cdc.yaml")]
    config: std::path::PathBuf,
}

#[derive(Subcommand)]
enum Commands {
    /// Start infrastructure containers
    Up {
        /// Force rebuild of dbmazz image
        #[arg(long)]
        rebuild: bool,
    },
    /// Stop infrastructure
    Down {
        /// Keep volumes
        #[arg(long)]
        keep_volumes: bool,
    },
    /// Run verification suite
    Verify {
        /// Source datasource name
        source: Option<String>,
        /// Sink datasource name
        sink: Option<String>,
        /// Run all source x sink combinations
        #[arg(long)]
        all: bool,
        /// Run quick mode (skip slow checks)
        #[arg(long)]
        quick: bool,
        /// Skip specific check IDs (comma-separated, e.g. "C3,D7")
        #[arg(long)]
        skip: Option<String>,
        /// Write JSON report to file
        #[arg(long)]
        json_report: Option<std::path::PathBuf>,
        /// Don't start infra/dbmazz (assume already running)
        #[arg(long)]
        no_up: bool,
        /// Keep stack running after verify
        #[arg(long)]
        keep_up: bool,
        /// Force rebuild of dbmazz image
        #[arg(long)]
        rebuild: bool,
    },
    /// Live monitoring dashboard
    Quickstart {
        /// Source datasource name
        source: Option<String>,
        /// Sink datasource name
        sink: Option<String>,
        /// Keep stack running after exit
        #[arg(long)]
        keep_up: bool,
        /// Force rebuild of dbmazz image
        #[arg(long)]
        rebuild: bool,
    },
    /// Datasource management
    #[command(alias = "ds")]
    Datasource {
        #[command(subcommand)]
        cmd: DsCommands,
    },
    /// Show daemon status
    Status,
    /// Tail container logs (use 'dbmazz' to see daemon logs)
    Logs {
        /// Service name (e.g. 'dbmazz', 'source-pg', 'sink-starrocks')
        service: Option<String>,
        /// Follow log output
        #[arg(long, short, default_value = "true")]
        follow: bool,
        /// Number of lines to show from the end
        #[arg(long, short, default_value = "100")]
        tail: u32,
    },
    /// Clean target database
    Clean {
        /// Source datasource name
        source: Option<String>,
        /// Sink datasource name
        sink: Option<String>,
        /// Skip confirmation prompt
        #[arg(long, short)]
        yes: bool,
    },
}

#[derive(Subcommand)]
enum DsCommands {
    /// List all datasources
    List,
    /// Show datasource details
    Show {
        /// Datasource name
        name: String,
        /// Reveal passwords
        #[arg(long)]
        reveal: bool,
    },
    /// Add a new datasource (interactive wizard)
    Add,
    /// Test datasource connectivity
    Test {
        /// Datasource name
        name: String,
    },
    /// Remove a datasource
    Remove {
        /// Datasource name
        name: String,
        /// Skip confirmation prompt
        #[arg(long, short)]
        yes: bool,
    },
    /// Initialize demo datasources
    Init,
}

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;

    let cli = Cli::parse();
    let config_path = cli.config;

    match cli.command {
        Some(cmd) => {
            tui::banner::render_banner_compact();

            let result: anyhow::Result<()> = match cmd {
                Commands::Up { rebuild } => {
                    commands::up::run_up(&config_path, rebuild).await
                }
                Commands::Down { keep_volumes } => {
                    commands::down::run_down(&config_path, keep_volumes).await
                }
                Commands::Verify {
                    source,
                    sink,
                    all,
                    quick,
                    skip,
                    json_report,
                    no_up,
                    keep_up,
                    rebuild,
                } => {
                    commands::verify_cmd::run_verify(
                        &config_path,
                        source,
                        sink,
                        all,
                        quick,
                        skip,
                        json_report,
                        no_up,
                        keep_up,
                        rebuild,
                    )
                    .await
                }
                Commands::Quickstart {
                    source,
                    sink,
                    keep_up,
                    rebuild,
                } => {
                    commands::quickstart::run_quickstart(
                        &config_path,
                        source,
                        sink,
                        keep_up,
                        rebuild,
                    )
                    .await
                }
                Commands::Datasource { cmd } => match cmd {
                    DsCommands::List => {
                        commands::datasource::run_ds_list(&config_path)
                    }
                    DsCommands::Show { name, reveal } => {
                        commands::datasource::run_ds_show(&config_path, &name, reveal)
                    }
                    DsCommands::Add => {
                        commands::datasource::run_ds_add(&config_path)
                    }
                    DsCommands::Test { name } => {
                        commands::datasource::run_ds_test(&config_path, &name).await
                    }
                    DsCommands::Remove { name, yes } => {
                        commands::datasource::run_ds_remove(&config_path, &name, yes)
                    }
                    DsCommands::Init => {
                        commands::datasource::run_ds_init(&config_path)
                    }
                },
                Commands::Status => {
                    commands::status::run_status().await
                }
                Commands::Logs { service, follow, tail } => {
                    commands::logs::run_logs(&config_path, service, follow, tail)
                }
                Commands::Clean { source, sink, yes } => {
                    commands::clean::run_clean(&config_path, source, sink, yes).await
                }
            };

            if let Err(e) = result {
                eprintln!("\n  {}\n", console::style(format!("{e}")).red());
                std::process::exit(1);
            }
        }
        None => {
            // No subcommand: interactive menu (if TTY) or help
            if atty::is(atty::Stream::Stdin) {
                tui::banner::render_banner();
                run_interactive_menu(&config_path).await?;
            } else {
                Cli::parse_from(["ez-cdc", "--help"]);
            }
        }
    }

    Ok(())
}

async fn run_interactive_menu(config_path: &std::path::Path) -> color_eyre::Result<()> {
    loop {
        // Detect state on every iteration.
        let config_exists = config_path.exists();
        let has_datasources = if config_exists {
            commands::load_store_or_empty(config_path)
                .map(|mut s| s.has_any().unwrap_or(false))
                .unwrap_or(false)
        } else {
            false
        };
        let infra_running = compose::runner::is_running(
            &compose::builder::infra_compose_path(),
        );

        // Print status line.
        if !config_exists {
            println!(
                "  {}",
                console::style("No config file found. Let's set things up.").dim()
            );
        } else if has_datasources {
            let mut store = commands::load_store_or_empty(config_path).unwrap();
            let n_src = store.list_sources().map(|v| v.len()).unwrap_or(0);
            let n_sk = store.list_sinks().map(|v| v.len()).unwrap_or(0);
            let infra_label = if infra_running {
                console::style("running").green().to_string()
            } else {
                console::style("stopped").dim().to_string()
            };
            println!(
                "  {} sources, {} sinks  ·  infra: {}",
                n_src, n_sk, infra_label,
            );
        }
        println!();

        // Build choices based on state.
        type Item = (String, String, String);
        let choices: Vec<Item> = if !config_exists || !has_datasources {
            // State 1: no config or empty config
            vec![
                ("init".into(), "Init config".into(), "Create ez-cdc.yaml with demo datasources".into()),
                ("ds".into(), "Datasources".into(), "List / add / remove source and sink configs".into()),
                ("exit".into(), "Exit".into(), String::new()),
            ]
        } else if infra_running {
            // State 3: config + stack running
            vec![
                ("quickstart".into(), "Quickstart".into(), "Open the live dashboard".into()),
                ("verify".into(), "Verify".into(), "Run e2e validation tests".into()),
                ("clean".into(), "Clean target".into(), "Truncate tables + drop audit columns".into()),
                ("logs".into(), "Logs".into(), "Tail infra container logs".into()),
                ("stop".into(), "Stop stack".into(), "Stop all infra containers".into()),
                ("ds".into(), "Datasources".into(), "List / add / remove source and sink configs".into()),
                ("exit".into(), "Exit".into(), String::new()),
            ]
        } else {
            // State 2: config exists, stack not running
            vec![
                ("up".into(), "Start stack".into(), "Start all infra containers".into()),
                ("quickstart".into(), "Quickstart".into(), "Open the live dashboard".into()),
                ("verify".into(), "Verify".into(), "Run e2e validation tests".into()),
                ("clean".into(), "Clean target".into(), "Truncate tables + drop audit columns".into()),
                ("ds".into(), "Datasources".into(), "List / add / remove source and sink configs".into()),
                ("exit".into(), "Exit".into(), String::new()),
            ]
        };

        let choice = match tui::prompts::select("What would you like to do?", choices) {
            Ok(c) => c,
            Err(_) => break, // Ctrl+C → exit
        };

        if choice == "exit" {
            break;
        }

        let result: anyhow::Result<()> = match choice.as_str() {
            "init" => {
                let r = commands::datasource::run_ds_init(config_path);
                // After init, loop back to show the updated menu.
                if r.is_ok() {
                    println!();
                    wait_for_keypress();
                }
                r
            }
            "up" => commands::up::run_up(config_path, false).await,
            "stop" => commands::down::run_down(config_path, false).await,
            "quickstart" => {
                commands::quickstart::run_quickstart(config_path, None, None, false, false).await
            }
            "verify" => {
                commands::verify_cmd::run_verify(
                    config_path, None, None, false, false, None, None, false, false, false,
                ).await
            }
            "clean" => commands::clean::run_clean(config_path, None, None, false).await,
            "logs" => commands::logs::run_logs(config_path, None, true, 100),
            "ds" => run_datasource_submenu(config_path).await,
            _ => Ok(()),
        };

        if let Err(e) = result {
            println!("\n  {}", console::style(format!("Error: {e}")).red());
        }

        println!();
        wait_for_keypress();

        // Clear screen before next menu iteration.
        print!("\x1b[2J\x1b[H");
        tui::banner::render_banner();
    }

    // Goodbye.
    println!("\n  {}\n", console::style("Goodbye!").dim());
    Ok(())
}

async fn run_datasource_submenu(config_path: &std::path::Path) -> anyhow::Result<()> {
    let ds_choices: Vec<(String, String, String)> = vec![
        ("list".into(), "List datasources".into(), "Show all configured sources and sinks".into()),
        ("add".into(), "Add datasource".into(), "Interactive wizard to add a source or sink".into()),
        ("show".into(), "Show details".into(), "View config of a datasource".into()),
        ("test".into(), "Test connection".into(), "Verify connectivity to a datasource".into()),
        ("remove".into(), "Remove datasource".into(), "Delete a datasource".into()),
        ("init".into(), "Init demos".into(), "Create demo datasources (StarRocks + PG)".into()),
    ];

    let ds_choice = match tui::prompts::select("Datasource management:", ds_choices) {
        Ok(c) => c,
        Err(_) => return Ok(()), // Ctrl+C → back to main menu
    };

    match ds_choice.as_str() {
        "list" => commands::datasource::run_ds_list(config_path)?,
        "add" => commands::datasource::run_ds_add(config_path)?,
        "init" => commands::datasource::run_ds_init(config_path)?,
        "show" => {
            let name = pick_datasource(config_path, "Show which datasource?")?;
            if let Some(name) = name {
                commands::datasource::run_ds_show(config_path, &name, false)?;
            }
        }
        "test" => {
            let name = pick_datasource(config_path, "Test which datasource?")?;
            if let Some(name) = name {
                commands::datasource::run_ds_test(config_path, &name).await?;
            }
        }
        "remove" => {
            let name = pick_datasource(config_path, "Remove which datasource?")?;
            if let Some(name) = name {
                commands::datasource::run_ds_remove(config_path, &name, false)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Interactively pick a datasource name from the configured ones.
fn pick_datasource(config_path: &std::path::Path, message: &str) -> anyhow::Result<Option<String>> {
    let mut store = commands::load_store_or_empty(config_path)?;
    let sources = store.list_sources().unwrap_or_default();
    let sinks = store.list_sinks().unwrap_or_default();

    let mut items: Vec<(String, String, String)> = Vec::new();
    for name in &sources {
        items.push((name.clone(), name.clone(), "source".into()));
    }
    for name in &sinks {
        items.push((name.clone(), name.clone(), "sink".into()));
    }

    if items.is_empty() {
        println!("  {} No datasources configured.", console::style("!").yellow());
        return Ok(None);
    }

    match tui::prompts::select(message, items) {
        Ok(name) => Ok(Some(name)),
        Err(_) => Ok(None), // Ctrl+C
    }
}

fn wait_for_keypress() {
    use std::io::{self, Read};
    if !atty::is(atty::Stream::Stdin) {
        return;
    }
    print!("  {}", console::style("Press any key to continue...").dim());
    let _ = io::stdout().flush();
    let _ = io::stdin().read(&mut [0u8]);
}

use std::io::Write;

/// Bridge anyhow::Result to color_eyre::Result.
async fn run_anyhow<F>(fut: F) -> color_eyre::Result<()>
where
    F: std::future::Future<Output = anyhow::Result<()>>,
{
    fut.await.map_err(|e| color_eyre::eyre::eyre!("{e:#}"))
}
