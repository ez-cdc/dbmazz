use clap::{CommandFactory, Parser, Subcommand};
use clap_complete::Shell;

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

    /// Path to config file (overrides $EZ_CDC_CONFIG and the default
    /// location at `$XDG_CONFIG_HOME/ez-cdc/config.yaml`).
    #[arg(long, short = 'c', global = true)]
    config: Option<std::path::PathBuf>,
}

/// Resolve the config file path using this precedence:
///
/// 1. `--config PATH` flag (explicit)
/// 2. `$EZ_CDC_CONFIG` env var
/// 3. `$XDG_CONFIG_HOME/ez-cdc/config.yaml` (or `$HOME/.config/ez-cdc/config.yaml`)
/// 4. `./ez-cdc.yaml` in the current working directory — only as a
///    fallback when nothing else is defined, for backwards
///    compatibility with the in-repo dev workflow.
///
/// Note: the path is returned regardless of whether the file exists.
/// Callers that need the file present must check existence themselves.
fn resolve_config_path(flag: Option<std::path::PathBuf>) -> std::path::PathBuf {
    if let Some(p) = flag {
        return p;
    }
    if let Ok(env) = std::env::var("EZ_CDC_CONFIG") {
        if !env.is_empty() {
            return std::path::PathBuf::from(env);
        }
    }
    let xdg_path = xdg_config_path();
    let cwd_path = std::path::PathBuf::from("ez-cdc.yaml");
    // Backwards compat: if we are running inside a clone of the repo
    // and there is already a local `ez-cdc.yaml` but no XDG one, keep
    // using the local file so existing in-repo workflows are not
    // disrupted. Once the XDG file exists, it takes precedence.
    if cwd_path.exists() && !xdg_path.exists() {
        return cwd_path;
    }
    xdg_path
}

/// Compute the XDG-style default config path.
///
/// Follows the XDG Base Directory Specification on Linux, and uses
/// the same `$HOME/.config/...` layout on macOS (matching `gh`,
/// `fly`, `pulumi`, and other modern CLIs).
fn xdg_config_path() -> std::path::PathBuf {
    let base = std::env::var("XDG_CONFIG_HOME")
        .ok()
        .filter(|s| !s.is_empty())
        .map(std::path::PathBuf::from)
        .or_else(|| {
            std::env::var("HOME")
                .ok()
                .filter(|s| !s.is_empty())
                .map(|h| std::path::PathBuf::from(h).join(".config"))
        })
        .unwrap_or_else(|| std::path::PathBuf::from("."));
    base.join("ez-cdc").join("config.yaml")
}

#[derive(Subcommand)]
enum Commands {
    /// Run verification suite
    Verify {
        /// Source datasource name
        #[arg(long)]
        source: Option<String>,
        /// Sink datasource name
        #[arg(long)]
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
        #[arg(long)]
        source: Option<String>,
        /// Sink datasource name
        #[arg(long)]
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
    /// Clean target tables and source replication state (for tests)
    Clean {
        /// Source datasource name
        #[arg(long)]
        source: Option<String>,
        /// Sink datasource name
        #[arg(long)]
        sink: Option<String>,
        /// Skip confirmation prompt
        #[arg(long, short)]
        yes: bool,
    },
    /// Print a shell completion script to stdout.
    ///
    /// Redirect the output to wherever your shell looks for completion
    /// files. For zsh:
    ///
    ///     ez-cdc completions zsh > "${fpath[1]}/_ez-cdc"
    ///
    /// For bash:
    ///
    ///     ez-cdc completions bash > ~/.local/share/bash-completion/completions/ez-cdc
    ///
    /// For fish:
    ///
    ///     ez-cdc completions fish > ~/.config/fish/completions/ez-cdc.fish
    Completions {
        /// Target shell (bash, zsh, fish, powershell, elvish).
        shell: Shell,
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
    /// Create a starter ez-cdc.yaml. Default writes a fully-commented
    /// template; use `--template demo` to add the in-repo demo
    /// datasources instead.
    Init {
        /// Which template to write.
        #[arg(long, value_enum, default_value = "blank")]
        template: TemplateKind,
    },
}

#[derive(Clone, Debug, clap::ValueEnum)]
enum TemplateKind {
    /// Empty template with all dbmazz variables documented and
    /// commented examples for every supported source and sink type.
    Blank,
    /// In-repo demo datasources (postgres + starrocks/postgres target)
    /// used by the e2e test harness.
    Demo,
}

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;

    let cli = Cli::parse();
    let config_path = resolve_config_path(cli.config);

    // The `completions` subcommand prints a shell script to stdout that is
    // meant to be piped into a completion file. Banner / color / any extra
    // output corrupts the script, so short-circuit before rendering.
    if let Some(Commands::Completions { shell }) = cli.command {
        let mut cmd = Cli::command();
        let bin = cmd.get_name().to_string();
        clap_complete::generate(shell, &mut cmd, bin, &mut std::io::stdout());
        return Ok(());
    }

    match cli.command {
        Some(cmd) => {
            tui::banner::render_banner_compact();

            let result: anyhow::Result<()> = match cmd {
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
                    DsCommands::Init { template } => {
                        let kind = match template {
                            TemplateKind::Blank => commands::datasource::InitTemplate::Blank,
                            TemplateKind::Demo => commands::datasource::InitTemplate::Demo,
                        };
                        commands::datasource::run_ds_init(&config_path, kind)
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
                Commands::Completions { .. } => {
                    // handled above the match via early return
                    unreachable!("Completions should be handled before the banner is printed");
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
            println!(
                "  {} sources, {} sinks",
                n_src, n_sk,
            );
        }
        println!();

        // Build choices based on state.
        type Item = (String, String, String);
        let choices: Vec<Item> = if !config_exists || !has_datasources {
            vec![
                ("init".into(), "Init config".into(), "Create a blank ez-cdc.yaml with all options documented".into()),
                ("ds".into(), "Datasources".into(), "Add / list / remove source and sink configs".into()),
                ("exit".into(), "Exit".into(), String::new()),
            ]
        } else {
            vec![
                ("quickstart".into(), "Quickstart".into(), "Run a pipeline and open the live dashboard".into()),
                ("verify".into(), "Verify".into(), "Run the e2e verification suite".into()),
                ("status".into(), "Status".into(), "Query the running daemon for stage, LSN, and counts".into()),
                ("logs".into(), "Logs".into(), "Tail the dbmazz container logs".into()),
                ("clean".into(), "Clean".into(), "Drop replication state and truncate target tables (reset for a new run)".into()),
                ("ds".into(), "Datasources".into(), "Add / list / remove source and sink configs".into()),
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
                let r = commands::datasource::run_ds_init(
                    config_path,
                    commands::datasource::InitTemplate::Blank,
                );
                // After init, loop back to show the updated menu.
                if r.is_ok() {
                    println!();
                    wait_for_keypress();
                }
                r
            }
            "quickstart" => {
                commands::quickstart::run_quickstart(config_path, None, None, false, false).await
            }
            "verify" => {
                commands::verify_cmd::run_verify(
                    config_path, None, None, false, false, None, None, false, false, false,
                ).await
            }
            "status" => commands::status::run_status().await,
            "logs" => commands::logs::run_logs(config_path, Some("dbmazz".into()), true, 100),
            "clean" => commands::clean::run_clean(config_path, None, None, false).await,
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
        ("init".into(), "Init config".into(), "Create a blank ez-cdc.yaml with all options documented".into()),
    ];

    let ds_choice = match tui::prompts::select("Datasource management:", ds_choices) {
        Ok(c) => c,
        Err(_) => return Ok(()), // Ctrl+C → back to main menu
    };

    match ds_choice.as_str() {
        "list" => commands::datasource::run_ds_list(config_path)?,
        "add" => commands::datasource::run_ds_add(config_path)?,
        "init" => commands::datasource::run_ds_init(
            config_path,
            commands::datasource::InitTemplate::Blank,
        )?,
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
#[allow(dead_code)]
async fn run_anyhow<F>(fut: F) -> color_eyre::Result<()>
where
    F: std::future::Future<Output = anyhow::Result<()>>,
{
    fut.await.map_err(|e| color_eyre::eyre::eyre!("{e:#}"))
}
