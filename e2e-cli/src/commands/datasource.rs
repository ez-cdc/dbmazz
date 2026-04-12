use std::path::Path;

use comfy_table::{Cell, Color, Table};
use console::style;

use crate::commands::{load_store, load_store_or_empty, redact_url_password};
use crate::config::presets::merge_demos_into;
use crate::config::schema::*;
use crate::instantiate::{instantiate_backend, instantiate_source};
use crate::tui::prompts;

/// `ez-cdc datasource list`
pub fn run_ds_list(config_path: &Path) -> anyhow::Result<()> {
    let mut store = load_store(config_path)?;
    let data = store.data()?.clone();

    if data.is_empty() {
        println!("  No datasources configured.");
        println!(
            "  Edit {} or run {} for a wizard.",
            style(config_path.display()).bold(),
            style("ez-cdc datasource add").cyan()
        );
        return Ok(());
    }

    let mut table = Table::new();
    table.set_header(vec![
        Cell::new("Role").fg(Color::Cyan),
        Cell::new("Name").fg(Color::Cyan),
        Cell::new("Type").fg(Color::Cyan),
        Cell::new("Connection").fg(Color::Cyan),
    ]);

    // Sources.
    let mut src_names = data.list_source_names();
    src_names.sort();
    for name in &src_names {
        if let Ok(spec) = data.get_source(name) {
            let (type_name, url) = match spec {
                SourceSpec::Postgres(inner) => ("postgres", redact_url_password(&inner.url)),
            };
            table.add_row(vec![
                Cell::new("source").fg(Color::Green),
                Cell::new(name),
                Cell::new(type_name),
                Cell::new(url),
            ]);
        }
    }

    // Sinks.
    let mut sk_names = data.list_sink_names();
    sk_names.sort();
    for name in &sk_names {
        if let Ok(spec) = data.get_sink(name) {
            let (type_name, url) = match spec {
                SinkSpec::Postgres(inner) => ("postgres", redact_url_password(&inner.url)),
                SinkSpec::Starrocks(inner) => ("starrocks", redact_url_password(&inner.url)),
                SinkSpec::Snowflake(inner) => ("snowflake", inner.account.clone()),
            };
            table.add_row(vec![
                Cell::new("sink").fg(Color::Magenta),
                Cell::new(name),
                Cell::new(type_name),
                Cell::new(url),
            ]);
        }
    }

    println!();
    println!("{table}");
    println!();
    Ok(())
}

/// `ez-cdc datasource show <name>`
pub fn run_ds_show(config_path: &Path, name: &str, reveal: bool) -> anyhow::Result<()> {
    let mut store = load_store(config_path)?;
    let data = store.data()?.clone();

    // Try source first, then sink.
    if let Ok(spec) = data.get_source(name) {
        println!();
        println!(
            "  {} {}  (source)",
            style("Name:").bold(),
            style(name).cyan()
        );
        match spec {
            SourceSpec::Postgres(inner) => {
                let url = if reveal {
                    inner.url.clone()
                } else {
                    redact_url_password(&inner.url)
                };
                println!("  Type:              postgres");
                println!("  URL:               {url}");
                if let Some(seed) = &inner.seed {
                    println!("  Seed:              {seed}");
                }
                println!("  Replication slot:  {}", inner.replication_slot);
                println!("  Publication:       {}", inner.publication);
                println!("  Tables:            {}", inner.tables.join(", "));
            }
        }
        println!();
        return Ok(());
    }

    if let Ok(spec) = data.get_sink(name) {
        println!();
        println!(
            "  {} {}  (sink)",
            style("Name:").bold(),
            style(name).cyan()
        );
        match spec {
            SinkSpec::Postgres(inner) => {
                let url = if reveal {
                    inner.url.clone()
                } else {
                    redact_url_password(&inner.url)
                };
                println!("  Type:     postgres");
                println!("  URL:      {url}");
                println!("  Database: {}", inner.database);
                println!("  Schema:   {}", inner.schema_name);
            }
            SinkSpec::Starrocks(inner) => {
                let url = if reveal {
                    inner.url.clone()
                } else {
                    redact_url_password(&inner.url)
                };
                println!("  Type:       starrocks");
                println!("  URL:        {url}");
                println!("  MySQL port: {}", inner.mysql_port);
                println!("  Database:   {}", inner.database);
                println!("  User:       {}", inner.user);
                println!(
                    "  Password:   {}",
                    if reveal {
                        &inner.password
                    } else {
                        "***"
                    }
                );
            }
            SinkSpec::Snowflake(inner) => {
                println!("  Type:       snowflake");
                println!("  Account:    {}", inner.account);
                println!("  User:       {}", inner.user);
                println!(
                    "  Password:   {}",
                    if reveal {
                        &inner.password
                    } else {
                        "***"
                    }
                );
                println!("  Database:   {}", inner.database);
                println!("  Schema:     {}", inner.schema_name);
                println!("  Warehouse:  {}", inner.warehouse);
                if let Some(role) = &inner.role {
                    println!("  Role:       {role}");
                }
                if let Some(key_path) = &inner.private_key_path {
                    println!("  Key path:   {key_path}");
                }
                println!(
                    "  Soft delete: {}",
                    if inner.soft_delete { "yes" } else { "no" }
                );
            }
        }
        println!();
        return Ok(());
    }

    anyhow::bail!("datasource '{}' not found", name);
}

/// `ez-cdc datasource add` — interactive wizard
pub fn run_ds_add(config_path: &Path) -> anyhow::Result<()> {
    use crate::config::schema::*;

    let mut store = load_store_or_empty(config_path)?;

    println!();
    println!(
        "  {} Add a new datasource. Press Ctrl+C to cancel.",
        style("ℹ").blue()
    );
    println!();

    // Step 1: name
    let name = loop {
        let raw = match prompts::text("Name (lowercase, hyphens/underscores ok):", "") {
            Ok(v) => v,
            Err(_) => { println!("  Cancelled."); return Ok(()); }
        };
        let name = raw.trim().to_lowercase();
        if name.is_empty() { continue; }
        if validate_datasource_name(&name).is_err() {
            println!("  {} Invalid name. Use lowercase letters, digits, hyphens, underscores (1-64 chars).", style("✗").red());
            continue;
        }
        if store.exists(&name)? {
            println!("  {} Name '{}' already exists. Pick a different name.", style("✗").red(), name);
            continue;
        }
        break name;
    };

    // Step 2: role
    let role = match prompts::select("What is this datasource for?", vec![
        ("source".to_string(), "Source".to_string(), "A database to replicate FROM (CDC source)".to_string()),
        ("sink".to_string(), "Sink".to_string(), "A database to replicate TO (CDC target)".to_string()),
    ]) {
        Ok(v) => v,
        Err(_) => { println!("  Cancelled."); return Ok(()); }
    };

    // Step 3: type
    let ds_type = if role == "source" {
        "postgres".to_string() // only PG sources today
    } else {
        match prompts::select("What kind of sink?", vec![
            ("postgres".to_string(), "PostgreSQL".to_string(), String::new()),
            ("starrocks".to_string(), "StarRocks".to_string(), String::new()),
            ("snowflake".to_string(), "Snowflake".to_string(), "(cloud-only, requires credentials)".to_string()),
        ]) {
            Ok(v) => v,
            Err(_) => { println!("  Cancelled."); return Ok(()); }
        }
    };

    // Step 4: type-specific prompts
    if role == "source" {
        let spec = match build_pg_source_wizard() {
            Ok(s) => s,
            Err(_) => { println!("  Cancelled."); return Ok(()); }
        };
        store.add_source(name.clone(), spec, false)?;
    } else {
        let spec = match ds_type.as_str() {
            "postgres" => build_pg_sink_wizard().map(SinkSpec::Postgres),
            "starrocks" => build_sr_sink_wizard().map(SinkSpec::Starrocks),
            "snowflake" => build_sf_sink_wizard().map(SinkSpec::Snowflake),
            _ => unreachable!(),
        };
        match spec {
            Ok(s) => store.add_sink(name.clone(), s, false)?,
            Err(_) => { println!("  Cancelled."); return Ok(()); }
        }
    }

    store.save()?;
    println!();
    println!(
        "  {} Datasource '{}' saved to {}",
        style("✓").green().bold(),
        name,
        config_path.display(),
    );
    println!();
    Ok(())
}

fn build_pg_source_wizard() -> Result<SourceSpec, prompts::PromptError> {
    use crate::config::schema::*;

    println!();
    println!("  {} PostgreSQL source — you'll need:", style("ℹ").blue());
    println!("  {}  A connection URL with replication permission", style("•").dim());
    println!("  {}  wal_level=logical on the database", style("•").dim());
    println!();

    let url = prompts::text(
        "Connection URL (postgres://user:pass@host:port/dbname):",
        "",
    )?;
    if url.trim().is_empty() { return Err(prompts::PromptError::Cancelled); }

    let slot = prompts::text("Replication slot name:", "dbmazz_slot")?;
    let publication = prompts::text("Publication name:", "dbmazz_pub")?;

    let tables_raw = prompts::text("Tables to replicate (comma-separated):", "")?;
    let tables: Vec<String> = tables_raw.split(',').map(|t| t.trim().to_string()).filter(|t| !t.is_empty()).collect();
    if tables.is_empty() {
        println!("  {} At least one table is required.", style("✗").red());
        return Err(prompts::PromptError::Cancelled);
    }

    Ok(SourceSpec::Postgres(PostgresSourceInner {
        url: url.trim().to_string(),
        seed: None,
        replication_slot: if slot.trim().is_empty() { "dbmazz_slot".into() } else { slot.trim().to_string() },
        publication: if publication.trim().is_empty() { "dbmazz_pub".into() } else { publication.trim().to_string() },
        tables,
    }))
}

fn build_pg_sink_wizard() -> Result<PostgresSinkInner, prompts::PromptError> {
    use crate::config::schema::*;

    println!();
    println!("  {} PostgreSQL sink.", style("ℹ").blue());
    println!();

    let url = prompts::text("Connection URL (postgres://user:pass@host:port/dbname):", "")?;
    if url.trim().is_empty() { return Err(prompts::PromptError::Cancelled); }

    let database = prompts::text("Target database name:", "")?;
    if database.trim().is_empty() { return Err(prompts::PromptError::Cancelled); }

    let schema = prompts::text("Target schema:", "public")?;

    Ok(PostgresSinkInner {
        url: url.trim().to_string(),
        database: database.trim().to_string(),
        schema_name: if schema.trim().is_empty() { "public".into() } else { schema.trim().to_string() },
    })
}

fn build_sr_sink_wizard() -> Result<StarRocksSinkInner, prompts::PromptError> {
    use crate::config::schema::*;

    println!();
    println!("  {} StarRocks sink.", style("ℹ").blue());
    println!();

    let url = prompts::text("FE HTTP URL (e.g., http://starrocks.local:8030):", "")?;
    if url.trim().is_empty() { return Err(prompts::PromptError::Cancelled); }

    let database = prompts::text("Target database name:", "")?;
    if database.trim().is_empty() { return Err(prompts::PromptError::Cancelled); }

    let user = prompts::text("User:", "root")?;
    let password = prompts::password("Password (leave empty if none):")?;

    Ok(StarRocksSinkInner {
        url: url.trim().to_string(),
        mysql_port: 9030,
        database: database.trim().to_string(),
        user: if user.trim().is_empty() { "root".into() } else { user.trim().to_string() },
        password,
    })
}

fn build_sf_sink_wizard() -> Result<SnowflakeSinkInner, prompts::PromptError> {
    use crate::config::schema::*;

    println!();
    println!("  {} Snowflake sink (cloud-only, requires credentials).", style("ℹ").blue());
    println!();

    let account = prompts::text("Account identifier (e.g., xy12345.us-east-1):", "")?;
    if account.trim().is_empty() { return Err(prompts::PromptError::Cancelled); }

    let user = prompts::text("User:", "")?;
    if user.trim().is_empty() { return Err(prompts::PromptError::Cancelled); }

    let password = prompts::password("Password:")?;

    let database = prompts::text("Database:", "")?;
    if database.trim().is_empty() { return Err(prompts::PromptError::Cancelled); }

    let schema = prompts::text("Schema:", "PUBLIC")?;
    let warehouse = prompts::text("Warehouse:", "")?;
    if warehouse.trim().is_empty() { return Err(prompts::PromptError::Cancelled); }

    let role = prompts::text("Role (leave empty for default):", "")?;

    Ok(SnowflakeSinkInner {
        account: account.trim().to_string(),
        user: user.trim().to_string(),
        password,
        database: database.trim().to_string(),
        schema_name: if schema.trim().is_empty() { "PUBLIC".into() } else { schema.trim().to_string() },
        warehouse: warehouse.trim().to_string(),
        role: if role.trim().is_empty() { None } else { Some(role.trim().to_string()) },
        private_key_path: None,
        soft_delete: true,
    })
}

/// `ez-cdc datasource test <name>`
pub async fn run_ds_test(config_path: &Path, name: &str) -> anyhow::Result<()> {
    let mut store = load_store(config_path)?;
    let data = store.data()?.clone();

    // Try source first.
    if let Ok(spec) = data.get_source(name) {
        println!(
            "  Testing source '{}' ({})...",
            style(name).cyan(),
            "postgres"
        );
        let mut source = instantiate_source(spec)?;
        match source.connect().await {
            Ok(()) => {
                println!(
                    "  {} Connection successful!",
                    style("\u{2713}").green().bold()
                );
                let _ = source.close().await;
            }
            Err(e) => {
                println!(
                    "  {} Connection failed: {}",
                    style("\u{2717}").red().bold(),
                    e
                );
            }
        }
        println!();
        return Ok(());
    }

    // Try sink.
    if let Ok(spec) = data.get_sink(name) {
        let type_name = spec.sink_type_name();
        println!(
            "  Testing sink '{}' ({})...",
            style(name).cyan(),
            type_name
        );
        let mut target = instantiate_backend(spec)?;
        match target.connect().await {
            Ok(()) => {
                println!(
                    "  {} Connection successful!",
                    style("\u{2713}").green().bold()
                );
                let _ = target.close().await;
            }
            Err(e) => {
                println!(
                    "  {} Connection failed: {}",
                    style("\u{2717}").red().bold(),
                    e
                );
            }
        }
        println!();
        return Ok(());
    }

    anyhow::bail!("datasource '{}' not found", name);
}

/// `ez-cdc datasource remove <name>`
pub fn run_ds_remove(config_path: &Path, name: &str, yes: bool) -> anyhow::Result<()> {
    let mut store = load_store(config_path)?;

    if !store.exists(name)? {
        anyhow::bail!("datasource '{}' not found", name);
    }

    if !yes {
        if !prompts::is_interactive() {
            anyhow::bail!("use --yes to skip confirmation in non-interactive mode");
        }
        let confirmed = prompts::confirm(
            &format!("Remove datasource '{name}'?"),
            false,
        )?;
        if !confirmed {
            println!("  Cancelled.");
            return Ok(());
        }
    }

    store.remove(name)?;
    store.save()?;

    println!(
        "  {} Removed datasource '{}'.",
        style("\u{2713}").green().bold(),
        name,
    );
    println!();
    Ok(())
}

/// `ez-cdc datasource init`
/// Which template `ez-cdc datasource init` should write.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InitTemplate {
    /// Empty template with every dbmazz variable documented inline.
    Blank,
    /// In-repo demo datasources (used by the e2e test harness).
    Demo,
}

/// The embedded blank template written by `datasource init --template blank`.
/// Shipped inside the binary via `include_str!` so the CLI is self-contained
/// even when installed from a GitHub release.
const BLANK_TEMPLATE: &str = include_str!("../../templates/config-blank.yaml");

fn write_blank_template(config_path: &Path) -> anyhow::Result<()> {
    if config_path.exists() {
        anyhow::bail!(
            "config file already exists at {}\n  \
             refusing to overwrite. Edit it by hand or run \
             `ez-cdc datasource add` to extend it.",
            config_path.display()
        );
    }
    if let Some(parent) = config_path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).map_err(|e| {
                anyhow::anyhow!("failed to create config dir {}: {e}", parent.display())
            })?;
        }
    }
    std::fs::write(config_path, BLANK_TEMPLATE).map_err(|e| {
        anyhow::anyhow!("failed to write template to {}: {e}", config_path.display())
    })?;
    Ok(())
}

pub fn run_ds_init(config_path: &Path, template: InitTemplate) -> anyhow::Result<()> {
    use crate::config::presets::DemoSink;

    if template == InitTemplate::Blank {
        write_blank_template(config_path)?;
        println!();
        println!(
            "  {} Config written to {}",
            style("✓").green().bold(),
            style(config_path.display()).bold()
        );
        println!();
        println!("  Next steps:");
        println!(
            "    {} Edit the file by hand, or",
            style("→").dim()
        );
        println!(
            "    {} Run {} for an interactive wizard",
            style("→").dim(),
            style("ez-cdc datasource add").cyan()
        );
        println!();
        return Ok(());
    }

    let mut store = load_store_or_empty(config_path)?;

    // Ask which sink to demo with.
    let sink_choice = if prompts::is_interactive() {
        match prompts::select(
            "Which sink for the demo?",
            vec![
                ("starrocks".to_string(), "StarRocks".to_string(), "Stream Load HTTP API (recommended for demo)".to_string()),
                ("postgres".to_string(), "PostgreSQL".to_string(), "COPY + MERGE to a target PG".to_string()),
            ],
        ) {
            Ok(v) => v,
            Err(_) => {
                println!("  Cancelled.");
                return Ok(());
            }
        }
    } else {
        "starrocks".to_string() // default for non-interactive
    };

    let demo_sink = match sink_choice.as_str() {
        "postgres" => DemoSink::Postgres,
        _ => DemoSink::StarRocks,
    };

    let (added_sources, added_sinks) = merge_demos_into(&mut store, demo_sink)?;
    store.save()?;

    if added_sources.is_empty() && added_sinks.is_empty() {
        println!(
            "  {} Demo datasources already present.",
            style("✓").dim()
        );
    } else {
        if !added_sources.is_empty() {
            println!(
                "  {} Added sources: {}",
                style("+").green().bold(),
                added_sources.join(", ")
            );
        }
        if !added_sinks.is_empty() {
            println!(
                "  {} Added sinks: {}",
                style("+").green().bold(),
                added_sinks.join(", ")
            );
        }
        println!();
        println!(
            "  Config written to {}",
            style(config_path.display()).bold()
        );
    }

    println!();
    Ok(())
}
