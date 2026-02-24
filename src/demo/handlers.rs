use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::State,
    response::{
        sse::{Event, Sse},
        Html, IntoResponse,
    },
    http::StatusCode,
    Json,
};
use futures::stream::Stream;
use serde::Deserialize;
use serde_json::json;
use tracing::{error, info, warn};

use super::{DemoPhase, DemoState, SinkDemoConfig, SourceDemoConfig};
use super::traffic::TrafficGenerator;
use crate::config::{
    Config, PostgresSourceConfig, SinkConfig, SinkType, SourceConfig, SourceType,
    StarRocksSinkConfig,
};
use crate::engine::CdcEngine;
use crate::grpc::CdcState;

// ---------------------------------------------------------------------------
// Request / response types
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct TestConnectionRequest {
    #[serde(rename = "type")]
    db_type: String,
    host: String,
    #[serde(default)]
    port: Option<u16>,
    database: String,
    user: String,
    password: String,
    http_port: Option<u16>,
    mysql_port: Option<u16>,
}

#[derive(Deserialize)]
pub struct StartReplicationRequest {
    tables: Vec<String>,
    postgres: Option<PgConnFields>,
    starrocks: Option<SrConnFields>,
}

#[derive(Deserialize)]
struct PgConnFields {
    host: String,
    port: u16,
    database: String,
    user: String,
    password: String,
}

#[derive(Deserialize)]
struct SrConnFields {
    host: String,
    http_port: Option<u16>,
    mysql_port: Option<u16>,
    database: String,
    user: String,
    password: String,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

pub async fn index() -> impl IntoResponse {
    Html(include_str!("assets/index.html"))
}

pub async fn test_connection(
    State(state): State<Arc<DemoState>>,
    Json(req): Json<TestConnectionRequest>,
) -> impl IntoResponse {
    match req.db_type.as_str() {
        "postgres" => test_postgres(state, req).await,
        "starrocks" => test_starrocks(state, req).await,
        other => (
            StatusCode::BAD_REQUEST,
            Json(json!({"ok": false, "error": format!("Unknown type: {}", other)})),
        )
            .into_response(),
    }
}

async fn test_postgres(state: Arc<DemoState>, req: TestConnectionRequest) -> axum::response::Response {
    let port = req.port.unwrap_or(5432);
    let url = format!(
        "postgres://{}:{}@{}:{}/{}",
        req.user, req.password, req.host, port, req.database
    );

    let connect_fut = tokio_postgres::connect(&url, tokio_postgres::NoTls);
    let (client, connection) = match tokio::time::timeout(Duration::from_secs(5), connect_fut).await
    {
        Ok(Ok(conn)) => conn,
        Ok(Err(e)) => {
            return (
                StatusCode::OK,
                Json(json!({"ok": false, "error": e.to_string()})),
            )
                .into_response()
        }
        Err(_) => {
            return (
                StatusCode::OK,
                Json(json!({"ok": false, "error": "Connection timed out (5s)"})),
            )
                .into_response()
        }
    };

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            warn!("Demo PG connection error: {}", e);
        }
    });

    let version = match client.query_one("SELECT version()", &[]).await {
        Ok(row) => {
            let v: String = row.get(0);
            v
        }
        Err(e) => {
            return (
                StatusCode::OK,
                Json(json!({"ok": false, "error": e.to_string()})),
            )
                .into_response()
        }
    };

    let wal_level = match client.query_one("SHOW wal_level", &[]).await {
        Ok(row) => {
            let v: String = row.get(0);
            v
        }
        Err(e) => {
            return (
                StatusCode::OK,
                Json(json!({"ok": false, "error": format!("SHOW wal_level failed: {}", e)})),
            )
                .into_response()
        }
    };

    *state.source_config.write().await = Some(SourceDemoConfig {
        host: req.host,
        port,
        database: req.database,
        user: req.user,
        password: req.password,
    });

    (
        StatusCode::OK,
        Json(json!({"ok": true, "version": version, "wal_level": wal_level})),
    )
        .into_response()
}

async fn test_starrocks(
    state: Arc<DemoState>,
    req: TestConnectionRequest,
) -> axum::response::Response {
    let mysql_port = req.mysql_port.unwrap_or(9030);
    let http_port = req.http_port.unwrap_or(8030);

    // Connect WITHOUT database name — the database may not exist yet.
    // prefer_socket(false): StarRocks doesn't support the 'socket' MySQL variable.
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname(req.host.clone())
        .tcp_port(mysql_port)
        .user(Some(req.user.clone()))
        .pass(Some(req.password.clone()))
        .prefer_socket(false);

    let pool = mysql_async::Pool::new(opts);
    let conn_fut = pool.get_conn();

    let mut conn = match tokio::time::timeout(Duration::from_secs(5), conn_fut).await {
        Ok(Ok(c)) => c,
        Ok(Err(e)) => {
            let _ = pool.disconnect().await;
            return (
                StatusCode::OK,
                Json(json!({"ok": false, "error": e.to_string()})),
            )
                .into_response();
        }
        Err(_) => {
            let _ = pool.disconnect().await;
            return (
                StatusCode::OK,
                Json(json!({"ok": false, "error": "Connection timed out (5s)"})),
            )
                .into_response();
        }
    };

    use mysql_async::prelude::Queryable;
    let version: Option<String> = match conn.query_first("SELECT version()").await {
        Ok(v) => v,
        Err(e) => {
            drop(conn);
            let _ = pool.disconnect().await;
            return (
                StatusCode::OK,
                Json(json!({"ok": false, "error": e.to_string()})),
            )
                .into_response();
        }
    };

    drop(conn);
    let _ = pool.disconnect().await;

    *state.sink_config.write().await = Some(SinkDemoConfig {
        host: req.host,
        fe_http_port: http_port,
        fe_mysql_port: mysql_port,
        database: req.database,
        user: req.user,
        password: req.password,
    });

    (
        StatusCode::OK,
        Json(json!({"ok": true, "version": version.unwrap_or_default()})),
    )
        .into_response()
}

pub async fn discover_tables(State(state): State<Arc<DemoState>>) -> impl IntoResponse {
    let src = {
        let guard = state.source_config.read().await;
        match guard.as_ref() {
            Some(s) => s.clone(),
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(
                        json!({"ok": false, "error": "Source not configured. Test connection first."}),
                    ),
                )
                    .into_response()
            }
        }
    };

    let url = format!(
        "postgres://{}:{}@{}:{}/{}",
        src.user, src.password, src.host, src.port, src.database
    );

    let connect_fut = tokio_postgres::connect(&url, tokio_postgres::NoTls);
    let (client, connection) = match tokio::time::timeout(Duration::from_secs(5), connect_fut).await
    {
        Ok(Ok(c)) => c,
        Ok(Err(e)) => {
            return (
                StatusCode::OK,
                Json(json!({"ok": false, "error": e.to_string()})),
            )
                .into_response()
        }
        Err(_) => {
            return (
                StatusCode::OK,
                Json(json!({"ok": false, "error": "Connection timed out (5s)"})),
            )
                .into_response()
        }
    };

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            warn!("Demo PG discover connection error: {}", e);
        }
    });

    let query = r#"
        SELECT t.table_name,
               (SELECT count(*) FROM information_schema.columns c
                WHERE c.table_name = t.table_name AND c.table_schema = 'public') as column_count,
               CASE c.relreplident
                   WHEN 'f' THEN 'full'
                   WHEN 'd' THEN 'default'
                   WHEN 'n' THEN 'nothing'
                   WHEN 'i' THEN 'index'
               END as replica_identity
        FROM information_schema.tables t
        JOIN pg_class c ON t.table_name = c.relname
        JOIN pg_namespace n ON c.relnamespace = n.oid AND n.nspname = 'public'
        WHERE t.table_schema = 'public'
          AND t.table_type = 'BASE TABLE'
          AND t.table_name NOT LIKE 'dbmazz_%'
    "#;

    let rows = match client.query(query, &[]).await {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::OK,
                Json(json!({"ok": false, "error": e.to_string()})),
            )
                .into_response()
        }
    };

    let tables: Vec<serde_json::Value> = rows
        .iter()
        .map(|row| {
            let name: String = row.get(0);
            let columns: i64 = row.get(1);
            let replica_identity: Option<String> = row.get(2);
            json!({
                "schema": "public",
                "name": name,
                "column_count": columns,
                "replica_identity": replica_identity.unwrap_or_default(),
            })
        })
        .collect();

    (StatusCode::OK, Json(json!(tables))).into_response()
}

// ---------------------------------------------------------------------------
// StarRocks table auto-creation from PostgreSQL schema
// ---------------------------------------------------------------------------

/// Map PostgreSQL udt_name to StarRocks column type
fn pg_udt_to_starrocks(
    udt_name: &str,
    precision: Option<i32>,
    scale: Option<i32>,
    max_len: Option<i32>,
) -> String {
    match udt_name {
        "bool" => "BOOLEAN".to_string(),
        "int2" => "SMALLINT".to_string(),
        "int4" => "INT".to_string(),
        "int8" => "BIGINT".to_string(),
        "float4" => "FLOAT".to_string(),
        "float8" => "DOUBLE".to_string(),
        "numeric" => {
            if let (Some(p), Some(s)) = (precision, scale) {
                format!("DECIMAL({},{})", p.min(38), s)
            } else {
                "DECIMAL(38,9)".to_string()
            }
        }
        "varchar" | "bpchar" => {
            if let Some(len) = max_len {
                format!("VARCHAR({})", len)
            } else {
                "STRING".to_string()
            }
        }
        "text" => "STRING".to_string(),
        "timestamp" | "timestamptz" => "DATETIME".to_string(),
        "date" => "DATE".to_string(),
        "time" | "timetz" => "STRING".to_string(),
        "json" | "jsonb" => "JSON".to_string(),
        "uuid" => "STRING".to_string(),
        "bytea" => "VARBINARY".to_string(),
        _ => "STRING".to_string(),
    }
}

/// Ensure StarRocks database and tables exist by reading schema from PostgreSQL.
/// StarRocks allin1 Docker image does NOT auto-execute init scripts,
/// so we must create tables programmatically before starting the engine.
async fn ensure_starrocks_tables(
    src: &SourceDemoConfig,
    sink: &SinkDemoConfig,
    tables: &[String],
) -> Result<(), String> {
    // 1. Connect to PostgreSQL to read schema
    let pg_url = format!(
        "postgres://{}:{}@{}:{}/{}",
        src.user, src.password, src.host, src.port, src.database
    );
    let (pg_client, pg_conn) = tokio_postgres::connect(&pg_url, tokio_postgres::NoTls)
        .await
        .map_err(|e| format!("PG connect: {}", e))?;
    tokio::spawn(async move {
        let _ = pg_conn.await;
    });

    // 2. Connect to StarRocks (without database — it may not exist yet)
    // prefer_socket(false) is required: StarRocks speaks MySQL wire protocol
    // but doesn't support the 'socket' system variable that mysql_async sets.
    let sr_opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname(sink.host.clone())
        .tcp_port(sink.fe_mysql_port)
        .user(Some(sink.user.clone()))
        .pass(Some(sink.password.clone()))
        .prefer_socket(false);

    let sr_pool = mysql_async::Pool::new(sr_opts);
    let mut sr_conn = sr_pool
        .get_conn()
        .await
        .map_err(|e| format!("StarRocks connect: {}", e))?;

    use mysql_async::prelude::Queryable;

    // 3. Create database if not exists
    sr_conn
        .query_drop(format!("CREATE DATABASE IF NOT EXISTS `{}`", sink.database))
        .await
        .map_err(|e| format!("Create database: {}", e))?;

    sr_conn
        .query_drop(format!("USE `{}`", sink.database))
        .await
        .map_err(|e| format!("Use database: {}", e))?;

    // 4. For each selected table, create in StarRocks if it doesn't exist
    for table in tables {
        let exists: Option<i32> = sr_conn
            .exec_first(
                "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
                (&sink.database, table),
            )
            .await
            .map_err(|e| format!("Check table {}: {}", table, e))?;

        if exists.is_some() {
            info!("StarRocks table {} already exists, skipping", table);
            continue;
        }

        // Query PG for column definitions
        let columns = pg_client
            .query(
                "SELECT column_name, udt_name, numeric_precision, numeric_scale, character_maximum_length
                 FROM information_schema.columns
                 WHERE table_schema = 'public' AND table_name = $1
                 ORDER BY ordinal_position",
                &[&table],
            )
            .await
            .map_err(|e| format!("PG schema for {}: {}", table, e))?;

        if columns.is_empty() {
            return Err(format!("Table {} has no columns in PostgreSQL", table));
        }

        // Query PG for primary key columns
        let pk_rows = pg_client
            .query(
                "SELECT kcu.column_name
                 FROM information_schema.table_constraints tc
                 JOIN information_schema.key_column_usage kcu
                   ON tc.constraint_name = kcu.constraint_name
                  AND tc.table_schema = kcu.table_schema
                 WHERE tc.table_schema = 'public'
                   AND tc.table_name = $1
                   AND tc.constraint_type = 'PRIMARY KEY'
                 ORDER BY kcu.ordinal_position",
                &[&table],
            )
            .await
            .map_err(|e| format!("PG primary key for {}: {}", table, e))?;

        let pk_columns: Vec<String> = pk_rows.iter().map(|r| r.get::<_, String>(0)).collect();

        // Build column definitions
        let mut col_defs = Vec::new();
        for row in &columns {
            let col_name: String = row.get(0);
            let udt_name: String = row.get(1);
            let precision: Option<i32> = row.get(2);
            let scale: Option<i32> = row.get(3);
            let max_len: Option<i32> = row.get(4);

            let sr_type = pg_udt_to_starrocks(&udt_name, precision, scale, max_len);
            let not_null = if pk_columns.contains(&col_name) {
                " NOT NULL"
            } else {
                ""
            };
            col_defs.push(format!("    `{}` {}{}", col_name, sr_type, not_null));
        }

        // Add dbmazz audit columns
        col_defs.push("    dbmazz_op_type TINYINT".to_string());
        col_defs.push("    dbmazz_is_deleted BOOLEAN".to_string());
        col_defs.push("    dbmazz_synced_at DATETIME".to_string());
        col_defs.push("    dbmazz_cdc_version BIGINT".to_string());

        let pk_clause = if !pk_columns.is_empty() {
            let quoted: Vec<String> = pk_columns.iter().map(|c| format!("`{}`", c)).collect();
            format!("\nPRIMARY KEY ({})", quoted.join(", "))
        } else {
            String::new()
        };

        let dist_col = if !pk_columns.is_empty() {
            pk_columns[0].clone()
        } else {
            columns[0].get::<_, String>(0)
        };

        let ddl = format!(
            "CREATE TABLE IF NOT EXISTS `{}` (\n{}\n){}\nDISTRIBUTED BY HASH(`{}`)\nPROPERTIES (\"replication_num\" = \"1\")",
            table,
            col_defs.join(",\n"),
            pk_clause,
            dist_col,
        );

        info!("Creating StarRocks table: {}", table);
        sr_conn
            .query_drop(&ddl)
            .await
            .map_err(|e| format!("Create table {}: {} — DDL: {}", table, e, ddl))?;
        info!("  [OK] Created StarRocks table: {}", table);
    }

    drop(sr_conn);
    let _ = sr_pool.disconnect().await;
    Ok(())
}

pub async fn start_replication(
    State(state): State<Arc<DemoState>>,
    Json(req): Json<StartReplicationRequest>,
) -> impl IntoResponse {
    // Store selected tables
    *state.selected_tables.write().await = req.tables.clone();

    // Use saved state or fall back to inline connection fields from request body
    let src = {
        let guard = state.source_config.read().await;
        match guard.as_ref() {
            Some(s) => s.clone(),
            None => match req.postgres {
                Some(pg) => {
                    let cfg = SourceDemoConfig {
                        host: pg.host,
                        port: pg.port,
                        database: pg.database,
                        user: pg.user,
                        password: pg.password,
                    };
                    drop(guard);
                    *state.source_config.write().await = Some(cfg.clone());
                    cfg
                }
                None => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(json!({"ok": false, "error": "Source not configured. Test connection first."})),
                    )
                }
            },
        }
    };

    let sink = {
        let guard = state.sink_config.read().await;
        match guard.as_ref() {
            Some(s) => s.clone(),
            None => match req.starrocks {
                Some(sr) => {
                    let cfg = SinkDemoConfig {
                        host: sr.host,
                        fe_http_port: sr.http_port.unwrap_or(8030),
                        fe_mysql_port: sr.mysql_port.unwrap_or(9030),
                        database: sr.database,
                        user: sr.user,
                        password: sr.password,
                    };
                    drop(guard);
                    *state.sink_config.write().await = Some(cfg.clone());
                    cfg
                }
                None => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(json!({"ok": false, "error": "Sink not configured. Test connection first."})),
                    )
                }
            },
        }
    };

    // Ensure StarRocks database and tables exist (auto-create from PG schema)
    if let Err(e) = ensure_starrocks_tables(&src, &sink, &req.tables).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"ok": false, "error": format!("Failed to prepare StarRocks tables: {}", e)})),
        );
    }

    let database_url = format!(
        "postgres://{}:{}@{}:{}/{}?replication=database",
        src.user, src.password, src.host, src.port, src.database
    );
    let slot_name = "dbmazz_demo_slot".to_string();
    let publication_name = "dbmazz_demo_pub".to_string();
    let starrocks_url = format!("http://{}:{}", sink.host, sink.fe_http_port);
    let tables = req.tables.clone();

    let source_config = SourceConfig {
        source_type: SourceType::Postgres,
        url: database_url.clone(),
        tables: tables.clone(),
        postgres: Some(PostgresSourceConfig {
            slot_name: slot_name.clone(),
            publication_name: publication_name.clone(),
        }),
    };

    let sink_config = SinkConfig {
        sink_type: SinkType::StarRocks,
        url: starrocks_url.clone(),
        port: sink.fe_mysql_port,
        database: sink.database.clone(),
        user: sink.user.clone(),
        password: sink.password.clone(),
        starrocks: Some(StarRocksSinkConfig {}),
    };

    let config = Config {
        source: source_config,
        sink: sink_config,
        database_url,
        slot_name,
        publication_name,
        tables,
        starrocks_url,
        starrocks_port: sink.fe_mysql_port,
        starrocks_db: sink.database,
        starrocks_user: sink.user,
        starrocks_pass: sink.password,
        flush_size: 2000,
        flush_interval_ms: 2000,
        grpc_port: 50051,
    };

    let engine = CdcEngine::new(config);
    let shared = engine.shared_state();

    *state.engine_state.write().await = Some(shared);
    *state.phase.write().await = DemoPhase::Replicating;

    let state_for_engine = state.clone();
    tokio::spawn(async move {
        if let Err(e) = engine.run().await {
            error!("CDC engine error: {}", e);
            *state_for_engine.phase.write().await = DemoPhase::Stopped;
            *state_for_engine.engine_state.write().await = None;
        }
    });

    (StatusCode::OK, Json(json!({"ok": true})))
}

pub async fn stop_replication(State(state): State<Arc<DemoState>>) -> impl IntoResponse {
    // Stop traffic first if running
    {
        let traffic = state.traffic_handle.read().await;
        if let Some(ref t) = *traffic {
            t.stop();
        }
    }
    *state.traffic_handle.write().await = None;

    // Stop engine
    {
        let engine = state.engine_state.read().await;
        if let Some(ref shared) = *engine {
            shared.set_skip_slot_cleanup(true);
            shared.set_state(CdcState::Stopped);
            let _ = shared.shutdown_tx.send(true);
        }
    }
    *state.engine_state.write().await = None;
    *state.phase.write().await = DemoPhase::Stopped;

    (StatusCode::OK, Json(json!({"ok": true, "state": "stopped"})))
}

pub async fn pause_replication(State(state): State<Arc<DemoState>>) -> impl IntoResponse {
    let engine = state.engine_state.read().await;
    match engine.as_ref() {
        Some(shared) => {
            shared.set_state(CdcState::Paused);
            (StatusCode::OK, Json(json!({"ok": true, "state": "paused"})))
        }
        None => (
            StatusCode::BAD_REQUEST,
            Json(json!({"ok": false, "error": "No active replication"})),
        ),
    }
}

pub async fn resume_replication(State(state): State<Arc<DemoState>>) -> impl IntoResponse {
    let engine = state.engine_state.read().await;
    match engine.as_ref() {
        Some(shared) => {
            shared.set_state(CdcState::Running);
            (StatusCode::OK, Json(json!({"ok": true, "state": "running"})))
        }
        None => (
            StatusCode::BAD_REQUEST,
            Json(json!({"ok": false, "error": "No active replication"})),
        ),
    }
}

pub async fn start_traffic(State(state): State<Arc<DemoState>>) -> impl IntoResponse {
    let src = {
        let guard = state.source_config.read().await;
        match guard.as_ref() {
            Some(s) => s.clone(),
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"ok": false, "error": "Source not configured"})),
                )
            }
        }
    };

    let url = format!(
        "postgres://{}:{}@{}:{}/{}",
        src.user, src.password, src.host, src.port, src.database
    );

    let generator = Arc::new(TrafficGenerator::new(url));
    *state.traffic_handle.write().await = Some(generator.clone());

    tokio::spawn(async move {
        generator.run().await;
    });

    (StatusCode::OK, Json(json!({"ok": true})))
}

pub async fn stop_traffic(State(state): State<Arc<DemoState>>) -> impl IntoResponse {
    {
        let traffic = state.traffic_handle.read().await;
        if let Some(ref t) = *traffic {
            t.stop();
        }
    }
    *state.traffic_handle.write().await = None;

    (StatusCode::OK, Json(json!({"ok": true})))
}

pub async fn status(State(state): State<Arc<DemoState>>) -> impl IntoResponse {
    let phase = *state.phase.read().await;
    let phase_str = match phase {
        DemoPhase::Setup => "setup",
        DemoPhase::Replicating => "replicating",
        DemoPhase::Stopped => "stopped",
    };

    let engine = state.engine_state.read().await;
    if let Some(ref shared) = *engine {
        let traffic = state.traffic_handle.read().await;
        let traffic_running = traffic.as_ref().map(|t| t.is_running()).unwrap_or(false);

        Json(json!({
            "phase": phase_str,
            "events_processed": shared.events_processed(),
            "events_per_second": shared.events_last_second(),
            "replication_lag_ms": shared.replication_lag_ms(),
            "batches_sent": shared.batches_sent(),
            "pending_events": shared.pending_events(),
            "traffic_running": traffic_running,
        }))
    } else {
        Json(json!({
            "phase": phase_str,
            "events_processed": 0,
            "events_per_second": 0,
            "replication_lag_ms": 0,
            "batches_sent": 0,
            "pending_events": 0,
            "traffic_running": false,
        }))
    }
}

// ---------------------------------------------------------------------------
// SSE metrics stream
// ---------------------------------------------------------------------------

struct SseState {
    demo: Arc<DemoState>,
    interval: tokio::time::Interval,
    event_rx: Option<tokio::sync::broadcast::Receiver<String>>,
    pending_cdc_events: Vec<String>,
}

pub async fn metrics_stream(
    State(state): State<Arc<DemoState>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let sse_state = SseState {
        demo: state,
        interval,
        event_rx: None,
        pending_cdc_events: Vec::new(),
    };

    let stream = futures::stream::unfold(sse_state, |mut s| async move {
        // Drain any pending CDC events first
        if let Some(event_str) = s.pending_cdc_events.pop() {
            let parts: Vec<&str> = event_str.splitn(2, ':').collect();
            if parts.len() == 2 {
                let json = json!({
                    "op": parts[0],
                    "table": parts[1],
                    "ts": chrono::Utc::now().timestamp_millis(),
                });
                let event = Event::default().event("cdc_event").data(json.to_string());
                return Some((Ok(event), s));
            }
        }

        // Wait for next tick
        s.interval.tick().await;

        // Try to subscribe to CDC event broadcast channel
        if s.event_rx.is_none() {
            let engine = s.demo.engine_state.read().await;
            if let Some(ref shared) = *engine {
                s.event_rx = Some(shared.demo_event_tx.subscribe());
            }
            drop(engine);
        }

        // Drain CDC events (non-blocking)
        if let Some(ref mut rx) = s.event_rx {
            while let Ok(ev) = rx.try_recv() {
                s.pending_cdc_events.push(ev);
            }
        }

        // Build metrics event — scope the read guards so they drop before we return `s`
        let event = {
            let engine = s.demo.engine_state.read().await;
            if let Some(ref shared) = *engine {
                let total = shared.events_processed();
                let eps = shared.events_last_second();
                let lag = shared.replication_lag_ms();
                let batches = shared.batches_sent();
                let pending = shared.pending_events();

                let phase = s.demo.phase.read().await;
                let phase_str = match *phase {
                    DemoPhase::Setup => "setup",
                    DemoPhase::Replicating => "replicating",
                    DemoPhase::Stopped => "stopped",
                };

                let traffic = s.demo.traffic_handle.read().await;
                let traffic_running = traffic.as_ref().map(|t| t.is_running()).unwrap_or(false);

                let json = json!({
                    "events_per_second": eps,
                    "events_processed": total,
                    "replication_lag_ms": lag,
                    "batches_sent": batches,
                    "pending_events": pending,
                    "phase": phase_str,
                    "traffic_running": traffic_running,
                });
                Event::default().event("metrics").data(json.to_string())
            } else {
                let json = json!({
                    "events_per_second": 0,
                    "events_processed": 0,
                    "replication_lag_ms": 0,
                    "batches_sent": 0,
                    "pending_events": 0,
                    "phase": "setup",
                    "traffic_running": false,
                });
                Event::default().event("metrics").data(json.to_string())
            }
        };

        Some((Ok(event), s))
    });

    Sse::new(stream)
}
