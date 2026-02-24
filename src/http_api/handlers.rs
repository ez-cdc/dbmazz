use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::State,
    http::{header, StatusCode},
    response::{Html, IntoResponse},
    Json,
};
use serde::Deserialize;
use serde_json::json;
use tracing::{error, info, warn};

use super::{HttpAppState, SinkSetupConfig, SourceSetupConfig};
use crate::config::{
    Config, PostgresSourceConfig, SinkConfig, SinkType, SourceConfig, SourceType,
    StarRocksSinkConfig,
};
use crate::engine::CdcEngine;
use crate::grpc::state::{CdcState, Stage};

// =============================================================================
// Request types
// =============================================================================

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
}

// =============================================================================
// Dashboard + status handlers
// =============================================================================

pub async fn index() -> impl IntoResponse {
    Html(include_str!("assets/status.html"))
}

pub async fn healthz(State(state): State<Arc<HttpAppState>>) -> impl IntoResponse {
    let engine = state.engine_state.read().await;
    let stage_str = if let Some(ref shared) = *engine {
        let (stage, _) = shared.stage().await;
        match stage {
            Stage::Init => "init",
            Stage::Setup => "setup",
            Stage::Cdc => "cdc",
        }
    } else {
        "waiting_for_config"
    };

    let uptime_secs = state.start_time.elapsed().as_secs();

    Json(json!({
        "status": "ok",
        "stage": stage_str,
        "uptime_secs": uptime_secs,
    }))
}

pub async fn status(State(state): State<Arc<HttpAppState>>) -> impl IntoResponse {
    let engine = state.engine_state.read().await;
    let uptime_secs = state.start_time.elapsed().as_secs();

    if let Some(ref s) = *engine {
        let (stage, detail) = s.stage().await;
        let stage_str = match stage {
            Stage::Init => "init",
            Stage::Setup => "setup",
            Stage::Cdc => "cdc",
        };
        let cdc_state = s.state();
        let state_str = match cdc_state {
            CdcState::Running => "running",
            CdcState::Paused => "paused",
            CdcState::Draining => "draining",
            CdcState::Stopped => "stopped",
        };
        let eps = s.events_last_second.load(Ordering::Relaxed);

        Json(json!({
            "engine_running": true,
            "state": state_str,
            "stage": stage_str,
            "stage_detail": detail,
            "uptime_secs": uptime_secs,
            "events_processed": s.events_processed(),
            "events_per_second": eps,
            "replication_lag_ms": s.replication_lag_ms(),
            "batches_sent": s.batches_sent(),
            "pending_events": s.pending_events(),
            "current_lsn": format!("0x{:X}", s.current_lsn()),
            "confirmed_lsn": format!("0x{:X}", s.confirmed_lsn()),
            "estimated_memory_bytes": s.estimate_memory(),
        }))
    } else {
        Json(json!({
            "engine_running": false,
            "state": "not_started",
            "stage": "waiting_for_config",
            "uptime_secs": uptime_secs,
        }))
    }
}

pub async fn prometheus(State(state): State<Arc<HttpAppState>>) -> impl IntoResponse {
    let engine = state.engine_state.read().await;
    let body = if let Some(ref s) = *engine {
        let eps = s.events_last_second.load(Ordering::Relaxed);
        format!(
            "# HELP dbmazz_events_processed_total Total CDC events processed.\n\
             # TYPE dbmazz_events_processed_total counter\n\
             dbmazz_events_processed_total {}\n\
             # HELP dbmazz_events_per_second Current event throughput.\n\
             # TYPE dbmazz_events_per_second gauge\n\
             dbmazz_events_per_second {}\n\
             # HELP dbmazz_replication_lag_ms Replication lag in milliseconds.\n\
             # TYPE dbmazz_replication_lag_ms gauge\n\
             dbmazz_replication_lag_ms {}\n\
             # HELP dbmazz_batches_sent_total Total batches sent to sink.\n\
             # TYPE dbmazz_batches_sent_total counter\n\
             dbmazz_batches_sent_total {}\n\
             # HELP dbmazz_pending_events Events buffered in pipeline.\n\
             # TYPE dbmazz_pending_events gauge\n\
             dbmazz_pending_events {}\n",
            s.events_processed(),
            eps,
            s.replication_lag_ms(),
            s.batches_sent(),
            s.pending_events(),
        )
    } else {
        "# dbmazz engine not running\n".to_string()
    };

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
}

pub async fn pause(State(state): State<Arc<HttpAppState>>) -> impl IntoResponse {
    let engine = state.engine_state.read().await;
    match engine.as_ref() {
        Some(shared) => {
            shared.set_state(CdcState::Paused);
            (StatusCode::OK, Json(json!({"ok": true, "state": "paused"}))).into_response()
        }
        None => (
            StatusCode::BAD_REQUEST,
            Json(json!({"ok": false, "error": "No active replication"})),
        )
            .into_response(),
    }
}

pub async fn resume(State(state): State<Arc<HttpAppState>>) -> impl IntoResponse {
    let engine = state.engine_state.read().await;
    match engine.as_ref() {
        Some(shared) => {
            shared.set_state(CdcState::Running);
            (StatusCode::OK, Json(json!({"ok": true, "state": "running"}))).into_response()
        }
        None => (
            StatusCode::BAD_REQUEST,
            Json(json!({"ok": false, "error": "No active replication"})),
        )
            .into_response(),
    }
}

pub async fn drain_stop(State(state): State<Arc<HttpAppState>>) -> impl IntoResponse {
    let engine = state.engine_state.read().await;
    match engine.as_ref() {
        Some(shared) => {
            shared.set_state(CdcState::Draining);
            (StatusCode::OK, Json(json!({"ok": true, "state": "draining"}))).into_response()
        }
        None => (
            StatusCode::BAD_REQUEST,
            Json(json!({"ok": false, "error": "No active replication"})),
        )
            .into_response(),
    }
}

// =============================================================================
// Setup handlers
// =============================================================================

pub async fn test_connection(
    State(state): State<Arc<HttpAppState>>,
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

async fn test_postgres(
    state: Arc<HttpAppState>,
    req: TestConnectionRequest,
) -> axum::response::Response {
    let port = req.port.unwrap_or(5432);
    let url = format!(
        "postgres://{}:{}@{}:{}/{}",
        req.user, req.password, req.host, port, req.database
    );

    let connect_fut = tokio_postgres::connect(&url, tokio_postgres::NoTls);
    let (client, connection) =
        match tokio::time::timeout(Duration::from_secs(5), connect_fut).await {
            Ok(Ok(conn)) => conn,
            Ok(Err(e)) => {
                return (StatusCode::OK, Json(json!({"ok": false, "error": e.to_string()})))
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
            warn!("PG connection error: {}", e);
        }
    });

    let version = match client.query_one("SELECT version()", &[]).await {
        Ok(row) => row.get::<_, String>(0),
        Err(e) => {
            return (StatusCode::OK, Json(json!({"ok": false, "error": e.to_string()})))
                .into_response()
        }
    };

    let wal_level = match client.query_one("SHOW wal_level", &[]).await {
        Ok(row) => row.get::<_, String>(0),
        Err(e) => {
            return (
                StatusCode::OK,
                Json(json!({"ok": false, "error": format!("SHOW wal_level failed: {}", e)})),
            )
                .into_response()
        }
    };

    *state.source_config.write().await = Some(SourceSetupConfig {
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
    state: Arc<HttpAppState>,
    req: TestConnectionRequest,
) -> axum::response::Response {
    let mysql_port = req.mysql_port.unwrap_or(9030);
    let http_port = req.http_port.unwrap_or(8030);

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
            return (StatusCode::OK, Json(json!({"ok": false, "error": e.to_string()})))
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
            return (StatusCode::OK, Json(json!({"ok": false, "error": e.to_string()})))
                .into_response();
        }
    };

    drop(conn);
    let _ = pool.disconnect().await;

    *state.sink_config.write().await = Some(SinkSetupConfig {
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

pub async fn discover_tables(State(state): State<Arc<HttpAppState>>) -> impl IntoResponse {
    let src = {
        let guard = state.source_config.read().await;
        match guard.as_ref() {
            Some(s) => s.clone(),
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"ok": false, "error": "Source not configured. Test connection first."})),
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
    let (client, connection) =
        match tokio::time::timeout(Duration::from_secs(5), connect_fut).await {
            Ok(Ok(c)) => c,
            Ok(Err(e)) => {
                return (StatusCode::OK, Json(json!({"ok": false, "error": e.to_string()})))
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
            warn!("PG discover connection error: {}", e);
        }
    });

    let query = r#"
        SELECT t.table_name,
               (SELECT count(*) FROM information_schema.columns c
                WHERE c.table_name = t.table_name AND c.table_schema = 'public') as column_count,
               CASE c.relreplident
                   WHEN 'f' THEN 'FULL'
                   WHEN 'd' THEN 'DEFAULT'
                   WHEN 'n' THEN 'NOTHING'
                   WHEN 'i' THEN 'INDEX'
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
            return (StatusCode::OK, Json(json!({"ok": false, "error": e.to_string()})))
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

// =============================================================================
// Start / Stop replication
// =============================================================================

pub async fn start_replication(
    State(state): State<Arc<HttpAppState>>,
    Json(req): Json<StartReplicationRequest>,
) -> impl IntoResponse {
    // Check if engine is already running
    {
        let engine = state.engine_state.read().await;
        if engine.is_some() {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"ok": false, "error": "Replication already running"})),
            );
        }
    }

    let src = {
        let guard = state.source_config.read().await;
        match guard.as_ref() {
            Some(s) => s.clone(),
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"ok": false, "error": "Source not configured. Test connection first."})),
                )
            }
        }
    };

    let sink = {
        let guard = state.sink_config.read().await;
        match guard.as_ref() {
            Some(s) => s.clone(),
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"ok": false, "error": "Sink not configured. Test connection first."})),
                )
            }
        }
    };

    // Auto-create StarRocks database and tables from PG schema
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
    let slot_name = "dbmazz_slot".to_string();
    let publication_name = "dbmazz_pub".to_string();
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

    let state_for_engine = state.clone();
    tokio::spawn(async move {
        if let Err(e) = engine.run().await {
            error!("CDC engine error: {}", e);
            *state_for_engine.engine_state.write().await = None;
        }
    });

    (StatusCode::OK, Json(json!({"ok": true})))
}

pub async fn stop_replication(State(state): State<Arc<HttpAppState>>) -> impl IntoResponse {
    let engine = state.engine_state.read().await;
    if let Some(ref shared) = *engine {
        shared.set_skip_slot_cleanup(true);
        shared.set_state(CdcState::Stopped);
        let _ = shared.shutdown_tx.send(true);
    }
    drop(engine);
    *state.engine_state.write().await = None;

    (StatusCode::OK, Json(json!({"ok": true, "state": "stopped"})))
}

// =============================================================================
// StarRocks table auto-creation from PostgreSQL schema
// =============================================================================

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

async fn ensure_starrocks_tables(
    src: &SourceSetupConfig,
    sink: &SinkSetupConfig,
    tables: &[String],
) -> Result<(), String> {
    // 1. Connect to PostgreSQL
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
