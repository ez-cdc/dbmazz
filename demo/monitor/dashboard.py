#!/usr/bin/env python3
"""
dbmazz Demo Monitor
Real-time dashboard showing CDC replication metrics
"""

import os
import time
import psycopg2
import pymysql
from datetime import datetime
from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.text import Text

POSTGRES_URL = os.getenv("POSTGRES_URL", "postgres://postgres:postgres@postgres:5432/demo_db")
STARROCKS_HOST = os.getenv("STARROCKS_HOST", "starrocks")
STARROCKS_PORT = int(os.getenv("STARROCKS_PORT", "9030"))
STARROCKS_USER = os.getenv("STARROCKS_USER", "root")

console = Console()

def connect_pg():
    """Connect to PostgreSQL"""
    while True:
        try:
            conn = psycopg2.connect(POSTGRES_URL)
            return conn
        except Exception as e:
            console.print(f"[yellow]⏳ Waiting for PostgreSQL...[/yellow]")
            time.sleep(2)

def connect_sr():
    """Connect to StarRocks with retry logic"""
    max_attempts = 60  # 5 minutos máximo (60 * 5 segundos)
    attempt = 0
    
    while attempt < max_attempts:
        try:
            conn = pymysql.connect(
                host=STARROCKS_HOST,
                port=STARROCKS_PORT,
                user=STARROCKS_USER,
                password="",
                database="demo_db",
                connect_timeout=5
            )
            # Verificar que la conexión funciona con un query
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            console.print("[green]✅ Connected to StarRocks[/green]")
            return conn
        except Exception as e:
            attempt += 1
            console.print(f"[yellow]⏳ Waiting for StarRocks... (attempt {attempt}/{max_attempts})[/yellow]")
            time.sleep(5)
    
    # Si llegamos aquí, timeout
    console.print(f"[red]❌ Could not connect to StarRocks after {max_attempts * 5}s[/red]")
    console.print(f"[yellow]Continuing without StarRocks connection...[/yellow]")
    return None

def discover_tables(pg_conn, sr_conn):
    """Descubre tablas replicadas (existen en ambas DBs)"""
    # Tablas en PostgreSQL (excluyendo sistema)
    pg_tables = set()
    cursor = pg_conn.cursor()
    try:
        cursor.execute("""
            SELECT tablename FROM pg_tables 
            WHERE schemaname = 'public' 
            AND tablename NOT LIKE 'pg_%'
            AND tablename != 'dbmazz_checkpoints'
        """)
        pg_tables = {row[0] for row in cursor.fetchall()}
    finally:
        cursor.close()
    
    # Tablas en StarRocks
    sr_tables = set()
    if sr_conn:
        cursor = sr_conn.cursor()
        try:
            cursor.execute("SHOW TABLES")
            sr_tables = {row[0] for row in cursor.fetchall()}
        finally:
            cursor.close()
    
    # Interseccion: tablas que existen en ambos
    return sorted(pg_tables & sr_tables)

def get_table_counts(pg_conn, sr_conn, tables):
    """Obtiene counts de todas las tablas"""
    results = {}
    
    for table in tables:
        pg_count = 0
        sr_count = 0
        sr_deleted = 0
        
        # PostgreSQL
        cursor = pg_conn.cursor()
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            pg_count = cursor.fetchone()[0]
        finally:
            cursor.close()
        
        # StarRocks (con audit columns)
        if sr_conn:
            cursor = sr_conn.cursor()
            try:
                cursor.execute(f"""
                    SELECT 
                        COUNT(*) as total,
                        SUM(CASE WHEN dbmazz_is_deleted = TRUE THEN 1 ELSE 0 END) as deleted
                    FROM {table}
                """)
                row = cursor.fetchone()
                sr_count = row[0] - (row[1] or 0)  # Active = total - deleted
                sr_deleted = row[1] or 0
            except:
                # Tabla puede no tener columnas audit
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                sr_count = cursor.fetchone()[0]
            finally:
                cursor.close()
        
        results[table] = {
            'pg': pg_count,
            'sr': sr_count,
            'deleted': sr_deleted
        }
    
    return results

def get_audit_info(sr_conn, tables):
    """Obtiene informacion de auditoria CDC"""
    if not sr_conn or not tables:
        return None, None, None
    
    # Intentar obtener de la primera tabla con audit columns
    for table in tables:
        cursor = sr_conn.cursor()
        try:
            cursor.execute(f"""
                SELECT 
                    dbmazz_synced_at as last_sync,
                    dbmazz_cdc_version as latest_lsn,
                    TIMESTAMPDIFF(SECOND, 
                        GREATEST(IFNULL(created_at, '1970-01-01'), IFNULL(updated_at, '1970-01-01')), 
                        dbmazz_synced_at
                    ) as replication_latency
                FROM {table}
                WHERE dbmazz_synced_at IS NOT NULL
                ORDER BY dbmazz_synced_at DESC
                LIMIT 1
            """)
            result = cursor.fetchone()
            if result:
                return result[0], result[1], result[2]
        except:
            # Tabla no tiene audit columns, probar siguiente
            pass
        finally:
            cursor.close()
    
    return None, None, None

def create_dashboard(table_counts, last_sync, latest_lsn, lag_seconds, cycle):
    """Create dashboard layout dinamicamente para N tablas"""
    layout = Layout()
    
    # Header
    header = Panel(
        Text("dbmazz - CDC Demo en Vivo", style="bold magenta", justify="center"),
        style="bold white on blue"
    )
    
    # Main table
    table = Table(show_header=True, header_style="bold cyan", expand=True)
    table.add_column("Tabla", style="cyan", width=25)
    table.add_column("PostgreSQL", justify="right", style="green")
    table.add_column("StarRocks", justify="right", style="yellow")
    table.add_column("Estado", justify="center")
    
    # Iconos por tabla
    table_icons = {
        'orders': '📦',
        'order_items': '📋',
        'toast_test': '🧪',
    }
    
    # Agregar filas dinamicamente
    total_pg = 0
    total_sr = 0
    total_deleted = 0
    
    for table_name, counts in sorted(table_counts.items()):
        pg_count = counts['pg']
        sr_count = counts['sr']
        deleted = counts['deleted']
        
        total_pg += pg_count
        total_sr += sr_count
        total_deleted += deleted
        
        # Calcular estado
        lag = pg_count - sr_count
        if lag == 0:
            status = "✅"
        elif lag < 100:
            status = f"⏳ ({sr_count}/{pg_count})"
        else:
            status = f"[yellow]⏳ ({sr_count}/{pg_count})[/yellow]"
        
        # Icono
        icon = table_icons.get(table_name, '📊')
        
        table.add_row(
            f"{icon} {table_name}",
            f"{pg_count:,}",
            f"{sr_count:,}",
            status
        )
    
    # Fila resumen
    if len(table_counts) > 1:
        total_lag = total_pg - total_sr
        lag_color = "green" if total_lag < 500 else "yellow" if total_lag < 5000 else "red"
        table.add_row(
            "[bold]Total[/bold]",
            f"[bold]{total_pg:,}[/bold]",
            f"[bold]{total_sr:,}[/bold]",
            f"[{lag_color}]Δ {total_lag:,}[/{lag_color}]"
        )
    
    # Fila de soft deletes si hay
    if total_deleted > 0:
        table.add_row(
            "🗑️  Soft Deleted",
            "-",
            f"{total_deleted:,}",
            "ℹ️"
        )
    
    # Stats panel with audit columns
    sync_rate = ((total_sr / total_pg * 100) if total_pg > 0 else 0)
    
    # Format audit info
    last_sync_str = last_sync.strftime('%H:%M:%S') if last_sync else "N/A"
    lsn_str = f"0x{latest_lsn:X}" if latest_lsn else "N/A"
    
    # Replication latency (tiempo real desde created_at hasta synced_at)
    replication_lag_str = f"{lag_seconds}s" if lag_seconds is not None else "N/A"
    replication_lag_color = "green" if lag_seconds is not None and lag_seconds < 5 else "yellow" if lag_seconds is not None and lag_seconds < 30 else "red"
    
    # Records lag (rezago de registros)
    total_lag_count = total_pg - total_sr
    records_lag_str = f"{total_lag_count:,} registros"
    records_lag_color = "green" if total_lag_count < 500 else "yellow" if total_lag_count < 5000 else "red"
    
    # Tablas monitoreadas
    num_tables = len(table_counts)
    
    stats = f"""
[bold]Estado de Sincronización:[/bold]
• Tablas: {num_tables}
• Tasa de Sync: {sync_rate:.1f}%
• Rezago de Registros: [{records_lag_color}]{records_lag_str}[/{records_lag_color}]
• Latencia de Replicación: [{replication_lag_color}]{replication_lag_str}[/{replication_lag_color}]

[bold]Auditoría CDC:[/bold]
• Última Sync: {last_sync_str}
• LSN Actual: {lsn_str}
• Ciclo: {cycle} | {datetime.now().strftime('%H:%M:%S')}

[bold green]✅ Sistema Operativo[/bold green]
"""
    
    stats_panel = Panel(stats, title="Estadísticas", style="green")
    
    # Footer
    footer = Panel(
        "[dim]Presiona Ctrl+C para detener | dbmazz v0.1.0[/dim]",
        style="white on blue"
    )
    
    # Assemble
    layout.split_column(
        Layout(header, size=3),
        Layout(table, size=10),
        Layout(stats_panel, size=8),
        Layout(footer, size=3)
    )
    
    return layout

def main():
    console.print("[bold green]🚀 dbmazz Monitor Starting...[/bold green]")
    
    pg_conn = connect_pg()
    sr_conn = connect_sr()
    
    console.print("[green]✅ Connected to all databases[/green]")
    
    # Descubrir tablas al inicio
    console.print("[cyan]🔍 Discovering tables...[/cyan]")
    tables = discover_tables(pg_conn, sr_conn)
    console.print(f"[green]✅ Found {len(tables)} replicated tables: {', '.join(tables)}[/green]")
    console.print()
    
    if not tables:
        console.print("[yellow]⚠️  No tables found to monitor[/yellow]")
        return
    
    cycle = 0
    
    try:
        with Live(console=console, refresh_per_second=1) as live:
            while True:
                try:
                    # Obtener counts de todas las tablas
                    table_counts = get_table_counts(pg_conn, sr_conn, tables)
                    
                    # Obtener info de auditoria
                    last_sync, latest_lsn, lag_seconds = get_audit_info(sr_conn, tables)
                    
                    cycle += 1
                    dashboard = create_dashboard(
                        table_counts,
                        last_sync, latest_lsn, lag_seconds,
                        cycle
                    )
                    
                    live.update(dashboard)
                    time.sleep(1)
                    
                except Exception as e:
                    console.print(f"[red]Error: {e}[/red]")
                    time.sleep(2)
                    
    except KeyboardInterrupt:
        console.print("\n[yellow]✋ Monitor stopped[/yellow]")
    finally:
        pg_conn.close()
        if sr_conn:
            sr_conn.close()

if __name__ == "__main__":
    main()

