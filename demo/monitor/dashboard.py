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

def get_pg_counts(conn):
    """Get counts from PostgreSQL"""
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM orders")
        orders = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM order_items")
        items = cursor.fetchone()[0]
        return orders, items
    finally:
        cursor.close()

def get_sr_counts(conn):
    """Get counts from StarRocks"""
    if conn is None:
        return 0, 0, 0, None, None, None
        
    cursor = conn.cursor()
    try:
        # Contar registros activos (no eliminados)
        cursor.execute("SELECT COUNT(*) FROM orders WHERE dbmazz_is_deleted = FALSE")
        orders = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM order_items WHERE dbmazz_is_deleted = FALSE")
        items = cursor.fetchone()[0]
        
        # Contar registros eliminados (soft delete)
        cursor.execute("SELECT COUNT(*) FROM orders WHERE dbmazz_is_deleted = TRUE")
        deleted = cursor.fetchone()[0]
        
        # Obtener última sincronización, LSN y latencia de replicación real
        # Obtener el último registro sincronizado y calcular su latencia
        cursor.execute("""
            SELECT 
                dbmazz_synced_at as last_sync,
                dbmazz_cdc_version as latest_lsn,
                TIMESTAMPDIFF(SECOND, 
                    GREATEST(IFNULL(created_at, '1970-01-01'), IFNULL(updated_at, '1970-01-01')), 
                    dbmazz_synced_at
                ) as replication_latency
            FROM orders
            WHERE dbmazz_synced_at IS NOT NULL
            ORDER BY dbmazz_synced_at DESC
            LIMIT 1
        """)
        result = cursor.fetchone()
        last_sync = result[0] if result and result[0] else None
        latest_lsn = result[1] if result and result[1] else None
        lag_seconds = result[2] if result and result[2] is not None else None
        
        return orders, items, deleted, last_sync, latest_lsn, lag_seconds
    except Exception as e:
        return 0, 0, 0, None, None, None
    finally:
        cursor.close()

def create_dashboard(pg_orders, pg_items, sr_orders, sr_items, sr_deleted, last_sync, latest_lsn, lag_seconds, cycle):
    """Create dashboard layout"""
    layout = Layout()
    
    # Header
    header = Panel(
        Text("dbmazz - CDC Demo en Vivo", style="bold magenta", justify="center"),
        style="bold white on blue"
    )
    
    # Main table
    table = Table(show_header=True, header_style="bold cyan", expand=True)
    table.add_column("Métrica", style="cyan", width=30)
    table.add_column("PostgreSQL (Source)", justify="right", style="green")
    table.add_column("StarRocks (Target)", justify="right", style="yellow")
    table.add_column("Estado", justify="center")
    
    # Calculate sync status and lag
    orders_lag = pg_orders - sr_orders
    items_lag = pg_items - sr_items
    orders_synced = "✅" if pg_orders == sr_orders else f"⏳ ({sr_orders}/{pg_orders})"
    items_synced = "✅" if pg_items == sr_items else f"⏳ ({sr_items}/{pg_items})"
    
    # Color code for lag
    orders_lag_color = "green" if orders_lag < 100 else "yellow" if orders_lag < 1000 else "red"
    items_lag_color = "green" if items_lag < 300 else "yellow" if items_lag < 3000 else "red"
    
    table.add_row(
        "📦 Orders (Active)",
        f"{pg_orders:,}",
        f"{sr_orders:,}",
        orders_synced
    )
    table.add_row(
        "📋 Order Items (Active)",
        f"{pg_items:,}",
        f"{sr_items:,}",
        items_synced
    )
    table.add_row(
        "🗑️  Soft Deleted Orders",
        "-",
        f"{sr_deleted:,}",
        "ℹ️"
    )
    table.add_row(
        "⏱️  Replication Lag",
        "-",
        f"[{orders_lag_color}]{orders_lag:,} orders[/{orders_lag_color}]",
        f"[{items_lag_color}]{items_lag:,} items[/{items_lag_color}]"
    )
    
    # Stats panel with audit columns
    sync_rate = ((sr_orders / pg_orders * 100) if pg_orders > 0 else 0)
    
    # Format audit info
    last_sync_str = last_sync.strftime('%H:%M:%S') if last_sync else "N/A"
    lsn_str = f"0x{latest_lsn:X}" if latest_lsn else "N/A"
    
    # Replication latency (tiempo real desde created_at hasta synced_at)
    replication_lag_str = f"{lag_seconds}s" if lag_seconds is not None else "N/A"
    replication_lag_color = "green" if lag_seconds is not None and lag_seconds < 5 else "yellow" if lag_seconds is not None and lag_seconds < 30 else "red"
    
    # Records lag (rezago de registros)
    total_lag = orders_lag + items_lag
    records_lag_str = f"{total_lag:,} registros"
    records_lag_color = "green" if total_lag < 500 else "yellow" if total_lag < 5000 else "red"
    
    stats = f"""
[bold]Estado de Sincronización:[/bold]
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
    console.print()
    
    cycle = 0
    
    try:
        with Live(console=console, refresh_per_second=1) as live:
            while True:
                try:
                    pg_orders, pg_items = get_pg_counts(pg_conn)
                    sr_orders, sr_items, sr_deleted, last_sync, latest_lsn, lag_seconds = get_sr_counts(sr_conn)
                    
                    cycle += 1
                    dashboard = create_dashboard(
                        pg_orders, pg_items,
                        sr_orders, sr_items, sr_deleted,
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
        sr_conn.close()

if __name__ == "__main__":
    main()

