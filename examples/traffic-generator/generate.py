#!/usr/bin/env python3
"""
dbmazz High-Throughput Traffic Generator
Generates configurable events/second using multiple worker threads
"""

import os
import time
import random
import psycopg2
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from datetime import datetime

# Configuration from environment
DATABASE_URL = os.getenv("DATABASE_URL", "postgres://postgres:postgres@postgres:5432/demo_db")
TARGET_EVENTS_PER_SECOND = int(os.getenv("TARGET_EVENTS_PER_SECOND", "3000"))
INSERT_RATIO = float(os.getenv("INSERT_RATIO", "0.70"))  # 70%
UPDATE_RATIO = float(os.getenv("UPDATE_RATIO", "0.25"))  # 25%
DELETE_RATIO = float(os.getenv("DELETE_RATIO", "0.05"))  # 5%

# Auto-calculate workers (each worker ~200 events/sec)
NUM_WORKERS = max(1, TARGET_EVENTS_PER_SECOND // 200)

# Stats tracking
stats_lock = Lock()
stats = {
    'operations': 0,
    'events': 0,
    'inserts': 0,
    'updates': 0,
    'deletes': 0
}

def connect():
    """Connect to PostgreSQL with retry"""
    while True:
        try:
            conn = psycopg2.connect(DATABASE_URL)
            conn.autocommit = True
            return conn
        except Exception as e:
            print(f"⏳ Waiting for PostgreSQL... {e}")
            time.sleep(2)

def generate_insert(cursor):
    """Generate order + items (4-6 events)"""
    customer_id = random.randint(1, 1000)
    total = round(random.uniform(10, 500), 2)
    status = 'pending'
    
    cursor.execute(
        "INSERT INTO orders (customer_id, total, status) VALUES (%s, %s, %s) RETURNING id",
        (customer_id, total, status)
    )
    order_id = cursor.fetchone()[0]
    
    num_items = random.randint(1, 5)
    for _ in range(num_items):
        product_name = f"Product {random.randint(1, 100)}"
        quantity = random.randint(1, 5)
        price = round(random.uniform(5, 100), 2)
        
        cursor.execute(
            "INSERT INTO order_items (order_id, product_name, quantity, price) VALUES (%s, %s, %s, %s)",
            (order_id, product_name, quantity, price)
        )
    
    return num_items + 1  # 1 order + N items

def generate_update(cursor):
    """Update order status (1 event)"""
    statuses = ['pending', 'processing', 'shipped', 'delivered']
    
    cursor.execute("SELECT id, status FROM orders ORDER BY random() LIMIT 1")
    result = cursor.fetchone()
    if not result:
        return 0
    
    order_id, current_status = result
    current_idx = statuses.index(current_status) if current_status in statuses else 0
    
    if current_idx < len(statuses) - 1:
        new_status = statuses[current_idx + 1]
        cursor.execute(
            "UPDATE orders SET status = %s, updated_at = NOW() WHERE id = %s",
            (new_status, order_id)
        )
        return 1
    return 0

def generate_delete(cursor):
    """Delete order (1+ events with cascade)"""
    cursor.execute(
        "DELETE FROM orders WHERE id IN (SELECT id FROM orders WHERE status = 'pending' ORDER BY random() LIMIT 1) RETURNING id"
    )
    result = cursor.fetchone()
    return 1 if result else 0

def worker(worker_id, target_ops_per_sec):
    """Worker thread generating operations"""
    conn = connect()
    cursor = conn.cursor()
    
    sleep_time = 1.0 / target_ops_per_sec
    
    print(f"   Worker {worker_id}: started ({target_ops_per_sec} ops/sec target)")
    
    try:
        while True:
            start = time.time()
            
            # Weighted random operation
            rand = random.random()
            
            if rand < INSERT_RATIO:
                events = generate_insert(cursor)
                op_type = 'inserts'
            elif rand < INSERT_RATIO + UPDATE_RATIO:
                events = generate_update(cursor)
                op_type = 'updates'
            else:
                events = generate_delete(cursor)
                op_type = 'deletes'
            
            # Update stats
            with stats_lock:
                stats['operations'] += 1
                stats['events'] += events
                stats[op_type] += 1
            
            # Sleep to maintain rate
            elapsed = time.time() - start
            sleep_duration = max(0, sleep_time - elapsed)
            if sleep_duration > 0:
                time.sleep(sleep_duration)
                
    except KeyboardInterrupt:
        pass
    finally:
        cursor.close()
        conn.close()
        print(f"   Worker {worker_id}: stopped")

def stats_reporter(start_time):
    """Report stats every second"""
    last_ops = 0
    last_events = 0
    
    while True:
        time.sleep(1)
        
        with stats_lock:
            current_ops = stats['operations']
            current_events = stats['events']
            inserts = stats['inserts']
            updates = stats['updates']
            deletes = stats['deletes']
        
        ops_this_sec = current_ops - last_ops
        events_this_sec = current_events - last_events
        last_ops = current_ops
        last_events = current_events
        
        elapsed = time.time() - start_time
        avg_events = current_events / elapsed if elapsed > 0 else 0
        
        print(f"📈 {current_events:,} events | {events_this_sec:,} eps | avg: {avg_events:.0f} eps | I:{inserts} U:{updates} D:{deletes}")

def main():
    print("🚀 dbmazz High-Throughput Traffic Generator")
    print(f"   Target: {TARGET_EVENTS_PER_SECOND} events/sec")
    print(f"   Workers: {NUM_WORKERS}")
    print(f"   Distribution: {INSERT_RATIO*100:.0f}% INSERT / {UPDATE_RATIO*100:.0f}% UPDATE / {DELETE_RATIO*100:.0f}% DELETE")
    print()
    
    # Test connection
    print("🔌 Testing connection...")
    conn = connect()
    conn.close()
    print("✅ Connected to PostgreSQL")
    print()
    
    # Calculate operations per worker
    # Each operation generates ~4 events (1 order + 3 items average)
    target_ops_total = TARGET_EVENTS_PER_SECOND // 4
    ops_per_worker = max(1, target_ops_total // NUM_WORKERS)
    
    print(f"📊 Starting {NUM_WORKERS} workers ({ops_per_worker} ops/sec each)...")
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=NUM_WORKERS + 1) as executor:
        # Start workers
        for i in range(NUM_WORKERS):
            executor.submit(worker, i + 1, ops_per_worker)
        
        # Start stats reporter
        executor.submit(stats_reporter, start_time)
        
        try:
            # Keep main thread alive
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n✋ Stopping traffic generator...")
            elapsed = time.time() - start_time
            with stats_lock:
                total_events = stats['events']
            print(f"📊 Total: {total_events:,} events in {elapsed:.1f}s")
            print(f"📊 Average: {total_events / elapsed:.1f} events/sec")

if __name__ == "__main__":
    main()
