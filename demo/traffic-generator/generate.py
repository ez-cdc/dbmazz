#!/usr/bin/env python3
"""
dbmazz Traffic Generator
Simulates realistic e-commerce traffic for demo purposes
"""

import os
import time
import random
import psycopg2
from datetime import datetime

DATABASE_URL = os.getenv("DATABASE_URL", "postgres://postgres:postgres@postgres:5432/demo_db")

def connect():
    """Connect to PostgreSQL"""
    while True:
        try:
            conn = psycopg2.connect(DATABASE_URL)
            conn.autocommit = True
            print("✅ Connected to PostgreSQL")
            return conn
        except Exception as e:
            print(f"⏳ Waiting for PostgreSQL... {e}")
            time.sleep(2)

def generate_insert(cursor):
    """Generate a new order"""
    customer_id = random.randint(1, 1000)
    total = round(random.uniform(10, 500), 2)
    status = 'pending'
    
    cursor.execute(
        "INSERT INTO orders (customer_id, total, status) VALUES (%s, %s, %s) RETURNING id",
        (customer_id, total, status)
    )
    order_id = cursor.fetchone()[0]
    
    # Add 1-5 items to the order
    num_items = random.randint(1, 5)
    for _ in range(num_items):
        product_name = f"Product {random.randint(1, 100)}"
        quantity = random.randint(1, 5)
        price = round(random.uniform(5, 100), 2)
        
        cursor.execute(
            "INSERT INTO order_items (order_id, product_name, quantity, price) VALUES (%s, %s, %s, %s)",
            (order_id, product_name, quantity, price)
        )
    
    return order_id

def generate_update(cursor):
    """Update order status"""
    statuses = ['pending', 'processing', 'shipped', 'delivered']
    
    cursor.execute("SELECT id, status FROM orders ORDER BY random() LIMIT 1")
    result = cursor.fetchone()
    if not result:
        return
    
    order_id, current_status = result
    current_idx = statuses.index(current_status) if current_status in statuses else 0
    
    if current_idx < len(statuses) - 1:
        new_status = statuses[current_idx + 1]
        cursor.execute(
            "UPDATE orders SET status = %s, updated_at = NOW() WHERE id = %s",
            (new_status, order_id)
        )

def generate_delete(cursor):
    """Delete random order (cancellation)"""
    cursor.execute(
        "DELETE FROM orders WHERE id IN (SELECT id FROM orders WHERE status = 'pending' ORDER BY random() LIMIT 1)"
    )

def main():
    print("🚀 dbmazz Traffic Generator Starting...")
    conn = connect()
    cursor = conn.cursor()
    
    operations = 0
    start_time = time.time()
    
    print("📊 Generating realistic traffic...")
    print("   - 70% Inserts")
    print("   - 25% Updates")
    print("   - 5% Deletes")
    print()
    
    try:
        while True:
            # Weighted random operations
            rand = random.random()
            
            if rand < 0.70:
                # 70% Insert
                order_id = generate_insert(cursor)
                op_type = "INSERT"
            elif rand < 0.95:
                # 25% Update
                generate_update(cursor)
                op_type = "UPDATE"
            else:
                # 5% Delete
                generate_delete(cursor)
                op_type = "DELETE"
            
            operations += 1
            
            if operations % 100 == 0:
                elapsed = time.time() - start_time
                ops_per_sec = operations / elapsed if elapsed > 0 else 0
                print(f"📈 {operations:,} ops | {ops_per_sec:.1f} ops/sec | Last: {op_type}")
            
            # Sleep to control rate (adjust for demo speed)
            time.sleep(0.01)  # 100 ops/sec
            
    except KeyboardInterrupt:
        print("\n✋ Traffic generator stopped")
        elapsed = time.time() - start_time
        print(f"📊 Total: {operations:,} operations in {elapsed:.1f}s")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()

