#!/usr/bin/env python3
"""
TOAST Generator - Genera datos grandes para probar TOAST/Partial Update
"""

import os
import time
import json
import random
import psycopg2
from datetime import datetime

# Config
PG_HOST = os.getenv('PG_HOST', 'postgres')
PG_PORT = int(os.getenv('PG_PORT', 5432))
PG_USER = os.getenv('PG_USER', 'postgres')
PG_PASSWORD = os.getenv('PG_PASSWORD', 'postgres')
PG_DATABASE = os.getenv('PG_DATABASE', 'demo_db')

# TOAST config
JSON_SIZE_KB = int(os.getenv('JSON_SIZE_KB', 100))  # Tamaño del JSON en KB
NUM_RECORDS = int(os.getenv('NUM_RECORDS', 10))     # Número de registros
UPDATE_INTERVAL = int(os.getenv('UPDATE_INTERVAL', 10))  # Segundos entre UPDATEs


def generate_large_json(size_kb: int) -> dict:
    """Genera un JSON de aproximadamente size_kb KB"""
    # Cada item tiene ~50 bytes
    items_needed = (size_kb * 1024) // 50
    
    return {
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "size_kb": size_kb,
            "items": items_needed
        },
        "data": [
            {
                "id": i,
                "uuid": f"uuid-{i:08d}-{random.randint(1000, 9999)}",
                "value": random.random(),
                "status": random.choice(["active", "pending", "completed"])
            }
            for i in range(items_needed)
        ]
    }


def wait_for_postgres(max_retries: int = 30):
    """Espera a que PostgreSQL esté disponible"""
    for i in range(max_retries):
        try:
            conn = psycopg2.connect(
                host=PG_HOST,
                port=PG_PORT,
                user=PG_USER,
                password=PG_PASSWORD,
                database=PG_DATABASE
            )
            print(f"✓ Conectado a PostgreSQL")
            return conn
        except psycopg2.OperationalError as e:
            print(f"Esperando PostgreSQL... ({i+1}/{max_retries})")
            time.sleep(2)
    
    raise Exception("PostgreSQL no disponible")


def insert_records(conn, num_records: int, json_size_kb: int):
    """Inserta registros con JSONs grandes"""
    print(f"\n📦 Insertando {num_records} registros con JSONs de {json_size_kb}KB...")
    
    with conn.cursor() as cur:
        for i in range(1, num_records + 1):
            name = f"TOAST Test Record {i}"
            large_json = generate_large_json(json_size_kb)
            
            cur.execute("""
                INSERT INTO toast_test (name, status, large_json)
                VALUES (%s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    large_json = EXCLUDED.large_json,
                    updated_at = NOW()
                RETURNING id
            """, (name, 'active', json.dumps(large_json)))
            
            record_id = cur.fetchone()[0]
            print(f"  ✓ Registro {record_id} insertado ({json_size_kb}KB JSON)")
        
        conn.commit()
    
    print(f"✓ {num_records} registros insertados con TOAST")


def update_without_json(conn):
    """
    UPDATE que NO modifica large_json → triggera TOAST 'u' en WAL
    Esto es lo que probará el Partial Update de StarRocks
    """
    new_status = random.choice(['processing', 'completed', 'archived'])
    
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE toast_test 
            SET status = %s, updated_at = NOW()
            WHERE id = (SELECT id FROM toast_test ORDER BY random() LIMIT 1)
            RETURNING id, status
        """, (new_status,))
        
        result = cur.fetchone()
        conn.commit()
        
        if result:
            print(f"🔄 UPDATE (TOAST): id={result[0]}, status={result[1]} → Partial Update esperado")
        else:
            print("⚠️  No hay registros para actualizar")


def update_with_json(conn, json_size_kb: int):
    """UPDATE que SÍ modifica large_json → Full Update normal"""
    with conn.cursor() as cur:
        new_json = generate_large_json(json_size_kb)
        
        cur.execute("""
            UPDATE toast_test 
            SET large_json = %s, updated_at = NOW()
            WHERE id = (SELECT id FROM toast_test ORDER BY random() LIMIT 1)
            RETURNING id
        """, (json.dumps(new_json),))
        
        result = cur.fetchone()
        conn.commit()
        
        if result:
            print(f"📝 UPDATE (Full): id={result[0]} → Full Update esperado")


def main():
    print("=" * 50)
    print("🧪 TOAST Generator - Testing Partial Update")
    print("=" * 50)
    print(f"JSON Size: {JSON_SIZE_KB}KB")
    print(f"Records: {NUM_RECORDS}")
    print(f"Update Interval: {UPDATE_INTERVAL}s")
    print("=" * 50)
    
    conn = wait_for_postgres()
    
    # Esperar para que el schema se propague
    print("\nEsperando 10s para propagación de schema...")
    time.sleep(10)
    
    # Insertar registros iniciales
    insert_records(conn, NUM_RECORDS, JSON_SIZE_KB)
    
    print(f"\n🔁 Iniciando loop de UPDATEs cada {UPDATE_INTERVAL}s...")
    print("   (UPDATEs sin modificar JSON → triggean TOAST/Partial Update)")
    print("")
    
    update_count = 0
    while True:
        try:
            # 80% UPDATEs sin JSON (TOAST), 20% con JSON (Full)
            if random.random() < 0.8:
                update_without_json(conn)
            else:
                update_with_json(conn, JSON_SIZE_KB)
            
            update_count += 1
            
            # Stats cada 10 updates
            if update_count % 10 == 0:
                print(f"\n📊 Stats: {update_count} updates ejecutados")
            
            time.sleep(UPDATE_INTERVAL)
            
        except KeyboardInterrupt:
            print("\n⏹️  Detenido por usuario")
            break
        except Exception as e:
            print(f"❌ Error: {e}")
            time.sleep(5)
    
    conn.close()


if __name__ == "__main__":
    main()

