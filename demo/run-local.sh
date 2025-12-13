#!/bin/bash

# Script para ejecutar dbmazz localmente conectándose a contenedores del demo
# Usado para profiling con flamegraph

export DATABASE_URL="postgres://postgres:postgres@localhost:15432/demo_db?replication=database"
export STARROCKS_URL="http://localhost:8040"
export STARROCKS_DB="demo_db"
export STARROCKS_USER="root"
export STARROCKS_PASS=""
export SLOT_NAME="dbmazz_slot"
export PUBLICATION_NAME="dbmazz_pub"
export TABLES="orders,order_items,toast_test"
export FLUSH_SIZE="10000"
export FLUSH_INTERVAL_MS="5000"
export GRPC_PORT="50051"

echo "🚀 Ejecutando dbmazz localmente para profiling..."
echo "📊 Conectando a:"
echo "   PostgreSQL: localhost:5432/demo_db"
echo "   StarRocks:  localhost:8040/demo_db"
echo ""

cd /home/happycoding/Documents/projects/dbmazz/dbmazz
./target/release/dbmazz

