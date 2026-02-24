#!/bin/bash
set -e

# Ensure we run from the demo directory
cd "$(dirname "$0")"

echo ""
echo "  Starting dbmazz demo..."
echo ""

# Check Docker
if ! command -v docker &> /dev/null; then
    echo "Error: Docker not found. Please install Docker first."
    exit 1
fi

# Build and start
docker compose -f docker-compose.demo.yml up --build -d

echo ""
echo "  ██████╗ ██████╗ ███╗   ███╗ █████╗ ███████╗███████╗"
echo "  ██╔══██╗██╔══██╗████╗ ████║██╔══██╗╚══███╔╝╚══███╔╝"
echo "  ██║  ██║██████╔╝██╔████╔██║███████║  ███╔╝   ███╔╝ "
echo "  ██║  ██║██╔══██╗██║╚██╔╝██║██╔══██║ ███╔╝   ███╔╝  "
echo "  ██████╔╝██████╔╝██║ ╚═╝ ██║██║  ██║███████╗███████╗"
echo "  ╚═════╝ ╚═════╝ ╚═╝     ╚═╝╚═╝  ╚═╝╚══════╝╚══════╝"
echo ""
echo "  Open http://localhost:3000 to get started"
echo ""
echo "  StarRocks takes ~60s to start on first run."
echo "  Stop with: docker compose -f docker-compose.demo.yml down -v"
echo ""
