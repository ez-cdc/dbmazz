#!/bin/bash

# dbmazz Demo Cleanup Script

echo "🧹 Stopping dbmazz demo..."
docker-compose -f docker-compose.demo.yml down -v

echo "✅ Demo stopped and cleaned up"
echo "📝 To start again: ./demo-start.sh"


