#!/bin/bash
set -e

echo "🚀 Deploying to Development Environment..."

# Load environment variables
if [ -f "infrastructure/docker/.env.dev" ]; then
    export $(cat infrastructure/docker/.env.dev | grep -v '^#' | xargs)
else
    echo "⚠️  Warning: infrastructure/docker/.env.dev not found, using defaults"
fi

# Pull latest code
echo "📦 Pulling latest code..."
git pull origin main

# Build Docker images
echo "🔨 Building Docker images..."
docker-compose -f infrastructure/docker/docker-compose.prod.yml build

# Stop existing containers
echo "🛑 Stopping existing containers..."
docker-compose -f infrastructure/docker/docker-compose.prod.yml down

# Start new containers
echo "▶️  Starting containers..."
docker-compose -f infrastructure/docker/docker-compose.prod.yml up -d

# Wait for services to be healthy
echo "⏳ Waiting for services to be healthy..."
sleep 10

# Check service health
echo "🏥 Checking service health..."
docker-compose -f infrastructure/docker/docker-compose.prod.yml ps

echo "✅ Development deployment complete!"
echo ""
echo "🌐 Services:"
echo "  - Web: http://localhost:3000"
echo "  - Trading API: http://localhost:8000/docs"
echo "  - Backtest API: http://localhost:8001/docs"
echo "  - Auth API: http://localhost:8003/docs"
echo "  - Notification API: http://localhost:8004/docs"
echo "  - Calculation API: http://localhost:8005/docs"
