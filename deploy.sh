#!/bin/bash

# SofaScore Odds System - Cloud Deployment Script
# This script automates the deployment process on your cloud server

set -e  # Exit on any error

echo "🚀 Starting SofaScore Odds System Cloud Deployment..."

# Check if Docker and Docker Compose are installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

# Create necessary directories
echo "📁 Creating necessary directories..."
mkdir -p logs data ssl

# Copy environment template if .env doesn't exist
if [ ! -f .env ]; then
    echo "📝 Creating .env file from template..."
    cp env.cloud.template .env
    echo "⚠️  Please edit .env file with your actual configuration values before continuing."
    echo "   Press Enter when ready to continue..."
    read
fi

# Check if .env has been configured
if grep -q "your_proxy_username" .env; then
    echo "⚠️  Please configure your .env file with actual values before continuing."
    exit 1
fi

# Build and start services
echo "🔨 Building and starting services..."
docker-compose up -d --build

# Wait for services to be healthy
echo "⏳ Waiting for services to be healthy..."
sleep 30

# Check service health
echo "🏥 Checking service health..."
docker-compose ps

# Show logs
echo "📋 Recent logs:"
docker-compose logs --tail=20

echo ""
echo "✅ Deployment completed successfully!"
echo ""
echo "🌐 Your services are now running:"
echo "   - PostgreSQL: localhost:5432"
echo "   - Redis: localhost:6379"
echo "   - Application: Check logs above for status"
echo ""
echo "📊 Useful commands:"
echo "   - View logs: docker-compose logs -f"
echo "   - Stop services: docker-compose down"
echo "   - Restart services: docker-compose restart"
echo "   - Update services: docker-compose pull && docker-compose up -d"
echo ""
echo "🔍 Check application status:"
echo "   docker-compose exec app python main.py status"
