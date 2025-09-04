#!/bin/bash

# SofaScore Odds System - Direct Python Deployment Script
# This script deploys the application directly on the server without Docker

set -e  # Exit on any error

echo "🚀 Starting SofaScore Odds System Direct Deployment..."

# Check if running as root
if [ "$EUID" -eq 0 ]; then
    echo "❌ Please don't run this script as root. Use a regular user."
    exit 1
fi

# Check if Python 3.11+ is installed
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is not installed. Installing Python 3.11..."
    sudo apt update
    sudo apt install -y python3.11 python3.11-venv python3.11-dev python3-pip
fi

# Check Python version
PYTHON_VERSION=$(python3 --version | cut -d' ' -f2 | cut -d'.' -f1,2)
if [ "$(echo "$PYTHON_VERSION >= 3.11" | bc -l)" -eq 0 ]; then
    echo "❌ Python 3.11+ is required. Current version: $PYTHON_VERSION"
    exit 1
fi

echo "✅ Python $PYTHON_VERSION found"

# Create application directory
APP_DIR="/opt/sofascore"
echo "📁 Setting up application directory: $APP_DIR"

# Create directory and set permissions
sudo mkdir -p $APP_DIR
sudo chown $USER:$USER $APP_DIR

# Copy application files
echo "📋 Copying application files..."
cp -r . $APP_DIR/
cd $APP_DIR

# Create virtual environment
echo "🐍 Creating Python virtual environment..."
python3 -m venv venv
source venv/bin/activate

# Install dependencies
echo "📦 Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# Create necessary directories
echo "📁 Creating necessary directories..."
mkdir -p logs data

# Set up environment file
if [ ! -f .env ]; then
    echo "📝 Creating .env file from template..."
    if [ -f env.cloud.template ]; then
        cp env.cloud.template .env
        echo "⚠️  Please edit .env file with your actual configuration values."
        echo "   Press Enter when ready to continue..."
        read
    else
        echo "❌ Environment template not found. Creating basic .env..."
        cat > .env << EOF
# Database Configuration
DATABASE_URL=postgresql://sofascore:your_password@localhost:5432/sofascore_odds

# Scheduler Configuration
DISCOVERY_INTERVAL_HOURS=2
POLL_INTERVAL_MINUTES=5
TIMEZONE=UTC

# Logging
LOG_LEVEL=INFO
EOF
    fi
fi

# Check if PostgreSQL is installed and running
if ! command -v psql &> /dev/null; then
    echo "🐘 PostgreSQL not found. Installing..."
    sudo apt update
    sudo apt install -y postgresql postgresql-contrib
    
    # Start PostgreSQL service
    sudo systemctl start postgresql
    sudo systemctl enable postgresql
fi

# Create database and user
echo "🗄️  Setting up PostgreSQL database..."
sudo -u postgres psql << EOF
CREATE DATABASE sofascore_odds;
CREATE USER sofascore WITH ENCRYPTED PASSWORD 'your_secure_password';
GRANT ALL PRIVILEGES ON DATABASE sofascore_odds TO sofascore;
ALTER USER sofascore CREATEDB;
\q
EOF

# Update .env with correct database URL
sed -i 's/your_password/your_secure_password/g' .env

# Test database connection
echo "🔍 Testing database connection..."
source venv/bin/activate
python -c "
from database import db_manager
if db_manager.test_connection():
    print('✅ Database connection successful!')
else:
    print('❌ Database connection failed!')
    exit(1)
"

# Create systemd service
echo "⚙️  Setting up systemd service..."
sudo cp sofascore.service /etc/systemd/system/
sudo sed -i "s|/opt/sofascore|$APP_DIR|g" /etc/systemd/system/sofascore.service

# Create system user
if ! id "sofascore" &>/dev/null; then
    echo "👤 Creating system user 'sofascore'..."
    sudo useradd -r -s /bin/false -d $APP_DIR sofascore
fi

# Set proper permissions
sudo chown -R sofascore:sofascore $APP_DIR
sudo chmod +x $APP_DIR/main.py

# Reload systemd and enable service
sudo systemctl daemon-reload
sudo systemctl enable sofascore

# Test the application
echo "🧪 Testing application..."
source venv/bin/activate
python main.py status

# Start the service
echo "🚀 Starting SofaScore service..."
sudo systemctl start sofascore

# Check service status
echo "📊 Service status:"
sudo systemctl status sofascore --no-pager -l

echo ""
echo "✅ Deployment completed successfully!"
echo ""
echo "🌐 Your SofaScore Odds System is now running as a service!"
echo ""
echo "📊 Useful commands:"
echo "   - Check status: sudo systemctl status sofascore"
echo "   - View logs: sudo journalctl -u sofascore -f"
echo "   - Stop service: sudo systemctl stop sofascore"
echo "   - Restart service: sudo systemctl restart sofascore"
echo "   - Check application: cd $APP_DIR && source venv/bin/activate && python main.py status"
echo ""
echo "🔧 Configuration:"
echo "   - Application directory: $APP_DIR"
echo "   - Environment file: $APP_DIR/.env"
echo "   - Logs: $APP_DIR/logs/"
echo "   - Database: PostgreSQL 'sofascore_odds'"
echo ""
echo "⚠️  Important:"
echo "   - Update the database password in .env file"
echo "   - Configure your proxy settings if needed"
echo "   - Set up firewall rules as needed"
echo "   - Consider setting up SSL certificates for production"
