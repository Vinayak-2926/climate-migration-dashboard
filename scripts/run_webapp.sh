#!/bin/bash

# Climate Migration Dashboard - Web Application Launcher
# Usage: ./scripts/run_webapp.sh [environment] [port]
# Environment: dev (default) or prod
# Port: 8501 (default)

# Set environment (default to dev if not provided)
export ENVIRONMENT=${1:-${ENVIRONMENT:-dev}}
export PORT=${2:-8501}

echo "=================================================="
echo "Starting Climate Migration Dashboard"
echo "Environment: $ENVIRONMENT"
echo "Port: $PORT"
echo "=================================================="

# Check if config can be imported (validates environment setup)
python -c "from src.shared.config import WebAppConfig; WebAppConfig().get_env_info()" || {
    echo "Error: Configuration validation failed. Check your .env.$ENVIRONMENT file."
    exit 1
}

# Check if database is accessible
python -c "from src.dashboard.database import db; db.connect(); print('Database connection verified')" || {
    echo "Error: Database connection failed. Ensure PostgreSQL is running."
    exit 1
}

echo "Starting Streamlit application..."
streamlit run src/dashboard/main.py --server.port=$PORT