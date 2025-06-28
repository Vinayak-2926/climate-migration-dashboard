#!/bin/bash

# Climate Migration Dashboard Data Pipeline
# Usage: ./scripts/pipeline.sh [environment]
# Environment: dev (default) or prod

# Set environment (default to dev if not provided)
export ENVIRONMENT=${1:-${ENVIRONMENT:-dev}}

echo "=================================================="
echo "Starting data pipeline for: $ENVIRONMENT environment"
echo "=================================================="

# Check if config can be imported (validates environment setup)
python -c "from src.shared.config import PipelineConfig; PipelineConfig().get_env_info()" || {
    echo "Error: Configuration validation failed. Check your .env.$ENVIRONMENT file."
    exit 1
}

# Run the new pipeline
python src/pipeline/scripts/run_acquisition.py && \
python src/pipeline/scripts/run_cleaning.py && \
python -m preprocessing.analysis.historical_population && \
python -m preprocessing.analysis.population_forecasting && \
python -m preprocessing.cleaning.clean_data && \
python -m preprocessing.analysis.indicator_forecasting && \
python src/pipeline/scripts/run_database_update.py

if [ $? -eq 0 ]; then
    echo "=================================================="
    echo "Pipeline completed successfully for $ENVIRONMENT environment!"
    echo "=================================================="
else
    echo "=================================================="
    echo "Pipeline failed for $ENVIRONMENT environment!"
    echo "=================================================="
    exit 1
fi

read -p "Press Enter to continue..."