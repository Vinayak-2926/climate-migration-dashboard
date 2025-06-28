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
python -c "from config import get_config; get_config().get_env_info()" || {
    echo "Error: Configuration validation failed. Check your .env.$ENVIRONMENT file."
    exit 1
}

# Run the pipeline
python -m preprocessing.acquisition.download_counties && \
python -m preprocessing.acquisition.download_raw_data && \
python -m preprocessing.cleaning.convert_xlsx_to_csvs && \
python -m preprocessing.analysis.historical_population && \
python -m preprocessing.analysis.population_forecasting && \
python -m preprocessing.cleaning.clean_data && \
python -m preprocessing.analysis.indicator_forecasting && \
python -m preprocessing.database.update_database

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