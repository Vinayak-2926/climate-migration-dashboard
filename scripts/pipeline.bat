@echo off

REM Climate Migration Dashboard Data Pipeline
REM Usage: .\scripts\pipeline.bat [environment]
REM Environment: dev (default) or prod

REM Set environment (default to dev if not provided)
if "%1"=="" (
    if "%ENVIRONMENT%"=="" (
        set ENVIRONMENT=dev
    )
) else (
    set ENVIRONMENT=%1
)

echo ==================================================
echo Starting data pipeline for: %ENVIRONMENT% environment
echo ==================================================

REM Check if config can be imported (validates environment setup)
python -c "from config import get_config; get_config().get_env_info()" || (
    echo Error: Configuration validation failed. Check your .env.%ENVIRONMENT% file.
    pause
    exit /b 1
)

REM Run the pipeline
python -m preprocessing.acquisition.download_counties && ^
python -m preprocessing.acquisition.download_raw_data && ^
python -m preprocessing.cleaning.convert_xlsx_to_csvs && ^
python -m preprocessing.analysis.historical_population && ^
python -m preprocessing.analysis.population_forecasting && ^
python -m preprocessing.cleaning.clean_data && ^
python -m preprocessing.analysis.indicator_forecasting && ^
python -m preprocessing.database.update_database

if %errorlevel% equ 0 (
    echo ==================================================
    echo Pipeline completed successfully for %ENVIRONMENT% environment!
    echo ==================================================
) else (
    echo ==================================================
    echo Pipeline failed for %ENVIRONMENT% environment!
    echo ==================================================
    pause
    exit /b 1
)

pause