@echo off
REM Climate Migration Dashboard - Web Application Launcher
REM Usage: .\scripts\run_webapp.bat [environment] [port]
REM Environment: dev (default) or prod
REM Port: 8501 (default)

REM Set environment (default to dev if not provided)
if "%1"=="" (
    if "%ENVIRONMENT%"=="" (
        set ENVIRONMENT=dev
    )
) else (
    set ENVIRONMENT=%1
)

REM Set port (default to 8501 if not provided)
if "%2"=="" (
    set PORT=8501
) else (
    set PORT=%2
)

echo ==================================================
echo Starting Climate Migration Dashboard
echo Environment: %ENVIRONMENT%
echo Port: %PORT%
echo ==================================================

REM Check if config can be imported (validates environment setup)
python -c "from src.shared.config import WebAppConfig; WebAppConfig().get_env_info()" || (
    echo Error: Configuration validation failed. Check your .env.%ENVIRONMENT% file.
    exit /b 1
)

REM Check if database is accessible
python -c "from src.dashboard.database import db; db.connect(); print('Database connection verified')" || (
    echo Error: Database connection failed. Ensure PostgreSQL is running.
    exit /b 1
)

echo Starting Streamlit application...
streamlit run src/dashboard/main.py --server.port=%PORT%