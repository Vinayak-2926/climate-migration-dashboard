# Climate Migration Dashboard

An interactive dashboard for analyzing climate-driven migration patterns across the United States.

## Prerequisites

- Python 3.8 or higher
- Docker Desktop
- US Census API key
- PostgreSQL

## Setup Instructions

### Windows

1. Clone the repository:
   ```
   git clone [repository-url]
   cd climate-migration-dashboard
   ```

2. Set up Python environment:
   ```
   python -m venv .venv
   .venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. Configure Docker:
   - Install Docker Desktop
   - Start Docker containers: `docker-compose up -d`
   - Stop containers when done: `docker-compose down`

4. Configure environment variables:
   - Create `.env` file in root directory
   - Add the following variables:
     ```
     US_CENSUS_API_KEY=your_api_key_here
     DATABASE_URL=your_postgres_connection_string
     ```

5. Set up secrets:
   ```
   mkdir secrets
   echo "your_postgres_password" > secrets\postgres_password.txt
   ```

6. Prepare data:
   - Ensure `.\data\raw` directory contains required manual downloads:
     - `monthly_job_openings_xlsx_data`
     - `decennial_county_population_data_1900_1990.csv`

7. Run data pipeline:
   - Windows: `.\scripts\pipeline.bat`
   - MacOS/Linux: `./scripts/pipeline.sh`

8. Launch dashboard:
   ```
   # Using deployment script (recommended)
   ./scripts/run_webapp.sh           # MacOS/Linux
   .\scripts\run_webapp.bat          # Windows
   
   # Or directly with Streamlit
   streamlit run src/dashboard/main.py
   ```

## Architecture

This project uses a modular architecture with shared components:

### Directory Structure
- `src/shared/` - Framework-agnostic shared components
  - `config/` - Configuration management for different environments
  - `database/` - Database connections, models, and base queries
  - `utils/` - Common utility functions
- `src/pipeline/` - Data processing pipeline
  - `acquisition/` - Data collection scripts
  - `cleaning/` - Data preprocessing
  - `database/` - Pipeline-specific database operations
  - `scripts/` - Pipeline execution scripts
- `src/dashboard/` - Streamlit web application
  - `components/` - UI components and visualizations
  - `database/` - Web app database client with caching
  - `pages/` - Streamlit page definitions
  - `main.py` - Application entry point

### Key Features
- **Separation of Concerns**: Pipeline and web app use separate database clients with context-specific functionality
- **Shared Components**: Common database models and queries are reused across pipeline and web app
- **Environment Configuration**: Supports dev/prod environments with separate configurations
- **Streamlit Caching**: Web app database client includes `@st.cache_data` decorators for performance

## Data Structure

### Raw Data Sources

#### Automatically Downloaded
- Counties data
- Economic indicators
- Education statistics
- Housing metrics
- Population data
- State crime statistics
- State-level data

#### Manual Downloads Required
- Monthly job openings (XLSX format)
- Historical county population data (1900-1990)

### Processed Data

#### Combined Datasets
- `cleaned_economic_data.csv`
- `cleaned_education_data.csv`
- `cleaned_housing_data.csv`
- `cleaned_crime_data.csv`
- `cleaned_job_openings_data.csv`
- `socioeconomic_indices.csv`
- `socioeconomic_indices_rankings.csv`
- `timeseries_population.csv`

#### Geographic Data
- `counties_with_geometry` (yearly data)

#### Projections
- `county_population_projections.csv`

## Troubleshooting

If you encounter issues:

- Ensure Docker Desktop is running
- Verify all environment variables are set correctly
- Check if required data files exist in `.\data\raw`
- Confirm PostgreSQL connection string is valid
