"""
Database models and table definitions shared across pipeline and web application.
"""

from enum import Enum


class Table(Enum):
    """Enumeration of database table names."""
    
    # County table
    COUNTY_METADATA = "county"

    # County data tables
    COUNTY_HOUSING_DATA = "cleaned_housing_data"
    COUNTY_ECONOMIC_DATA = "cleaned_economic_data"
    COUNTY_EDUCATION_DATA = "cleaned_education_data"
    COUNTY_CRIME_DATA = "cleaned_crime_data"
    COUNTY_FEMA_DATA = "cleaned_fema_nri_data"
    COUNTY_CBSA_DATA = "cleaned_cbsa_data"
    COUNTY_JOB_OPENING_DATA = "cleaned_job_openings_data"
    COUNTY_SOCIOECONOMIC_INDEX_DATA = "socioeconomic_indices"
    COUNTY_SOCIOECONOMIC_RANKING_DATA = "socioeconomic_indices_rankings"
    COUNTY_PROJECTED_INDICES = "projected_socioeconomic_indices"

    # Combined projections
    COUNTY_COMBINED_PROJECTIONS = "combined_2065_data"

    # Population related tables
    POPULATION_HISTORY = "timeseries_population"
    POPULATION_PROJECTIONS = "county_population_projections"