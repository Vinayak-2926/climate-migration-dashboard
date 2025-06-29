"""
Web application database client with Streamlit integration.

This module provides database functionality specifically designed for the
Streamlit web application, extending the shared database components with
Streamlit-specific caching and error handling.
"""

import streamlit as st
from typing import Optional, List, Union

from src.shared.database import DatabaseConnection, BaseQueries, Table
from src.shared.config import WebAppConfig


class WebAppDatabase(BaseQueries):
    """Web application database client with Streamlit caching and error handling."""

    def __init__(self, config: Optional[WebAppConfig] = None):
        """Initialize web app database client.
        
        Args:
            config: WebAppConfig instance. If None, creates a new one.
        """
        if config is None:
            config = WebAppConfig()
        
        self.config = config
        self.connection = DatabaseConnection(config)
        super().__init__(self.connection)

    def connect(self):
        """Connect to the database and return connection."""
        return self.connection.connect()

    def close(self):
        """Close the database connection."""
        self.connection.close()

    # Override all BaseQueries methods with Streamlit caching
    
    @st.cache_data
    def get_population_projections_by_fips(_self, county_fips: Optional[Union[str, List[str]]] = None):
        """Get population projections with Streamlit caching."""
        try:
            return super(WebAppDatabase, _self).get_population_projections_by_fips(county_fips)
        except Exception as e:
            st.error(f"Error loading population projections: {str(e)}")
            st.stop()

    @st.cache_data
    def get_population_timeseries(_self, county_fips: Optional[Union[str, List[str]]] = None):
        """Get population timeseries with Streamlit caching."""
        try:
            return super(WebAppDatabase, _self).get_population_timeseries(county_fips)
        except Exception as e:
            st.error(f"Error loading historical population counts: {str(e)}")
            st.stop()

    @st.cache_data
    def get_timeseries_median_gross_rent(_self, county_fips: Optional[Union[str, List[str]]] = None):
        """Get timeseries median gross rent with Streamlit caching."""
        try:
            return super(WebAppDatabase, _self).get_timeseries_median_gross_rent(county_fips)
        except Exception as e:
            st.error(f"Error loading historical median gross rent: {str(e)}")
            st.stop()

    @st.cache_data
    def get_stat_var(_self, table: Table, indicator_name: str, county_fips: str, year: Optional[int] = None):
        """Get statistical variable with Streamlit caching."""
        try:
            return super(WebAppDatabase, _self).get_stat_var(table, indicator_name, county_fips, year)
        except Exception as e:
            st.error(f"Error loading time series data: {str(e)}")
            st.stop()

    @st.cache_data
    def get_county_metadata(_self, county_fips: Optional[Union[str, List[str]]] = None):
        """Get county metadata with Streamlit caching."""
        try:
            return super(WebAppDatabase, _self).get_county_metadata(county_fips)
        except Exception as e:
            st.error(f"Error loading county data: {str(e)}")
            st.stop()

    @st.cache_data
    def get_cbsa_counties(_self, filter: Optional[str] = None):
        """Get CBSA counties with Streamlit caching."""
        try:
            return super(WebAppDatabase, _self).get_cbsa_counties(filter)
        except Exception as e:
            st.error(f"Error loading county CBSA data: {str(e)}")
            st.stop()

    @st.cache_data
    def get_projections_by_county(_self, county_fips: str):
        """Get projections by county with Streamlit caching."""
        try:
            return super(WebAppDatabase, _self).get_projections_by_county(county_fips)
        except Exception as e:
            st.error(f"Error loading socioeconomic indices: {str(e)}")
            st.stop()

    @st.cache_data
    def get_table_for_county(_self, table: Table, county_fips: str):
        """Get table data for county with Streamlit caching."""
        try:
            return super(WebAppDatabase, _self).get_table_for_county(table, county_fips)
        except Exception as e:
            st.error(f"Error loading data from {table.value}: {str(e)}")
            st.stop()

    @st.cache_data
    def get_index_projections(_self, county_fips: str, scenario: str):
        """Get index projections with Streamlit caching."""
        try:
            return super(WebAppDatabase, _self).get_index_projections(county_fips, scenario)
        except Exception as e:
            st.error(f"Error loading index projections: {str(e)}")
            st.stop()


# Singleton pattern for webapp database
_webapp_db_instance = None

def get_webapp_db() -> WebAppDatabase:
    """
    Get a singleton WebAppDatabase instance.
    
    Returns:
    --------
    WebAppDatabase
        Configured webapp database client
    """
    global _webapp_db_instance
    if _webapp_db_instance is None:
        _webapp_db_instance = WebAppDatabase()
    return _webapp_db_instance


def get_db_connection():
    """
    For backwards compatibility - returns the database connection.
    This maintains the same interface as the old app/src/db.py
    """
    return get_webapp_db().connect()


# Create a singleton instance for easy import (maintains backward compatibility)
db = get_webapp_db()