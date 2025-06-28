"""
Framework-agnostic database query methods.

This module provides database query functionality that can be used
by both pipeline and web application contexts without framework dependencies.
"""

import pandas as pd
from sqlalchemy import text
from typing import Optional, List, Union
from .models import Table


class BaseQueries:
    """Base class for database queries without framework dependencies."""

    def __init__(self, connection):
        """Initialize with a database connection.
        
        Args:
            connection: DatabaseConnection instance
        """
        self.connection = connection

    def get_population_projections_by_fips(self, county_fips: Optional[Union[str, List[str]]] = None) -> pd.DataFrame:
        """
        Get population projections for a specific county by FIPS code

        Parameters:
        -----------
        county_fips : str or list, optional
            County FIPS code(s) to query. If None, returns all counties.

        Returns:
        --------
        df : pandas.DataFrame
            DataFrame containing population projection data
        """
        conn = self.connection.connect()
        try:
            query = "SELECT * FROM county_population_projections"

            # Add COUNTY_FIPS filter if provided
            if county_fips is not None:
                if isinstance(county_fips, list):
                    fips_list = ", ".join(str(fips) for fips in county_fips)
                    query += f" WHERE COUNTY_FIPS IN ({fips_list})"
                else:
                    query += f" WHERE COUNTY_FIPS = {county_fips}"

            # Execute query and return as DataFrame
            df = pd.read_sql(query, conn)
            return df
        except Exception as e:
            raise Exception(f"Error loading population projections: {str(e)}")

    def get_population_timeseries(self, county_fips: Optional[Union[str, List[str]]] = None) -> pd.DataFrame:
        """
        Get population history for a specific county by FIPS code

        Parameters:
        -----------
        county_fips : str or list, optional
            County FIPS code(s) to query. If None, returns all counties.

        Returns:
        --------
        df : pandas.DataFrame
            DataFrame containing population history data
        """
        conn = self.connection.connect()
        try:
            query = "SELECT * FROM timeseries_population"

            # Add COUNTY_FIPS filter if provided
            if county_fips is not None:
                if isinstance(county_fips, list):
                    fips_list = ", ".join(str(fips) for fips in county_fips)
                    query += f" WHERE COUNTY_FIPS IN ({fips_list})"
                else:
                    query += f" WHERE COUNTY_FIPS = {county_fips}"

            # Execute query and return as DataFrame
            df = pd.read_sql(query, conn)
            return df
        except Exception as e:
            raise Exception(f"Error loading historical population counts: {str(e)}")

    def get_timeseries_median_gross_rent(self, county_fips: Optional[Union[str, List[str]]] = None) -> pd.DataFrame:
        """
        Get time series data for median gross rent for the specified county by FIPS code

        Parameters:
        -----------
        county_fips : str or list, optional
            County FIPS code(s) to query. If None, returns all counties.

        Returns:
        --------
        df : pandas.DataFrame
            DataFrame containing median gross rent data (transposed)
        """
        conn = self.connection.connect()
        try:
            query = "SELECT * FROM timeseries_median_gross_rent"

            # Add COUNTY_FIPS filter if provided
            if county_fips is not None:
                if isinstance(county_fips, list):
                    fips_list = ", ".join(str(fips) for fips in county_fips)
                    query += f" WHERE \"COUNTY_FIPS\" IN ({fips_list})"
                else:
                    query += f" WHERE \"COUNTY_FIPS\" = {county_fips}"

            # Execute query and return as DataFrame
            df = pd.read_sql(query, conn).set_index("COUNTY_FIPS")
            return df.T
        except Exception as e:
            raise Exception(f"Error loading historical median gross rent: {str(e)}")

    def get_stat_var(self, table: Table, indicator_name: str, county_fips: str, year: Optional[int] = None) -> pd.DataFrame:
        """
        Get county data from a statistical variable's specified table

        Parameters:
        -----------
        table : Table
            Enum for the table to query in the database.
        indicator_name : str
            Name of the indicator to pull from the table.
        county_fips : str or list
            County FIPS code(s) to query.
        year : int, optional
            Specific year to query. If None, returns all years.

        Returns:
        --------
        df : pandas.DataFrame
            DataFrame containing statistical variable data
        """
        conn = self.connection.connect()
        table_name = table.value

        try:
            # Initialize parameters dictionary
            params = {}

            # Add COUNTY_FIPS filter if provided
            if county_fips is not None:
                if isinstance(county_fips, list):
                    # Create base query
                    query = f'SELECT "YEAR", "{indicator_name}", "COUNTY_FIPS" FROM "{table_name}"'

                    # For multiple counties
                    query += " WHERE \"COUNTY_FIPS\" IN :county_fips"
                    params['county_fips'] = tuple(
                        str(fips) for fips in county_fips)
                else:
                    # Create base query
                    query = f'SELECT "YEAR", "{indicator_name}" FROM "{table_name}"'

                    # For single county
                    query += " WHERE \"COUNTY_FIPS\" = :county_fips"
                    params['county_fips'] = str(county_fips)

                if year:
                    query += f' AND "YEAR" = :year'
                    params['year'] = year

            # Sort the results of the query
            query += f" ORDER BY \"{table_name}\".\"YEAR\" ASC"

            # Convert to SQLAlchemy text object
            sql_query = text(query)

            # Execute query and return as DataFrame
            df = pd.read_sql(sql_query, conn, params=params)

            df.YEAR = pd.to_datetime(df.YEAR, format='%Y').dt.year
            df = df.set_index("YEAR")

            return df
        except Exception as e:
            raise Exception(f"Error loading time series data: {str(e)}")

    def get_county_metadata(self, county_fips: Optional[Union[str, List[str]]] = None) -> pd.DataFrame:
        """
        Get county metadata from the county table

        Parameters:
        -----------
        county_fips : str or list, optional
            County FIPS code(s) to query. If None, returns all counties.

        Returns:
        --------
        df : pandas.DataFrame
            DataFrame containing county metadata
        """
        conn = self.connection.connect()
        try:
            # Start with the base query
            query = f"SELECT * FROM {Table.COUNTY_METADATA.value}"

            # Add COUNTY_FIPS filter if provided
            if county_fips is not None:
                if isinstance(county_fips, list):
                    # Create proper parameter placeholders for IN clause
                    placeholders = ", ".join(
                        f":fips_{i}" for i in range(len(county_fips)))
                    query += f" WHERE \"COUNTY_FIPS\" IN ({placeholders})"

                    # Create a dictionary of parameters
                    params = {f"fips_{i}": fips for i,
                              fips in enumerate(county_fips)}
                else:
                    query += " WHERE \"COUNTY_FIPS\" = :county_fips"
                    params = {'county_fips': county_fips}
            else:
                params = {}

            # Convert to SQLAlchemy text object
            sql_query = text(query)

            # Execute query and return as DataFrame
            df = pd.read_sql(sql_query, conn, params=params)
            return df
        except Exception as e:
            raise Exception(f"Error loading county data: {str(e)}")

    def get_cbsa_counties(self, filter: Optional[str] = None) -> pd.DataFrame:
        """
        Get counties that belong to a metropolitan statistical area (MSA) along with their metadata
        
        Parameters:
        -----------
        filter : str, optional
            Type of CBSA to filter for.
            Valid values: 'metro', 'micro', or None (returns all)
            Default is None.
            
        Returns:
        --------
        df : pandas.DataFrame
            DataFrame containing the counties along with MSA data and county metadata
        """
        conn = self.connection.connect()
        try:
            # Base query with JOIN to get metadata for matching COUNTY_FIPS
            query = f'''
                SELECT 
                    cbsa."CBSA", 
                    cbsa."TYPE",
                    meta.*
                FROM {Table.COUNTY_CBSA_DATA.value} cbsa
                JOIN {Table.COUNTY_METADATA.value} meta
                ON cbsa."COUNTY_FIPS" = meta."COUNTY_FIPS"
            '''

            # Apply filter if provided
            if filter is not None and isinstance(filter, str):
                if filter == 'metro':
                    query += f" WHERE cbsa.\"TYPE\" = 'Metropolitan Statistical Area'"
                elif filter == 'micro':
                    query += f" WHERE cbsa.\"TYPE\" = 'Micropolitan Statistical Area'"

            # Execute query and return as DataFrame
            df = pd.read_sql(query, conn)
            return df
        except Exception as e:
            raise Exception(f"Error loading county CBSA data: {str(e)}")

    def get_projections_by_county(self, county_fips: str) -> pd.DataFrame:
        """
        Get socioeconomic indices for a specific county by FIPS code

        Parameters:
        -----------
        county_fips : str
            County FIPS code to query.

        Returns:
        --------
        df : pandas.DataFrame
            DataFrame containing socioeconomic indices for the specified county
        """
        conn = self.connection.connect()
        try:
            query = text(
                "SELECT * FROM projected_socioeconomic_indices WHERE \"COUNTY_FIPS\" = :county_fips")

            # Execute query with parameter
            df = pd.read_sql(query, conn, params={'county_fips': county_fips})

            # Reset index and drop the old index
            df = df.reset_index(drop=True)
            return df
        except Exception as e:
            raise Exception(f"Error loading socioeconomic indices: {str(e)}")

    def get_table_for_county(self, table: Table, county_fips: str) -> pd.DataFrame:
        """
        Get data from a specific table for a specific county by FIPS code

        Parameters:
        -----------
        table : Table
            Table enum to query.
        county_fips : str
            County FIPS code to query.

        Returns:
        --------
        df : pandas.DataFrame
            DataFrame containing data for the specified county
        """
        conn = self.connection.connect()
        try:
            query = text(
                f"SELECT * FROM {table.value} WHERE \"COUNTY_FIPS\" = :county_fips")

            # Execute query with parameter
            df = pd.read_sql(query, conn, params={'county_fips': county_fips})

            # Reset index and drop the old index
            df = df.reset_index(drop=True)
            return df
        except Exception as e:
            raise Exception(f"Error loading data from {table.value}: {str(e)}")

    def get_index_projections(self, county_fips: str, scenario: str):
        """
        Get index projections for a specific county and scenario

        Parameters:
        -----------
        county_fips : str
            County FIPS code to query.
        scenario : str
            Scenario identifier.

        Returns:
        --------
        pandas.Series
            Series containing projection data for the specified scenario
        """
        conn = self.connection.connect()
        try:
            query = text(
                'SELECT "SCENARIO", "z_STUDENT_TEACHER_RATIO", "z_AVAILABLE_HOUSING_UNITS", "z_UNEMPLOYMENT_RATE" '
                f'FROM {Table.COUNTY_COMBINED_PROJECTIONS.value} '
                'WHERE "COUNTY_FIPS" = :county_fips'
            )
            
            # Execute query with parameter
            df = pd.read_sql(query, conn, params={'county_fips': county_fips})

            # Reset index and drop the old index
            df = df.reset_index(drop=True)
            
            scenario_id = scenario.split("_")[-1]
            df = df[df["SCENARIO"] == scenario_id]

            return df.iloc[0]
        except Exception as e:
            raise Exception(f"Error loading index projections: {str(e)}")