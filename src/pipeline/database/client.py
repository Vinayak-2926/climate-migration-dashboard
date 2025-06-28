"""
Pipeline-specific database client.

This module provides database functionality specifically designed for the
data processing pipeline, extending the shared database components with
pipeline-specific operations like bulk uploads and data processing tasks.
"""

import os
import pandas as pd
from pathlib import Path
from typing import Optional, Union, List

from src.shared.database import DatabaseConnection, BaseQueries, Table
from src.shared.config import PipelineConfig


class PipelineDatabase(BaseQueries):
    """Pipeline-specific database client with bulk operations and data processing features."""

    def __init__(self, config: Optional[PipelineConfig] = None):
        """Initialize pipeline database client.
        
        Args:
            config: PipelineConfig instance. If None, creates a new one.
        """
        if config is None:
            config = PipelineConfig()
        
        self.config = config
        self.connection = DatabaseConnection(config)
        super().__init__(self.connection)

    def connect(self):
        """Connect to the database and return connection."""
        return self.connection.connect()

    def close(self):
        """Close the database connection."""
        self.connection.close()

    def bulk_upload_csv(self, file_path: Union[str, Path], table_name: str, 
                       schema: str = "public", if_exists: str = "replace",
                       chunksize: int = 1000) -> None:
        """
        Upload a CSV file to PostgreSQL table with optimized bulk insert.

        Parameters:
        -----------
        file_path : str or Path
            Path to the CSV file to upload
        table_name : str
            Name of the target table in the database
        schema : str
            Database schema name (default: "public")
        if_exists : str
            How to behave if the table already exists: "fail", "replace", or "append"
        chunksize : int
            Number of rows to insert at a time for memory efficiency
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {file_path}")

        # Read CSV with COUNTY_FIPS as string to preserve leading zeros
        df = pd.read_csv(file_path, dtype={'COUNTY_FIPS': str})
        
        # Connect to database
        conn = self.connect()
        
        try:
            # Upload to PostgreSQL
            df.to_sql(
                name=table_name.lower(),  # Lowercase for consistency
                con=conn,
                schema=schema,
                if_exists=if_exists,
                index=False,
                method="multi",  # Batch insert for speed
                chunksize=chunksize,
            )
            print(f"✅ Uploaded {file_path.name} → {schema}.{table_name.lower()} ({len(df)} rows)")
            
        except Exception as e:
            print(f"❌ Error uploading {file_path.name}: {e}")
            raise
        
    def bulk_upload_directory(self, directory_path: Union[str, Path], 
                             schema: str = "public", file_pattern: str = "*.csv") -> None:
        """
        Upload all CSV files in a directory to PostgreSQL.

        Parameters:
        -----------
        directory_path : str or Path
            Path to directory containing CSV files
        schema : str
            Database schema name (default: "public")
        file_pattern : str
            Glob pattern for files to upload (default: "*.csv")
        """
        directory_path = Path(directory_path)
        
        if not directory_path.exists():
            raise FileNotFoundError(f"Directory not found: {directory_path}")

        # Find all matching files
        csv_files = list(directory_path.glob(file_pattern))
        
        if not csv_files:
            print(f"⚠️  No files matching '{file_pattern}' found in {directory_path}")
            return

        print(f"📁 Uploading {len(csv_files)} files from {directory_path}")
        
        successful_uploads = 0
        failed_uploads = 0
        
        for file_path in csv_files:
            try:
                # Extract table name from filename (remove extension)
                table_name = file_path.stem.lower()
                
                self.bulk_upload_csv(
                    file_path=file_path,
                    table_name=table_name,
                    schema=schema
                )
                successful_uploads += 1
                
            except Exception as e:
                print(f"❌ Failed to upload {file_path.name}: {e}")
                failed_uploads += 1
                continue

        print(f"📊 Upload complete: {successful_uploads} successful, {failed_uploads} failed")

    def execute_sql_file(self, sql_file_path: Union[str, Path]) -> None:
        """
        Execute SQL commands from a file.

        Parameters:
        -----------
        sql_file_path : str or Path
            Path to SQL file to execute
        """
        sql_file_path = Path(sql_file_path)
        
        if not sql_file_path.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_file_path}")

        # Read SQL file
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()

        # Connect and execute
        conn = self.connect()
        
        try:
            # Split by semicolons and execute each statement
            statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
            
            for i, statement in enumerate(statements, 1):
                conn.execute(statement)
                print(f"✅ Executed SQL statement {i}/{len(statements)}")
                
            print(f"📜 Successfully executed {sql_file_path.name}")
            
        except Exception as e:
            print(f"❌ Error executing SQL file {sql_file_path.name}: {e}")
            raise

    def verify_table_exists(self, table_name: str, schema: str = "public") -> bool:
        """
        Check if a table exists in the database.

        Parameters:
        -----------
        table_name : str
            Name of the table to check
        schema : str
            Database schema name (default: "public")

        Returns:
        --------
        bool
            True if table exists, False otherwise
        """
        conn = self.connect()
        
        try:
            from sqlalchemy import text
            query = text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = :schema 
                    AND table_name = :table_name
                );
            """)
            
            result = conn.execute(query, {"schema": schema, "table_name": table_name.lower()})
            return result.fetchone()[0]
            
        except Exception as e:
            print(f"❌ Error checking table existence: {e}")
            return False

    def get_table_row_count(self, table_name: str, schema: str = "public") -> int:
        """
        Get the number of rows in a table.

        Parameters:
        -----------
        table_name : str
            Name of the table to count
        schema : str
            Database schema name (default: "public")

        Returns:
        --------
        int
            Number of rows in the table
        """
        conn = self.connect()
        
        try:
            from sqlalchemy import text
            query = text(f'SELECT COUNT(*) FROM "{schema}"."{table_name.lower()}"')
            result = conn.execute(query)
            return result.fetchone()[0]
            
        except Exception as e:
            print(f"❌ Error counting rows in {table_name}: {e}")
            return 0

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


# Convenience function for creating a pipeline database instance
def get_pipeline_db() -> PipelineDatabase:
    """
    Create and return a PipelineDatabase instance.
    
    Returns:
    --------
    PipelineDatabase
        Configured pipeline database client
    """
    return PipelineDatabase()