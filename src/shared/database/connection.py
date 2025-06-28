"""
Framework-agnostic database connection management.

This module provides database connection functionality that can be used
by both pipeline and web application contexts without framework dependencies.
"""

from sqlalchemy import create_engine
from typing import Optional


class DatabaseConnection:
    """Framework-agnostic database connection manager."""

    def __init__(self, config):
        """Initialize database connection with configuration.
        
        Args:
            config: Configuration object with database_url, ssl_mode, and get_env_info() method
        """
        self.config = config
        self.database_url = config.database_url
        self.ssl_mode = config.ssl_mode
        self.environment = config.environment
        self.conn = None
        self.engine = None

    def connect(self):
        """Create and return a PostgreSQL database connection."""
        if self.conn is not None:
            return self.conn

        try:
            self.engine = create_engine(
                self.database_url,
                connect_args={"sslmode": self.ssl_mode},
            )

            self.conn = self.engine.connect()

            # Print environment info for debugging
            env_info = self.config.get_env_info()
            print(f"Database running from \033[1m{env_info['environment']}\033[0m environment")
            print(f"Database connection established to: \033[1m{env_info['database_url_host']}\033[0m")
            print(f"SSL Mode: \033[1m{env_info['ssl_mode']}\033[0m")
            print()

            return self.conn
        except Exception as e:
            raise Exception(f"Database connection failed: {str(e)}")

    def close(self):
        """Close the database connection."""
        if self.conn is not None:
            self.conn.close()
            self.conn = None
        if self.engine is not None:
            self.engine.dispose()
            self.engine = None

    def __enter__(self):
        """Context manager entry."""
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()