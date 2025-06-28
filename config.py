"""
Centralized configuration management for the Climate Migration Dashboard.

This module handles environment-specific configuration loading and provides
a single source of truth for environment variables across the application.
"""

import os
from pathlib import Path
from dotenv import load_dotenv


class Config:
    """Configuration manager that handles environment-specific settings."""

    def __init__(self):
        self.environment = self._determine_environment()
        self._load_environment_file()
        self._validate_required_vars()

    def _determine_environment(self) -> str:
        """Determine the current environment, defaulting to 'dev' for safety."""
        env = os.getenv("ENVIRONMENT", "dev").lower()
        valid_environments = ["dev", "prod"]

        if env not in valid_environments:
            print(f"Warning: Invalid environment '{env}'. Defaulting to 'dev'.")
            env = "dev"

        return env

    def _load_environment_file(self) -> None:
        """Load the appropriate .env file based on the environment."""
        project_root = Path(__file__).parent
        env_file = project_root / f".env.{self.environment}"

        if env_file.exists():
            load_dotenv(env_file, override=True)
            print(f"Loaded environment configuration from {env_file}")
        else:
            # Fallback to .env if specific environment file doesn't exist
            fallback_file = project_root / ".env"
            if fallback_file.exists():
                load_dotenv(fallback_file, override=True)
                print(f"Warning: {env_file} not found. Using fallback {fallback_file}")
            else:
                raise FileNotFoundError(
                    f"No environment file found. Expected {env_file} or {fallback_file}"
                )

    def _validate_required_vars(self) -> None:
        """Validate that all required environment variables are present."""
        required_vars = ["DATABASE_URL", "US_CENSUS_API_KEY"]
        missing_vars = []

        for var in required_vars:
            if not os.getenv(var):
                missing_vars.append(var)

        if missing_vars:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing_vars)}\n"
                f"Please check your .env.{self.environment} file."
            )

    @property
    def database_url(self) -> str:
        """Get the database URL, fixing Heroku postgres:// scheme if needed."""
        url = os.getenv("DATABASE_URL")
        if url and url.startswith("postgres://"):
            return url.replace("postgres://", "postgresql://", 1)
        return url

    @property
    def us_census_api_key(self) -> str:
        """Get the US Census API key."""
        return os.getenv("US_CENSUS_API_KEY")

    @property
    def ssl_mode(self) -> str:
        """Get the appropriate SSL mode based on environment."""
        return "require" if self.environment == "prod" else "disable"

    @property
    def debug(self) -> bool:
        """Get debug mode setting."""
        return os.getenv("DEBUG", "false").lower() in ("true", "1", "yes")

    @property
    def log_level(self) -> str:
        """Get the log level setting."""
        return os.getenv("LOG_LEVEL", "info").upper()

    def get_env_info(self) -> dict:
        """Get environment information for debugging."""
        db_host = "localhost"
        if "@" in self.database_url:
            db_host = self.database_url.split("@")[-1].split("/")[0]
        return {
            "environment": self.environment,
            "database_url_host": db_host,
            "ssl_mode": self.ssl_mode,
            "debug": self.debug,
            "log_level": self.log_level
        }


# Create a global config instance
config = Config()

# For backwards compatibility, export commonly used values
ENVIRONMENT = config.environment
DATABASE_URL = config.database_url
US_CENSUS_API_KEY = config.us_census_api_key
SSL_MODE = config.ssl_mode


def get_config() -> Config:
    """Get the global configuration instance."""
    return config


def print_env_info() -> None:
    """Print current environment information for debugging."""
    info = config.get_env_info()
    print("\n" + "="*50)
    print("ENVIRONMENT CONFIGURATION")
    print("="*50)
    for key, value in info.items():
        print(f"{key.upper()}: {value}")
    print("="*50 + "\n")
