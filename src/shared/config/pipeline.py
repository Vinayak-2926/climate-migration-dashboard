"""
Pipeline-specific configuration management.

This module extends the base configuration with settings and validations
specific to the data processing pipeline.
"""

import os
from .base import BaseConfig


class PipelineConfig(BaseConfig):
    """Configuration manager for data processing pipeline."""

    def _validate_required_vars(self) -> None:
        """Validate that all required environment variables for pipeline are present."""
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
    def us_census_api_key(self) -> str:
        """Get the US Census API key."""
        return os.getenv("US_CENSUS_API_KEY")

    def get_env_info(self) -> dict:
        """Get environment information for debugging with pipeline-specific details."""
        info = super().get_env_info()
        info["us_census_api_key_present"] = bool(self.us_census_api_key)
        return info