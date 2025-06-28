"""
Web application-specific configuration management.

This module extends the base configuration with settings and validations
specific to the Streamlit web application.
"""

import os
from .base import BaseConfig


class WebAppConfig(BaseConfig):
    """Configuration manager for Streamlit web application."""

    def _validate_required_vars(self) -> None:
        """Validate that all required environment variables for web app are present."""
        required_vars = ["DATABASE_URL"]
        missing_vars = []

        for var in required_vars:
            if not os.getenv(var):
                missing_vars.append(var)

        if missing_vars:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing_vars)}\n"
                f"Please check your .env.{self.environment} file."
            )

    def get_env_info(self) -> dict:
        """Get environment information for debugging with web app-specific details."""
        info = super().get_env_info()
        info["debug_mode"] = self.debug
        return info