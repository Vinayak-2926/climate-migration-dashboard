from sqlalchemy import create_engine
from config import get_config

# Get configuration from centralized config module
config = get_config()
DATABASE_URL = config.database_url
SSL_MODE = config.ssl_mode


def get_db_connection():
    """Create and return a PostgreSQL database connection"""
    try:
        engine = create_engine(
            DATABASE_URL,
            connect_args={"sslmode": SSL_MODE},
        )
        conn = engine.connect()

        # Print environment info for debugging
        env_info = config.get_env_info()
        print(f"Pipeline connecting to \033[1m{env_info['environment']}\033[0m environment")
        print(f"Database host: \033[1m{env_info['database_url_host']}\033[0m")

        return conn
    except Exception as e:
        raise Exception(f"Database connection failed: {str(e)}")
