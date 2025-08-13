import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment-specific .env file
# Default to dev, change to prod when deploying
ENVIRONMENT = os.getenv("ENVIRONMENT", "dev")
env_file = f".env.{ENVIRONMENT}"
load_dotenv(env_file, override=True)

# Fix Heroku connection string
DATABASE_URL = os.getenv("DATABASE_URL")

# Set SSL mode based on environment
SSL_MODE = "require" if ENVIRONMENT == "prod" else "disable"


def get_db_connection():
    """Create and return a PostgreSQL database connection"""
    try:
        engine = create_engine(
            DATABASE_URL.replace("postgres://", "postgresql://", 1),
            connect_args={"sslmode": SSL_MODE},
        )
        conn = engine.connect()
        return conn
    except Exception as e:
        raise Exception(f"Database connection failed: {str(e)}")
