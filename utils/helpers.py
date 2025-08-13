import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
from urllib.parse import urlparse

# Load environment-specific .env file
ENVIRONMENT = os.getenv(
    "ENVIRONMENT", "prod"
)  # Default to dev, change to prod when deploying
env_file = f".env.{ENVIRONMENT}"
load_dotenv(env_file, override=True)

# Fix Heroku connection string
DATABASE_URL = os.getenv("DATABASE_URL")

# Set SSL mode based on environment
SSL_MODE = "require" if ENVIRONMENT == "prod" else "disable"

# Parse the database URL to extract components
parsed_url = urlparse(DATABASE_URL)
db_host = parsed_url.hostname
db_port = parsed_url.port
db_user = parsed_url.username
db_name = parsed_url.path.lstrip('/')

# Print configuration
print("--- Database Configuration (helpers.py) ---")
print(f"  Environment: {ENVIRONMENT.upper()}")
print(f"  Host: {db_host}")
print(f"  Port: {db_port}")
print(f"  User: {db_user}")
print(f"  Database: {db_name}")
print(f"  SSL Mode: {SSL_MODE}")
print("------------------------------------------")
print()


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
