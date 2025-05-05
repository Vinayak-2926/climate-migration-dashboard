import os
from pathlib import Path
import pandas as pd

from utils.helpers import get_db_connection

db_con = get_db_connection()


def upload_csvs_to_postgres(folder_path: str, schema: str = "public") -> None:
    """
    Uploads all CSV files in a folder (including subdirectories) to PostgreSQL.
    - Uses the filename (without extension) as the table name
    - Replaces existing tables with fresh data
    """
    for root, _, files in os.walk(folder_path):
        for filename in files:
            if filename.endswith(".csv"):
                # Extract table name from filename
                table_name = os.path.splitext(filename)[
                    0
                ].lower()  # Lowercase for consistency
                filepath = os.path.join(root, filename)

                # Read CSV
                df = pd.read_csv(filepath, dtype={"COUNTY_FIPS": str})

                try:
                    # Upload to PostgreSQL
                    df.to_sql(
                        name=table_name,
                        con=db_con,
                        schema=schema,
                        if_exists="replace",  # Overwrite existing data
                        index=False,
                        method="multi",  # Batch insert for speed
                        chunksize=1000,
                    )
                    print(f"Uploaded {filename} ➔ {schema}.{table_name}")
                except Exception as e:
                    print(f"Error uploading {filename}: {e}")
                    continue


if __name__ == "__main__":
    data_folder = Path("./data/processed")

    print("\nUploading cleaned data...")
    upload_csvs_to_postgres(data_folder / "cleaned_data")

    print("\nUploading projected data...")
    upload_csvs_to_postgres(data_folder / "projected_data")

    print("\nAll data uploaded to PostgreSQL!")
