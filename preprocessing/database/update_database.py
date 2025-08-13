import os
from pathlib import Path
import pandas as pd
from sqlalchemy.engine.base import Connection

from utils.helpers import get_db_connection


def upload_csvs_to_postgres(
    folder_path: str, db_con: Connection, schema: str = "public"
) -> None:
    """
    Uploads all CSV files in a folder to PostgreSQL.
    - Uses the filename (without extension) as the table name
    - Replaces existing tables with fresh data
    """
    for filename in os.listdir(folder_path):
        if filename.endswith(".csv"):
            # Extract table name from filename
            table_name = os.path.splitext(filename)[
                0
            ].lower()  # Lowercase for consistency
            filepath = os.path.join(folder_path, filename)

            print(f"Processing {filename}...")
            # Read CSV
            df = pd.read_csv(filepath, dtype={"COUNTY_FIPS": str})

            try:
                # Use a transaction to ensure atomicity for each file
                with db_con.begin() as transaction:
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
                print(f"  ✔ Uploaded {filename} ➔ {schema}.{table_name}")
            except Exception as e:
                print(f"  ✘ Error uploading {filename}: {e}")
                # The 'with db_con.begin()' context manager handles the rollback automatically
                continue


if __name__ == "__main__":
    db_con = None  # Initialize to None
    try:
        db_con = get_db_connection()
        data_folder = Path("./data/processed")

        print("\nUploading cleaned data...")
        upload_csvs_to_postgres(data_folder / "cleaned_data", db_con)

        print("\nUploading projected data...")
        upload_csvs_to_postgres(data_folder / "projected_data", db_con)

        print("\n✅ All data upload tasks complete!")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")

    finally:
        if db_con is not None:
            db_con.close()
            print("\nDatabase connection closed.")