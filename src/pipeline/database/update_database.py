"""
Database update module for the data processing pipeline.

This module handles uploading processed CSV data to the PostgreSQL database
using the new pipeline database client with improved error handling and
bulk upload capabilities.
"""

from pathlib import Path
from .client import PipelineDatabase


def main():
    """Main function to upload all processed data to PostgreSQL."""
    data_folder = Path("./data/processed")
    
    # Use context manager for automatic connection cleanup
    with PipelineDatabase() as db:
        print("\n" + "="*50)
        print("📊 UPLOADING DATA TO POSTGRESQL")
        print("="*50)
        
        # Upload cleaned data
        cleaned_data_path = data_folder / "cleaned_data"
        if cleaned_data_path.exists():
            print(f"\n📁 Uploading cleaned data from {cleaned_data_path}")
            db.bulk_upload_directory(cleaned_data_path, schema="public")
        else:
            print(f"⚠️  Cleaned data directory not found: {cleaned_data_path}")

        # Upload projected data
        projected_data_path = data_folder / "projected_data"
        if projected_data_path.exists():
            print(f"\n📁 Uploading projected data from {projected_data_path}")
            db.bulk_upload_directory(projected_data_path, schema="public")
        else:
            print(f"⚠️  Projected data directory not found: {projected_data_path}")

        print("\n" + "="*50)
        print("🎉 ALL DATA UPLOADED TO POSTGRESQL!")
        print("="*50)


if __name__ == "__main__":
    main()