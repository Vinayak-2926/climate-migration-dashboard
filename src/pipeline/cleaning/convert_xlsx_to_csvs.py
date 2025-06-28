"""
XLSX to CSV conversion module for the data processing pipeline.

This module converts Excel files (XLSX/XLS) to CSV format for easier processing
in the data pipeline. It handles job openings data and public school data with
proper error handling and data validation.
"""

import pandas as pd
import os
from pathlib import Path
import sys
import warnings

from src.shared.utils import ensure_directory_exists

warnings.filterwarnings('ignore', category=UserWarning)


class XLSXToCSVConverter:
    """Converter for Excel files to CSV format with data validation."""

    def __init__(self):
        """Initialize the converter with directory setup."""
        self.setup_directories()

    def setup_directories(self):
        """Set up input and output directories for data conversion."""
        try:
            # Use project root data directory
            base_dir = Path("./data")
            raw_dir = base_dir / "raw"
            
            # Job openings directories
            self.job_input_dir = raw_dir / "monthly_job_openings_xlsx_data"
            self.job_output_dir = ensure_directory_exists(raw_dir / "monthly_job_openings_csvs_data")
            
            # Public school directories
            self.school_input_dir = raw_dir / "public_school_xlsx_data"
            self.school_output_dir = ensure_directory_exists(raw_dir / "public_school_csvs_data")
            
            print(f"📁 Job openings input: {self.job_input_dir}")
            print(f"📁 Job openings output: {self.job_output_dir}")
            print(f"📁 Public school input: {self.school_input_dir}")
            print(f"📁 Public school output: {self.school_output_dir}")
            
        except Exception as e:
            print(f"❌ Error setting up directories: {e}")
            raise

    # -------------- Job Openings Processing Logic --------------

    def extract_state_fips(self, series_id: str) -> str:
        """Extract the state FIPS code from the Series ID."""
        if len(series_id) >= 13:
            return series_id[9:11]
        return None

    def process_job_openings_file(self, file_path: Path):
        """Process a single job openings Excel file and return the state FIPS code and data."""
        try:
            # Read the Series ID line
            first_row = pd.read_excel(file_path, header=None)
            series_id_line = first_row.iloc[3, 1]

            # Validate Series ID format
            if not str(series_id_line).startswith("JTS"):
                print(f"⚠️  Skipping file {file_path.name}: Invalid Series ID format")
                return None, None

            # Extract state FIPS code
            state_fips = self.extract_state_fips(series_id_line)
            if not state_fips:
                print(f"⚠️  Skipping file {file_path.name}: Unable to extract FIPS code")
                return None, None

            # Read the data table
            df = pd.read_excel(file_path, skiprows=13)

            # Validate required columns
            required_columns = [
                "Year", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
            ]
            if not set(required_columns).issubset(df.columns):
                print(f"⚠️  Skipping file {file_path.name}: Missing required columns")
                return None, None

            print(f"✅ Processed {file_path.name} - State FIPS: {state_fips}")
            return state_fips, df

        except Exception as e:
            print(f"❌ Error processing {file_path.name}: {str(e)}")
            return None, None

    def extract_yearly_data(self, state_fips: str, df: pd.DataFrame) -> dict:
        """Extract data by year from a dataframe."""
        yearly_data = {}

        for _, row in df.iterrows():
            if pd.isna(row["Year"]):
                continue

            year = int(row["Year"])

            # Extract monthly data
            monthly_columns = [
                "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
            ]

            # Check if any monthly data is missing
            if any(pd.isna(row[month]) for month in monthly_columns):
                print(f"⚠️  Skipping year {year} for state {state_fips}: incomplete monthly data")
                continue

            monthly_data = {month: row[month] for month in monthly_columns}

            # Initialize the year's data structure if not already present
            if year not in yearly_data:
                yearly_data[year] = {}

            # Add this state's data to the year
            yearly_data[year][state_fips] = monthly_data

        return yearly_data

    def create_job_openings_csvs(self, yearly_data: dict, output_dir: Path):
        """Create CSV files for each year's job openings data."""
        for year, states_data in yearly_data.items():
            if not states_data:
                print(f"⚠️  No data for year {year}, skipping CSV creation")
                continue

            # Create a DataFrame for this year
            df_data = []
            for state_fips, months in states_data.items():
                row_data = {"STATE": state_fips}
                row_data.update(months)
                df_data.append(row_data)

            # Create DataFrame from the collected data
            df_year = pd.DataFrame(df_data)

            # Set FIPS as index
            if "STATE" in df_year.columns:
                df_year.set_index("STATE", inplace=True)
                df_year.sort_index(inplace=True)

            # Save to CSV
            output_path = output_dir / f"state_job_opening_data_{year}.csv"
            df_year.to_csv(output_path)
            print(f"✅ Saved {output_path}")

    def process_job_openings(self) -> bool:
        """Process all job openings Excel files and create CSV outputs."""
        if not self.job_input_dir.is_dir():
            print(f"⚠️  Job openings input directory not found: {self.job_input_dir}")
            return False
        
        print(f"📊 Processing job openings data from {self.job_input_dir}")
        yearly_data = {}

        # Process each Excel file
        xlsx_files = list(self.job_input_dir.glob("*.xlsx"))
        if not xlsx_files:
            print(f"⚠️  No XLSX files found in {self.job_input_dir}")
            return False

        print(f"📊 Found {len(xlsx_files)} XLSX files to process")

        for file_path in xlsx_files:
            state_fips, df = self.process_job_openings_file(file_path)
            if state_fips and df is not None:
                # Extract and merge data into the yearly_data dictionary
                file_yearly_data = self.extract_yearly_data(state_fips, df)
                for year, states_data in file_yearly_data.items():
                    if year not in yearly_data:
                        yearly_data[year] = {}
                    yearly_data[year].update(states_data)

        # Create CSV files for each year
        if yearly_data:
            self.create_job_openings_csvs(yearly_data, self.job_output_dir)
            print(f"✅ Job openings processing complete: {len(yearly_data)} years processed")
            return True
        else:
            print("⚠️  No valid job openings data was processed")
            return False

    # -------------- Public School Processing Logic --------------

    def consolidate_public_school_data(self) -> bool:
        """Reads and consolidates all public school Excel files into a single CSV."""
        if not self.school_input_dir.is_dir():
            print(f"⚠️  Public school input directory not found: {self.school_input_dir}")
            return False
        
        all_files = [
            self.school_input_dir / f
            for f in os.listdir(self.school_input_dir)
            if f.endswith('.xls') or f.endswith('.xlsx')
        ]

        if not all_files:
            print(f"⚠️  No Excel files found in {self.school_input_dir}")
            return False

        print(f"📊 Processing {len(all_files)} public school Excel files from: {self.school_input_dir}")

        dfs = []
        successful_files = 0
        
        for file_path in all_files:
            try:
                df = pd.read_excel(file_path)
                dfs.append(df)
                successful_files += 1
                print(f"✅ Successfully read {file_path.name}")
            except Exception as e:
                print(f"❌ Failed to read {file_path.name}: {e}")

        if not dfs:
            print("❌ No data was successfully processed from public school files")
            return False

        df_combined = pd.concat(dfs, ignore_index=True)
        
        # Determine the year - use 2023 as default
        year = "2023"
        
        # Save to CSV
        output_path = self.school_output_dir / f"public_school_data_{year}.csv"
        df_combined.to_csv(output_path, index=False)
        
        print(f"✅ Consolidated public school DataFrame has {len(df_combined)} rows")
        print(f"✅ Saved to {output_path}")
        print(f"📊 Successfully processed {successful_files}/{len(all_files)} files")
        
        return True

    def convert_all(self) -> bool:
        """Convert all Excel files to CSV format."""
        print("\n" + "="*60)
        print("📄 CONVERTING XLSX FILES TO CSV")
        print("="*60)
        
        # Process job openings data
        job_success = False
        if self.job_input_dir.exists():
            print("\n📊 Processing job openings data...")
            job_success = self.process_job_openings()
        else:
            print(f"⚠️  Job openings directory not found: {self.job_input_dir}")
        
        # Process public school data
        school_success = False
        if self.school_input_dir.exists():
            print("\n📊 Processing public school data...")
            school_success = self.consolidate_public_school_data()
        else:
            print(f"⚠️  Public school directory not found: {self.school_input_dir}")
        
        print("\n" + "="*60)
        if job_success or school_success:
            print("🎉 XLSX TO CSV CONVERSION COMPLETED!")
            print(f"✅ Job openings: {'Success' if job_success else 'Skipped/Failed'}")
            print(f"✅ Public schools: {'Success' if school_success else 'Skipped/Failed'}")
        else:
            print("⚠️  CONVERSION COMPLETED BUT NO DATA WAS PROCESSED")
        print("="*60)
        
        return job_success or school_success


def main():
    """Main function to run the XLSX to CSV conversion process."""
    try:
        converter = XLSXToCSVConverter()
        success = converter.convert_all()
        return 0 if success else 1
            
    except Exception as e:
        print(f"❌ Error in main process: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())