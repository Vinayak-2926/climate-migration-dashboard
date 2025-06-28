"""
County data acquisition module.

This module downloads county metadata and geometry information from the
US Census Bureau's American Community Survey for use in the climate
migration dashboard.
"""

import re
import censusdis.data as ced
from pathlib import Path
from censusdis.datasets import ACS5

from src.shared.config import PipelineConfig
from src.shared.utils import ensure_directory_exists


def download_counties_data():
    """
    Download county metadata and geometry from US Census Bureau.
    
    Downloads county names and geometry data for all US counties and saves
    to the processed data directory for further pipeline processing.
    """
    # Get pipeline configuration
    config = PipelineConfig()
    
    # Set up output directory
    data_dir = ensure_directory_exists("./data/processed/cleaned_data/")
    
    print("📊 Downloading county metadata and geometry...")
    print(f"📁 Output directory: {data_dir}")
    
    try:
        # Download county data with geometry
        counties = ced.download(
            dataset=ACS5,
            vintage=2020,
            download_variables=["NAME"],
            state="*",
            county="*",
            with_geometry=True,
            api_key=config.us_census_api_key
        )
        
        print(f"✅ Downloaded data for {len(counties)} counties")
        
        # Construct COUNTY_FIPS by combining state and county FIPS
        counties["COUNTY_FIPS"] = counties["STATE"] + counties["COUNTY"]
        
        # Set index and rename columns for consistency
        counties = counties.set_index("COUNTY_FIPS")
        counties = counties.rename(columns={
            "geometry": "GEOMETRY",
        })
        
        # Save to CSV
        output_file = data_dir / "county.csv"
        counties.to_csv(output_file)
        
        print(f"✅ Saved county data to {output_file}")
        print(f"📊 Total counties processed: {len(counties)}")
        
    except Exception as e:
        print(f"❌ Error downloading county data: {e}")
        raise


def main():
    """Main function to download county data."""
    print("\n" + "="*50)
    print("🏛️  DOWNLOADING COUNTY DATA")
    print("="*50)
    
    download_counties_data()
    
    print("\n" + "="*50)
    print("🎉 COUNTY DATA DOWNLOAD COMPLETE!")
    print("="*50)


if __name__ == "__main__":
    main()