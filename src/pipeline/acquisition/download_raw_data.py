"""
Raw data acquisition module for the climate migration dashboard.

This module downloads raw data from various sources including the US Census Bureau,
Data Commons, and other APIs for use in the climate migration analysis pipeline.
"""

from pathlib import Path
import pandas as pd
import censusdis.data as ced
import datacommons as dc
from typing import List, Tuple, Dict, Optional
import os
import concurrent.futures
import time

from src.shared.config import PipelineConfig
from src.shared.utils import ensure_directory_exists


class DataDownloader:
    """Enhanced data downloader using the new pipeline configuration."""

    def __init__(self):
        """Initialize the data downloader with pipeline configuration."""
        self.config = PipelineConfig()
        self._validate_api_key()
        
        # Set up data directory
        self.base_data_dir = ensure_directory_exists("./data/raw")
        
        # Configuration for datasets
        self.excluded_states = [
            "11", "72", "15", "02", "78"  # DC, PR, HI, AK, VI as FIPS codes
        ]
        
        self.max_workers = min(32, (os.cpu_count() or 1) + 4)
        self.max_county_workers = min(50, (os.cpu_count() or 1) * 2)
        
        # Initialize state and county data
        self.contiguous_states = self._get_contiguous_states()
        self.counties_by_state = self._get_counties_by_state()
        
        # Dataset configurations
        self.datasets = self._get_dataset_configs()

    def _validate_api_key(self):
        """Validate that the US Census API key is available."""
        if not self.config.us_census_api_key:
            raise ValueError("US_CENSUS_API_KEY not found in environment configuration")

    def _get_dataset_configs(self) -> Dict:
        """Get dataset configuration dictionary."""
        return {
            "HOUSING": {
                "DATASET": "acs/acs5/profile",
                "YEARS": (2010, 2023),
                "VARIABLES": {
                    (2010, 2014): ["DP04_0001E", "DP04_0044E", "DP04_0088E", "DP04_0132E"],
                    (2015, 2023): ["DP04_0001E", "DP04_0002E", "DP04_0089E", "DP04_0134E"],
                },
            },
            "POPULATION": {
                "DATASET": "acs/acs5",
                "YEARS": (2010, 2023),
                "VARIABLE": "B01003_001E",
            },
            "EDUCATION": {
                "DATASET": "acs/acs5",
                "YEARS": (2011, 2023),
                "VARIABLE": [
                    "B23006_001E", "B23006_002E", "B23006_009E", "B23006_016E", "B23006_023E",
                    "B14001_001E", "B14001_002E", "B14001_003E", "B14001_004E", "B14001_005E",
                    "B14001_006E", "B14001_007E", "B14001_008E", "B14001_009E",
                    "B23006_007E", "B23006_014E", "B23006_021E", "B23006_028E",
                    "B01001_004E", "B01001_005E", "B01001_006E",  # Male 5-9, 10-14, 15-17
                    "B01001_028E", "B01001_029E", "B01001_030E"   # Female 5-9, 10-14, 15-17
                ],
            },
            "ECONOMIC": {
                "DATASET": "acs/acs5",
                "YEARS": (2011, 2023),
                "VARIABLE": ["B19301_001E", "B23025_004E", "B23025_005E", "B23025_003E"],
            },
            "CRIME": {
                "DATA_SOURCE": "datacommons",
                "YEARS": (2010, 2023),
                "VARIABLES": ["Count_CriminalActivities_CombinedCrime"],
                "STATE_RANGE": (1, 80),
                "LEVEL": "state",
            },
            "FEMA_NRI": {
                "DATA_SOURCE": "datacommons",
                "YEARS": (2021, 2023),
                "VARIABLES": ["FemaNaturalHazardRiskIndex_NaturalHazardImpact"],
                "LEVEL": "county",
            },
            "COUNTIES": {
                "DATASET": "acs/acs5",
                "YEARS": (2010, 2023),
                "VARIABLE": ["NAME"],
            },
        }

    def _get_contiguous_states(self) -> List[str]:
        """Get list of contiguous state codes for all datasets."""
        state_data_dir = ensure_directory_exists(self.base_data_dir / "state_data")
        state_file = state_data_dir / "state_names.csv"

        if not state_file.exists():
            print("📊 Downloading state metadata...")
            state_df = ced.download(
                "acs/acs5",
                2010,
                state="*",
                download_variables=["NAME"],
                api_key=self.config.us_census_api_key,
            )
            # Filter out excluded states using FIPS codes
            state_df = state_df[
                ~state_df["STATE"].astype(str).isin(self.excluded_states)
            ]
            state_df.to_csv(state_file, index=False)
            print(f"✅ Saved state data to {state_file}")

        state_df = pd.read_csv(state_file)
        return state_df["STATE"].astype(str).str.zfill(2).tolist()
    
    def _get_counties_by_state(self) -> Dict[str, List[str]]:
        """Get a mapping of state codes to their county codes."""
        counties_data_dir = ensure_directory_exists(self.base_data_dir / "county_data")
        counties_file = counties_data_dir / "county_names.csv"
        
        if not counties_file.exists():
            print("📊 Downloading county metadata...")
            counties_df = ced.download(
                "acs/acs5",
                2010,
                state=self.contiguous_states,
                county="*",
                download_variables=["NAME"],
                api_key=self.config.us_census_api_key,
            )
            counties_df.to_csv(counties_file, index=False)
            print(f"✅ Saved county data to {counties_file}")
        
        counties_df = pd.read_csv(counties_file)
        
        # Create dictionary mapping state to county codes
        counties_by_state = {}
        for state in self.contiguous_states:
            state_counties = counties_df[counties_df["STATE"] == int(state)]["COUNTY"].astype(str).str.zfill(3).tolist()
            counties_by_state[state] = state_counties
            
        return counties_by_state

    @staticmethod
    def _get_years_from_range(year_range: Tuple[int, int]) -> List[int]:
        """Generate inclusive list of years from range tuple."""
        return list(range(year_range[0], year_range[1] + 1))

    def _get_variables_for_year(self, dataset: str, year: int) -> List[str]:
        """Dynamically get variables based on year and dataset with flexible configuration."""
        dataset_config = self.datasets[dataset]

        # Check if dataset has a nested VARIABLES dictionary
        if "VARIABLES" in dataset_config:
            # Check if the variables are defined as a dictionary by year ranges
            if isinstance(dataset_config["VARIABLES"], dict):
                for (start, end), variables in dataset_config["VARIABLES"].items():
                    if start <= year <= end:
                        return ["NAME"] + variables
                raise ValueError(f"No variables defined for {dataset} in {year}")
            else:
                # Variables defined as a list directly
                return ["NAME"] + dataset_config["VARIABLES"]

        # Check if dataset has a single VARIABLE key (string or list)
        if "VARIABLE" in dataset_config:
            if isinstance(dataset_config["VARIABLE"], list):  # Multiple variables
                return ["NAME"] + dataset_config["VARIABLE"]
            elif isinstance(dataset_config["VARIABLE"], str):  # Single variable
                return ["NAME", dataset_config["VARIABLE"]]

        raise ValueError(f"Invalid variable configuration for {dataset}")

    def _download_single_dataset_year(self, dataset: str, year: int) -> None:
        """Download a single dataset for a specific year."""
        dataset_config = self.datasets[dataset]

        # Skip if dataset is from Data Commons - it's handled separately
        if dataset_config.get("DATA_SOURCE") == "datacommons":
            return

        data_dir = ensure_directory_exists(self.base_data_dir / f"{dataset.lower()}_data")
        output_file = data_dir / f"census_{dataset.lower()}_data_{year}.csv"
        
        if output_file.exists():
            print(f"⏭️  Skipping existing {dataset} {year}")
            return

        try:
            variables = self._get_variables_for_year(dataset, year)
            print(f"📊 Downloading {dataset} data for {year}...")

            df = ced.download(
                dataset_config["DATASET"],
                year,
                download_variables=variables,
                state=self.contiguous_states,
                county="*",
                with_geometry=("COUNTIES" in dataset),
                api_key=self.config.us_census_api_key,
            )

            df.to_csv(output_file, index=False)
            print(f"✅ Saved {dataset} {year} with {len(df)} records")

        except Exception as e:
            print(f"❌ Failed {dataset} {year}: {str(e)}")

    def _download_dataset(self, dataset: str) -> None:
        """Parallel download handler for a dataset."""
        dataset_config = self.datasets[dataset]
        
        # Handle Data Commons datasets
        if dataset_config.get("DATA_SOURCE") == "datacommons":
            self._download_datacommons_dataset(dataset)
            return

        years = self._get_years_from_range(dataset_config["YEARS"])

        # Use concurrent futures for parallel downloading
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [
                executor.submit(self._download_single_dataset_year, dataset, year)
                for year in years
            ]
            concurrent.futures.wait(futures)

    def _download_datacommons_dataset(self, dataset: str) -> None:
        """Generalized method to download data from Data Commons API."""
        dataset_config = self.datasets[dataset]
        
        level = dataset_config.get("LEVEL", "state")
        output_dir = ensure_directory_exists(self.base_data_dir / f"{level}_{dataset.lower()}_data")
        years_range = range(dataset_config["YEARS"][0], dataset_config["YEARS"][1] + 1)
        
        # Check which years already exist
        existing_files = {
            int(f.stem.split("_")[-1]): f
            for f in output_dir.glob(f"{level}_{dataset.lower()}_data_*.csv")
        }
        
        # Skip if all years exist
        if all(year in existing_files for year in years_range):
            print(f"⏭️  All {dataset} data files already exist, skipping download")
            return
        
        print(f"📊 Downloading missing {dataset} data from Data Commons...")
        
        all_data = {year: [] for year in years_range if year not in existing_files}
        
        if level == "state":
            self._download_state_level_data(dataset_config, all_data, years_range, existing_files)
        elif level == "county":
            self._download_county_level_data(dataset_config, all_data, years_range, existing_files)
        
        # Save separate CSV files for each year
        for year, data in all_data.items():
            if data:
                df = pd.DataFrame(data)
                file_path = output_dir / f"{level}_{dataset.lower()}_data_{year}.csv"
                df.to_csv(file_path, index=False)
                print(f"✅ Saved {dataset} data for year {year} with {len(df)} records")

    def _download_state_level_data(self, dataset_config, all_data, years_range, existing_files):
        """Download state-level data from Data Commons."""
        state_range = dataset_config.get("STATE_RANGE", (1, 80))
        state_ids = [
            f"{state_id:02d}" 
            for state_id in range(state_range[0], state_range[1])
            if f"{state_id:02d}" not in self.excluded_states
        ]
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            for state_fips in state_ids:
                futures.append(
                    executor.submit(
                        self._fetch_datacommons_for_geo,
                        geo_id=f"geoId/{state_fips}",
                        variables=dataset_config["VARIABLES"],
                        all_data=all_data,
                        years_range=years_range,
                        existing_files=existing_files,
                        state=state_fips
                    )
                )
            
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"❌ Error in state download thread: {str(e)}")

    def _download_county_level_data(self, dataset_config, all_data, years_range, existing_files):
        """Download county-level data from Data Commons."""
        county_tasks = []
        for state_fips in self.contiguous_states:
            for county_fips in self.counties_by_state.get(state_fips, []):
                county_tasks.append((state_fips, county_fips))
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_county_workers) as executor:
            futures = []
            
            for state_fips, county_fips in county_tasks:
                geo_id = f"geoId/{state_fips}{county_fips}"
                futures.append(
                    executor.submit(
                        self._fetch_datacommons_for_geo,
                        geo_id=geo_id,
                        variables=dataset_config["VARIABLES"],
                        all_data=all_data,
                        years_range=years_range,
                        existing_files=existing_files,
                        state=state_fips,
                        county=county_fips
                    )
                )
            
            completed = 0
            total = len(futures)
            
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                    completed += 1
                    if completed % 50 == 0 or completed == total:
                        print(f"📊 Progress: {completed}/{total} counties processed ({completed/total*100:.1f}%)")
                except Exception as e:
                    print(f"❌ Error in county download thread: {str(e)}")
                    completed += 1
    
    def _fetch_datacommons_for_geo(
        self, 
        geo_id: str, 
        variables: List[str], 
        all_data: Dict,
        years_range: range,
        existing_files: Dict,
        state: str,
        county: Optional[str] = None
    ) -> None:
        """Fetch Data Commons data for a specific geography."""
        try:
            geo_name = f"FIPS: {state}" if county is None else f"FIPS: {state}{county}"
            
            if county is None:
                print(f"📊 Fetching data for {geo_name}...")
            
            for variable in variables:
                data = dc.get_stat_series(geo_id, variable) 
                
                if data:
                    for year, value in data.items():
                        if len(year) != 4:
                            year = year[:4]
                        year_int = int(year)
                        
                        if county is None:
                            print(f"📊 Processing year {year_int} for {geo_id}, variable {variable}")
                        
                        if (year_int in years_range and year_int not in existing_files):
                            entry = {
                                "STATE": state,
                                variable: value,
                            }
                            
                            if county:
                                entry["COUNTY"] = county
                                
                            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as lock_executor:
                                lock_executor.submit(lambda: all_data[year_int].append(entry))
        except Exception as e:
            if county is None:
                print(f"❌ Error fetching data for {geo_id}: {str(e)}")
            elif "404" not in str(e):
                print(f"❌ Error for {geo_id}: {str(e)[:100]}...")

    def download_all_data(self):
        """Download all datasets with timing."""
        start_time = time.time()
        
        print("\n" + "="*60)
        print("🌍 STARTING COMPREHENSIVE DATA DOWNLOAD")
        print("="*60)
        print(f"📊 Datasets to download: {list(self.datasets.keys())}")
        print(f"🌎 States: {len(self.contiguous_states)} contiguous states")
        print(f"🏘️  Counties: {sum(len(counties) for counties in self.counties_by_state.values())} total counties")
        print("="*60)

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [
                executor.submit(self._download_dataset, dataset)
                for dataset in self.datasets
            ]
            concurrent.futures.wait(futures)

        end_time = time.time()
        
        print("\n" + "="*60)
        print("🎉 ALL DATASET DOWNLOADS COMPLETED!")
        print(f"⏱️  Total download time: {end_time - start_time:.2f} seconds")
        print("="*60)


def main():
    """Main function to download all raw data."""
    downloader = DataDownloader()
    downloader.download_all_data()


if __name__ == "__main__":
    main()