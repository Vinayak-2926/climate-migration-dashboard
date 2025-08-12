import re
import censusdis.data as ced
import pandas as pd

from pathlib import Path
from censusdis.datasets import ACS5


DATA_DIR = Path("./data/processed/cleaned_data/")
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Download 2010 county data
counties_2010 = ced.download(
    dataset=ACS5,
    vintage=2010,
    download_variables=["NAME"],
    state="*",
    county="*",
    with_geometry=True,
)
counties_2010["COUNTY_FIPS"] = counties_2010["STATE"] + counties_2010["COUNTY"]
counties_2010 = counties_2010.set_index("COUNTY_FIPS")

# Download 2020 county data
counties_2020 = ced.download(
    dataset=ACS5,
    vintage=2020,
    download_variables=["NAME"],
    state="*",
    county="*",
    with_geometry=True,
)
counties_2020["COUNTY_FIPS"] = counties_2020["STATE"] + counties_2020["COUNTY"]
counties_2020 = counties_2020.set_index("COUNTY_FIPS")

# Combine the datasets
# Prioritize 2020 data for common FIPS, keep unique 2010 FIPS
# Concatenate 2010 first, then 2020. Drop duplicates keeping the last (2020)
combined_counties = pd.concat([counties_2010, counties_2020])
counties = combined_counties[~combined_counties.index.duplicated(keep='last')]

counties = counties.rename(
    columns={
        "geometry": "GEOMETRY",
    }
)

counties.to_csv(DATA_DIR / "county.csv")
