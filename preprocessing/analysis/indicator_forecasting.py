import pandas as pd
from pathlib import Path
from sklearn.discriminant_analysis import StandardScaler

# Define directory paths
DATA_DIR = Path("./data")
PROCESSED_DIR = DATA_DIR / "processed"
RAW_DATA_DIR = DATA_DIR / "raw"
POPULATION_DIR = RAW_DATA_DIR / "population_data"
CLEANED_DIR = PROCESSED_DIR / "cleaned_data"
PROJECTED_DATA = PROCESSED_DIR / "projected_data"

# Define file paths
CRIME_DATA = CLEANED_DIR / "cleaned_crime_data.csv"
ECONOMIC_DATA = CLEANED_DIR / "cleaned_economic_data.csv"
EDUCATION_DATA = CLEANED_DIR / "cleaned_education_data.csv"
HOUSING_DATA = CLEANED_DIR / "cleaned_housing_data.csv"
JOB_OPENINGS_DATA = CLEANED_DIR / "cleaned_job_openings_data.csv"
STUDENT_TEACHER_DATA = CLEANED_DIR / "erie_student_teacher.csv"
POP_PROJECT = PROJECTED_DATA / "county_population_projections.csv"
POP_2023 = POPULATION_DIR / "census_population_data_2023.csv"
PUBLIC_SCHOOL_DATA = CLEANED_DIR / "cleaned_public_school_data.csv"

def calculate_z_scores(df):
    """
    Calculate z-scores for student_teacher_ratio, available_housing_units, unemployment_rate
    for each county compared to all other counties nationally
    """    
    # Define indicators to calculate z-scores for
    indicators = ['student_teacher_ratio', 'available_housing_units', 'unemployment_rate']
    
    # Make sure indicators exist in the dataframe
    for indicator in indicators:
        if indicator not in df.columns:
            print(f"Warning: {indicator} not found in the data")
    
    # Group by scenario to calculate z-scores within each scenario
    for scenario in df['scenario'].unique():
        # Filter data for current scenario
        scenario_mask = df['scenario'] == scenario
        
        # Calculate z-scores for each indicator
        for indicator in indicators:
            if indicator in df.columns:
                # Get data for this indicator in this scenario
                data = df.loc[scenario_mask, indicator]
                
                # Skip if all values are NaN
                if data.isna().all():
                    print(f"Warning: All values are NaN for {indicator} in scenario {scenario}")
                    continue
                
                # Calculate mean and standard deviation for non-null values
                mean = data.mean()
                std = data.std()
                
                # Avoid division by zero
                if std == 0:
                    print(f"Warning: Standard deviation is 0 for {indicator} in scenario {scenario}")
                    # Set all z-scores to 0 if std dev is 0
                    df.loc[scenario_mask, f"z_{indicator}"] = 0
                else:
                    # Manually calculate z-scores: (value - mean) / std
                    df.loc[scenario_mask, f"z_{indicator}"] = ((data - mean) / std).round(4)
    return df

def load_and_merge_data():
    """Load and merge all datasets into a single dataframe"""
    # Load individual datasets
    economic_df = pd.read_csv(ECONOMIC_DATA)
    education_df = pd.read_csv(EDUCATION_DATA)
    housing_df = pd.read_csv(HOUSING_DATA)
    job_openings_df = pd.read_csv(JOB_OPENINGS_DATA)
    public_school_df = pd.read_csv(PUBLIC_SCHOOL_DATA)
    
    # Merge all dataframes on COUNTY_FIPS
    merged_df = economic_df.merge(
        education_df, on=['county_fips','state','county','name','population', 'year'], how='inner'
    ).merge(
        housing_df, on=['county_fips','state','county','name','population', 'year'], how='inner'
    ).merge(
        job_openings_df, on=['county_fips','state','county','name','population', 'year'], how='inner'
    ).merge(
        public_school_df, on=['county_fips','state','county','name','population', 'year'], how='outer'
    )
    
    # Drop columns containing 'z_score'
    merged_df = merged_df.loc[:, ~merged_df.columns.str.contains('z_score', case=False)]
    
    # Set school data to 0 for years other than 2023
    merged_df.loc[merged_df["year"] != 2023, ["public_school_students", "public_school_teachers", "student_teacher_ratio"]] = 0
    
    return merged_df

def prepare_filtered_data(merged_df):
    """Prepare filtered data for 2023"""
    filter_columns = [
        'public_school_students', 'elementary_school_population', 
        'middle_school_population', 'high_school_population', 
        'county_fips', 'state', 'county', 'name', 
        'total_employed_population', 'total_labor_force',
        'job_opening_jan', 'job_opening_feb', 'job_opening_mar', 
        'job_opening_apr', 'job_opening_may', 'job_opening_jun', 
        'job_opening_jul', 'job_opening_aug', 'job_opening_sep', 
        'job_opening_oct', 'job_opening_nov', 'job_opening_dec',
        'population', 'year', 'occupied_housing_units',
    ]
    
    filtered_df = merged_df[filter_columns]
    filtered_df = filtered_df[filtered_df["year"] == 2023]
    filtered_df['county_fips'] = filtered_df['county_fips'].astype(str).str.zfill(5)
    
    return filtered_df

def process_population_data():
    """Process population data and calculate percentage changes"""
    pop_project_df = pd.read_csv(POP_PROJECT)
    pop_2023 = pd.read_csv(POP_2023)

    pop_2023.columns = pop_2023.columns.str.lower()
    
    # Format county FIPS codes
    pop_2023["state"] = pop_2023["state"].astype(str).str.zfill(2)
    pop_2023["county"] = pop_2023["county"].astype(str).str.zfill(3)
    pop_2023["county_fips"] = pop_2023["state"] + pop_2023["county"]
    pop_project_df["county_fips"] = pop_project_df["county_fips"].astype(str).str.zfill(5)
    
    # Merge population datasets
    pop_combined = pop_project_df.merge(
        pop_2023,
        on=['county_fips'],
        how='left'
    )
    
    # Rename and select columns
    pop_combined.rename(columns={"b01003_001e": "population_2023"}, inplace=True)
    pop_combined = pop_combined[[
        "county_fips", "state", "county", "name", "population_2023", 
        "population_2065_s3", "population_2065_s5b", "population_2065_s5a", 
        "population_2065_s5c", "climate_region", "population_2010"
    ]]
    
    # Calculate percentage changes for each scenario
    pop_combined["s3_percentage_change"] = ((pop_combined["population_2065_s3"] - pop_combined["population_2023"]) / pop_combined["population_2023"]) * 100
    pop_combined["s5b_percentage_change"] = ((pop_combined["population_2065_s5b"] - pop_combined["population_2023"]) / pop_combined["population_2023"]) * 100
    pop_combined["s5a_percentage_change"] = ((pop_combined["population_2065_s5a"] - pop_combined["population_2023"]) / pop_combined["population_2023"]) * 100
    pop_combined["s5c_percentage_change"] = ((pop_combined["population_2065_s5c"] - pop_combined["population_2023"]) / pop_combined["population_2023"]) * 100
    
    return pop_combined

def calculate_projected_values(df, base_year, percentage_change, scenario_name):
    """Calculate projected values based on percentage change"""
    projected_df = df[df["year"] == base_year].copy()
    projected_df["scenario"] = scenario_name
    
    # Exclude columns that should not be scaled
    columns_to_exclude = ["county_fips", "state", "county", "year", "name", "scenario"]
    numeric_cols = [col for col in df.columns if col not in columns_to_exclude]
    
    # Scale the numeric columns for the projected values
    for col in numeric_cols:
        projected_df[col] = round(projected_df[col] * (1 + percentage_change / 100))
    
    return projected_df

def generate_county_projections(filtered_df, pop_combined):
    """Generate projections for all counties under different scenarios"""
    # Get all unique counties
    all_counties = filtered_df['county_fips'].unique()
    
    # Create an empty DataFrame to store all results
    all_counties_2065_combined = pd.DataFrame()
    
    # Process each county
    for county in all_counties:
        # Filter data for the current county
        county_df = filtered_df[filtered_df['county_fips'] == county].copy()
        
        # Get original data for base year
        original_df = county_df[county_df["year"] == 2023].copy()
        original_df["scenario"] = "original"
        
        # Extract this county's percentage changes
        county_proj = pop_combined[pop_combined["county_fips"] == county]
        if county_proj.empty:
            print(f"Skipping county_fips {county} - no projection data found.")
            continue
            
        percentage_changes = county_proj[
            ["s3_percentage_change", "s5b_percentage_change", "s5a_percentage_change", "s5c_percentage_change"]
        ].iloc[0].to_dict()
        
        # Calculate projections for each scenario
        s3_2065 = calculate_projected_values(county_df, base_year=2023,
                                           percentage_change=percentage_changes["s3_percentage_change"],
                                           scenario_name="s3")
        
        s5b_2065 = calculate_projected_values(county_df, base_year=2023,
                                            percentage_change=percentage_changes["s5b_percentage_change"],
                                            scenario_name="s5b")
        
        s5a_2065 = calculate_projected_values(county_df, base_year=2023,
                                            percentage_change=percentage_changes["s5a_percentage_change"],
                                            scenario_name="s5a")
        
        s5c_2065 = calculate_projected_values(county_df, base_year=2023,
                                            percentage_change=percentage_changes["s5c_percentage_change"],
                                            scenario_name="s5c")
        
        # Combine all scenarios for this county
        county_2065_combined = pd.concat([original_df, s3_2065, s5b_2065, s5a_2065, s5c_2065],
                                       ignore_index=True)
        
        # Add to the master DataFrame
        all_counties_2065_combined = pd.concat([all_counties_2065_combined, county_2065_combined],
                                             ignore_index=True)
    
    return all_counties_2065_combined

def calculate_derived_metrics(all_counties_2065_combined, merged_df):
    """Calculate derived metrics for projected data"""
    merged_df_2023 = merged_df[merged_df["year"] == 2023].copy()
    merged_df_2023["county_fips"] = merged_df_2023["county_fips"].astype(str).str.zfill(5)
    all_counties = merged_df_2023['county_fips'].unique()
    
    for county in all_counties:
        all_counties_2065_combined["county_fips"] = all_counties_2065_combined["county_fips"].astype(str).str.zfill(5)
        county_df = merged_df_2023[merged_df_2023['county_fips'] == county].copy()
        teachers_2023 = county_df["public_school_teachers"]
        housing_units_2023 = county_df["total_housing_units"]
        employed_population_2023 = county_df["total_employed_population"]
        
        if teachers_2023.empty or housing_units_2023.empty or employed_population_2023.empty:
            print(f"Missing data for county_fips {county}")
            
        # Ensure the correct teacher count is applied for each county
        teacher_count = teachers_2023.values[0] if not teachers_2023.empty else 1  # Avoid division by zero
        all_counties_2065_combined.loc[all_counties_2065_combined['county_fips'] == county, "student_teacher_ratio"] = (
            all_counties_2065_combined.loc[all_counties_2065_combined['county_fips'] == county, "public_school_students"] / teacher_count
        )
        
        housing_units_count = housing_units_2023.values[0] if not housing_units_2023.empty else 1  # Avoid division by zero
        all_counties_2065_combined.loc[all_counties_2065_combined['county_fips'] == county, "available_housing_units"] = (
            housing_units_count - all_counties_2065_combined.loc[all_counties_2065_combined['county_fips'] == county, "occupied_housing_units"])
        
        employed_population_count = employed_population_2023.values[0] if not employed_population_2023.empty else 1  # Avoid division by zero
        all_counties_2065_combined.loc[all_counties_2065_combined['county_fips'] == county, "total_employed_percentage"] = (
            employed_population_count / all_counties_2065_combined.loc[all_counties_2065_combined['county_fips'] == county, "total_labor_force"]) * 100
        
        all_counties_2065_combined.loc[all_counties_2065_combined['county_fips'] == county, "unemployment_rate"] = (
            100 - all_counties_2065_combined.loc[all_counties_2065_combined['county_fips'] == county, "total_employed_percentage"])
    # Format the state and county codes
    all_counties_2065_combined["state"] = all_counties_2065_combined["state"].astype(str).str.zfill(2)
    all_counties_2065_combined["county"] = all_counties_2065_combined["county"].astype(str).str.zfill(3)
    
    return all_counties_2065_combined

def calculate_indices(all_counties_2065_combined):
    """Calculate socioeconomic indices from the projected data"""
    # Filter to include only counties with school data
    index_df = all_counties_2065_combined[all_counties_2065_combined['public_school_students'] > 0].copy()
    
    # Columns to standardize
    cols = ['unemployment_rate', 'student_teacher_ratio', 'available_housing_units']
    
    # Standardize the data
    df_scaled = index_df.copy()
    z_scaler = StandardScaler()
    df_scaled[[f'z_{c}' for c in cols]] = z_scaler.fit_transform(index_df[cols])
    
    # Calculate different indices with different weights
    # Flip unemployment and student-teacher ratio (lower is better)
    df_scaled["index_balanced"] = (
        (-df_scaled["z_unemployment_rate"]) * 0.33 + 
        (-df_scaled["z_student_teacher_ratio"]) * 0.33 + 
        df_scaled["z_available_housing_units"] * 0.33
    )
    
    df_scaled["index_employment"] = (
        (-df_scaled["z_unemployment_rate"]) * 0.6 + 
        (-df_scaled["z_student_teacher_ratio"]) * 0.2 + 
        df_scaled["z_available_housing_units"] * 0.2
    )
    
    df_scaled["index_housing"] = (  # Fixed typo in variable name
        (-df_scaled["z_unemployment_rate"]) * 0.2 + 
        (-df_scaled["z_student_teacher_ratio"]) * 0.2 + 
        df_scaled["z_available_housing_units"] * 0.6
    )
    
    df_scaled["index_education"] = (
        (-df_scaled["z_unemployment_rate"]) * 0.2 + 
        (-df_scaled["z_student_teacher_ratio"]) * 0.6 + 
        df_scaled["z_available_housing_units"] * 0.2
    )
    
    # Extract results and return
    results_df = df_scaled[['county_fips', 'scenario', 'index_balanced', 'index_employment', 'index_housing', 'index_education']]
    return results_df

def main():
    # Load and prepare data
    merged_df = load_and_merge_data()
    filtered_df = prepare_filtered_data(merged_df)
    
    # Process population data
    pop_combined = process_population_data()
    
    # Generate projections
    all_counties_2065_combined = generate_county_projections(filtered_df, pop_combined)
    
    # Calculate derived metrics
    all_counties_2065_combined = calculate_derived_metrics(all_counties_2065_combined, merged_df)

    all_counties_2065_combined = calculate_z_scores(all_counties_2065_combined)

    all_counties_2065_combined.columns = all_counties_2065_combined.columns.str.lower()
    
    # Save combined projected data
    all_counties_2065_combined.to_csv(PROJECTED_DATA / "combined_2065_data.csv", index=False)
    
    # Calculate socioeconomic indices
    results_df = calculate_indices(all_counties_2065_combined)

    results_df.columns = results_df.columns.str.lower()
    
    # Save results
    output_path = PROJECTED_DATA / "projected_socioeconomic_indices.csv"
    results_df.to_csv(output_path, index=False)
    
    print(f"Analysis complete. Results saved to {output_path}")

if __name__ == "__main__":
    main()