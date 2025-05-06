import streamlit as st
import src.components as cmpt

from src.db import db as database, Table, get_db_connection

# Display title with custom CSS
st.html(
    '<h1 class="custom-title">Is America Ready to Move?</h1>'
)

# Initialize the Database connection
get_db_connection()

# Make all database calls using database instead of just db
counties = database.get_county_metadata().set_index('COUNTY_FIPS')

population_historical = database.get_population_timeseries().set_index('COUNTY_FIPS')

population_projections = database.get_population_projections_by_fips(
).set_index('COUNTY_FIPS')

selected_county_fips = '36029'

# Add components to the sidebar
with st.sidebar:
    selected_county_fips = st.selectbox(
        'Select a county',
        counties.index,
        format_func=lambda fips: counties['NAME'].loc[fips],
        placeholder='Type to search...',
        index=counties.index.get_loc(selected_county_fips)
    )

    scenario_names = {
        # "POPULATION_2065_S3": "Baseline",
        "POPULATION_2065_S5a": "Low",
        "POPULATION_2065_S5b": "Medium",
        "POPULATION_2065_S5c": "High"
    }

    selected_scenario = st.selectbox(
        "Select a climate impact scenario:",
        # Exclude Scenario S3 (baseline)
        options=list(scenario_names.keys()),
        format_func=lambda sel: scenario_names.get(sel),
        index=0
    )
    
    cmpt.vertical_spacer(2)

    cmpt.display_migration_impact_analysis(
        population_projections.loc[selected_county_fips],
        selected_scenario
    )

    cmpt.vertical_spacer(2)

    cmpt.display_county_indicators(selected_county_fips, selected_scenario)

    # cmpt.vertical_spacer(2)

    # cmpt.plot_nri_score(selected_county_fips)


# Short paragraph explaining why climate migration will occur and how
st.markdown("""
# Climate-Induced Migration
Climate change is increasingly driving population shifts across the United States. As extreme weather events become more frequent and severe, communities around the country face challenges including sea-level rise, extreme heat, drought, wildfires, and flooding. These environmental pressures are expected to force increasingly more people to relocate from high-risk areas to regions with better climate resilience, impacting local economies, housing markets, and public services.
""")

# Climate migration choropleth of US counties
cmpt.plot_nri_choropleth(selected_scenario)

st.markdown("""
            ### Climate Vulnerability Isn't the Whole Story
            """)
st.markdown("""
            Of course, climate vulnerability won't be the only factor that drives migration decisions. While some people may consider leaving areas prone to climate hazards, research shows that economic factors like job opportunities and wages will still play a dominant role in determining if, when, and where people relocate.
            """)

cmpt.feature_cards(
    [
        {"icon": "house", "title": "Housing Cost",
            "description": "Availability of affordable housing"},
        {"icon": "work", "title": "Labor Demand",
            "description": "Strength of local job markets"},
        {"icon": "cloud_alert", "title": "Climate Risks",
            "description": "Vulnerability to climate hazards"},
    ]
)


# Explain factors that will affect the magnitude of climate-induced migration

with st.expander("Read more about migration factors", icon=":material/article:"):
    st.markdown("""When regions experiencing population loss due to climate concerns face labor shortages, wages tend to rise, creating an economic incentive for some people to stay or even move into these areas despite climate risks. Housing prices also adjust, becoming more affordable in areas experiencing outmigration, which further complicates migration patterns. This economic "dampening effect" means that even highly climate-vulnerable counties won't see mass exoduses, as financial considerations, family ties, and community connections often outweigh climate concerns in people's decision-making process. Migration is ultimately a complex interplay of climate, economic, social, and personal factors rather than a simple response to climate vulnerability alone. The key migration decision factors included in this model are:""")

cmpt.vertical_spacer(5)

# Get the County FIPS code, which will be used for all future queries
if selected_county_fips:
    county_metadata = database.get_county_metadata(
        selected_county_fips).iloc[0]
    # Separate the county and state names
    full_name = county_metadata['NAME']
    county_name, state_name = full_name.split(', ')
else:
    county_name = state_name = selected_county_fips = None

if selected_county_fips:

    cmpt.population_by_climate_region(selected_scenario)

    st.markdown("""
                These projections help visualize how climate change could reshape population distribution across regions, with some areas experiencing population growth (Northeast, West, California) and others facing decline (South, Midwest) due to climate-related migration pressures.

                The data is derived from research on climate-induced migration patterns, which considers factors including extreme weather events, economic opportunities, and regional climate vulnerabilities.
                """)

    projected_data = database.get_table_for_county(
        Table.COUNTY_COMBINED_PROJECTIONS, selected_county_fips)

    cmpt.display_housing_indicators(
        county_name, state_name, selected_county_fips)
    
    cmpt.display_economic_indicators(county_name, state_name, selected_county_fips)

    cmpt.display_education_indicators(county_name, state_name, selected_county_fips)

    # Display the impact analysis
    cmpt.display_scenario_impact_analysis(
        county_name, state_name, projected_data)

    # Display policy recommendations
    cmpt.generate_policy_recommendations(projected_data)
