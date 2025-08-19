import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import src.components as cmpt
from plotly.subplots import make_subplots

from src.db import db as database, Table, get_db_connection

# Initialize the Database connection
get_db_connection()

st.html(
    """
    <div class="full-width-container">
        <div class="title-container">
            <h1 class="hero-title">Is America Ready to Move?</h1>
            <h2 class="hero-subtitle">Exploring the effects of climate-induced migration on US counties</h5>
        </div>
        <div>
            <span class="arrow-icon">&darr;</span>
        </div>
    </div>
    """
)

cmpt.quote_box("Climate change is already profoundly reshaping where Americans reside and where continued habitation is no longer viable. The increasing frequency of wildfires, floods, extreme heat waves, and rising sea levels has already displaced over 3.2 million people in the United States between 2000 and 2020 alone. Projections indicate that by 2070, sea level rise could disrupt the lives of an additional 13 million individuals.")
cmpt.vertical_spacer(2)

counties = database.get_cbsa_counties(filter="metro").set_index('county_fips')

counties = counties[counties["state"] != 6]

population_historical = database.get_population_timeseries().set_index('county_fips')

population_projections = database.get_population_projections_by_fips(
).set_index('county_fips')

selected_county_fips = 36029

# Add components to the sidebar
with st.sidebar:
    selected_county_fips = st.selectbox(
        'Select a county',
        counties.index,
        format_func=lambda fips: counties['name'].loc[fips],
        placeholder='Type to search...',
        index=counties.index.get_loc(selected_county_fips)
    )

    scenario_names = {
        # "population_2065_s3": "Baseline",
        "population_2065_s5a": "Low",
        "population_2065_s5b": "Medium",
        "population_2065_s5c": "High"
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

    cmpt.vertical_spacer(2)

    cmpt.plot_nri_score(selected_county_fips)

# Climate migration choropleth of US counties
cmpt.plot_nri_choropleth_mpl()

cmpt.quote_box("""Climate change will undoubtedly impact the lives of all Americans, but as the map above shows, the hazards and risks associated with a changing climate vary across U.S. regions. Counties with limited exposure and/or high adaptability to those hazards are expected to attract climate migrants. These "receiver places" tend to be inland and north of the Sun Belt, often former industrial cities with underused infrastructure and walkable, mixed-use neighborhoods.""")

cmpt.vertical_spacer(2)

cmpt.receiver_places_choropleth_mpl()

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
        {"icon": "cloud_alert", "title": "Climate Risk",
            "description": "Vulnerability to climate hazards"},
    ],
    border=False,
    gap="large"
)


# Explain factors that will affect the magnitude of climate-induced migration
with st.expander("Read more about migration factors", icon=":material/article:"):
    st.markdown("""When regions experiencing population loss due to climate concerns face labor shortages, wages tend to rise, creating an economic incentive for some people to stay or even move into these areas despite climate risks. Housing prices also adjust, becoming more affordable in areas experiencing outmigration, which further complicates migration patterns. This economic "dampening effect" means that even highly climate-vulnerable counties won't see mass exoduses, as financial considerations, family ties, and community connections often outweigh climate concerns in people's decision-making process. Migration is ultimately a complex interplay of climate, economic, social, and personal factors rather than a simple response to climate vulnerability alone. The key migration decision factors included in this model are:""")

cmpt.vertical_spacer(2)

# Get the County FIPS code, which will be used for all future queries
if selected_county_fips:
    county_metadata = database.get_county_metadata(
        selected_county_fips).iloc[0]
    # Separate the county and state names
    full_name = county_metadata['name']
    county_name, state_name = full_name.split(', ')
else:
    county_name = state_name = selected_county_fips = None

if selected_county_fips:

    cmpt.population_by_climate_region_mpl(selected_scenario)

    st.markdown("""
                These projections help visualize how climate change could reshape population distribution across regions, with some areas experiencing population growth (Northeast, West, California) and others facing decline (South, Midwest) due to climate-related migration pressures.

                The data is derived from research on climate-induced migration patterns, which considers factors including extreme weather events, economic opportunities, and regional climate vulnerabilities.
                """)

    

    # Current County Performance Analysis
    cmpt.vertical_spacer(2)
    
    st.markdown("""
        # Current County Performance
        """)
    
    cmpt.vertical_spacer(2)
    
    projected_data = database.get_table_for_county(
        Table.COUNTY_COMBINED_PROJECTIONS, selected_county_fips)

    # Current State of County
    cmpt.display_housing_indicators(
        county_name, state_name, selected_county_fips)

    cmpt.vertical_spacer(2)

    cmpt.display_economic_indicators(
        county_name, state_name, selected_county_fips)

    cmpt.vertical_spacer(2)

    cmpt.display_education_indicators(
        county_name, state_name, selected_county_fips)

    # Climate Impact Analysis
    # st.header("Climate Impact Analysis")
    cmpt.vertical_spacer(2)
    st.markdown("""
        # Climate Migration Impacts
        """)
    
    cmpt.vertical_spacer(2)

    
    # Display the impact analysis
    cmpt.display_scenario_impact_analysis(
        county_name, state_name, projected_data)

    # Display policy recommendations
    cmpt.vertical_spacer(2)
    cmpt.generate_policy_recommendations(projected_data)
