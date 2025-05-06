import json
import streamlit as st
import pandas as pd
import geopandas as gpd
from shapely.ops import unary_union
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from src.components.utils import *

from src.db import db as database, Table

from shapely import wkt
from urllib.request import urlopen
from plotly.subplots import make_subplots


__all__ = [
    "generate_policy_recommendations",
    "display_migration_impact_analysis",
    "display_scenario_impact_analysis",
    "display_housing_indicators",
    "display_economic_indicators",
    "display_education_indicators",
    "display_county_indicators",
    "feature_cards",
    "plot_nri_choropleth",
    "plot_nri_score",
    "plot_climate_hazards",
    "plot_socioeconomic_indices",
    "plot_socioeconomic_radar",
    "population_by_climate_region",
    "socioeconomic_projections",
    "display_housing_burden_plot",
    "display_housing_vacancy_plot"
]

# Define the color palette globally to avoid duplication
RISK_COLORS_RGB = [
    (0, 196, 218),
    (134, 210, 222),
    (231, 214, 189),
    (214, 103, 103),
    (209, 55, 52),
]


# Generate color formats once
RISK_COLORS_RGBA = [f"rgba({r}, {g}, {b}, 1)" for r, g, b in RISK_COLORS_RGB]
RISK_COLORS_RGB_STR = [f"rgb({r}, {g}, {b})" for r, g, b in RISK_COLORS_RGB]

# Risk level labels
RISK_LEVELS = ['Very Low', 'Low', 'Moderate', 'High', 'Very High']

# Map risk categories to colors
RISK_COLOR_MAPPING = dict(zip(RISK_LEVELS, RISK_COLORS_RGB_STR))

choropleth_config = {
    'displayModeBar': False,
    'scrollZoom': False,
}

def get_risk_color(score, opacity=1.0):
    """Get color for a risk score with specified opacity"""
    color_index = min(int(score // 20), 4)
    r, g, b = RISK_COLORS_RGB[color_index]
    return f"rgba({r}, {g}, {b}, {opacity})"


def plot_nri_score(county_fips):
    fema_df = database.get_stat_var(
        Table.COUNTY_FEMA_DATA, "FEMA_NRI", county_fips, 2023)

    # Dummy NRI data for demonstration
    nri_score = fema_df["FEMA_NRI"].iloc[0]

    # Use light gray for the gauge bar
    bar_color = "rgba(255, 255, 255, 0.5)"

    # Display the NRI score with a gauge chart
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=nri_score,
        domain={'x': [0, 1], 'y': [0, 1]},
        title={'text': "National Risk Index Score"},
        gauge={
            'axis': {'range': [0, 100]},
            'bar': {
                'thickness': 0.5,
                'color': bar_color,
            },
            'steps': [
                {'range': [0, 20], 'color': RISK_COLORS_RGBA[0]},
                {'range': [20, 40], 'color': RISK_COLORS_RGBA[1]},
                {'range': [40, 60], 'color': RISK_COLORS_RGBA[2]},
                {'range': [60, 80], 'color': RISK_COLORS_RGBA[3]},
                {'range': [80, 100], 'color': RISK_COLORS_RGBA[4]},
            ]
        }
    ))

    fig.update_layout(
        # width=480,
        height=240,
        margin=dict(
            b=0,
            t=40,
            l=80,
            r=80,
        ),
        autosize=True,
        xaxis=dict(
            domain=[0, 0.95]
        )
    )

    st.plotly_chart(fig)


def plot_climate_hazards(county_fips, county_name):
    # Display top hazards
    hazard_data = {
        "Hazard Type": ["Extreme Heat", "Drought", "Riverine Flooding", "Wildfire", "Hurricane"],
        "Risk Score": [82.4, 64.7, 42.3, 37.8, 15.2]
    }

    hazards_df = pd.DataFrame(hazard_data)
    hazards_df = hazards_df.sort_values("Risk Score", ascending=False)

    # Create a color mapping based on risk score ranges
    hazards_df['Color Category'] = pd.cut(
        hazards_df['Risk Score'],
        bins=[0, 20, 40, 60, 80, 100],
        labels=RISK_LEVELS,
        include_lowest=True,
    )

    # Create a horizontal bar chart
    fig = px.bar(
        hazards_df,
        x="Risk Score",
        y="Hazard Type",
        orientation='h',
        color="Color Category",
        color_discrete_map=RISK_COLOR_MAPPING,
        title="Climate Hazards",
        labels={"Risk Score": "Risk Score (Higher = Greater Risk)"}
    )

    st.plotly_chart(fig)


def plot_nri_choropleth(scenario):
    try:
        # --- Load county data with geometry and projections ---
        counties_data = database.get_county_metadata(year=2021)
        counties_data = counties_data.merge(
            database.get_population_projections_by_fips(),
            how='outer',
            on='COUNTY_FIPS'
        )

        # Get FEMA data
        fema_df = database.get_stat_var(
            Table.COUNTY_FEMA_DATA, "FEMA_NRI",
            county_fips=counties_data['COUNTY_FIPS'].tolist(),
            year=2023
        )
        counties_data = counties_data.merge(fema_df, how="outer", on="COUNTY_FIPS")

        # Handle Oglala Lakota fallback
        source_row = counties_data[counties_data['COUNTY_FIPS'] == "46102"]
        if not source_row.empty:
            counties_data.loc[counties_data['COUNTY_FIPS'] == "46113", ['FEMA_NRI', 'COUNTY_FIPS', 'STATE', 'COUNTY', 'NAME']] = \
                source_row[['FEMA_NRI', 'COUNTY_FIPS', 'STATE', 'COUNTY', 'NAME']].values[0]

        counties_data.dropna(subset=['FEMA_NRI'], inplace=True)

        # Convert county WKT to geometry
        counties_data['geometry'] = counties_data['GEOMETRY'].apply(
            lambda x: wkt.loads(x) if isinstance(x, str) else x
        )
        counties_gdf = gpd.GeoDataFrame(counties_data, geometry='geometry', crs='EPSG:4326')
        counties_gdf['geometry'] = counties_gdf['geometry'].simplify(tolerance=0.01, preserve_topology=True)

        # --- Analysis columns ---
        counties_gdf['VARIATION'] = counties_gdf[scenario] - counties_gdf['POPULATION_2065_S3']
        counties_gdf['VARIATION_PCT'] = (
            counties_gdf['VARIATION'] / counties_gdf['POPULATION_2065_S3']) * 100

        min_pop = counties_gdf[scenario].min()
        max_pop = counties_gdf[scenario].max()
        counties_gdf['NORMALIZED_POP'] = (counties_gdf[scenario] - min_pop) / (max_pop - min_pop)

        counties_gdf['NRI_BUCKET'] = pd.cut(
            counties_gdf['FEMA_NRI'],
            bins=[0, 20, 40, 60, 80, 100],
            labels=['Very Low', 'Low', 'Moderate', 'High', 'Very High'],
            include_lowest=True
        )

        # Convert to GeoJSON for base map
        counties_geojson = json.loads(counties_gdf.to_json())

        # --- Climate Region Overlay via State Metadata ---
        if 'CLIMATE_REGION' not in counties_gdf.columns:
            st.error("Climate region data not available")
            return None

        # Get dominant climate region per state
        counties_gdf['STATE_FIPS'] = counties_gdf['COUNTY_FIPS'].str[:2]
        state_climate = counties_gdf.groupby('STATE_FIPS')['CLIMATE_REGION'].agg(
            lambda x: x.mode()[0] if not x.mode().empty else None
        ).reset_index().dropna()

        # Get state metadata with geometries
        states_data = database.get_state_metadata()
        states_data['geometry'] = states_data['GEOMETRY'].apply(
            lambda x: wkt.loads(x) if isinstance(x, str) else x
        )
        states_gdf = gpd.GeoDataFrame(states_data, geometry='geometry', crs='EPSG:4326')

        # Merge in climate regions
        states_gdf['STATE_FIPS'] = states_gdf['STATE_FIPS'].astype(str).str.zfill(2)
        states_gdf = states_gdf.merge(state_climate, how='inner', on='STATE_FIPS')

        # Dissolve by CLIMATE_REGION
        climate_regions_gdf = states_gdf.dissolve(by='CLIMATE_REGION')
        climate_regions_gdf['geometry'] = climate_regions_gdf['geometry'].simplify(tolerance=0.01, preserve_topology=True)
        climate_regions_geojson = json.loads(climate_regions_gdf.to_json())

        # --- Create plotly choropleth ---
        fig = px.choropleth(
            counties_gdf,
            geojson=counties_geojson,
            locations='COUNTY_FIPS',
            featureidkey="properties.COUNTY_FIPS",
            color='NRI_BUCKET',
            color_discrete_sequence=[
                RISK_COLORS_RGBA[3],
                RISK_COLORS_RGBA[4],
                RISK_COLORS_RGBA[2],
                RISK_COLORS_RGBA[1],
                RISK_COLORS_RGBA[0],
            ],
            hover_data={
                'NAME': True,
                'CLIMATE_REGION': True,
                'FEMA_NRI': True,
                'COUNTY_FIPS': False
            },
            custom_data=['NAME', 'CLIMATE_REGION', 'FEMA_NRI'],
            scope="usa"
        )

        fig.update_traces(
            hovertemplate='<b>%{customdata[0]}</b><br>' +
                          'Climate Region: %{customdata[1]}<br>' +
                          'National Risk Index: %{customdata[2]:.1f}<br>' +
                          '<extra></extra>'
        )

        # Climate region overlay with thick white borders
        fig.add_trace(
            go.Choropleth(
                geojson=climate_regions_geojson,
                locations=climate_regions_gdf.index.tolist(),
                z=[1] * len(climate_regions_gdf),
                colorscale=[[0, 'rgba(0,0,0,0)'], [1, 'rgba(0,0,0,0)']],
                marker_line_color='white',
                marker_line_width=5,
                showscale=False,
                name="Climate Regions",
                hoverinfo='skip'
            )
        )

        fig.update_geos(
            visible=False,
            scope="usa",
            showcoastlines=True,
            projection_type="albers usa"
        )

        fig.update_layout(
            height=800,
            title=dict(
                text="Natural Hazard Risk Index Across Counties",
                automargin=True,
                y=0.95
            ),
            legend=dict(
                title="",
                itemsizing="constant",
                groupclick="toggleitem",
                tracegroupgap=20,
                yanchor="top",
                y=0.9,
                xanchor="left",
                x=1.01,
                orientation="v"
            ),
            margin=dict(t=100, b=50, l=50, r=50),
            autosize=True,
        )

        return st.plotly_chart(fig, on_select="ignore", selection_mode=["points"], config=choropleth_config)

    except Exception as e:
        st.error(f"Could not create map: {e}")
        print(f"Map creation failed: {e}")
        return None


def population_by_climate_region(scenario):
    """
    Display a choropleth map of population by county for a given scenario, 
    with climate regions highlighted.
    """
    try:
        # --- Load county metadata ---
        counties_data = database.get_county_metadata(year=2010)
        counties_data = counties_data.merge(
            database.get_population_projections_by_fips(),
            how='inner',
            on='COUNTY_FIPS'
        )

        # Convert WKT to shapely geometry
        counties_data['geometry'] = counties_data['GEOMETRY'].apply(wkt.loads)

        # Create GeoDataFrame
        counties_gdf = gpd.GeoDataFrame(counties_data, geometry='geometry', crs='EPSG:4326')
        counties_gdf['geometry'] = counties_gdf['geometry'].simplify(tolerance=0.01, preserve_topology=True)

        # Compute scenario variation
        counties_gdf['VARIATION'] = counties_gdf[scenario] - counties_gdf['POPULATION_2065_S3']
        counties_gdf['VARIATION_PCT'] = (
            counties_gdf['VARIATION'] / counties_gdf['POPULATION_2065_S3']) * 100

        # Get state FIPS
        counties_gdf['STATE_FIPS'] = counties_gdf['COUNTY_FIPS'].str[:2]

        # Validate presence of climate data
        if 'CLIMATE_REGION' not in counties_gdf.columns:
            st.error("Climate region data not available")
            return None

        # Get dominant climate region per state
        state_climate_regions = counties_gdf.groupby('STATE_FIPS')['CLIMATE_REGION'].agg(
            lambda x: x.mode()[0] if not x.mode().empty else None
        ).reset_index().dropna()

        # Load state geometries
        states_data = database.get_state_metadata()
        states_data['geometry'] = states_data['GEOMETRY'].apply(wkt.loads)
        states_gdf = gpd.GeoDataFrame(states_data, geometry='geometry', crs='EPSG:4326')

        # Join climate regions to state geometries
        states_gdf['STATE_FIPS'] = states_gdf['STATE_FIPS'].astype(str).str.zfill(2)
        states_gdf = states_gdf.merge(state_climate_regions, how='inner', on='STATE_FIPS')

        # Dissolve states by climate region
        climate_regions_gdf = states_gdf.dissolve(by='CLIMATE_REGION')
        climate_regions_gdf['geometry'] = climate_regions_gdf['geometry'].simplify(tolerance=0.01, preserve_topology=True)
        climate_regions_geojson = json.loads(climate_regions_gdf.to_json())

        # Create GeoJSON from county geometries
        counties_geojson = json.loads(counties_gdf.to_json())

        # Get max abs variation
        max_abs_pct_change = max(
            abs(counties_gdf['VARIATION_PCT'].min()),
            abs(counties_gdf['VARIATION_PCT'].max())
        )

        # Base choropleth map
        fig = px.choropleth(
            counties_gdf,
            geojson=counties_geojson,
            color='VARIATION_PCT',
            color_continuous_scale='RdBu_r',
            range_color=[-max_abs_pct_change, max_abs_pct_change],
            locations='COUNTY_FIPS',
            featureidkey="properties.COUNTY_FIPS",
            scope="usa",
            labels={
                'COUNTY_NAME': 'County',
                'CLIMATE_REGION': 'Climate Region',
                scenario: 'Population (2065)',
                'VARIATION_PCT': 'Population Change (%)'
            },
            basemap_visible=False,
            hover_data={
                'COUNTY_NAME': True,
                'CLIMATE_REGION': True,
                scenario: True,
                'VARIATION_PCT': ':.2f',
                'COUNTY_FIPS': False
            },
            custom_data=['COUNTY_NAME', 'CLIMATE_REGION', scenario, 'VARIATION_PCT']
        )

        # Hover template
        fig.update_traces(
            hovertemplate='<b>%{customdata[0]}</b><br>' +
                          'Climate Region: %{customdata[1]}<br>' +
                          'Population (2065): %{customdata[2]:,.0f}<br>' +
                          'Change from Baseline: %{customdata[3]:.2f}%<br>' +
                          '<extra></extra>'
        )

        # Overlay: white-bordered climate regions
        fig.add_trace(
            go.Choropleth(
                geojson=climate_regions_geojson,
                locations=climate_regions_gdf.index.tolist(),
                z=[1] * len(climate_regions_gdf),
                colorscale=[[0, 'rgba(0,0,0,0)'], [1, 'rgba(0,0,0,0)']],
                marker_line_color='white',
                marker_line_width=5,
                showscale=False,
                name="Climate Regions",
                hoverinfo='skip'
            )
        )

        # Update layout
        fig.update_geos(
            visible=False,
            scope="usa",
            showcoastlines=True,
            projection_type="albers usa"
        )

        fig.update_coloraxes(
            colorbar_title="Population<br>Change (%)",
            colorbar_title_font_size=12,
            colorbar_title_side="right"
        )

        scenario_labels = {
            'POPULATION_2065_S5a': 'Low Impact Climate Migration',
            'POPULATION_2065_S5b': 'Medium Impact Climate Migration',
            'POPULATION_2065_S5c': 'High Impact Climate Migration'
        }
        scenario_title = scenario_labels.get(scenario, scenario)

        fig.update_layout(
            height=800,
            title=dict(
                text=f"Projected Population Change by 2065<br><sub>{scenario_title}</sub>",
                automargin=True,
                y=0.95
            ),
            legend=dict(
                title="",
                itemsizing="constant",
                groupclick="toggleitem",
                tracegroupgap=20,
                yanchor="top",
                y=0.9,
                xanchor="left",
                x=1.01,
                orientation="v"
            ),
            margin=dict(t=100, b=50, l=50, r=50),
            autosize=True,
        )

        # Add region labels
        for region, row in climate_regions_gdf.iterrows():
            centroid = row.geometry.centroid
            fig.add_annotation(
                x=centroid.x,
                y=centroid.y,
                text=region,
                showarrow=False,
                font=dict(family="Arial", size=16, color="black"),
                bgcolor="white",
                bordercolor="black",
                borderwidth=1,
                borderpad=4,
                opacity=0.8
            )

        return st.plotly_chart(fig, use_container_width=True, config=choropleth_config)

    except Exception as e:
        st.error(f"Could not create map: {e}")
        print(f"Map creation failed: {e}")
        return None
def plot_socioeconomic_indices(df, title=None):
    """
    Create a Plotly line chart showing socioeconomic indices over time

    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame containing socioeconomic indices data with at least:
        - 'Year' column
        - Index columns (socioeconomic_index_*) 
    title : str, optional
        Custom title for the chart. If None, a default title is used.

    Returns:
    --------
    fig : plotly.graph_objects.Figure
        The plotly figure object that can be displayed with st.plotly_chart()
    """
    import plotly.graph_objects as go

    # Create color palette with shades of #509BC7
    base_color = "#509BC7"
    colors = [
        "#8FC1DB",  # Lighter shade
        "#6BAED1",  # Light shade
        "#509BC7",  # Base color
        "#3E7A9E",  # Dark shade
    ]

    # Get the list of years for the x-axis
    years = sorted(df['Year'].unique())

    # Get the index columns (columns that start with 'socioeconomic_index_')
    index_columns = [col for col in df.columns if col.startswith(
        'socioeconomic_index_')]

    # Create figure
    fig = go.Figure()

    # Add traces for each socioeconomic index
    for i, column in enumerate(index_columns):
        # Create a more readable name for the legend
        display_name = column.replace(
            'socioeconomic_index_', '').replace('_', ' ').title()

        # Add the trace
        fig.add_trace(
            go.Scatter(
                x=years,
                y=df.sort_values('Year')[column],
                mode='lines+markers',
                name=display_name,
                line=dict(color=colors[i % len(colors)], width=3),
                marker=dict(size=8)
            )
        )

    # Use provided title or default
    chart_title = title if title else "Socioeconomic Indices Over Time"

    # Update layout
    fig.update_layout(
        title=chart_title,
        title_font_size=20,
        xaxis_title="Year",
        yaxis_title="Index Value",
        legend_title="Index Type",
        template="plotly_white",
        height=600,
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.2,
            xanchor="center",
            x=0.5
        ),
        margin=dict(t=60, b=120, l=80, r=80),
    )

    # Add grid lines for better readability
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')

    st.plotly_chart(fig)


def plot_socioeconomic_radar(df, selected_years=None):
    """
    Create a radar chart showing socioeconomic indices for selected years

    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame containing socioeconomic indices data
    selected_years : list, optional
        List of years to display. If None, shows first, middle, and last year.

    Returns:
    --------
    fig : plotly.graph_objects.Figure
        The plotly radar chart
    """
    import plotly.graph_objects as go

    # Get all available years
    years = sorted(df['Year'].unique())

    # If no years selected, choose first, middle and last year
    if not selected_years:
        if len(years) >= 3:
            selected_years = [years[0], years[len(years)//2], years[-1]]
        else:
            selected_years = years

    # Get index columns
    index_columns = [col for col in df.columns if col.startswith(
        'socioeconomic_index_')]
    categories = [col.replace('socioeconomic_index_', '').replace(
        '_', ' ').title() for col in index_columns]

    # Create color palette with shades of #509BC7
    colors = ["#8FC1DB", "#6BAED1", "#509BC7", "#3E7A9E", "#2C5876"]

    # Create figure
    fig = go.Figure()

    # Add traces for each selected year
    for i, year in enumerate(selected_years):
        year_data = df[df['Year'] == year]
        if not year_data.empty:
            fig.add_trace(go.Scatterpolar(
                r=[year_data[col].values[0] for col in index_columns],
                theta=categories,
                fill='toself',
                name=f'Year {year}',
                line=dict(color=colors[i % len(colors)], width=3),
            ))

    # Update layout
    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[-2, 2]  # Adjust based on your data range
            )
        ),
        title="Socioeconomic Profile Evolution",
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom",
                    y=-0.2, xanchor="center", x=0.5),
        height=600,
        margin=dict(t=60, b=100, l=80, r=80),
    )

    st.plotly_chart(fig)


def display_migration_impact_analysis(projections_dict, scenario):
    impact_map = {
        "Scenario S5b": "Low",
        "Scenario S5a": "Medium",
        "Scenario S5c": "High"
    }

    # Calculate metrics based on selected scenario vs baseline
    baseline_pop_2065 = projections_dict["POPULATION_2065_S3"]
    selected_pop_2065 = projections_dict[scenario]

    # Calculate additional residents (difference between selected scenario and baseline)
    additional_residents = int(selected_pop_2065 - baseline_pop_2065)

    # Calculate percentage increase relative to baseline
    percent_increase = round(
        (additional_residents / baseline_pop_2065) * 100, 1)

    st.metric(
        label="Estimated Population by 2065",
        value=f"{selected_pop_2065:,}",
        delta=None if additional_residents == 0 else (
            f"{additional_residents:,.0f}" if additional_residents > 0 else f"{additional_residents:,.0f}")
    )

    st.metric(
        label="Population Increase",
        value=f"{percent_increase}%",
    ),

    # Display metrics in same row
    # split_row(
    #     lambda: ,
    #     lambda:
    #     [0.5, 0.5]
    # )


def feature_cards(items):
    """
    Display a grid of feature cards with material icons, titles, and descriptions.

    Parameters:
    - items: List of dictionaries, each containing:
        - icon: Material icon name (without the 'material/' prefix)
        - title: Card title
        - description: Card description
    """
    # Add CSS for card styling
    st.markdown("""
    <style>
        .card-grid {
            display: flex;
            flex-wrap: wrap;
            gap: 16px;
            margin: 24px 0;
        }
        .feature-card {
            flex: 1;
            min-width: 200px;
            background-color: blue;
            border: 1px solid rgba(49, 51, 63, 0.2);
            border-radius: 8px;
            padding: 0.5em;
            box-sizing: border-box;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
            transition: transform 0.3s ease, box-shadow 0.3s ease;
        }
        .feature-card:hover {
            transform: translateY(-4px);
            box-shadow: 0 8px 15px rgba(0,0,0,0.1);
        }
        .card-title {
            font-weight: bold;
            font-size: 1.1rem;
            margin-bottom: 10px;
            display: flex;
            align-items: center;
            gap: 8px;
        }
        .card-description {
            color: #666;
        }
    </style>
    """, unsafe_allow_html=True)

    # Create columns for the cards
    cols = st.columns(len(items))

    # Generate each card in the appropriate column
    for col, item in zip(cols, items):
        with col:
            # Each column gets its own card
            with st.container():
                # Show the icon and title
                if 'icon' in item.keys():
                    st.markdown(f"## **:material/{item['icon']}:**")

                st.markdown(f"##### **{item['title']}**")

                # Show the description
                st.markdown(item['description'])

                # Add spacing
                st.markdown("<br>", unsafe_allow_html=True)


def display_scenario_impact_analysis(county_name, state_name, projected_data):
    """
    Display comprehensive impact analysis based on projected data
    """
    st.header(f"Migration Impact Analysis")

    # Add explanation of the scenarios
    with st.expander("About the Scenarios", expanded=False):
        st.markdown("""
        ### Understanding the Scenarios
        """)

        # 6. Show current population and projected populations of the county
        st.markdown("""
            The population projections shown in this dashboard represent different scenarios for how climate change might affect migration patterns and population distribution across U.S. regions by 2065.

            #### What These Scenarios Mean:

        """)

        feature_cards([
            {"title": "No Impact",
                "description": "The projection model only considers labor and housing feedback mechanisms"},
            {"title": "Low Impact",
                "description": "Model includes modest climate-influenced migration (50% of projected effect)"},
            {"title": "Medium Impact",
                "description": "The expected influence of climate migration on migration decisions (100% of projected effect)"},
            {"title": "High Impact",
                "description": "Illustrates an intensified scenario where climate factors are more severe (200% of projected effect)"},
        ])

    # Create tabs for different impact categories
    tab1, tab2, tab3 = st.tabs(["Employment", "Education", "Housing"])

    with tab1:
        st.subheader("Employment Impact")
        st.markdown("""
        This chart shows how different migration scenarios could affect employment rates in your community. 
        The 4% unemployment line represents the Non-Accelerating Inflation Rate of Unemployment (NAIRU), 
        generally considered to be a healthy level of unemployment in a stable economy.
        """)

        # Display employment chart
        employment_chart = create_employment_chart(projected_data)
        st.plotly_chart(employment_chart, use_container_width=True)

        # Add interpretation based on the data
        unemployment_above_threshold = any(
            100 - row['TOTAL_EMPLOYED_PERCENTAGE'] > 4.0 for _, row in projected_data.iterrows())

        if unemployment_above_threshold:
            st.warning(
                ":material/warning: Under some scenarios, unemployment may rise above the 4% NAIRU threshold, which could indicate economic stress.")
        else:
            st.success(
                ":material/check_circle_outline: Employment levels remain healthy across all scenarios, suggesting economic resilience.")

    with tab2:
        st.subheader("Education Impact")
        st.markdown("""
        This chart displays the projected student-teacher ratios under different scenarios. 
        The national average is approximately 16:1, with higher ratios potentially indicating 
        strained educational resources.
        """)

        # Display education chart
        education_chart = create_student_teacher_chart(projected_data)
        st.plotly_chart(education_chart, use_container_width=True)

        # Add interpretation based on the data
        high_ratio_scenarios = [row['SCENARIO'] for _, row in projected_data.iterrows(
        ) if row['STUDENT_TEACHER_RATIO'] > 16.0]

        if high_ratio_scenarios:
            st.warning(
                f"⚠️ The student-teacher ratio exceeds the recommended level in {', '.join(high_ratio_scenarios)}. This may require additional educational resources or staff.")
        else:
            st.success(
                ":material/check_circle_outline: Educational resources appear adequate across all scenarios.")

    with tab3:
        st.subheader("Housing Impact")
        st.markdown("""
        This visualization shows housing availability across scenarios. A healthy housing market typically 
        maintains a vacancy rate between 5-8% (occupancy rate of 92-95%). Rates outside this range may 
        indicate housing shortages or excess vacancy.
        """)

        # Display housing chart
        housing_chart = create_housing_chart(projected_data)
        st.plotly_chart(housing_chart, use_container_width=True)

        # Calculate and add interpretation
        for _, row in projected_data.iterrows():
            occupancy_rate = (row['OCCUPIED_HOUSING_UNITS'] / (
                row['OCCUPIED_HOUSING_UNITS'] + row['AVAILABLE_HOUSING_UNITS'])) * 100
            vacancy_rate = 100 - occupancy_rate

            if vacancy_rate < 5:
                st.warning(
                    f"In the {row['SCENARIO']} scenario, the vacancy rate is below 5%, indicating a potential housing shortage.")
            elif vacancy_rate > 8:
                st.info(
                    f"In the {row['SCENARIO']} scenario, the vacancy rate is above 8%, suggesting potential excess housing capacity.")


def create_housing_chart(projected_data):
    # Make a copy of the dataframe to avoid modifying the original
    df = projected_data.copy()

    # Sort the dataframe by SCENARIO
    df = df.sort_values('SCENARIO')

    # Get the max absolute value for symmetric axis
    max_value = max(abs(df['AVAILABLE_HOUSING_UNITS'].max()),
                    abs(df['AVAILABLE_HOUSING_UNITS'].min()))

    # Calculate housing metrics if not already in the dataframe
    if 'HOUSING_OCCUPANCY_RATE' not in df.columns:
        df['HOUSING_OCCUPANCY_RATE'] = (df['OCCUPIED_HOUSING_UNITS'] / (
            df['OCCUPIED_HOUSING_UNITS'] + df['AVAILABLE_HOUSING_UNITS'])) * 100

    # Create the horizontal bar chart
    fig = go.Figure()

    # Sort the data by AVAILABLE_HOUSING_UNITS for better visualization
    sorted_data = df.sort_values('AVAILABLE_HOUSING_UNITS')

    fig.add_trace(go.Bar(
        y=sorted_data['SCENARIO'],
        x=sorted_data['AVAILABLE_HOUSING_UNITS'],
        orientation='h',
        marker=dict(
            color=sorted_data['AVAILABLE_HOUSING_UNITS'].apply(
                lambda x: '#E07069' if x < 0 else '#509BC7'),
            line=dict(color='rgba(0, 0, 0, 0.2)', width=1)
        )
    ))

    # Update layout for better appearance
    fig.update_layout(
        title="Projected Available Housing Units by Scenario in 2065",
        xaxis=dict(
            title="Available Housing Units in 2065",
            range=[-max_value, max_value],  # Symmetric x-axis
            zeroline=True,
            zerolinecolor='black',
            zerolinewidth=1
        ),
        yaxis=dict(
            title="Scenario",
            autorange="reversed"  # To have the largest value at the top
        ),
        height=500,
        margin=dict(l=100, r=20, t=70, b=70),
        template="plotly_white"
    )

    # Adding a vertical reference line at x=0
    fig.add_shape(
        type="line",
        x0=0, y0=-0.5,
        x1=0, y1=len(sorted_data) - 0.5,
        line=dict(color="black", width=1, dash="solid")
    )

    # Display the chart
    return fig


def create_student_teacher_chart(projected_data):
    # Make a copy of the dataframe to avoid modifying the original
    df = projected_data.copy()

    # Sort the dataframe by SCENARIO
    df = df.sort_values('SCENARIO')

    # Create figure
    fig = go.Figure()

    # Define the optimal student-teacher ratio threshold
    optimal_ratio = 16.0  # National average is around 16:1

    # Add bar for each scenario
    fig.add_trace(
        go.Bar(
            x=df['SCENARIO'],
            y=df['STUDENT_TEACHER_RATIO'],
            marker=dict(
                color=[
                    '#E07069' if ratio > optimal_ratio else '#509BC7'
                    for ratio in df['STUDENT_TEACHER_RATIO']
                ]
            ),
            text=[f"{ratio:.1f}" for ratio in df['STUDENT_TEACHER_RATIO']],
            textposition='auto',
            hovertemplate='Student-Teacher Ratio: %{y:.1f}<extra></extra>'
        )
    )

    # Add threshold line
    fig.add_shape(
        type="line",
        x0=-0.5,
        y0=optimal_ratio,
        x1=len(df) - 0.5,
        y1=optimal_ratio,
        line=dict(
            color="gray",
            width=2,
            dash="dash",
        ),
    )

    # Add annotation for the threshold
    fig.add_annotation(
        x=len(df) - 1,
        y=optimal_ratio + 0.5,
        text="Optimal Ratio (16:1)",
        showarrow=False,
        font=dict(
            color="gray"
        )
    )

    # Update layout
    fig.update_layout(
        title='Projected Student-Teacher Ratio by Scenario',
        xaxis=dict(
            title='Scenario',
            tickmode='array',
            tickvals=list(range(len(df))),
            ticktext=df['SCENARIO']
        ),
        yaxis=dict(
            title='Student-Teacher Ratio',
            range=[0, max(df['STUDENT_TEACHER_RATIO'])
                   * 1.2]  # Add some padding
        ),
        margin=dict(l=50, r=50, t=80, b=50),
        height=400,
    )

    return fig


def format_percentage(percentage):
    return f"{percentage:.1f}%"


def create_employment_chart(projected_data):
    # Make a copy of the dataframe to avoid modifying the original
    df = projected_data.copy()

    # Calculate the unemployed percentage for each scenario
    df['UNEMPLOYED_PERCENTAGE'] = 100 - df['TOTAL_EMPLOYED_PERCENTAGE']

    # Sort the dataframe by SCENARIO
    df = df.sort_values('SCENARIO')

    # Define the NAIRU threshold
    nairu_threshold = 4.0

    # Create figure with secondary y-axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Add traces for employed and unemployed percentages
    for index, row in df.iterrows():
        # Determine color for unemployed percentage bar
        unemployed_color = '#E07069' if row['UNEMPLOYED_PERCENTAGE'] > nairu_threshold else '#F0D55D'

        # Add employed percentage bar
        fig.add_trace(
            go.Bar(
                name='Employed',
                y=[row['SCENARIO']],
                x=[row['TOTAL_EMPLOYED_PERCENTAGE']],
                orientation='h',
                marker=dict(color='#509BC7'),
                text=[format_percentage(row['TOTAL_EMPLOYED_PERCENTAGE'])],
                textposition='inside',
                hoverinfo='text',
                hovertext=[
                    f"Employed: {format_percentage(row['TOTAL_EMPLOYED_PERCENTAGE'])}"],
                showlegend=index == 0  # Only show in legend for the first entry
            )
        )

        # Add unemployed percentage bar
        fig.add_trace(
            go.Bar(
                name='Unemployed',
                y=[row['SCENARIO']],
                x=[row['UNEMPLOYED_PERCENTAGE']],
                orientation='h',
                marker=dict(color=unemployed_color),
                text=[format_percentage(row['UNEMPLOYED_PERCENTAGE'])],
                textposition='inside',
                hoverinfo='text',
                hovertext=[
                    f"Unemployed: {format_percentage(row['UNEMPLOYED_PERCENTAGE'])}"],
                showlegend=index == 0  # Only show in legend for the first entry
            )
        )

    # Add NAIRU threshold line
    fig.add_trace(
        go.Scatter(
            name='NAIRU Threshold (4%)',
            x=[nairu_threshold],
            y=df['SCENARIO'],
            mode='lines',
            line=dict(color='gray', width=2, dash='dash'),
            opacity=0.8,
            hoverinfo='text',
            hovertext=['NAIRU Threshold: 4%'],
            showlegend=True
        ),
        secondary_y=False
    )

    # Update layout
    fig.update_layout(
        title='Projected Employment by Scenario',
        barmode='stack',
        xaxis=dict(
            title='Percentage (%)',
            range=[0, 100],
            tickvals=[0, 20, 40, 60, 80, 100],
            ticktext=['0%', '20%', '40%', '60%', '80%', '100%']
        ),
        yaxis=dict(
            title='Scenario',
            categoryorder='array',
            categoryarray=df['SCENARIO'].tolist()
        ),
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=1.02,
            xanchor='right',
            x=1
        ),
        margin=dict(l=50, r=50, t=80, b=50),
        height=400,
    )

    return fig


def socioeconomic_projections(county_fips):
    indices_df = database.get_projections_by_county(county_fips)

    st.write(indices_df)


def generate_policy_recommendations(projected_data):
    """Generate policy recommendations based on the projected data"""
    st.write("# Policy Recommendations")

    # Calculate metrics for recommendations
    recommendations = []

    # Check employment metrics
    for _, row in projected_data.iterrows():
        unemployment_rate = 100 - row['TOTAL_EMPLOYED_PERCENTAGE']
        if unemployment_rate > 4.0 and row['SCENARIO'] in ['S5b', 'S5c']:
            recommendations.append({
                'category': 'Employment',
                'scenario': row['SCENARIO'],
                'issue': f"Projected unemployment rate of {unemployment_rate:.1f}% exceeds optimal levels",
                'recommendation': "Consider workforce development programs and economic incentives to attract industries likely to thrive in changing climate conditions."
            })

    # Check education metrics
    for _, row in projected_data.iterrows():
        if row['STUDENT_TEACHER_RATIO'] > 16.0 and row['SCENARIO'] in ['S5b', 'S5c']:
            recommendations.append({
                'category': 'Education',
                'scenario': row['SCENARIO'],
                'issue': f"Student-teacher ratio of {row['STUDENT_TEACHER_RATIO']:.1f} exceeds national average",
                'recommendation': "Plan for educational infrastructure expansion and teacher recruitment to maintain educational quality with population growth."
            })

    # Check housing metrics
    for _, row in projected_data.iterrows():
        occupancy_rate = (row['OCCUPIED_HOUSING_UNITS'] / (
            row['OCCUPIED_HOUSING_UNITS'] + row['AVAILABLE_HOUSING_UNITS'])) * 100
        vacancy_rate = 100 - occupancy_rate

        if vacancy_rate <= 0 and row['SCENARIO'] in ['S5b', 'S5c']:
            recommendations.append({
                'category': 'Housing',
                'scenario': row['SCENARIO'],
                'issue': f"Negative vacancy rate of {vacancy_rate:.1f}% indicates a shortage of housing.",
                'recommendation': "Implement zoning reforms and incentives for affordable housing development to accommodate projected population growth."
            })
        elif vacancy_rate < 5 and row['SCENARIO'] in ['S5b', 'S5c']:
            recommendations.append({
                'category': 'Housing',
                'scenario': row['SCENARIO'],
                'issue': f"Low vacancy rate of {vacancy_rate:.1f}% indicates potential housing shortage",
                'recommendation': "Implement zoning reforms and incentives for affordable housing development to accommodate projected population growth."
            })
        elif vacancy_rate > 8 and row['SCENARIO'] in ['S5b', 'S5c']:
            recommendations.append({
                'category': 'Housing',
                'scenario': row['SCENARIO'],
                'issue': f"High vacancy rate of {vacancy_rate:.1f}% indicates potential housing surplus",
                'recommendation': "Consider adaptive reuse strategies for vacant properties and focus on maintaining existing housing stock quality."
            })

    # Display recommendations
    if recommendations:
        for category in ['Employment', 'Education', 'Housing']:
            category_recommendations = [
                r for r in recommendations if r['category'] == category]
            if category_recommendations:
                st.write(f"##### {category} Recommendations")
                for rec in category_recommendations:
                    with st.expander(f"{rec['issue']} in {rec['scenario']} scenario"):
                        st.write(rec['recommendation'])
    else:
        st.info("Based on current projections, no critical interventions are needed as metrics remain within healthy ranges across scenarios.")


def display_population_projections(county_name, state_name, county_fips, population_historical, population_projections):
    st.write(f"### Population Projections for {county_name}, {state_name}")

    county_pop_historical = population_historical.loc[county_fips]

    # If the county has multiple rows of data, select the row with the most complete data
    if county_pop_historical.shape[0] > 1:
        # Count the number of missing values in each row
        missing_counts = county_pop_historical.isna().sum(axis=1)

        # Get the index of the row with the minimum number of missing values
        min_missing_idx = missing_counts.idxmin()

        county_pop_historical = county_pop_historical.loc[min_missing_idx]

    county_pop_projections = population_projections.loc[county_fips]

    # TODO: Rewrite to work with any number of scenarios that are included in the projections
    scenarios = [
        'POPULATION_2065_S3',
        'POPULATION_2065_S5b',
        'POPULATION_2065_S5a',
        'POPULATION_2065_S5c',
    ]

    scenario_labels = [
        'Scenario S3',
        'Scenario S5a',
        'Scenario S5b',
        'Scenario S5c'
    ]

    # Create a dictionary to store all projection scenarios
    projections_dict = {}

    # Add each projection scenario to the dictionary
    for scenario, label in zip(scenarios, scenario_labels):
        # Get the projected 2065 population for this scenario
        projected_pop_2065 = county_pop_projections[scenario]

        # Create a copy of the historical data for this scenario
        scenario_data = county_pop_historical.copy()

        # Add the 2065 projection to this scenario's data
        scenario_data['2065'] = projected_pop_2065

        # Add this scenario to the main dictionary
        projections_dict[label] = scenario_data

        # Convert the dictionary to a DataFrame with scenarios as the index

    projection_df = pd.DataFrame(projections_dict)

    # Drop the COUNTY_FIPS column which would otherwise be included as a datapoint on the x-axis
    projection_df = projection_df.drop(index='COUNTY_FIPS')
    projection_df = projection_df.set_index(
        pd.to_datetime(projection_df.index, format='%Y'))

    # Create the chart
    st.line_chart(projection_df)


def display_housing_indicators(county_name, state_name, county_fips):
    st.header('Housing Analysis')

    split_row(
        lambda: display_housing_burden_plot(
            county_name, state_name, county_fips),
        lambda: display_housing_vacancy_plot(
            county_name, state_name, county_fips),
        [0.5, 0.5]
    )


def display_housing_burden_plot(county_name, state_name, county_fips):
    rent_df = database.get_stat_var(
        table=Table.COUNTY_HOUSING_DATA,
        indicator_name="MEDIAN_GROSS_RENT",
        county_fips=county_fips
    )

    try:
        income_df = database.get_stat_var(
            table=Table.COUNTY_ECONOMIC_DATA,
            indicator_name="MEDIAN_INCOME",
            county_fips=county_fips
        )
    except AttributeError:
        st.error("Could not retrieve Median Income data. Please ensure 'COUNTY_ECONOMIC_DATA' table and 'MEDIAN_INCOME' variable exist and are accessible.")
        return
    except Exception as e:
        st.error(f"An error occurred fetching income data: {e}")
        return

    if rent_df.empty or income_df.empty:
        st.warning(
            f"Housing burden data not available for {county_name}, {state_name}.")
        return

    merged_df = pd.merge(rent_df, income_df, left_index=True,
                         right_index=True, how='inner')

    if merged_df.empty:
        st.warning(
            f"Matching rent and income data by year not available for {county_name}, {state_name}.")
        return

    # Calculation using the renamed columns
    merged_df['Median_Rent_Burden_Pct'] = (
        (merged_df['MEDIAN_GROSS_RENT'] * 12) / merged_df['MEDIAN_INCOME']) * 100

    # Reset index so 'YEAR' becomes a column for Plotly
    merged_df.reset_index(inplace=True)

    # Convert the 'YEAR' column to datetime
    merged_df['YEAR'] = pd.to_datetime(merged_df['YEAR'], format='%Y')

    fig = px.line(
        merged_df,
        x='YEAR',  # Use the column name from the index reset
        y='Median_Rent_Burden_Pct',
        title=f'Median Rent Burden Over Time'
    )

    fig.update_layout(
        xaxis_title='Year',  # Display title can be 'Year'
        yaxis_title='Median Rent Burden (%)',
        hovermode='x unified'
    )

    fig.add_shape(
        type="line",
        x0=merged_df['YEAR'].min(),  # Use the column name
        x1=merged_df['YEAR'].max(),  # Use the column name
        y0=30,
        y1=30,
        line=dict(color="orange", dash="dash", width=2),
        name="30% Threshold"
    )

    fig.add_shape(
        type="line",
        x0=merged_df['YEAR'].min(),  # Use the column name
        x1=merged_df['YEAR'].max(),  # Use the column name
        y0=50,
        y1=50,
        line=dict(color="red", dash="dash", width=2),
        name="50% Threshold"
    )

    fig.add_annotation(
        x=merged_df['YEAR'].iloc[-1],  # Use the column name
        y=30,
        text="30% Burden",
        showarrow=False,
        yshift=10,
        xshift=20,
        font=dict(color="orange")
    )

    fig.add_annotation(
        x=merged_df['YEAR'].iloc[-1],  # Use the column name
        y=50,
        text="50% Burden",
        showarrow=False,
        yshift=10,
        xshift=20,
        font=dict(color="red")
    )

    st.plotly_chart(fig, use_container_width=True)


def display_housing_vacancy_plot(county_name, state_name, county_fips):
    # Use indicator_name and expect column named "TOTAL_HOUSING_UNITS"
    total_units_df = database.get_stat_var(
        table=Table.COUNTY_HOUSING_DATA,
        indicator_name="TOTAL_HOUSING_UNITS",
        county_fips=county_fips
    )

    # Use indicator_name and expect column named "OCCUPIED_HOUSING_UNITS"
    occupied_units_df = database.get_stat_var(
        table=Table.COUNTY_HOUSING_DATA,
        indicator_name="OCCUPIED_HOUSING_UNITS",
        county_fips=county_fips
    )

    if total_units_df.empty or occupied_units_df.empty:
        st.warning(
            f"Housing unit data not available for {county_name}, {state_name}.")
        return

    # No renaming needed if columns are already named correctly
    # total_units_df = total_units_df.rename(columns={'Value': 'Total_Units'})
    # occupied_units_df = occupied_units_df.rename(columns={'Value': 'Occupied_Units'})

    # Merge on index (assumed 'YEAR')
    merged_df = pd.merge(total_units_df, occupied_units_df,
                         left_index=True, right_index=True, how='inner')

    if merged_df.empty:
        st.warning(
            f"Matching total and occupied housing unit data by year not available for {county_name}, {state_name}.")
        return

    # Calculate Vacant Units using the direct column names
    merged_df['Vacant_Units'] = merged_df["TOTAL_HOUSING_UNITS"] - \
        merged_df["OCCUPIED_HOUSING_UNITS"]

    # Calculate Vacancy Rate Percentage using the direct column name for total
    merged_df['Vacancy_Rate_Pct'] = (
        merged_df['Vacant_Units'] / merged_df["TOTAL_HOUSING_UNITS"]) * 100
    merged_df['Vacancy_Rate_Pct'] = merged_df['Vacancy_Rate_Pct'].replace(
        [float('inf'), float('-inf')], pd.NA).fillna(0)

    merged_df.reset_index(inplace=True)
    merged_df['YEAR'] = pd.to_datetime(merged_df['YEAR'], format='%Y')

    fig = px.line(
        merged_df,
        x='YEAR',
        y='Vacancy_Rate_Pct',
        title=f'Housing Vacancy Rate Over Time'
    )

    fig.update_layout(
        xaxis_title='Year',
        yaxis_title='Vacancy Rate (%)',
        hovermode='x unified'
    )

    healthy_vacancy_threshold = 7
    fig.add_shape(
        type="line",
        x0=merged_df['YEAR'].min(),
        x1=merged_df['YEAR'].max(),
        y0=healthy_vacancy_threshold,
        y1=healthy_vacancy_threshold,
        line=dict(color="green", dash="dash", width=2),
        name=f"{healthy_vacancy_threshold}% Threshold"
    )
    fig.add_annotation(
        x=merged_df['YEAR'].iloc[-1],
        y=healthy_vacancy_threshold,
        text=f"{healthy_vacancy_threshold}% Threshold",
        showarrow=False,
        yshift=10,
        xshift=20,
        font=dict(color="green")
    )

    st.plotly_chart(fig, use_container_width=True)


def display_economic_indicators(county_name, state_name, county_fips):
    st.header('Economic Analysis')
    split_row(
        lambda: display_unemployment_rate(
            county_name, state_name, county_fips),
        lambda: display_labor_participation(
            county_name, state_name, county_fips),
        [0.5, 0.5]
    )


def display_unemployment_rate(county_name, state_name, county_fips):
    unemployment_df = database.get_stat_var(
        table=Table.COUNTY_ECONOMIC_DATA,
        indicator_name="UNEMPLOYMENT_RATE",
        county_fips=county_fips
    )

    if unemployment_df.empty:
        st.warning(
            f"Matching total and occupied housing unit data by year not available for {county_name}, {state_name}.")
        return

    unemployment_df.reset_index(inplace=True)
    unemployment_df['YEAR'] = pd.to_datetime(
        unemployment_df['YEAR'], format='%Y')

    fig = px.line(
        unemployment_df,
        x='YEAR',
        y='UNEMPLOYMENT_RATE',
        title=f'Unemployment Rate Over Time'
    )

    fig.update_layout(
        xaxis_title='Year',
        yaxis_title='Unemployment Rate (%)',
        hovermode='x unified',
        yaxis_range=[0, None]
    )

    healthy_vacancy_threshold = 4
    fig.add_shape(
        type="line",
        x0=unemployment_df['YEAR'].min(),
        x1=unemployment_df['YEAR'].max(),
        y0=healthy_vacancy_threshold,
        y1=healthy_vacancy_threshold,
        line=dict(color="green", dash="dash", width=2),
        name=f"NAIRU Threshold"
    )

    fig.add_annotation(
        x=unemployment_df['YEAR'].iloc[-1],
        y=healthy_vacancy_threshold,
        text=f"NAIRU Threshold",
        showarrow=False,
        yshift=10,
        xshift=20,
        font=dict(color="green")
    )

    st.plotly_chart(fig, use_container_width=True)


def display_labor_participation(county_name, state_name, county_fips):
    # Fetch data from database
    labor_df = database.get_stat_var(
        table=Table.COUNTY_ECONOMIC_DATA,
        indicator_name="TOTAL_LABOR_FORCE",
        county_fips=county_fips
    )
    total_population_df = database.get_stat_var(
        table=Table.COUNTY_ECONOMIC_DATA,
        indicator_name="POPULATION",
        county_fips=county_fips
    )

    employed_population_df = database.get_stat_var(
        table=Table.COUNTY_ECONOMIC_DATA,
        indicator_name="TOTAL_EMPLOYED_POPULATION",
        county_fips=county_fips
    )

    # Merge datasets
    merged_df = pd.merge(labor_df, total_population_df,
                         left_index=True, right_index=True, how="inner")

    merged_df = pd.merge(merged_df, employed_population_df,
                         left_index=True, right_index=True, how="inner")

    if merged_df.empty:
        st.warning(f"Data not found for {county_name}, {state_name}.")
        return

    # Calculate labor force participation rate
    merged_df['LABOR_FORCE_PARTICIPATION_RATE'] = (
        merged_df['TOTAL_LABOR_FORCE'] / merged_df['POPULATION']) * 100

    # Reset index to make YEAR a column
    merged_df = merged_df.reset_index()

    # Create figure
    fig = go.Figure()

    # Add total population first (as base layer)
    fig.add_trace(
        go.Scatter(
            x=merged_df['YEAR'],
            y=merged_df['POPULATION'],
            fill='tozeroy',
            mode='lines',
            name='Total Population',
            line=dict(color='#b1d2e7', width=1),
            fillcolor='#b1d2e7'
        )
    )

    # Add labor force on top
    fig.add_trace(
        go.Scatter(
            x=merged_df['YEAR'],
            y=merged_df['TOTAL_LABOR_FORCE'],
            fill='tozeroy',
            mode='lines',
            name='Labor Force',
            line=dict(color='#E07069', width=2),
            fillcolor='#E07069'
        )
    )

    # Add labor force on top
    fig.add_trace(
        go.Scatter(
            x=merged_df['YEAR'],
            y=merged_df['TOTAL_EMPLOYED_POPULATION'],
            fill='tozeroy',
            mode='lines',
            name='Employed Population',
            line=dict(color='#265c7d', width=2),
            fillcolor='#265c7d'
        )
    )

    # Update layout with title and axis labels
    fig.update_layout(
        title=f'Labor Force Participation in {county_name}, {state_name}',
        xaxis_title='Year',
        yaxis_title='Population',
        hovermode='x unified',
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=0.95,
            xanchor='center',
            x=0.5
        ),
        margin=dict(l=60, r=60, t=50, b=50)
    )

    # Display the chart in Streamlit
    st.plotly_chart(fig, use_container_width=True)


def display_education_indicators(county_name, state_name, county_fips):
    st.header('Education Analysis')

    # Retrieve all the educational attainment data
    less_than_hs_df = database.get_stat_var(
        Table.COUNTY_EDUCATION_DATA, "LESS_THAN_HIGH_SCHOOL_TOTAL", county_fips=county_fips)
    hs_graduate_df = database.get_stat_var(
        Table.COUNTY_EDUCATION_DATA, "HIGH_SCHOOL_GRADUATE_TOTAL", county_fips=county_fips)
    some_college_df = database.get_stat_var(
        Table.COUNTY_EDUCATION_DATA, "SOME_COLLEGE_TOTAL", county_fips=county_fips)
    bachelors_higher_df = database.get_stat_var(
        Table.COUNTY_EDUCATION_DATA, "BACHELORS_OR_HIGHER_TOTAL", county_fips=county_fips)
    total_pop_25_64_df = database.get_stat_var(
        Table.COUNTY_EDUCATION_DATA, "TOTAL_POPULATION_25_64", county_fips=county_fips)

    # Combine all dataframes into one
    final_df = pd.DataFrame()
    final_df["YEAR"] = less_than_hs_df.index
    final_df["LESS_THAN_HIGH_SCHOOL_TOTAL"] = less_than_hs_df.values
    final_df["HIGH_SCHOOL_GRADUATE_TOTAL"] = hs_graduate_df.values
    final_df["SOME_COLLEGE_TOTAL"] = some_college_df.values
    final_df["BACHELORS_OR_HIGHER_TOTAL"] = bachelors_higher_df.values
    final_df["TOTAL_POPULATION_25_64"] = total_pop_25_64_df.values

    # Calculate percentages
    final_df["LessThanHighSchool_Perc"] = (
        final_df["LESS_THAN_HIGH_SCHOOL_TOTAL"] / final_df["TOTAL_POPULATION_25_64"]) * 100
    final_df["HighSchoolGraduate_Perc"] = (
        final_df["HIGH_SCHOOL_GRADUATE_TOTAL"] / final_df["TOTAL_POPULATION_25_64"]) * 100
    final_df["SomeCollege_Perc"] = (
        final_df["SOME_COLLEGE_TOTAL"] / final_df["TOTAL_POPULATION_25_64"]) * 100
    final_df["BachelorsOrHigher_Perc"] = (
        final_df["BACHELORS_OR_HIGHER_TOTAL"] / final_df["TOTAL_POPULATION_25_64"]) * 100

    # Create a title for the chart
    st.write(f"### Educational Attainment in {county_name}, {state_name}")

    # Create a figure for the stacked area chart
    fig = go.Figure()

    # Define colors for better visualization
    colors = {
        "Less than High School": RISK_COLORS_RGBA[3],
        "High School Graduate": RISK_COLORS_RGBA[2],
        "Some College": RISK_COLORS_RGBA[1],
        "Bachelor's or Higher": RISK_COLORS_RGBA[0]
    }

    # Add traces in reverse order (highest education first) for better stacking visualization
    # Each educational level is stacked on top of the previous one

    # Bachelor's or Higher (Bottom layer)
    fig.add_trace(
        go.Scatter(
            x=final_df["YEAR"],
            y=final_df["BachelorsOrHigher_Perc"],
            mode="lines",
            line=dict(width=0.5, color=colors["Bachelor's or Higher"]),
            fill="tozeroy",
            fillcolor=colors["Bachelor's or Higher"],
            name="Bachelor's Degree or Higher",
            hovertemplate="%{y:.1f}%<extra></extra>"
        )
    )

    # Some College (Second layer)
    # We add the percentages to create proper stacking
    fig.add_trace(
        go.Scatter(
            x=final_df["YEAR"],
            y=final_df["BachelorsOrHigher_Perc"] +
            final_df["SomeCollege_Perc"],
            mode="lines",
            line=dict(width=0.5, color=colors["Some College"]),
            fill="tonexty",
            fillcolor=colors["Some College"],
            name="Some College or Associate's Degree",
            hovertemplate="%{y:.1f}%<extra></extra>"
        )
    )

    # High School Graduate (Third layer)
    fig.add_trace(
        go.Scatter(
            x=final_df["YEAR"],
            y=final_df["BachelorsOrHigher_Perc"] + final_df["SomeCollege_Perc"] +
            final_df["HighSchoolGraduate_Perc"],
            mode="lines",
            line=dict(width=0.5, color=colors["High School Graduate"]),
            fill="tonexty",
            fillcolor=colors["High School Graduate"],
            name="High School Graduate",
            hovertemplate="%{y:.1f}%<extra></extra>"
        )
    )

    # Less than High School (Top layer)
    # This should add up to 100%
    fig.add_trace(
        go.Scatter(
            x=final_df["YEAR"],
            y=final_df["BachelorsOrHigher_Perc"] + final_df["SomeCollege_Perc"] +
            final_df["HighSchoolGraduate_Perc"] +
            final_df["LessThanHighSchool_Perc"],
            mode="lines",
            line=dict(width=0.5, color=colors["Less than High School"]),
            fill="tonexty",
            fillcolor=colors["Less than High School"],
            name="Less than High School",
            hovertemplate="%{y:.1f}%<extra></extra>"
        )
    )

    # Update layout
    fig.update_layout(
        xaxis=dict(
            title="Year",
            showgrid=True,
            gridwidth=1,
            gridcolor='rgba(0,0,0,0.1)'
        ),
        yaxis=dict(
            title="Percentage of Population (25-64)",
            showgrid=True,
            gridwidth=1,
            gridcolor='rgba(0,0,0,0.1)',
            range=[0, 100],  # Fix the y-axis range from 0 to 100%
            ticksuffix="%"
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.25,
            xanchor="center",
            x=0.5
        ),
        margin=dict(l=40, r=40, t=40, b=100),
        autosize=True,
        hovermode="x unified"
    )

    # Display the chart
    st.plotly_chart(fig, use_container_width=True)

    # Optional: Add a note about the data
    st.caption(
        "Note: Data represents educational attainment for the population aged 25-64.")

    # Display the latest year's data in a table format
    latest_year = final_df["YEAR"].max()
    latest_data = final_df[final_df["YEAR"] == latest_year].iloc[0]

    st.write(f"### Latest Educational Attainment ({latest_year})")

    col1, col2 = st.columns(2)

    with col1:
        st.metric(
            "Less than High School",
            f"{latest_data['LessThanHighSchool_Perc']:.1f}%"
        )
        st.metric(
            "High School Graduate",
            f"{latest_data['HighSchoolGraduate_Perc']:.1f}%"
        )

    with col2:
        st.metric(
            "Some College or Associate's",
            f"{latest_data['SomeCollege_Perc']:.1f}%"
        )
        st.metric(
            "Bachelor's or Higher",
            f"{latest_data['BachelorsOrHigher_Perc']:.1f}%"
        )


def display_unemployment_indicators(county_name, state_name, county_fips):
    st.header('Unemployment Analysis')

    # Retrieve the unemployment data needed for the chart
    # Using the same pattern as your education function but with economic data table
    total_labor_force_df = database.get_stat_var(
        Table.COUNTY_ECONOMIC_DATA, "TOTAL_LABOR_FORCE", county_fips=county_fips)
    unemployed_persons_df = database.get_stat_var(
        Table.COUNTY_ECONOMIC_DATA, "UNEMPLOYED_PERSONS", county_fips=county_fips)
    unemployment_rate_df = database.get_stat_var(
        Table.COUNTY_ECONOMIC_DATA, "UNEMPLOYMENT_RATE", county_fips=county_fips)

    # Combine all dataframes into one
    total_unemployment = pd.DataFrame()
    total_unemployment["YEAR"] = total_labor_force_df.index
    total_unemployment["TotalLaborForce"] = total_labor_force_df.values
    total_unemployment["TotalUnemployed"] = unemployed_persons_df.values
    total_unemployment["UnemploymentRate"] = unemployment_rate_df.values

    # Create a title for the chart
    st.write(
        f"###### Total Labor Force, Unemployed Population, and Unemployment Rate (2011-2023)")

    # Create a figure with secondary y-axis using Plotly
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Add trace for Total Labor Force (left y-axis)
    fig.add_trace(
        go.Scatter(x=total_unemployment["YEAR"], y=total_unemployment["TotalLaborForce"],
                   mode="lines+markers", name="Total Labor Force",
                   line=dict(color="blue"),
                   marker=dict(symbol="circle", color="blue")),
        secondary_y=False
    )

    # Add trace for Total Unemployed (left y-axis)
    fig.add_trace(
        go.Scatter(x=total_unemployment["YEAR"], y=total_unemployment["TotalUnemployed"],
                   mode="lines+markers", name="Total Unemployed",
                   line=dict(color="red"),
                   marker=dict(symbol="square", color="red")),
        secondary_y=False
    )

    # Add trace for Unemployment Rate (right y-axis)
    fig.add_trace(
        go.Scatter(x=total_unemployment["YEAR"], y=total_unemployment["UnemploymentRate"],
                   mode="lines+markers", name="Unemployment Rate (%)",
                   line=dict(dash="dash", color="green"),
                   marker=dict(symbol="triangle-up", color="green")),
        secondary_y=True
    )

    # Set axis titles
    fig.update_xaxes(title_text="YEAR")
    fig.update_yaxes(title_text="Number of People", secondary_y=False)
    fig.update_yaxes(title_text="Unemployment Rate (%)",
                     secondary_y=True, color="green")

    # Update layout to match the matplotlib style
    fig.update_layout(
        xaxis=dict(showgrid=True, gridwidth=1, gridcolor='rgba(0,0,0,0.1)'),
        yaxis=dict(showgrid=True, gridwidth=1, gridcolor='rgba(0,0,0,0.1)'),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.3,
            xanchor="center",
            x=0.5
        ),
        margin=dict(l=40, r=40, t=40, b=100),
        autosize=True,
    )

    # Display the chart
    st.plotly_chart(fig, use_container_width=True)


def display_unemployment_by_education(county_name, state_name, county_fips):
    st.header('Unemployment by Education Level')

    # Retrieve raw counts for each education level - both unemployed and total population
    # Unemployed counts
    less_than_hs_unemployed_df = database.get_stat_var(
        Table.COUNTY_EDUCATION_DATA, "LESS_THAN_HIGH_SCHOOL_UNEMPLOYED", county_fips=county_fips)
    hs_graduate_unemployed_df = database.get_stat_var(
        Table.COUNTY_EDUCATION_DATA, "HIGH_SCHOOL_GRADUATE_UNEMPLOYED", county_fips=county_fips)
    some_college_unemployed_df = database.get_stat_var(
        Table.COUNTY_EDUCATION_DATA, "SOME_COLLEGE_UNEMPLOYED", county_fips=county_fips)
    bachelors_higher_unemployed_df = database.get_stat_var(
        Table.COUNTY_EDUCATION_DATA, "BACHELORS_OR_HIGHER_UNEMPLOYED", county_fips=county_fips)

    # Total population counts
    less_than_hs_total_df = database.get_stat_var(
        Table.COUNTY_EDUCATION_DATA, "LESS_THAN_HIGH_SCHOOL_TOTAL", county_fips=county_fips)
    hs_graduate_total_df = database.get_stat_var(
        Table.COUNTY_EDUCATION_DATA, "HIGH_SCHOOL_GRADUATE_TOTAL", county_fips=county_fips)
    some_college_total_df = database.get_stat_var(
        Table.COUNTY_EDUCATION_DATA, "SOME_COLLEGE_TOTAL", county_fips=county_fips)
    bachelors_higher_total_df = database.get_stat_var(
        Table.COUNTY_EDUCATION_DATA, "BACHELORS_OR_HIGHER_TOTAL", county_fips=county_fips)

    # Combine all dataframes into one
    unemployment_by_edulevel = pd.DataFrame()
    unemployment_by_edulevel["YEAR"] = less_than_hs_unemployed_df.index

    # Store raw counts
    unemployment_by_edulevel["LessThanHighSchool_Unemployed"] = less_than_hs_unemployed_df.values
    unemployment_by_edulevel["HighSchoolGraduate_Unemployed"] = hs_graduate_unemployed_df.values
    unemployment_by_edulevel["SomeCollege_Unemployed"] = some_college_unemployed_df.values
    unemployment_by_edulevel["BachelorsOrHigher_Unemployed"] = bachelors_higher_unemployed_df.values

    unemployment_by_edulevel["LessThanHighSchool_Total"] = less_than_hs_total_df.values
    unemployment_by_edulevel["HighSchoolGraduate_Total"] = hs_graduate_total_df.values
    unemployment_by_edulevel["SomeCollege_Total"] = some_college_total_df.values
    unemployment_by_edulevel["BachelorsOrHigher_Total"] = bachelors_higher_total_df.values

    # Calculate unemployment rates by dividing unemployed by total population
    unemployment_by_edulevel["LessThanHighSchool_UnemploymentRate"] = (
        unemployment_by_edulevel["LessThanHighSchool_Unemployed"] /
        unemployment_by_edulevel["LessThanHighSchool_Total"] * 100
    )

    unemployment_by_edulevel["HighSchoolGraduate_UnemploymentRate"] = (
        unemployment_by_edulevel["HighSchoolGraduate_Unemployed"] /
        unemployment_by_edulevel["HighSchoolGraduate_Total"] * 100
    )

    unemployment_by_edulevel["SomeCollege_UnemploymentRate"] = (
        unemployment_by_edulevel["SomeCollege_Unemployed"] /
        unemployment_by_edulevel["SomeCollege_Total"] * 100
    )

    unemployment_by_edulevel["BachelorsOrHigher_UnemploymentRate"] = (
        unemployment_by_edulevel["BachelorsOrHigher_Unemployed"] /
        unemployment_by_edulevel["BachelorsOrHigher_Total"] * 100
    )

    # Create a title for the chart
    st.write(
        f"###### Unemployment Rate by Education Level (2011-2023)")

    # Create a figure using Plotly
    fig = go.Figure()

    # Add traces for each education level's unemployment rate
    fig.add_trace(
        go.Scatter(x=unemployment_by_edulevel["YEAR"],
                   y=unemployment_by_edulevel["LessThanHighSchool_UnemploymentRate"],
                   mode="lines+markers",
                   name="Less Than High School",
                   marker=dict(symbol="circle"))
    )

    fig.add_trace(
        go.Scatter(x=unemployment_by_edulevel["YEAR"],
                   y=unemployment_by_edulevel["HighSchoolGraduate_UnemploymentRate"],
                   mode="lines+markers",
                   name="High School Graduate",
                   marker=dict(symbol="square"))
    )

    fig.add_trace(
        go.Scatter(x=unemployment_by_edulevel["YEAR"],
                   y=unemployment_by_edulevel["SomeCollege_UnemploymentRate"],
                   mode="lines+markers",
                   name="Some College or Associate's Degree",
                   marker=dict(symbol="triangle-up"))
    )

    fig.add_trace(
        go.Scatter(x=unemployment_by_edulevel["YEAR"],
                   y=unemployment_by_edulevel["BachelorsOrHigher_UnemploymentRate"],
                   mode="lines+markers",
                   name="Bachelor's Degree or Higher",
                   marker=dict(symbol="diamond"))
    )

    # Set axis titles and layout
    fig.update_xaxes(title_text="YEAR")
    fig.update_yaxes(title_text="Unemployment Rate (%)")

    fig.update_layout(
        xaxis=dict(showgrid=True, gridwidth=1, gridcolor='rgba(0,0,0,0.1)'),
        yaxis=dict(showgrid=True, gridwidth=1, gridcolor='rgba(0,0,0,0.1)'),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.3,
            xanchor="center",
            x=0.5
        ),
        margin=dict(l=40, r=40, t=40, b=100),
        autosize=True,
    )

    # Display the chart
    st.plotly_chart(fig, use_container_width=True)


def display_county_indicators(county_fips, scenario):
    """
    Display key county indicators with simple descriptions based on z-scores.

    Args:
        county_fips (str): FIPS code for the selected county
    """
    # Fetch z-scores for the county (replace with your actual data retrieval)
    scenario_values = database.get_index_projections(county_fips, scenario)

    # indicators_df = indicators_df[indicators_df["COUNTY_FIPS"] == county_fips]

    # Extract z-scores (assuming your database function returns these values)
    z_student_teacher = scenario_values.get('z_STUDENT_TEACHER_RATIO', 0)
    z_housing = scenario_values.get('z_AVAILABLE_HOUSING_UNITS', 0)
    z_unemployment = scenario_values.get('z_UNEMPLOYMENT_RATE', 0)

    st.markdown("### Key Performance Indicators")

    # Function to get description based on z-score
    def get_description(z_score, is_inverse=False):
        if is_inverse:
            # For inverse indicators (lower is better)
            if z_score < -1.5:
                return "Excellent"
            elif z_score < -0.5:
                return "Good"
            elif z_score < 0.5:
                return "Average"
            elif z_score < 1.5:
                return "Below Average"
            else:
                return "Poor"
        else:
            # For regular indicators (higher is better)
            if z_score > 1.5:
                return "Excellent"
            elif z_score > 0.5:
                return "Good"
            elif z_score > -0.5:
                return "Average"
            elif z_score > -1.5:
                return "Below Average"
            else:
                return "Poor"

    # Student-Teacher Ratio (lower is better)
    if z_student_teacher:
        st.metric(
            label="Education",
            value=get_description(z_student_teacher, is_inverse=True),
            delta=f"{z_student_teacher:.1f}σ",
            delta_color="inverse"
        )
    else:
        st.metric(
            label="Education",
            value="N/A"
        )

    # Housing Availability (higher is better)
    if z_housing:
        st.metric(
            label="Housing",
            value=get_description(z_housing),
            delta=f"{z_housing:.1f}σ",
            delta_color="normal"
        )
    else:
        st.metric(
            label="Housing",
            value="N/A"
        )

    # Unemployment Rate (lower is better)
    if z_unemployment:
        st.metric(
            label="Labor",
            value=get_description(z_unemployment, is_inverse=True),
            delta=f"{z_unemployment:.1f}σ",
            delta_color="inverse"
        )
    else:
        st.metric(
            label="Labor",
            value="N/A"
        )

    vertical_spacer(1)

    # Optional: Add small explainer
    with st.expander("About these indicators"):
        st.caption(
            "Values show how this county compares to the national average.")
        st.caption("σ represents standard deviations from the mean.")
