# To use: streamlit run app.py
import streamlit as st
import duckdb
import pandas as pd
import re 
import requests

# Configuration and Initialization

# URL for submission
API_URL = "http://localhost:8000/submit_samples_csv"

# Initialize DuckDB connection
con = duckdb.connect()

st.set_page_config(page_title="Aquatic Samples Explorer", layout="wide")

# Environmental Metadata Columns 
ENV_COLS = [
    "depth", "temperature", "salinity", "ph", "carbon", "phosphorus",
    "carbon_dioxide", "organic_carbon", "inorganic_carbon", "nitrate", "nitrite",
    "nitrogen", "oxygen_concentration", "phosphate", "chlorophyll", "chloride",
    "methane", "date"
]

# =========================================================
# Caching Function 
# =========================================================
@st.cache_data(ttl=3600)  # Cache results for up to 1 hour
def run_duckdb_query(query):
    """Executes a DuckDB query and returns the DataFrame."""
    try:
        df = con.execute(query).fetchdf()
        return df
    except Exception as e:
        st.error(f"Error running query: {e}")
        return pd.DataFrame() # Return empty DataFrame on failure

# ========================================
# Helper Function for Parsing String Depths #

def parse_depth_string(depth_str):
    """Parses depth strings (e.g., '10-20' or '5.5') and returns the lower bound as a float."""
    if not depth_str or pd.isna(depth_str):
        return None
    try:
        if '-' in str(depth_str):
            return float(str(depth_str).split('-')[0])
        return float(depth_str)
    except ValueError:
        return None

# Data Loading and Pre-Filtering Setup #

#pre-populate the full species list (cached for summary)
full_species_list_query = """
    SELECT DISTINCT species 
    FROM read_parquet('database/AQUERY_long.parquet')
    ORDER BY species
"""
full_species_df = run_duckdb_query(full_species_list_query)
total_unique_species = len(full_species_df)

# Query to calculate the total unique samples in the full dataset 
total_samples_query = "SELECT COUNT(DISTINCT accession) AS total_samples FROM read_parquet('database/AQUERY_long.parquet');"
total_unique_samples = run_duckdb_query(total_samples_query)['total_samples'].iloc[0]

# Query to pre-populate the species list
SPECIES_SELECT_LIMIT = 500
limited_species_list_query = f"""
    SELECT DISTINCT species 
    FROM read_parquet('database/AQUERY_long.parquet')
    ORDER BY species
    LIMIT {SPECIES_SELECT_LIMIT}
"""
species_list = run_duckdb_query(limited_species_list_query)["species"].tolist()

# Query to pre-populate the season list
season_query = "SELECT DISTINCT season FROM read_parquet('database/AQUERY_long.parquet') WHERE season IS NOT NULL AND season != '' ORDER BY season;"
seasons = run_duckdb_query(season_query)['season'].tolist()

# Depth calculation for slider bounds
unique_depths_df = run_duckdb_query("SELECT DISTINCT depth FROM read_parquet('database/AQUERY_long.parquet') WHERE depth IS NOT NULL;")

if not unique_depths_df.empty:
    unique_depth_strings = unique_depths_df['depth'].astype(str).tolist()
    parsed_depths = [parse_depth_string(d) for d in unique_depth_strings]
    parsed_depths = [d for d in parsed_depths if d is not None]

    if parsed_depths:
        min_depth_val = min(parsed_depths)
        max_depth_val = max(parsed_depths)
    else:
        min_depth_val, max_depth_val = 0.0, 1000.0
else:
    min_depth_val, max_depth_val = 0.0, 1000.0

min_depth_val = float(min_depth_val)
max_depth_val = float(max_depth_val)

# =========================================================
# Tabs

st.title("Aquatic Samples Data Application")
explorer_tab, submission_tab = st.tabs(["Data Explorer & Analysis", "Submit New Samples"])


# Data Explorer Tab

with explorer_tab:
    st.header("Data Exploration and Filtering")
    
    # --- Sidebar Section & Example Queries ---
    st.sidebar.header("Example Queries")
    
    example_queries = {
        "View first 20 rows (long)": "SELECT * FROM read_parquet('database/AQUERY_long.parquet') LIMIT 20;",
        "View first 20 rows (wide)": "SELECT * FROM read_parquet('database/AQUERY.parquet') LIMIT 20;",
        "List all columns (wide)": "PRAGMA table_info(read_parquet('database/AQUERY.parquet'));",
        "Count total samples": "SELECT COUNT(*) AS total_samples FROM read_parquet('database/AQUERY_long.parquet');",
        "Metadata, Species & Abundance (>0)": """
            -- Select all metadata + species/abundance where abundance is greater than 0
            SELECT 
                accession, latitude, longitude, season, depth, temperature, salinity, ph, 
                carbon, phosphorus, carbon_dioxide, nitrogen, oxygen_concentration, 
                phosphate, chlorophyll, chloride, date, species, abundance
            FROM read_parquet('database/AQUERY_long.parquet')
            WHERE abundance > 0
            LIMIT 50;
        """,
        "Distinct lat/long pairs": """
            SELECT DISTINCT latitude, longitude
            FROM read_parquet('database/AQUERY_long.parquet')
            LIMIT 20;
        """,
        "Top 10 species by abundance": """
            SELECT species, SUM(abundance) AS total_abundance
            FROM read_parquet('database/AQUERY_long.parquet')
            GROUP BY species
            ORDER BY total_abundance DESC
            LIMIT 10;
        """,
    }

    selected_example = st.sidebar.selectbox("Pick a query", list(example_queries.keys()))

    # Quick Filters (Environmental Metadata & Species)
    st.sidebar.header("Quick Filters")

    # --- Species and Abundance Filters (With Limit Note) ---
    if total_unique_species > SPECIES_SELECT_LIMIT:
        st.sidebar.caption(f"Showing top {SPECIES_SELECT_LIMIT} of {total_unique_species:,} species for performance.")

    species_choice = st.sidebar.selectbox("Filter by species", ["(All)"] + species_list)
    accession_input = st.sidebar.text_input("Sample ID (Accession) contains:")
    min_abundance = st.sidebar.number_input("Min abundance", value=0.0, step=0.00001, format="%.5f")

    # --- Environmental Filters ---
    season_choice = st.sidebar.multiselect("Filter by Season", seasons, default=seasons)

    # Depth
    if min_depth_val != max_depth_val:
        depth_range = st.sidebar.slider(
            "Filter by Depth (m)",
            min_value=min_depth_val,
            max_value=max_depth_val,
            value=(min_depth_val, max_depth_val),
            step=(max_depth_val - min_depth_val) / 100.0 if (max_depth_val - min_depth_val) > 0.01 else 0.1
        )
    else:
        depth_range = (min_depth_val, max_depth_val) 

    # WHERE clause for DuckDB 
    filter_conditions = []
    if species_choice != "(All)":
        filter_conditions.append(f"species = '{species_choice}'")
    if accession_input:
        filter_conditions.append(f"accession ILIKE '%{accession_input}%'")
    if min_abundance > 0:
        filter_conditions.append(f"abundance >= {min_abundance}")
    if season_choice and len(season_choice) < len(seasons):
        season_str = ', '.join([f"'{s.replace("'", "''")}'" for s in season_choice]) # Escape quotes for SQL
        filter_conditions.append(f"season IN ({season_str})")

    where_clause = "WHERE " + " AND ".join(filter_conditions) if filter_conditions else ""
    filter_query = f"""
        SELECT *
        FROM read_parquet('database/AQUERY_long.parquet')
        {where_clause}
        LIMIT 100000
    """

    df_filter_long = run_duckdb_query(filter_query)

    # ===========================
    # Pandas Filtering for Depth 

    if not df_filter_long.empty and depth_range != (min_depth_val, max_depth_val):
        st.info(f"Applying depth filter to {len(df_filter_long):,} rows...")
        
        df_filter_long['parsed_depth'] = df_filter_long['depth'].apply(parse_depth_string)
        
        # Filter DataFrame using the slider values
        df_filter_long = df_filter_long[
            (df_filter_long['parsed_depth'] >= depth_range[0]) & 
            (df_filter_long['parsed_depth'] <= depth_range[1])
        ]
        
        # Drop temporary column
        df_filter_long = df_filter_long.drop(columns=['parsed_depth'])


    # ====================
    # Analysis Summary 
    # ===================
    st.subheader("Analysis Summary")

    if not df_filter_long.empty:
        
        col1, col2, col3, col4 = st.columns(4)
        
        # Summary
        num_samples_filtered = len(df_filter_long['accession'].unique())
        num_species_filtered = len(df_filter_long['species'].unique()) 
        
        col1.metric("Total Species Available", f"{total_unique_species:,}")
        col2.metric("Total Samples Available", f"{total_unique_samples:,}")
        col3.metric("Samples Count (Filtered)", f"{num_samples_filtered:,}")
        col4.metric("Species Count (Filtered)", f"{num_species_filtered:,}")
        
        # Top 10 Species Chart
        st.markdown("#### Top 10 Species by Abundance in Filtered Data")
        
        if 'abundance' in df_filter_long.columns and num_species_filtered > 0:
            top_10_df = df_filter_long.groupby('species')['abundance'].sum().nlargest(10).reset_index()
            top_10_df.columns = ['species', 'Total Abundance']
            st.bar_chart(top_10_df.set_index('species'))
        else:
            st.info("Insufficient species data to generate Top 10 chart.")
        
        # Full Species List Viewer
        if total_unique_species > SPECIES_SELECT_LIMIT:
            with st.expander(f"View Full List of {total_unique_species:,} Available Species"):
                st.dataframe(full_species_df, width='stretch', height=300)
                st.download_button(
                    "Download Full Species List (CSV)",
                    data=full_species_df.to_csv(index=False).encode("utf-8"),
                    file_name="full_species_list.csv",
                    mime="text/csv"
                )

    else:
        st.warning("No data matches the current filter settings. Try adjusting the filters.")

    # ======================
    # Viewer in wide format 
    # =======================
    st.subheader("Filtered Results (wide format, like CSV)")

    if not df_filter_long.empty and 'species' in df_filter_long.columns and 'abundance' in df_filter_long.columns:
        
        pivot_cols = [c for c in df_filter_long.columns if c not in ['species', 'abundance']]
        
        try:
            df_filter_wide = df_filter_long.pivot(
                index=pivot_cols,
                columns="species",
                values="abundance"
            ).reset_index()

            st.success(f"Showing {len(df_filter_wide)} rows (wide format)")
            st.dataframe(df_filter_wide)

            st.download_button(
                "Download filtered results (CSV)",
                data=df_filter_wide.to_csv(index=False).encode("utf-8"),
                file_name="filtered_results_wide.csv",
                mime="text/csv"
            )
        except Exception as e:
            st.error(f"Could not pivot data to wide format (too many unique species or index issues): {e}")

    # ====================
    # Map Visualization 
    # ===================
    if {"latitude", "longitude"}.issubset(df_filter_long.columns) and not df_filter_long.empty:
        st.subheader("Sample Locations")
        coords = df_filter_long[["latitude", "longitude"]].dropna().copy()
        
        try:
            coords["latitude"] = coords["latitude"].astype(float)
            coords["longitude"] = coords["longitude"].astype(float)
            coords = coords[(coords['latitude'] >= -90) & (coords['latitude'] <= 90)]
            coords = coords[(coords['longitude'] >= -180) & (coords['longitude'] <= 180)]
            
            if not coords.empty:
                st.map(coords, zoom=1) 
            else:
                st.info("No valid latitude/longitude data in the filtered results for mapping.")
                
        except Exception:
            st.info("Latitude/longitude columns are not numeric and cannot be mapped.")

    # =====================
    # Correlation Analysis 
    # =====================
    st.subheader("Correlation Analysis")

    if not df_filter_long.empty and 'abundance' in df_filter_long.columns:
        
        full_species_list = full_species_df['species'].tolist()

        st.markdown(
            """
            <p style='font-size: small; color: gray;'>
            Species lists for correlation are drawn from the **full set of {total_unique_species:,} available species** for comprehensive testing. 
            If a selected species is not present in the current filters, the calculation will be skipped.
            </p>
            """.format(total_unique_species=total_unique_species),
            unsafe_allow_html=True
        )

        st.markdown("##### Select Analysis Type")
        correlation_type = st.radio(
            "Compare:",
            ["Species Abundance vs. Environmental Factor", "Species Abundance vs. Species Abundance"],
            index=0,
            key='corr_type'
        )

        if correlation_type == "Species Abundance vs. Environmental Factor":
            
            col_a, col_b, col_c = st.columns([1, 1, 1])
            target_species = col_a.selectbox("Select Target Species", ["(Select Species)"] + full_species_list, key='env_species')
            numeric_env_cols = [c for c in ENV_COLS if c in df_filter_long.columns]
            target_env = col_b.selectbox("Select Environmental Factor", ["(Select Factor)"] + numeric_env_cols, key='env_factor')
            
            if target_species != "(Select Species)" and target_env != "(Select Factor)":
                
                if target_species not in df_filter_long['species'].unique():
                      st.info(f"The selected species **{target_species}** is not present in the current filtered data. Please adjust your sidebar filters.")
                else:
                    try:
                        # Prepare data 
                        corr_df = df_filter_long[df_filter_long['species'] == target_species]
                        
                        # Aggregate to one row per sample ID 
                        corr_data = corr_df.groupby('accession').agg({
                            'abundance': 'max',
                            target_env: 'first' 
                        }).dropna()
                        
                        # 2. Ensure both columns are numeric for correlation
                        corr_data['abundance'] = pd.to_numeric(corr_data['abundance'], errors='coerce')
                        corr_data[target_env] = pd.to_numeric(corr_data[target_env], errors='coerce')
                        corr_data = corr_data.dropna()
                        
                        if len(corr_data) > 1:
                            # Calculate Correlation
                            correlation = corr_data['abundance'].corr(corr_data[target_env], method='pearson')
                            
                            col_c.metric(
                                "Pearson Correlation (r)",
                                f"{correlation:.4f}",
                                help=f"Measures the linear relationship between {target_species} abundance and {target_env} across {len(corr_data)} unique samples."
                            )
                            
                            # Scatter Plot 
                            st.markdown("##### Abundance vs. Environmental Factor")
                            chart_data = corr_data.rename(columns={'abundance': f'{target_species} Abundance', target_env: target_env.replace('_', ' ').title()})
                            st.scatter_chart(chart_data, x=target_env.replace('_', ' ').title(), y=f'{target_species} Abundance')
                        else:
                            st.info("Not enough unique samples (need > 1) in the filtered data to calculate correlation for this combination.")
                    except Exception as e:
                        st.error(f"An error occurred during correlation calculation: {e}")
                
            else:
                st.info("Select a species and an environmental factor above to analyze their relationship.")

        elif correlation_type == "Species Abundance vs. Species Abundance":
            
            selected_species = st.multiselect(
                "Select Species (Minimum 2, Maximum 10)", 
                full_species_list, 
                key='species_multi',
                max_selections=10,
                help="Select 2 or more species to calculate their co-occurrence correlation matrix."
            )

            if len(selected_species) >= 2:
                try:
                    # 1. Pivot the long data to get one column per selected species
                    species_df = df_filter_long[df_filter_long['species'].isin(selected_species)]
                    
                    if species_df.empty:
                        st.info("None of the selected species are present in the current filtered data. Please adjust your sidebar filters.")
                    else:
                        # Define pivot columns (all metadata except species/abundance)
                        species_pivot_cols = [c for c in species_df.columns if c not in ['species', 'abundance']]
                        
                        # Pivot to wide format for just these two species and fill NA with 0
                        species_wide = species_df.pivot_table(
                            index=species_pivot_cols, 
                            columns='species', 
                            values='abundance', 
                            fill_value=0
                        ).reset_index()
                        
                        # 2. Select only the abundance columns and ensure they are numeric
                        abundance_cols = [s for s in selected_species if s in species_wide.columns]
                        corr_data = species_wide[abundance_cols].apply(pd.to_numeric, errors='coerce').dropna()
                        
                        if len(corr_data) > 1 and len(abundance_cols) >= 2:
                            # Calculate Correlation Matrix
                            correlation_matrix = corr_data.corr(method='pearson')
                            
                            st.markdown(f"##### Correlation Matrix (Pearson $r$) across {len(corr_data)} Unique Samples")
                            st.info("Values range from -1 (strong negative correlation) to 1 (strong positive correlation).")
                            
                            styled_matrix = correlation_matrix.style.background_gradient(
                                cmap='RdYlBu', 
                                axis=None, 
                                vmin=-1, 
                                vmax=1
                            ).format(precision=4)
                            
                            st.dataframe(styled_matrix, use_container_width=True)
                            
                            if len(selected_species) == 2:
                                col1, col2 = selected_species[0], selected_species[1]
                                
                                st.markdown("---")
                                
                                col_met, _, _ = st.columns([1, 1, 1])
                                col_met.metric(
                                    "Pairwise $r$",
                                    f"{correlation_matrix.loc[col1, col2]:.4f}", 
                                    help=f"Pearson Correlation between {col1} and {col2}."
                                )
                                
                                st.markdown("##### Abundance Scatter Plot (Pairwise)")
                                chart_data = corr_data.rename(columns={col1: f'{col1} Abundance', col2: f'{col2} Abundance'})
                                st.scatter_chart(chart_data, x=f'{col1} Abundance', y=f'{col2} Abundance')
                        else:
                            st.info("Not enough unique samples (need > 1) or species selected to calculate correlation.")

                except Exception as e:
                    st.error(f"An error occurred during species-species correlation calculation: {e}")
                    
            else:
                st.info("Select two or more species above to analyze their co-occurrence.")
                
    else:
        st.info("No data available to perform correlation analysis.")

    # =================
    # SQL Query Editor
    # =================
    st.subheader("SQL Query Editor")

    default_query = example_queries[selected_example]
    query = st.text_area("Write or edit SQL query:", default_query, height=200)

    # Toggle between wide and long file
    sql_mode = st.radio(
        "Choose data source for SQL query:",
        ["database/AQUERY_long.parquet (tidy)", "AQUERY.parquet (wide)"],
        index=0
    )
    if "wide" in sql_mode:
        query = query.replace("database/AQUERY_long.parquet", "AQUERY.parquet")

    if st.button("Run Query"):
        df = run_duckdb_query(query) # Use the cached function

        if not df.empty:
            st.success(f"Query returned {len(df)} rows")
            st.dataframe(df)

            # Download buttons
            csv = df.to_csv(index=False).encode("utf-8")
            st.download_button("Download as CSV", data=csv, file_name="query_results.csv", mime="text/csv")
            
######################
# Submit new samples #
with submission_tab:
    st.header("New Sample Data Submission for Review")
    st.markdown("""
        Upload a CSV file containing new aquatic sample data in **long format**. 
        The file will be sent to the FastAPI server for automated schema and data type validation.
        
        **Required Columns:** `accession`, `species`, `abundance`, `latitude`, `longitude`, 
        and any relevant environmental columns (`depth`, `temperature`, `salinity`, etc.).
    """)
    st.warning("**Ensure your FastAPI server is running at** `http://localhost:8000` **before submitting.**")
    
    # --- Wide Format Submission Controls ---
    col_uploader, col_button = st.columns([0.7, 0.3])

    # File Uploader (in the wider column)
    uploaded_file = col_uploader.file_uploader("Upload Samples CSV (Long Format)", type="csv")

    # Handle submission logic if file is uploaded
    if uploaded_file is not None:
        col_button.markdown("<br>", unsafe_allow_html=True) 
        
        if col_button.button("Submit Data for Validation and Review", key="submit_button", use_container_width=True):
            
            with st.spinner("Submitting data to API for validation..."):
                try:
                    # Prepare file for data submission
                    files = {'file': (uploaded_file.name, uploaded_file.getvalue(), 'text/csv')}
                    
                    # Make the POST request to the FastAPI endpoint
                    response = requests.post(API_URL, files=files)

                    if response.status_code == 200:
                        st.success("Submission successful! Data passed initial validation.")
                        st.balloons()
                        st.markdown("---")
                        st.subheader("Validation Summary")
                        # Display the JSON output from the API
                        st.json(response.json())
                        
                    else:
                        st.error(f"Submission failed (HTTP {response.status_code}). Please check the errors below and correct your CSV.")
                        st.markdown("---")
                        st.subheader("Detailed API Error Response")
                        
                        # Try to parse JSON response for detailed error message
                        try:
                            error_data = response.json()
                            st.json(error_data)
                        except requests.exceptions.JSONDecodeError:
                            st.text(response.text)
                            
                except requests.exceptions.ConnectionError:
                    st.error("Connection Error: Could not connect to the FastAPI server. Please ensure the server is running at `http://localhost:8000`.")
                except Exception as e:
                    st.error(f"An unexpected error occurred: {e}")

# =========================================================
# Styling/Design
st.markdown(
    """
    <style>
    /* Style/design
    .stButton>button {
        background-color: #163e64;
        color: white;
        border-radius: 10px;
        padding: 10px 20px;
        font-weight: bold;
    }
    .stApp {
        color: #2c8c99;
    }
    [data-testid="stMetric"] > div {
        background-color: #15836E; 
        padding: 15px;
        border-radius: 10px;
        border-left: 5px solid #a9d8da; 
    }
    .stHorizontalBlock {
        margin-bottom: 20px;
    }
    .stTabs [data-baseweb="tab-list"] button[aria-selected="true"] {
        border-bottom-color: #163e64 !important; 
        color: #163e64 !important; 
        font-weight: bold;
    }
    </style>
    """,
    unsafe_allow_html=True
)
