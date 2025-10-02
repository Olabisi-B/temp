import duckdb
import os
import pandas as pd

# --- Configuration ---
parquet_file = "AQUERY.parquet"
database_file = "AQUERY.duckdb" 

# Check if the required Parquet file exists
if not os.path.exists(parquet_file):
    print(f"Error: The data file '{parquet_file}' was not found.")
    print("Please ensure AQUERY.parquet is in the same directory.")
    exit()

# Connect to or create the DuckDB database
con = duckdb.connect(database=database_file)
print(f"Connected to DuckDB database: {database_file}")

# --- 1. Initial Setup: Create View from Parquet ---
print("\n--- 1. Setting up temporary data structures (Views and Tables) ---")

# Create a view directly on the wide parquet file (raw_data)
con.execute(f"""
    CREATE OR REPLACE VIEW raw_data AS
    SELECT * FROM read_parquet('{parquet_file}')
""")
print("Created VIEW: raw_data (Wide Format)")

# List of all metadata columns to exclude during UNPIVOT
# NOTE: These names MUST exactly match the columns in your Parquet file.
metadata_cols = [
    "accession","latitude","longitude","environmental_condition","season","depth",
    "temperature","salinity","ph","carbon","phosphorus","carbon_dioxide",
    "organic_carbon","inorganic_carbon","nitrate","nitrite","nitrogen",
    "oxygen_concentration","phosphate","chlorophyll","chloride","methane","date"
]
metadata_cols_str = ", ".join(metadata_cols)

# --- 2. Transformation: Wide → Long Format (UNPIVOT) ---
con.execute(f"""
    CREATE OR REPLACE TABLE aquatic_samples_long AS
    UNPIVOT raw_data
    ON * EXCLUDE ({metadata_cols_str})
    INTO NAME species VALUE abundance
""")
print("Created TABLE: aquatic_samples_long (Unpivoted)")

# --- 3. Cleaning: Cast Abundance to Numeric Type ---
con.execute("""
    CREATE OR REPLACE TABLE aquatic_samples_long AS
    SELECT 
        *, -- Select all columns from the long table
        TRY_CAST(abundance AS DOUBLE) AS abundance_double
    FROM aquatic_samples_long
""")
print("Updated TABLE: aquatic_samples_long (Abundance cast to DOUBLE)")
print("-" * 50)


# --- Example Queries ---

# Query A (Your original query): Top 10 Species by Mean Abundance
print("\n--- Example Query A: Top 10 Species by Mean Abundance ---")
df_a = con.execute("""
    SELECT 
        species, 
        AVG(abundance_double) as mean_abundance
    FROM aquatic_samples_long
    WHERE abundance_double IS NOT NULL
    GROUP BY species
    ORDER BY mean_abundance DESC
    LIMIT 10
""").fetchdf()

print(df_a)

# Query B: Total Samples and Unique Species in the 'Winter' Season
print("\n--- Example Query B: Total Samples and Unique Species in 'Winter' ---")
df_b = con.execute("""
    SELECT 
        COUNT(DISTINCT accession) AS total_samples,
        COUNT(DISTINCT species) AS total_species
    FROM aquatic_samples_long
    WHERE LOWER(season) = 'winter'
""").fetchdf()

print(df_b)


# Query C: Environmental Conditions for Samples with High Carbon Dioxide
print("\n--- Example Query C: Samples with CO2 > 500 (Filtered Metadata) ---")
df_c = con.execute("""
    SELECT 
        accession, 
        temperature, 
        salinity,
        carbon_dioxide
    FROM raw_data
    WHERE TRY_CAST(carbon_dioxide AS DOUBLE) > 500
    LIMIT 5
""").fetchdf()

print(df_c)


# Close the database connection
con.close()
print("\nDatabase connection closed.")
