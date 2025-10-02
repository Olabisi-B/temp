# To run use: uvicorn aquery_api:app --reload
#To check docs after running look at http://127.0.0.1:8000/docs
from fastapi import FastAPI, HTTPException, Query, Depends, UploadFile, File
import duckdb
import pandas as pd
from typing import Optional, List, Dict, Any, Generator, Union

app = FastAPI(title="Aquatic Samples API", version="1.4 - Full Feature Set")

# --- Configuration ---
# File names
WIDE_FILE = "AQUERY.parquet" 
LONG_FILE = "AQUERY_long.parquet"

# List of valid environmental variables for the /environmental_stats endpoint
ENVIRONMENTAL_VARS = [
    "temperature", "salinity", "ph", "carbon", "phosphorus", "carbon_dioxide",
    "organic_carbon", "inorganic_carbon", "nitrate", "nitrite", "nitrogen",
    "oxygen_concentration", "phosphate", "chlorophyll", "chloride", "methane"
]

# --- Database Connection Dependency ---

# DuckDB connections should be opened and closed per request for thread safety
def get_db() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Provides a fresh DuckDB connection for each request."""
    con = duckdb.connect()
    try:
        yield con
    finally:
        con.close()

# --- Utility Functions ---

def fetch_data(db: duckdb.DuckDBPyConnection, sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Executes a parameterized query and returns data as a list of dicts.
    Enhanced error handling for easier debugging.
    """
    try:
        result = db.execute(sql, params).fetchdf()
        if result.empty:
            return []
        return result.to_dict(orient="records")
    except duckdb.ParserException as e:
        # 400 Bad Request: Syntax error in the SQL query
        raise HTTPException(status_code=400, detail=f"SQL Parsing Error: {e}")
    except Exception as e:
        error_detail = f"Database Execution Error (Check File Paths or Data Issues): {e}"
        raise HTTPException(status_code=500, detail=error_detail)


# --------------------------
# Landing page
# --------------------------
@app.get("/")
def root():
    return {
        "message": "Aquatic Samples API is running! Check /docs for endpoints.",
        "endpoints": {
            # Data Retrieval
            "Full Sample Metadata (Wide)": "/full_row/{accession}",
            "Species Abundance in Sample (Long)": "/sample/{accession}",
            "Abundance by Species across all samples": "/species/{species_name}",
            "Filtered Samples (Wide)": "/samples",
            "Location Data (Lat/Long)": "/locations",
            "Advanced Filter (Long)": "/filter_long",
            "--- Analytical ---": "-------------------------",
            "Top N Species (Total Abundance)": "/species_top?limit=10",
            "Top N Species (Mean Abundance)": "/species_mean_top?limit=10",
            "Species Count in Sample": "/sample_species_count/{accession}",
            "Sample Count by Season": "/samples_by_season", # NEW
            "Environmental Statistics": "/environmental_stats/{variable_name}", 
            "--- Utility & Export ---": "-------------------------",
            "Data Export (Filtered, No Limit)": "/export_data",
            "Submit New Samples (CSV Upload)": "/submit_samples_csv",
            "Get Schema": "/schema/{file_type}",
            "Execute Raw Query (DANGER)": "/query_raw"
        },
        "docs": "/docs"
    }

# --------------------------
# Endpoint: Submit New Samples for Review
# --------------------------
@app.post("/submit_samples_csv", summary="Upload a CSV file containing new long-format sample data for review and validation.")
async def submit_samples_csv(
    file: UploadFile = File(..., description="CSV file with long-format data (accession, species, abundance, etc.)."),
    db: duckdb.DuckDBPyConnection = Depends(get_db)
):
    if not file.filename.lower().endswith('.csv'):
        raise HTTPException(status_code=400, detail="Invalid file format. Must be a CSV file.")
    try:
        contents = await file.read()
        csv_string = contents.decode('utf-8')
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading file content: {e}")

# 2. Use DuckDB to read the CSV content into a temporary table for validation
    try:
    # Create a temporary view from the CSV string, allowing DuckDB to infer types
        db.execute(f"CREATE OR REPLACE TEMPORARY VIEW new_samples AS SELECT * FROM read_csv_text($csv_content, auto_detect=true)", {'csv_content': csv_string})
        row_count = db.execute("SELECT COUNT(*) FROM new_samples").fetchone()[0]
        db.execute("SELECT accession, species, CAST(abundance AS DOUBLE) FROM new_samples LIMIT 1").fetchdf()

    except Exception as e:
    # Catch common CSV parsing/type errors
        raise HTTPException(status_code=422, detail=f"CSV Parsing or Data Type Error. Ensure column headers and types match the long file structure. Error: {e}")

# 3. Success confirmation
    return {
        "message": f"Successfully validated {row_count} records from the submitted CSV.",
        "status": "Data format looks correct and is ready for an external reviewer."
    }

# --------------------------
# Endpoint: Filtered Data Export
# --------------------------
@app.get("/export_data", response_model=List[Dict[str, Any]], summary="Export filtered species abundance data (long format) without row limit.")
def export_long_data(
    species: Optional[str] = Query(None, description="Exact species name match."),
    min_abundance: float = Query(0.0, ge=0.0, description="Minimum relative abundance threshold."),
    season: Optional[str] = Query(None, description="Exact season name match (e.g., Spring, Winter)."),
    min_temp: Optional[float] = Query(None, description=r"Minimum temperature ($^{\circ}C$)."),
    max_temp: Optional[float] = Query(None, description=r"Maximum temperature ($^{\circ}C$)."),
    db: duckdb.DuckDBPyConnection = Depends(get_db)
):
    sql_base = f"SELECT * FROM read_parquet('{LONG_FILE}')"
    filters = []
    params = {'min_abundance': min_abundance} # No limit parameter here

    # 1. Minimum Abundance
    filters.append("CAST(abundance AS DOUBLE) >= $min_abundance")

    # 2. Species Filter
    if species:
        filters.append("species = $species_name")
        params['species_name'] = species

    # 3. Season Filter
    if season:
        filters.append("season = $season_name")
        params['season_name'] = season
    
    # 4. Temperature Range Filter
    if min_temp is not None:
        filters.append("CAST(temperature AS DOUBLE) >= $min_temp")
        params['min_temp'] = min_temp
    if max_temp is not None:
        filters.append("CAST(temperature AS DOUBLE) <= $max_temp")
        params['max_temp'] = max_temp


    if filters:
        sql = sql_base + " WHERE " + " AND ".join(filters)
    else:
        sql = sql_base

    # Order by accession for consistency (NO LIMIT)
    sql += " ORDER BY accession, species" 

    data = fetch_data(db, sql, params)

    if not data:
        raise HTTPException(status_code=404, detail="No data matched the provided filters for export.") 
    return data

# --------------------------
# Endpoint: Get Schema 
# --------------------------
@app.get("/schema/{file_type}", response_model=List[Dict[str, Any]], summary="Get the schema (columns and types) for the specified file format.")
def get_schema(file_type: str, db: duckdb.DuckDBPyConnection = Depends(get_db)):
    """
    Returns the column names and data types for either the 'wide' or 'long' file.
    """
    file_type = file_type.lower()
    if file_type == 'wide':
        file_path = WIDE_FILE
    elif file_type == 'long':
        file_path = LONG_FILE
    else:
        raise HTTPException(status_code=400, detail="Invalid file_type. Must be 'wide' or 'long'.")
    # Use the DuckDB PRAGMA to get table information
    sql = f"PRAGMA table_info(read_parquet('{file_path}'));"
    data = fetch_data(db, sql, params=None) 
    if not data:
        raise HTTPException(status_code=404, detail=f"Could not retrieve schema for {file_type} file.")
    return data


# --------------------------
# Endpoint: Location Data
# --------------------------
@app.get("/locations", response_model=List[Dict[str, Any]], summary="Get latitude, longitude, and metadata for all unique samples, with optional spatial filters.")
def get_locations(
    min_lat: Optional[float] = Query(None, description="Minimum latitude bound (South)."),
    max_lat: Optional[float] = Query(None, description="Maximum latitude bound (North)."),
    min_long: Optional[float] = Query(None, description="Minimum longitude bound (West)."),
    max_long: Optional[float] = Query(None, description="Maximum longitude bound (East)."),
    limit: int = Query(500, ge=1, le=10000, description="Maximum number of unique samples to return."),
    db: duckdb.DuckDBPyConnection = Depends(get_db)
):
    sql_base = f"""
        SELECT 
            DISTINCT accession, 
            CAST(latitude AS DOUBLE) AS latitude, 
            CAST(longitude AS DOUBLE) AS longitude, 
            season, 
            depth
        FROM read_parquet('{LONG_FILE}')
    """
    filters = []
    params = {'limit': limit}

# Latitude Filters
    if min_lat is not None:
        filters.append("CAST(latitude AS DOUBLE) >= $min_lat")
        params['min_lat'] = min_lat
    if max_lat is not None:
        filters.append("CAST(latitude AS DOUBLE) <= $max_lat")
        params['max_lat'] = max_lat

# Longitude Filters
    if min_long is not None:
        filters.append("CAST(longitude AS DOUBLE) >= $min_long")
        params['min_long'] = min_long
    if max_long is not None:
        filters.append("CAST(longitude AS DOUBLE) <= $max_long")
        params['max_long'] = max_long

    if filters:
        sql = sql_base + " WHERE " + " AND ".join(filters)
    else:
        sql = sql_base

# Order by accession to ensure consistent results, then apply limit
    sql += " ORDER BY accession LIMIT $limit"

    data = fetch_data(db, sql, params)
    if not data:
        raise HTTPException(status_code=404, detail="No sample locations matched the provided filters.")
    return data


# --------------------------
# Endpoint: Advanced Filter
# --------------------------
@app.get("/filter_long", response_model=List[Dict[str, Any]], summary="Filter species abundance data by environmental factors and minimum abundance (Long Format).")
def filter_long_data(
    species: Optional[str] = Query(None, description="Exact species name match."),
    min_abundance: float = Query(0.0, ge=0.0, description="Minimum relative abundance threshold."),
    season: Optional[str] = Query(None, description="Exact season name match (e.g., Spring, Winter)."),
    min_temp: Optional[float] = Query(None, description=r"Minimum temperature ($^{\circ}C$)."),
    max_temp: Optional[float] = Query(None, description=r"Maximum temperature ($^{\circ}C$)."),
    limit: int = Query(100, ge=1, le=10000, description="Maximum number of rows to return."),
    db: duckdb.DuckDBPyConnection = Depends(get_db)
):
    sql_base = f"SELECT * FROM read_parquet('{LONG_FILE}')"
    filters = []
    params = {'limit': limit, 'min_abundance': min_abundance}

    # 1. Minimum Abundance
    # Abundance is cast to DOUBLE to ensure consistent numeric comparison
    filters.append("CAST(abundance AS DOUBLE) >= $min_abundance")

    # 2. Species Filter
    if species:
        filters.append("species = $species_name")
        params['species_name'] = species

    # 3. Season Filter
    if season:
        filters.append("season = $season_name")
        params['season_name'] = season
    # 4. Temperature Range Filter
    # Temperature is cast to DOUBLE to ensure consistent numeric comparison
    if min_temp is not None:
        filters.append("CAST(temperature AS DOUBLE) >= $min_temp")
        params['min_temp'] = min_temp
    if max_temp is not None:
        filters.append("CAST(temperature AS DOUBLE) <= $max_temp")
        params['max_temp'] = max_temp


    if filters:
        sql = sql_base + " WHERE " + " AND ".join(filters)
    else:
        sql = sql_base

    # Add ordering and limit
    sql += " ORDER BY accession, species LIMIT $limit"
    data = fetch_data(db, sql, params)

    if not data:
        raise HTTPException(status_code=404, detail="No data matched the provided filters.") 
    return data


# --------------------------
# Endpoint: Get full wide row
# --------------------------
@app.get("/full_row/{accession}", response_model=List[Dict[str, Any]], summary="Get all metadata and species abundance for a single sample ID (Wide Row).")
def get_full_row(accession: str, db: duckdb.DuckDBPyConnection = Depends(get_db)):
    sql = f"""
        SELECT *
        FROM read_parquet('{WIDE_FILE}')
        WHERE accession = $accession
    """
    params = {'accession': accession}
    data = fetch_data(db, sql, params)

    if not data:
        raise HTTPException(status_code=404, detail=f"Sample '{accession}' not found in wide format.")
    return data


# --------------------------
# Endpoint: List top species by TOTAL abundance
# --------------------------
@app.get("/species_top", response_model=List[Dict[str, Any]], summary="List top species globally by total abundance.")
def get_species_top(
    limit: int = Query(10, ge=1, le=1000, description="Number of top species to return"),
    db: duckdb.DuckDBPyConnection = Depends(get_db)
):
    sql = f"""
        SELECT species, SUM(CAST(abundance AS DOUBLE)) AS total_abundance
        FROM read_parquet('{LONG_FILE}')
        GROUP BY species
        ORDER BY total_abundance DESC
        LIMIT $limit
    """
    params = {'limit': limit}
    data = fetch_data(db, sql, params)

    if not data:
        raise HTTPException(status_code=404, detail="No species data found.")
    return data

# --------------------------
# Endpoint: List top species by MEAN abundance
# --------------------------
@app.get("/species_mean_top", response_model=List[Dict[str, Any]], summary="List top species globally by mean abundance (useful for ranking consistency).")
def get_top_mean_species(
    limit: int = Query(10, ge=1, le=1000, description="Number of top species to return."),
    db: duckdb.DuckDBPyConnection = Depends(get_db)
):
    sql = f"""
        SELECT species, AVG(CAST(abundance AS DOUBLE)) as mean_abundance
        FROM read_parquet('{LONG_FILE}')
        GROUP BY species
        ORDER BY mean_abundance DESC
        LIMIT $limit
    """
    params = {'limit': limit}
    data = fetch_data(db, sql, params)
    if not data:
        raise HTTPException(status_code=404, detail="No species data found for aggregation.")
    return data

# --------------------------
# Endpoint: Sample Count by Season (NEW)
# --------------------------
@app.get("/samples_by_season", response_model=List[Dict[str, Any]], summary="Count the number of unique samples for each season.")
def get_samples_by_season(db: duckdb.DuckDBPyConnection = Depends(get_db)):
    sql = f"""
        SELECT 
            season, 
            COUNT(DISTINCT accession) AS sample_count
        FROM read_parquet('{LONG_FILE}')
        WHERE season IS NOT NULL
        GROUP BY season
        ORDER BY sample_count DESC
    """
    data = fetch_data(db, sql, params=None)
    if not data:
        raise HTTPException(status_code=404, detail="No season data found.")
    return data


# --------------------------
# Endpoint: Environmental Statistics (NEW)
# --------------------------
@app.get("/environmental_stats/{variable_name}", response_model=List[Dict[str, Any]], summary="Get AVG, MIN, MAX stats for a specific environmental variable.")
def get_environmental_stats(variable_name: str, db: duckdb.DuckDBPyConnection = Depends(get_db)):
    if variable_name.lower() not in ENVIRONMENTAL_VARS:
        raise HTTPException(status_code=400, detail=f"Invalid variable '{variable_name}'. Must be one of: {', '.join(ENVIRONMENTAL_VARS)}")

# Use f-string for the column name since it cannot be parameterized
    sql = f"""
        SELECT 
            AVG(TRY_CAST({variable_name} AS DOUBLE)) as average_{variable_name},
            MIN(TRY_CAST({variable_name} AS DOUBLE)) as minimum_{variable_name},
            MAX(TRY_CAST({variable_name} AS DOUBLE)) as maximum_{variable_name},
            COUNT(*) as total_records
        FROM read_parquet('{WIDE_FILE}')
    """
    data = fetch_data(db, sql, params=None)

    if not data or data[0].get(f"average_{variable_name}") is None:
        raise HTTPException(status_code=404, detail=f"Data for '{variable_name}' not found or is entirely non-numeric.")
    return data


# --------------------------
# Endpoint: Get all species in a sample
# --------------------------
@app.get("/sample/{accession}", response_model=List[Dict[str, Any]], summary="Get species and abundance data for a specific sample ID.")
def get_sample_species(accession: str, db: duckdb.DuckDBPyConnection = Depends(get_db)):
    sql = f"""
        SELECT species, CAST(abundance AS DOUBLE) AS abundance
        FROM read_parquet('{LONG_FILE}')
        WHERE accession = $accession
        AND CAST(abundance AS DOUBLE) > 0 -- Only show species with recorded abundance
    """
    params = {'accession': accession}
    data = fetch_data(db, sql, params)

    if not data:
        raise HTTPException(status_code=404, detail=f"Sample '{accession}' not found in long format or has no species data.")
    return data

# --------------------------
# Endpoint: Count Species per Sample
# --------------------------
@app.get("/sample_species_count/{accession}", response_model=List[Dict[str, Any]], summary="Get the count of unique species found in a specific sample ID.")
def get_sample_species_count(accession: str, db: duckdb.DuckDBPyConnection = Depends(get_db)):
    sql = f"""
        SELECT 
            accession, 
            COUNT(DISTINCT species) AS unique_species_count
        FROM read_parquet('{LONG_FILE}')
        WHERE accession = $accession
        AND CAST(abundance AS DOUBLE) > 0  -- Only count species present
        GROUP BY accession
    """
    params = {'accession': accession}
    data = fetch_data(db, sql, params)

    if not data:
        raise HTTPException(status_code=404, detail=f"Sample '{accession}' not found or has no recorded species.")
    return data

# --------------------------
# Endpoint: Query all samples for one species
# --------------------------
@app.get("/species/{species_name}", response_model=List[Dict[str, Any]], summary="Get abundance and location for a single species across all samples.")
def get_species_abundance(species_name: str, db: duckdb.DuckDBPyConnection = Depends(get_db)):
    sql = f"""
        SELECT accession, latitude, longitude, CAST(abundance AS DOUBLE) AS abundance, depth, date
        FROM read_parquet('{LONG_FILE}')
        WHERE species = $species_name
    """
    params = {'species_name': species_name}
    data = fetch_data(db, sql, params)

    if not data:
        raise HTTPException(status_code=404, detail=f"Species '{species_name}' not found.")
    return data


# --------------------------
# Endpoint: All samples with optional filters
# --------------------------
@app.get("/samples", response_model=List[Dict[str, Any]], summary="Get a list of samples with optional latitude/longitude filters (Wide Format).")
def get_samples(
    latitude: Optional[float] = Query(None, description="Exact latitude match."),
    longitude: Optional[float] = Query(None, description="Exact longitude match."),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of rows to return."),
    db: duckdb.DuckDBPyConnection = Depends(get_db)
):
    sql = f"SELECT * FROM read_parquet('{WIDE_FILE}')"
    params = {'limit': limit}
    filters = []

    if latitude is not None:
        filters.append("CAST(latitude AS DOUBLE) = $latitude")
        params['latitude'] = latitude
    if longitude is not None:
        # Use parameters for values
        filters.append("CAST(longitude AS DOUBLE) = $longitude")
        params['longitude'] = longitude

    if filters:
        sql += " WHERE " + " AND ".join(filters)
    sql += " LIMIT $limit"
    data = fetch_data(db, sql, params)
    return data


# --------------------------
# Endpoint: Raw Query Executor (Use with caution)
# --------------------------
@app.post("/query_raw", summary="Execute raw DuckDB SQL (Long File context only). Use with CAUTION.", response_model=List[Dict[str, Any]])
def execute_raw_query(
    query_body: Dict[str, str], 
    db: duckdb.DuckDBPyConnection = Depends(get_db)
):
    """
    Allows executing a custom query. The query MUST reference `read_parquet('AQUERY_long.parquet')`.
    DO NOT pass user input directly into this function in a production app.
    """
    query = query_body.get("sql", "")
    if not query:
        raise HTTPException(status_code=400, detail="SQL query is missing in the request body.")
    if "DROP" in query.upper() or "DELETE" in query.upper() or "UPDATE" in query.upper():
        raise HTTPException(status_code=403, detail="Modifying queries are forbidden on this endpoint.")
# Execute the query (no parameters used here, relying on user input filtering)
    data = fetch_data(db, query, params=None)
    return data
