"""@bruin

# TODO: Set the asset name (recommended pattern: schema.asset_name).
# - Convention in this module: use an `ingestion.` schema for raw ingestion tables.
name: ingestion.trips

# TODO: Set the asset type.
# Docs: https://getbruin.com/docs/bruin/assets/python
type: python

# TODO: Pick a Python image version (Bruin runs Python in isolated environments).
# Example: python:3.11
image: python:3.11 

# TODO: Set the connection.
connection: duckdb-default

# TODO: Choose materialization (optional, but recommended).
# Bruin feature: Python materialization lets you return a DataFrame (or list[dict]) and Bruin loads it into your destination.
# This is usually the easiest way to build ingestion assets in Bruin.
# Alternative (advanced): you can skip Bruin Python materialization and write a "plain" Python asset that manually writes
# into DuckDB (or another destination) using your own client library and SQL. In that case:
# - you typically omit the `materialization:` block
# - you do NOT need a `materialize()` function; you just run Python code
# Docs: https://getbruin.com/docs/bruin/assets/python#materialization
materialization:
  # TODO: choose `table` or `view` (ingestion generally should be a table)
  type: table
  # TODO: pick a strategy.
  # suggested strategy: append
  strategy: append

# TODO: Define output columns (names + types) for metadata, lineage, and quality checks.
# Tip: mark stable identifiers as `primary_key: true` if you plan to use `merge` later.
# Docs: https://getbruin.com/docs/bruin/assets/columns
columns:
  - name: vendor_id
    type: integer
  - name: pickup_datetime
    type: datetime
  - name: dropoff_datetime
    type: datetime
  - name: passenger_count
    type: integer
  - name: trip_distance
    type: double
  - name: rate_code_id
    type: integer
  - name: store_and_fwd_flag
    type: string
  - name: pickup_location_id
    type: integer
  - name: dropoff_location_id
    type: integer
  - name: payment_type  
    type: integer
  - name: fare_amount
    type: double
  - name: extra
    type: double
  - name: mta_tax
    type: double
  - name: trip_amount
    type: double
  - name: tolls_amount
    type: double
  - name: improvement_surcharge
    type: double
  - name: total_amount
    type: double
  - name: congestion_surcharge
    type: double
  - name: extracted_at
    type: datetime


@bruin"""

# TODO: Add imports needed for your ingestion (e.g., pandas, requests).
# - Put dependencies in the nearest `requirements.txt` (this template has one at the pipeline root).
# Docs: https://getbruin.com/docs/bruin/assets/python
import pandas as pd 
import os
import json 
from datetime import datetime, timedelta

# TODO: Only implement `materialize()` if you are using Bruin Python materialization.
# If you choose the manual-write approach (no `materialization:` block), remove this function and implement ingestion
# as a standard Python script instead.
def materialize():
    """
    TODO: Implement ingestion using Bruin runtime context.
    
    Required Bruin concepts to use here:
    - Built-in date window variables:
      - BRUIN_START_DATE / BRUIN_END_DATE (YYYY-MM-DD)
      - BRUIN_START_DATETIME / BRUIN_END_DATETIME (ISO datetime)
      Docs: https://getbruin.com/docs/bruin/assets/python#environment-variables
    - Pipeline variables:
      - Read JSON from BRUIN_VARS, e.g. `taxi_types`
      Docs: https://getbruin.com/docs/bruin/getting-started/pipeline-variables

    Design TODOs (keep logic minimal, focus on architecture):
    - Use start/end dates + `taxi_types` to generate a list of source endpoints for the run window.
    - Fetch data for each endpoint, parse into DataFrames, and concatenate.
    - Add a column like `extracted_at` for lineage/debugging (timestamp of extraction).
    - Prefer append-only in ingestion; handle duplicates in staging.
    """
    # return final_dataframe
    vars = json.loads(os.getenv("BRUIN_VARS", "{}"))
    taxi_types = vars.get("taxi_types", ["yellow"])

    start_date = vars.get("BRUIN_START_DATE")

    all_data = []

    for taxi in taxi_types:
      url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi}/{taxi}_tripdata_2019-01.csv.gz"
      print(f"Fetching {taxi} data from {url}")
      df = pd.read_csv(url, compression='gzip', nrows=10000)  # Limit rows for testing

      df.columns = df.columns.str.lower()  # Standardize column names
      df['extracted_at'] = datetime.now()  # Add extraction timestamp
      all_data.append(df)
    
    return pd.concat(all_data, ignore_index=True)