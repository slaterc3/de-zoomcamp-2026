"""@bruin
name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default
materialization:
  type: table
  strategy: create+replace
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
  - name: fare_amount
    type: double
  - name: total_amount
    type: double
  - name: extracted_at
    type: datetime
@bruin"""
import pandas as pd
import os
import json
from datetime import datetime

def materialize():
    vars = json.loads(os.getenv("BRUIN_VARS", "{}"))
    taxi_types = vars.get("taxi_types", ["yellow"])
    all_data = []
    
    for taxi in taxi_types:
        url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi}/{taxi}_tripdata_2019-01.csv.gz"
        df = pd.read_csv(url, compression='gzip', nrows=1000)
        df.columns = [c.lower() for c in df.columns]
        
        # Mirroring the exact logic from your week_4 dbt staging model
        df = df.rename(columns={
            'vendorid': 'vendor_id',
            'tpep_pickup_datetime': 'pickup_datetime',
            'tpep_dropoff_datetime': 'dropoff_datetime',
            'lpep_pickup_datetime': 'pickup_datetime',
            'lpep_dropoff_datetime': 'dropoff_datetime'
        })
        
        df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
        df['dropoff_datetime'] = pd.to_datetime(df['dropoff_datetime'])
        df['extracted_at'] = datetime.now()
        
        # Enforce the exact 8-column schema contract Bruin expects
        cols = [
            'vendor_id', 'pickup_datetime', 'dropoff_datetime', 
            'passenger_count', 'trip_distance', 'fare_amount', 
            'total_amount', 'extracted_at'
        ]
        all_data.append(df[cols])
    
    return pd.concat(all_data, ignore_index=True)
