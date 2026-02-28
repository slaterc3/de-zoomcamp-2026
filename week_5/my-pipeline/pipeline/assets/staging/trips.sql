/* @bruin
name: staging.trips
type: duckdb.sql
depends:
  - ingestion.trips
materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp
columns:
  - name: pickup_datetime
    checks:
      - name: not_null
@bruin */

SELECT *
FROM ingestion.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
