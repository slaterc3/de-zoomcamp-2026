/* @bruin
name: reports.trips_report
type: duckdb.sql
depends:
  - staging.trips
materialization:
  type: table
  strategy: create+replace
columns:
  - name: report_date
    type: date
    primary_key: true
  - name: total_revenue
    type: double
    checks:
      - name: non_negative
@bruin */
SELECT 
    CAST(pickup_datetime AS DATE) as report_date,
    vendor_id,
    SUM(total_amount) as total_revenue,
    COUNT(*) as trip_count
FROM staging.trips
GROUP BY 1, 2
