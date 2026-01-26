# de-zoomcamp-2026
Data Engineering Zoomcamp 2026 with Alexey Grigorev
MODULE 1 HOMEWORK:
-- HOMEWORK MODULE 1 2026/01/26 - DE ZOOMCAMP 2026
-- C. GUY SLATER

-- SELECT * FROM green_taxi_trips LIMiT 10;

-- QUESTION #3

SELECT COUNT(*)
FROM green_taxi_trips g
WHERE trip_distance <= 1.0;

-- question # 4

SELECT DATE(lpep_pickup_datetime) AS pickup_date, MAX(trip_distance) as max_dist
FROM green_taxi_trips
WHERE trip_distance <= 100
GROUP BY 1
ORDER BY 2 DESC;

 -- QUESTION #5
SELECT
    z."Zone",
    SUM(t.total_amount) AS total_revenue
FROM green_taxi_trips t
JOIN zones z ON t."PULocationID" = z."LocationID"
WHERE t.lpep_pickup_datetime >= '2025-11-18 00:00:00'
  AND t.lpep_pickup_datetime < '2025-11-19 00:00:00'
GROUP BY 1
ORDER BY 2 DESC
LIMIT 1;

-- SELECT * FROM green_taxi_trips LIMIT 5;

SELECT
    zdo."Zone",
    MAX(t.tip_amount) AS max_tip
FROM green_taxi_trips t
JOIN zones zpu ON t."PULocationID" = zpu."LocationID"
JOIN zones zdo ON t."DOLocationID" = zdo."LocationID"
WHERE zpu."Zone" LIKE '%Harlem Nor%'
GROUP BY 1
ORDER BY 2 DESC;
