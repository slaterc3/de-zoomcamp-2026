CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-105558.zoomcamp.yellow_tripdata_2024_ext`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://kestra-zoomcamp-slater-2026/yellow_tripdata_2024-*.parquet']
);

CREATE OR REPLACE TABLE `de-zoomcamp-105558.zoomcamp.yellow_tripdata_2024` AS
SELECT * FROM `de-zoomcamp-105558.zoomcamp.yellow_tripdata_2024_ext`;


-- question 1
SELECT count(*) FROM `de-zoomcamp-105558.zoomcamp.yellow_tripdata_2024`;

-- question 2 
-- external table:

SELECT 
    COUNT(DISTINCT(PULocationID)) 
FROM `de-zoomcamp-105558.zoomcamp.yellow_tripdata_2024_ext`;

-- materialized table

SELECT 
    COUNT(DISTINCT(PULocationID)) 
FROM `de-zoomcamp-105558.zoomcamp.yellow_tripdata_2024`;

-- question 3 
-- A. Just PULocationID

SELECT 
    PULocationID
FROM `de-zoomcamp-105558.zoomcamp.yellow_tripdata_2024`;

-- B. PULocationID & DOLocationID

SELECT 
    PULocationID,
    DOLocationID
FROM `de-zoomcamp-105558.zoomcamp.yellow_tripdata_2024`;

-- question 4

-- Records where fare_amount is $0.00 

SELECT 
    COUNT(*)
FROM `de-zoomcamp-105558.zoomcamp.yellow_tripdata_2024`
WHERE fare_amount = 0.0;

-- question 5 

CREATE OR REPLACE TABLE `de-zoomcamp-105558.zoomcamp.yellow_tripdata_2024_partitioned`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `de-zoomcamp-105558.zoomcamp.yellow_tripdata_2024_ext`;


-- question 6

-- 6A.
SELECT 
    DISTINCT(VendorID)
FROM `de-zoomcamp-105558.zoomcamp.yellow_tripdata_2024`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' and '2024-03-15'; 

-- 6B
SELECT 
    DISTINCT(VendorID)
FROM `de-zoomcamp-105558.zoomcamp.yellow_tripdata_2024_partitioned`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' and '2024-03-15'; 


-- Question 9

SELECT COUNT(*) FROM `de-zoomcamp-105558.zoomcamp.yellow_tripdata_2024`;

