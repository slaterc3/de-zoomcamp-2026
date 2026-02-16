# Data Engineering Zoomcamp 2026 - Week 4 Homework v1
## Christopher Guy Slater
## 2026/02/16
**Orchestration Tool:** dbt<br>
**Database/Data Warehouse:** Google BigQuery<br>
**Environment:** GitHub Codespaces<br>

### Question 1. dbt Lineage and Execution

Given a dbt project with the following structure:

```
models/
├── staging/
│   ├── stg_green_tripdata.sql
│   └── stg_yellow_tripdata.sql
└── intermediate/
    └── int_trips_unioned.sql (depends on stg_green_tripdata & stg_yellow_tripdata)
```

If you run `dbt run --select int_trips_unioned`, what models will be built?

- `stg_green_tripdata`, `stg_yellow_tripdata`, and `int_trips_unioned` (upstream dependencies)
- Any model with upstream and downstream dependencies to `int_trips_unioned`
- ✅ **Answer** $\rightarrow$ **`int_trips_unioned` only**
- `int_trips_unioned`, `int_trips`, and `fct_trips` (downstream dependencies)

### Question 1 Explanation 

If we run `dbt run --select int_trips_unioned`, only this model we'll be built. This is because dbt will take the commnand literally. <br>
If we want to instruct dbt to build the models `int_trips_unioned` depends on, we can use the `+` operator *before* the model name: <br>
```
dbt-run --select +int_trips_unioned
```
From the dbt documentation:
```python
dbt run --select "my_model+"         # select my_model and all descendants
dbt run --select "+my_model"         # select my_model and all ancestors
dbt run --select "+my_model+"        # select my_model, and all of its ancestors and descendants
```
**References**:<br>
[Graph operators: the "plus" operator (dbt docs)](https://docs.getdbt.com/reference/node-selection/graph-operators)
___

### Question 2. dbt Tests

You've configured a generic test like this in your `schema.yml`:

```yaml
columns:
  - name: payment_type
    data_tests:
      - accepted_values:
          arguments:
            values: [1, 2, 3, 4, 5]
            quote: false
```

Your model `fct_trips` has been running successfully for months. A new value `6` now appears in the source data.

What happens when you run `dbt test --select fct_trips`?

- dbt will skip the test because the model didn't change
- dbt will fail the test, returning a non-zero exit code
- dbt will pass the test with a warning about the new value
- dbt will update the configuration to include the new value

### Question #2 Explanation

`accepted_values` is one of the four generic built-in tests available in dbt. This allows us to verify that values are restricted to predetermined values.<br> 
Values that conflict with these will cause the test to fail with *a non-zero exit code*.
<br><br>According to the documentation [accepted_values (dbt docs)](https://docs.getdbt.com/reference/resource-properties/data-tests#accepted_values):<br>
*This data test validates that all of the non-null values in a column are present in a supplied list of `values`. If any values other than those provided in the list are present, then the data test will fail.*

*The `accepted_values` test supports an optional `quote` parameter which, by default, will single-quote the list of accepted values in the test query. To test non-strings (like integers or boolean values) explicitly set the `quote` config to `false`.*
<br><br>
**References**: <br>
[Generic (built-in) tests in dbt (datacamp.com)](https://www.datacamp.com/tutorial/dbt-tests#1.-generic-(built-in)-tests-these)<br>
[accepted_values (dbt docs)](https://docs.getdbt.com/reference/resource-properties/data-tests#accepted_values)

---
### Question 3. Counting Records in `fct_monthly_zone_revenue`

After running your dbt project, query the `fct_monthly_zone_revenue` model.

What is the count of records in the `fct_monthly_zone_revenue` model?

- 12,998
- 14,120
- ✅ **Answer** $\rightarrow$ 12,184
- 15,421

```sql
SELECT count(*) FROM de-zoomcamp-105558.zoomcamp.fct_monthly_zone_revenue;
```

---

### Question 4. Best Performing Zone for Green Taxis (2020)

Using the `fct_monthly_zone_revenue` table, find the pickup zone with the **highest total revenue** (`revenue_monthly_total_amount`) for **Green** taxi trips in 2020.

Which zone had the highest revenue?

- ✅ **Answer** $\rightarrow$ East Harlem North
- Morningside Heights
- East Harlem South
- Washington Heights South

```sql
SELECT 
    pickup_zone,
    SUM(revenue_monthly_total_amount) as total_revenue
FROM `de-zoomcamp-105558.zoomcamp.fct_monthly_zone_revenue`
WHERE service_type = 'Green'
  AND EXTRACT(YEAR FROM revenue_month) = 2020
GROUP BY 1
ORDER BY 2 DESC
LIMIT 1;
```

---

### Question 5. Green Taxi Trip Counts (October 2019)

Using the `fct_monthly_zone_revenue` table, what is the **total number of trips** (`total_monthly_trips`) for Green taxis in October 2019?

- 500,234
- 350,891
- ✅ **Answer** $\rightarrow$ 384,624
- 421,509

```sql
SELECT 
    SUM(total_monthly_trips) as total_trips
FROM `de-zoomcamp-105558.zoomcamp.fct_monthly_zone_revenue`
WHERE service_type = 'Green'
  AND EXTRACT(YEAR FROM revenue_month) = 2019
  AND EXTRACT(MONTH FROM revenue_month) = 10;
```

---
### Question 6. Build a Staging Model for FHV Data

Create a staging model for the **For-Hire Vehicle (FHV)** trip data for 2019.

1. Load the [FHV trip data for 2019](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv) into your data warehouse
2. Create a staging model `stg_fhv_tripdata` with these requirements:
   - Filter out records where `dispatching_base_num IS NULL`
   - Rename fields to match your project's naming conventions (e.g., `PUlocationID` → `pickup_location_id`)

What is the count of records in `stg_fhv_tripdata`?

- 42,084,899
- ✅ **Answer** $\rightarrow$ 43,244,693
- 22,998,722
- 44,112,187

```sql
SELECT count(*) 
FROM `de-zoomcamp-105558.zoomcamp.fhv_tripdata`
WHERE pickup_datetime >= '2019-01-01' AND pickup_datetime < '2020-01-01';
```

---
