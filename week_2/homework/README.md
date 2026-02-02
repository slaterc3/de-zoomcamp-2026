# Data Engineering Zoomcamp 2026 - Week 2 Homework
**Orchestration Tool:** Kestra  
**Database:** Postgres (Dockerized)  
**Environment:** GitHub Codespaces

---

## Homework Answers

| Question | Answer |
| :--- | :--- | :--- |
| **Q1** | **128.3 MiB** | 
| **Q2** | **green_tripdata_2020-04.csv** |
| **Q3** | **24,648,499** | 
| **Q4** | **1,734,051** | 
| **Q5** | **1,925,152** | 
| **Q6** | **America/New_York** |

---

## Question Breakdown

### Question 1: Uncompressed File Size of Yellow Taxi data for Dec. 2020

The first step was to use Kestra to do a backfill on the Yellow taxi data for December 2020:
![Backfill data](/workspaces/de-zoomcamp-2026/week_2/homework/images/1-backfill.jpg)

### Question 2: Rendered Variable
Testing Kestra's expression engine with inputs: `taxi: green`, `year: 2020`, `month: 04`.  
The rendered string follows the pattern: `{{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv`

