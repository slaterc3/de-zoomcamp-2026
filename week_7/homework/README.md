# Data Engineering Zoomcamp 2026 - Week 7 Homework - Streaming
## C. Guy Slater
## 2026/03/16


## Question 1. Redpanda version

What version of Redpanda are you running?

✅ `v25.3.9`


## Question 2. Sending data to Redpanda


How long did it take to send the data?

- ✅ 10 seconds (5.1 seconds)
- 60 seconds
- 120 seconds
- 300 seconds


## Question 3. Consumer - trip distance

Write a Kafka consumer that reads all messages from the `green-trips` topic
(set `auto_offset_reset='earliest'`).

Count how many trips have a `trip_distance` greater than 5.0 kilometers.

How many trips have `trip_distance` > 5?

- 6506
- 7506
- ✅ 8506
- 9506


## Part 2: PyFlink (Questions 4-6)


## Question 4. Tumbling window - pickup location

Create a Flink job that reads from `green-trips` and uses a 5-minute
tumbling window to count trips per `PULocationID`.

Write the results to a PostgreSQL table with columns:
`window_start`, `PULocationID`, `num_trips`.

After the job processes all data, query the results:

```sql
SELECT PULocationID, num_trips
FROM <your_table>
ORDER BY num_trips DESC
LIMIT 3;
```

Which `PULocationID` had the most trips in a single 5-minute window?

- 42
- ✅ 74
- 75
- 166


## Question 5. Session window - longest streak

Create another Flink job that uses a session window with a 5-minute gap
on `PULocationID`, using `lpep_pickup_datetime` as the event time
with a 5-second watermark tolerance.

A session window groups events that arrive within 5 minutes of each other.
When there's a gap of more than 5 minutes, the window closes.

Write the results to a PostgreSQL table and find the `PULocationID`
with the longest session (most trips in a single session).

How many trips were in the longest session?

- 12
- ✅ 31
- 51
- 81


## Question 6. Tumbling window - largest tip

Create a Flink job that uses a 1-hour tumbling window to compute the
total `tip_amount` per hour (across all locations).

Which hour had the highest total tip amount?

- 2025-10-01 18:00:00
- 2025-10-16 18:00:00
- 2025-10-22 08:00:00
- ✅ 2025-10-30 16:00:00

