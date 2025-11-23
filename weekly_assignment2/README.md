## DLT pipeline using the NYC Taxi sample dataset 
The design follows the classic Medallion Architecture:
- Bronze: Raw ingestion with minimal transformation

- Silver: Data cleaned, filtered and enriched

- Gold: Aggregated and high-value analytics output

### The project simulates a Delta Live Tables pipeline using streaming and SQL transformations.

Dataset: NYC Taxi Trips
Location: samples.nyctaxi.trips (Unity Catalog Shared Dataset)

### 1. Bronze Layer

Table: taxi_raw_records

Expectation: Drop rows where trip_distance <= 0

| Column               | Type      | Description          |
| -------------------- | --------- | -------------------- |
| tpep_pickup_datetime | timestamp | Pickup timestamp     |
| pickup_zip           | string    | Pickup ZIP code      |
| dropoff_zip          | string    | Dropoff ZIP          |
| passenger_count      | int       | Number of passengers |
| trip_distance        | double    | Distance traveled    |
| fare_amount          | double    | Trip fare            |

### 2. Silver Layer
Table: flagged_rides

Filters:
- Short trips but high fare
- Pickup and dropoff in same ZIP but fare > 50

| Column | Type   | Description                 |
| ------ | ------ | --------------------------- |
| week   | date   | Week extracted for grouping |
| zip    | string | Simplified pickup zone      |

Table: weekly_stats

Aggregates:
- Average fare

- Average trip distance

- Grouped by week

| Column       | Type   |
| ------------ | ------ |
| week         | date   |
| avg_amount   | double |
| avg_distance | double |

### 3. Gold Layer
Table: top_n

Columns:
- Week
- Average metrics from aggregated data
- Actual ride fare and distance

### Lineage Demonstrates:
- Data origin (source dataset)
- Transformations applied
- Final business output relationships

### Test Checks Performed:
 
- Bronze count ≥ Silver count (bad rows removed)

- Flagged rides show real anomalies

- Weekly stats output contains one row per week

- Gold contains 3 highest-fare rides