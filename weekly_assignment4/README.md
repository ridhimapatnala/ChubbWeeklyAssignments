

# ShopVerse Daily ETL Pipeline

## 1. Airflow Setup

### Variables

- shopverse_data_base_path : Base folder where daily input files arrive. 
Example: /opt/airflow/data/landing 

- shopverse_min_order_threshold:  Minimum valid order count to trigger normal branch (default: 10).                

### Connections

#### 1. postgres_dwh

* Conn Type = Postgres
* Host = postgres
* Schema = dwh_shopverse

#### 2. fs_default
* Conn Type = File (or Filesystem)
* Extra {"path":"/opt/airflow"}

## 2. Input File Placement

Place your daily files in:
```
  /opt/airflow/data/landing/
```

Folder structure:

```
landing/
  customers/
    customers_YYYYMMDD.csv
  products/
    products_YYYYMMDD.csv
  orders/
    orders_YYYYMMDD.json
```

## 3. Running the Pipeline

Go to Airflow UI → DAGs → shopverse_daily_pipeline → Trigger DAG.

Airflow will:

1. Wait for the three files for the execution date.
2. Load staging tables (truncate + load).
3. Transform data into:
   * dim_customers
   * dim_products
   * fact_orders
4. Run data quality checks.
5. Branch:

   * If valid orders < threshold → warning branch.
   * Else → normal completion.


## 4. Backfilling Older Dates

To run for previous dates:

### Option A: Airflow UI

DAG → Run → Choose a past date.

### Option B: CLI

Inside the Airflow CLI container:

```bash
airflow dags backfill shopverse_daily_pipeline \
  --start-date 2025-01-01 \
  --end-date 2025-01-05
```

Ensure files exist in landing for each date being backfilled.

## 5. Data Quality Checks Used

The pipeline applies multiple checks before proceeding:

### 1. Dimension Tables Must Not Be Empty

```
SELECT COUNT(*) FROM dim_customers;
SELECT COUNT(*) FROM dim_products;
```

### 2. Fact Orders Must Match Valid Staging Orders

Ensures no records were lost during transformation.

### 3. No NULL Keys in Fact Table

```
customer_id IS NOT NULL AND product_id IS NOT NULL
```

### 4. Negative or Invalid Values Are Filtered

Rows with negative quantity or missing mandatory fields are dropped.

If any check fails, the DAG stops and triggers a failure notification task.


##  Summary

This project demonstrates:

* Airflow DAG with TaskFlow API
* Sensors for file arrival
* Dynamic task groups for staging & warehouse layers
* PostgresOperator + PythonOperator mix
* Data quality enforcement
* Branching logic based on business rules
* Backfill support for historical days


