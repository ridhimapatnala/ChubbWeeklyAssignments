from __future__ import annotations
import csv
import io
import json
from datetime import datetime, timedelta
from typing import List, Dict
import logging

from airflow import DAG
from airflow.decorators import task, dag
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, get_current_context
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule

DEFAULT_ARGS = {
    "owner": "you",
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

log = logging.getLogger(__name__)

# TASK 1: Create and register the DAG
@dag(
    dag_id="shopverse_daily_pipeline",
    default_args=DEFAULT_ARGS,
    description="ShopVerse daily ingestion + warehouse build pipeline",
    schedule_interval="0 1 * * *",  # daily at 01:00
    start_date=datetime(2025, 1, 1),
    catchup=True,
    max_active_runs=1,
    tags=["assignment", "shopverse"],
)

# TASK 2: Configure Airflow Variables & Connections
def shopverse_daily_pipeline():

    base_path = Variable.get("shopverse_data_base_path", default_var="/opt/airflow/data")
    min_order_threshold = int(Variable.get("shopverse_min_order_threshold", default_var="10"))
    pg_conn_id = "postgres_dwh"

    ds_nodash = "{{ ds_nodash }}"  # template placeholder used for sensors & file names

    """ customers_file = f"{base_path}/landing/customers/customers_{ds_nodash}.csv"
    products_file = f"{base_path}/landing/products/products_{ds_nodash}.csv"
    orders_file = f"{base_path}/landing/orders/orders_{ds_nodash}.json" """

    customers_file = f"{base_path}/landing/customers/customers_{{{{  macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%Y%m%d')  }}}}.csv"
    products_file = f"{base_path}/landing/products/products_{{{{  macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%Y%m%d')  }}}}.csv"
    orders_file = f"{base_path}/landing/orders/orders_{{{{  macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%Y%m%d')  }}}}.json"

    start = EmptyOperator(task_id="start")

    # TASK 4: Implement file sensors
    wait_customers = FileSensor(
        task_id="wait_for_customers_file",
        filepath=customers_file,
        # fs_conn_id="fs_default",
        poke_interval=30,
        timeout=60 * 60,  
        mode="poke",
    )

    wait_products = FileSensor(
        task_id="wait_for_products_file",
        filepath=products_file,
        # fs_conn_id="fs_default",
        poke_interval=30,
        timeout=60 * 60,
        mode="poke",
    )

    wait_orders = FileSensor(
        task_id="wait_for_orders_file",
        filepath=orders_file,
        # fs_conn_id="fs_default",
        poke_interval=30,
        timeout=60 * 60,
        mode="poke",
    )

    wait_all = EmptyOperator(task_id="wait_all_files")

    # chain sensors into single join
    [wait_customers, wait_products, wait_orders] >> wait_all

    # TASK 3: Create staging and warehouse tables (SQL)
    create_tables = PostgresOperator(
        task_id="create_tables_if_not_exists",
        postgres_conn_id=pg_conn_id,
        sql="sql/create_tables_sql.sql",
    )

    # TASK 5: Implement staging tasks (TaskGroup: staging)
    @task
    def _truncate_table(table_name: str):
        hook = PostgresHook(postgres_conn_id=pg_conn_id)
        hook.run(f"TRUNCATE TABLE {table_name};")
        return f"truncated:{table_name}"
    @task
    def _load_customers(file_path: str) -> int:
        """
        Read CSV and upsert into stg_customers (truncate+load behavior implemented globally)
        Returns number of rows loaded.
        """
        hook = PostgresHook(postgres_conn_id=pg_conn_id)
        # read CSV (templated path will be rendered by Airflow)
        ctx = get_current_context()
        rendered_path = ctx["ti"].xcom_pull(key="return_value", task_ids=None)  # not used; we receive file_path param
        # simple CSV load
        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = [r for r in reader]
        if not rows:
            return 0

        # Bulk insert using COPY via StringIO
        sio = io.StringIO()
        writer = csv.writer(sio)
        for r in rows:
            writer.writerow([
                r["customer_id"],
                r["first_name"],
                r["last_name"],
                r["email"],
                r["signup_date"],
                r["country"],
            ])
        sio.seek(0)
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.copy_expert(
            "COPY stg_customers (customer_id, first_name, last_name, email, signup_date, country) FROM STDIN WITH CSV",
            sio,
        )
        conn.commit()
        return len(rows)

    @task
    def _load_products(file_path: str) -> int:
        hook = PostgresHook(postgres_conn_id=pg_conn_id)
        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = [r for r in reader]
        if not rows:
            return 0
        sio = io.StringIO()
        writer = csv.writer(sio)
        for r in rows:
            writer.writerow([r["product_id"], r["product_name"], r["category"], r["unit_price"]])
        sio.seek(0)
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.copy_expert(
            "COPY stg_products (product_id, product_name, category, unit_price) FROM STDIN WITH CSV",
            sio,
        )
        conn.commit()
        return len(rows)

    @task
    def _load_orders(file_path: str) -> int:
        """
        Load orders JSON file into stg_orders.
        """
        hook = PostgresHook(postgres_conn_id=pg_conn_id)
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not data:
            return 0
        sio = io.StringIO()
        writer = csv.writer(sio)
        for r in data:
            # ensure required keys exist; invalid rows will be filtered later in transformations
            writer.writerow([
                r.get("order_id"),
                r.get("order_timestamp"),
                r.get("customer_id"),
                r.get("product_id"),
                r.get("quantity"),
                r.get("total_amount"),
                r.get("currency"),
                r.get("status"),
            ])
        sio.seek(0)
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.copy_expert(
            "COPY stg_orders (order_id, order_timestamp, customer_id, product_id, quantity, total_amount, currency, status) FROM STDIN WITH CSV",
            sio,
        )
        conn.commit()
        return len(data)

    # Staging group
    with TaskGroup("staging", tooltip="Truncate and load staging tables") as staging:
        t_truncate_customers = _truncate_table.override(task_id="truncate_stg_customers")("stg_customers")
        t_truncate_products = _truncate_table.override(task_id="truncate_stg_products")("stg_products")
        t_truncate_orders = _truncate_table.override(task_id="truncate_stg_orders")("stg_orders")

        # load tasks (paths are templated)
        t_load_customers = _load_customers(customers_file)
        t_load_products = _load_products(products_file)
        t_load_orders = _load_orders(orders_file)

        # enforce truncate before load (simple dependency)
        [t_truncate_customers >> t_load_customers,
         t_truncate_products >> t_load_products,
         t_truncate_orders >> t_load_orders]

    # TASK 6: Implement transformation tasks (TaskGroup: warehouse)

    @task
    def _refresh_dim_customers() -> int:
        """
        Refresh dim_customers from stg_customers (simple full refresh/upsert logic).
        """
        hook = PostgresHook(postgres_conn_id=pg_conn_id)
        # upsert: copy staging to temp table then upsert into dim_customers
        sql = """
        BEGIN;
        TRUNCATE TABLE tmp_dim_customers;
        INSERT INTO tmp_dim_customers (customer_id, first_name, last_name, email, signup_date, country)
            SELECT customer_id, first_name, last_name, email, signup_date::date, country FROM stg_customers;
        -- simple swap/upsert
        DELETE FROM dim_customers WHERE customer_id IN (SELECT customer_id FROM tmp_dim_customers);
        INSERT INTO dim_customers (customer_id, first_name, last_name, email, signup_date, country, updated_at)
            SELECT customer_id, first_name, last_name, email, signup_date, country, now() FROM tmp_dim_customers;
        COMMIT;
        """
        # Ensure tmp table exists
        hook.run("""
        CREATE TABLE IF NOT EXISTS tmp_dim_customers (
            customer_id INTEGER PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            email TEXT,
            signup_date DATE,
            country TEXT
        );
        """)
        hook.run(sql)
        # return rows in dim_customers
        rows = hook.get_first("SELECT COUNT(*) FROM dim_customers;")[0]
        return int(rows)

    @task
    def _refresh_dim_products() -> int:
        hook = PostgresHook(postgres_conn_id=pg_conn_id)
        hook.run("""
        CREATE TABLE IF NOT EXISTS tmp_dim_products (
            product_id INTEGER PRIMARY KEY,
            product_name TEXT,
            category TEXT,
            unit_price NUMERIC
        );
        """)
        sql = """
        BEGIN;
        TRUNCATE TABLE tmp_dim_products;
        INSERT INTO tmp_dim_products (product_id, product_name, category, unit_price)
            SELECT product_id, product_name, category, unit_price FROM stg_products;
        DELETE FROM dim_products WHERE product_id IN (SELECT product_id FROM tmp_dim_products);
        INSERT INTO dim_products (product_id, product_name, category, unit_price, updated_at)
            SELECT product_id, product_name, category, unit_price, now() FROM tmp_dim_products;
        COMMIT;
        """
        hook.run(sql)
        rows = hook.get_first("SELECT COUNT(*) FROM dim_products;")[0]
        return int(rows)

    @task
    def _build_fact_orders(execution_date=None) -> int:
        """
        Transform stg_orders -> fact_orders with:
        - timestamp normalization to UTC (assuming incoming timestamps are ISO +Z or local)
        - currency normalization flag (if currency != 'USD' then flag True)
        - remove invalid rows (negative quantity, null keys)
        - deduplicate by order_id keeping latest order_timestamp
        - set load_date based on logical date (execution_date)
        """
        hook = PostgresHook(postgres_conn_id=pg_conn_id)
        ctx = get_current_context()
        logical_date = ctx["logical_date"].date() if "logical_date" in ctx else ctx["ds"]
        # create temp table for cleaned orders
        hook.run("""
        CREATE TABLE IF NOT EXISTS tmp_stg_orders_clean (
            order_id INTEGER,
            order_timestamp TIMESTAMPTZ,
            customer_id INTEGER,
            product_id INTEGER,
            quantity INTEGER,
            total_amount NUMERIC,
            currency TEXT,
            status TEXT
        );
        TRUNCATE TABLE tmp_stg_orders_clean;
        """)
        # Insert cleaned rows
        # - ignore rows with missing order_id/customer_id/product_id
        # - ignore negative quantity
        # - parse order_timestamp as timestamptz
        insert_sql = """
        INSERT INTO tmp_stg_orders_clean (order_id, order_timestamp, customer_id, product_id, quantity, total_amount, currency, status)
        SELECT
            (order_id)::int,
            (order_timestamp)::timestamptz,
            (customer_id)::int,
            (product_id)::int,
            (quantity)::int,
            (total_amount)::numeric,
            currency,
            status
        FROM stg_orders
        WHERE order_id IS NOT NULL
          AND customer_id IS NOT NULL
          AND product_id IS NOT NULL
          AND quantity IS NOT NULL
          AND (quantity::int) > 0;
        """
        hook.run(insert_sql)

        # Deduplicate: keep latest order_timestamp per order_id
        hook.run("""
        CREATE TABLE IF NOT EXISTS tmp_fact_orders_dedup AS
        SELECT DISTINCT ON (order_id)
            order_id,
            order_timestamp,
            customer_id,
            product_id,
            quantity,
            total_amount,
            currency,
            status
        FROM tmp_stg_orders_clean
        ORDER BY order_id, order_timestamp DESC;
        """)
        # Upsert into fact_orders: remove existing rows for the order_ids being loaded for this run
        hook.run("""
        BEGIN;
        DELETE FROM fact_orders WHERE load_date = %s::date;
        INSERT INTO fact_orders (
            order_id, order_timestamp, customer_id, product_id, quantity, total_amount, currency, currency_mismatch_flag, status, load_date
        )
        SELECT
            order_id,
            order_timestamp AT TIME ZONE 'UTC',
            customer_id,
            product_id,
            quantity,
            total_amount,
            currency,
            CASE WHEN UPPER(currency) <> 'USD' THEN TRUE ELSE FALSE END AS currency_mismatch_flag,
            status,
            %s::date
        FROM tmp_fact_orders_dedup;
        COMMIT;
        """, parameters=(logical_date,logical_date))
        # Return number of rows inserted for this run (load_date = logical_date)
        rows = hook.get_first("SELECT COUNT(*) FROM fact_orders WHERE load_date = %s::date;", parameters=(logical_date,))
        return int(rows[0] if rows else 0)

    # Warehouse group
    with TaskGroup("warehouse", tooltip="Transform and build warehouse tables") as warehouse:
        t_refresh_dim_customers = _refresh_dim_customers()
        t_refresh_dim_products = _refresh_dim_products()
        t_build_fact_orders = _build_fact_orders()

    # TASK 7: Implement data quality checks

    # Define checks as lambdas returning (check_sql, comparator, expected)
    dq_checks = [
        {
            "check_id": "dq_dim_customers_nonzero",
            "sql": "SELECT COUNT(*) FROM dim_customers;",
            "expectation": lambda x: int(x[0]) > 0,
            "failure_msg": "dim_customers row count is 0",
        },
        {
            "check_id": "dq_no_nulls_in_fact_keys",
            "sql": "SELECT COUNT(*) FROM fact_orders WHERE customer_id IS NULL OR product_id IS NULL;",
            "expectation": lambda x: int(x[0]) == 0,
            "failure_msg": "NULL customer_id or product_id found in fact_orders",
        },
        {
            "check_id": "dq_fact_orders_matches_staging_count",
            "sql": None,  # handled inside function: compare fact_orders for load_date vs staging valid rows
            "expectation": None,
            "failure_msg": "fact_orders count does not match valid staging orders for the execution date",
        },
    ]

    @task
    def run_single_dq_check(check: dict) -> dict:
        hook = PostgresHook(postgres_conn_id=pg_conn_id)
        if check["check_id"] == "dq_fact_orders_matches_staging_count":
            # compute staging valid count for logical date
            # We consider orders loaded in stg_orders for this run (but we filtered invalid rows in build_fact_orders)
            staging_count = hook.get_first("SELECT COUNT(DISTINCT order_id) FROM stg_orders WHERE quantity::int > 0 AND order_id IS NOT NULL;")
            # fact_orders for logical_date
            ctx = get_current_context()
            logical_date = ctx["logical_date"].date() if "logical_date" in ctx else ctx["ds"]
            fact_count = hook.get_first("SELECT COUNT(*) FROM fact_orders WHERE load_date = %s::date;", parameters=(logical_date,))
            staging_count_val = int(staging_count[0] if staging_count else 0)
            fact_count_val = int(fact_count[0] if fact_count else 0)
            passed = (staging_count_val == fact_count_val)
            return {"check_id": check["check_id"], "passed": passed, "staging_count": staging_count_val, "fact_count": fact_count_val, "msg": check["failure_msg"] if not passed else "OK"}
        else:
            res = hook.get_first(check["sql"])
            passed = check["expectation"](res)
            return {"check_id": check["check_id"], "passed": passed, "result": res[0] if res else None, "msg": check["failure_msg"] if not passed else "OK"}

    # map the DQ checks dynamically
    dq_task_list = []
    for c in dq_checks:
        dq_task_list.append(run_single_dq_check.override(task_id=c["check_id"])(c))

    @task
    def evaluate_dq_results(results: List[dict]) -> bool:
        failed = [r for r in results if not r.get("passed", False)]
        if failed:
            # attach details to XCom (visible in logs)
            raise Exception(f"Data quality checks failed: {failed}")
        return True

    # TASK 8: Implement branching logic
    @task
    def decide_branch(min_threshold: int) -> str:
        hook = PostgresHook(postgres_conn_id=pg_conn_id)
        ctx = get_current_context()
        logical_date = ctx["logical_date"].date() if "logical_date" in ctx else ctx["ds"]
        fact_count = hook.get_first("SELECT COUNT(*) FROM fact_orders WHERE load_date = %s::date;", parameters=(logical_date,))
        fact_cnt = int(fact_count[0] if fact_count else 0)
        if fact_cnt < min_threshold:
            return "warn_low_volume"
        return "normal_completion"

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def warn_low_volume():
        """
        Writes a small summary file to data/anomalies/ and logs a warning.
        """
        ctx = get_current_context()
        logical_date = ctx["logical_date"].date() if "logical_date" in ctx else ctx["ds"]
        base = Variable.get("shopverse_data_base_path", default_var="/opt/airflow/data")
        anomalies_dir = f"{base}/anomalies"
        import os
        os.makedirs(anomalies_dir, exist_ok=True)
        hook = PostgresHook(postgres_conn_id=pg_conn_id)
        fact_count = hook.get_first("SELECT COUNT(*) FROM fact_orders WHERE load_date = %s::date;", parameters=(logical_date,))
        fact_cnt = int(fact_count[0] if fact_count else 0)
        summary = {
            "logical_date": str(logical_date),
            "fact_order_count": fact_cnt,
            "note": "Low volume warning"
        }
        out_file = f"{anomalies_dir}/anomaly_{logical_date}.json"
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)
        # also send an alert (EmailOperator below will be used on failure; here we just log)
        return out_file

    normal_completion = EmptyOperator(task_id="normal_completion")
    warn_node = warn_low_volume()

    # TASK 9: Implement notifications
    # Simple EmailOperator placeholder; configure smtp in airflow.cfg or swap for SlackWebhookOperator
    notify_failure = EmailOperator(
        task_id="notify_on_failure",
        to=Variable.get("notify_email", default_var="you@example.com"),
        subject="Airflow DAG Failure: shopverse_daily_pipeline",
        html_content="""<h3>shopverse_daily_pipeline failed</h3>
                        <p>Check Airflow logs for details.</p>""",
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # ----------------------------
    # DAG wiring
    # ----------------------------
    start >> wait_all >> create_tables >> staging >> warehouse

    # After warehouse: run DQ checks
    warehouse >> dq_task_list >> evaluate_dq_results(dq_task_list)  # evaluate_dq_results will raise if any fails

    # After DQ evaluate: branching
    evaluate = evaluate_dq_results(dq_task_list)
    evaluate >> decide_branch(min_order_threshold)  # returns next task id
    decide = decide_branch(min_order_threshold)
    decide_op = BranchPythonOperator(
        task_id="branch_on_volume",
        python_callable=lambda: None,  # placeholder, we use TaskFlow decide_branch above for logic
        do_xcom_push=False,
    )
    # Connect TaskFlow decide result to branches manually
    decide_op >> [warn_node, normal_completion]
    # The actual branch selection will be resolved by the returned task id from TaskFlow decide_branch,
    # so we set upstreams accordingly:
    decide >> warn_node
    decide >> normal_completion

    # On any task failure notify
    # Make notify_failure run on any failure in DAG by connecting from start and using trigger_rule
    start >> notify_failure

    # Final join
    [warn_node, normal_completion] >> EmptyOperator(task_id="end")

shopverse_daily_pipeline_dag = shopverse_daily_pipeline()
