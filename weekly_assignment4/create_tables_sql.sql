CREATE TABLE IF NOT EXISTS stg_customers (
    customer_id INTEGER,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    signup_date DATE,
    country TEXT
);

CREATE TABLE IF NOT EXISTS stg_products (
    product_id INTEGER,
    product_name TEXT,
    category TEXT,
    unit_price NUMERIC
);

CREATE TABLE IF NOT EXISTS stg_orders (
    order_id INTEGER,
    order_timestamp TEXT,
    customer_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    total_amount NUMERIC,
    currency TEXT,
    status TEXT
);

CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id INTEGER PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    signup_date DATE,
    country TEXT,
    updated_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dim_products (
    product_id INTEGER PRIMARY KEY,
    product_name TEXT,
    category TEXT,
    unit_price NUMERIC,
    updated_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS fact_orders (
    order_id INTEGER PRIMARY KEY,
    order_timestamp TIMESTAMPTZ,
    customer_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    total_amount NUMERIC,
    currency TEXT,
    currency_mismatch_flag BOOLEAN,
    status TEXT,
    load_date DATE
);
