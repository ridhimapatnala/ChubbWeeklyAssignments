# Delta Lake–based data storage
## Dataset
A synthetic dataset of 1000 daily order records was generated, containing order details such as timestamps, customer IDs, countries, amounts, and status fields.

## Objectives

- Create and manage a Delta table in Unity Catalog.

- Demonstrate core Delta features: partitioning, partition pruning, schema evolution, time travel, updates, deletes, and table optimization.

- Understand how table structure and file layout affect performance.

## Observations

- Partitioning by country and order_date improves filtered query performance through partition pruning.

- Schema evolution allows the table to adapt as new fields (payment_method, coupon_code) are introduced.

- Time travel enables auditing and rollback across table versions.

- Excessive partitioning can create many small files, but OPTIMIZE reduces file counts and improves scan efficiency.