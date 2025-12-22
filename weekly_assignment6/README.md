# Enterprise Sales Analytics Pipeline

This project implements an end-to-end data engineering pipeline using the Medallion Architecture (Bronze, Silver, Gold) on Azure Databricks. The pipeline ingests raw sales data, applies data quality and calibration rules, and produces analytics-ready datasets for Power BI reporting.

The solution is built using Delta Live Tables (DLT) to support incremental ingestion, schema enforcement, data validation, and reliable processing.


## Objectives
- Ingest raw CSV data incrementally into a data lake
- Clean, standardize, and validate data
- Apply business calibration rules
- Produce optimized analytics-ready datasets
- Enable reliable Power BI consumption


## Tools Used
- **Platform**: Azure Databricks
- **Processing Engine**: Apache Spark (PySpark / Spark SQL)
- **Table Format**: Delta Lake (ACID, versioning)
- **Pipeline Framework**: Delta Live Tables (DLT)
- **Analytics Layer**: Power BI


## Environment Separation Strategy

| Environment | Purpose |
|------------|--------|
| Dev | Pipeline development and testing |
| Test | Data validation |
| Prod | Production-grade analytics consumption |

## Access Control Strategy
- Unity Catalog manages table-level and schema-level access
- Read-only access for Power BI service principals
- Write access restricted to pipeline execution identities
- No direct user writes to Silver or Gold tables

## Data Lineage & Flow
Source CSV files → Bronze (raw) → Silver (cleaned & validated) → Gold (aggregated KPIs) → Power BI dashboards

DLT automatically captures lineage and dependencies between tables.

## Assumptions
- **Data Volume**: ~10,000 rows per day
- **Data Frequency**: Daily incremental ingestion
- **Data Quality**: Source may contain nulls, duplicates, and calculation errors

## Validation Performed
- End-to-end pipeline execution
- Incremental load validation
- Revenue reconciliation between layers
- Dashboard accuracy checks

## Documentation
Attatched as "Sales DLT Pipeline Final Report" including:
- Architecture document
- Data model documentation
- Incremental logic explanation
- Data calibration explanation