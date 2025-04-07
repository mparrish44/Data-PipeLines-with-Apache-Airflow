# Data Pipeline with Apache Airflow and AWS Redshift


## Overview

This project implements a robust ETL (Extract, Transform, Load) data pipeline orchestrated using **Apache Airflow**. The pipeline extracts data from a source system (e.g., S3), stages it in **AWS Redshift**, transforms the data, and loads it into dimension and fact tables to create a star schema data warehouse optimized for analytics queries.

The primary goal is to automate the process of regularly updating the Redshift data warehouse with fresh data, ensuring data quality and integrity along the way.

## Features

- **Automated ETL Workflow**: Managed by an Airflow Directed Acyclic Graph (DAG).
- **Modular Design with Custom Operators**:
    - `StageToRedshiftOperator`: Efficiently copies data (e.g., JSON/CSV) from S3 to staging tables in Redshift. Handles credentials and data formatting.
    - `LoadFactOperator`: Populates the primary fact table using data from staging tables, applying necessary transformations. Supports different load patterns (append-only, delete-load).
    - `LoadDimensionOperator`: Populates dimension tables from staging tables. Supports different load patterns (append-only, delete-load).
    - `DataQualityOperator`: Performs customizable data quality checks (e.g., row counts, null checks) to ensure data integrity after loading.
- **SQL Query Management**: Centralized SQL queries within a `helpers` module for better organization and reusability.
- **Configuration Driven**: Utilizes Airflow Connections and Variables for managing sensitive credentials and configurations (AWS keys, Redshift connection details, S3 paths).

## Pipeline Architecture (Conceptual)

