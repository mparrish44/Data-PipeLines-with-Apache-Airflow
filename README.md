# Data Pipeline with Apache Airflow and AWS Redshift

## Overview

This project implements a robust ETL (Extract, Transform, Load) data pipeline orchestrated using **Apache Airflow**. The pipeline extracts data from a source system (e.g., S3), stages it in **AWS Redshift**, transforms the data, and loads it into dimension and fact tables to create a star schema data warehouse optimized for analytics queries.

The primary goal is to automate the process of regularly updating the Redshift data warehouse with fresh data, ensuring data quality and integrity along the way.

## Features

* **Automated ETL Workflow**: Managed by an Airflow Directed Acyclic Graph (DAG).
  
* **Modular Design with Custom Operators**:
    * `StageToRedshiftOperator`: Efficiently copies data (e.g., JSON/CSV) from S3 to staging tables in Redshift. Handles credentials and data formatting.
    * `LoadFactOperator`: Populates the primary fact table using data from staging tables, applying necessary transformations. Supports different load patterns (append-only, delete-load).
    * `LoadDimensionOperator`: Populates dimension tables from staging tables. Supports different load patterns (append-only, delete-load).
    * `DataQualityOperator`: Performs customizable data quality checks (e.g., row counts, null checks) to ensure data integrity after loading.
* **SQL Query Management**: Centralized SQL queries within a `helpers` module for better organization and reusability.
  
* **Configuration Driven**: Utilizes Airflow Connections and Variables for managing sensitive credentials and configurations (AWS keys, Redshift connection details, S3 paths).

## Pipeline Architecture (Conceptual)

```
[Source Data (e.g., S3)] --> [StageToRedshiftOperator] --> [Redshift Staging Tables]
|
+--> [LoadDimensionOperator] --> [Redshift Dimension Tables]
|
+--> [LoadFactOperator] ------> [Redshift Fact Table]
|
+--> [DataQualityOperator] --> [Quality Checks Passed/Failed]
```


## Technologies Used

* **Orchestration**: Apache Airflow
* **Data Warehouse**: AWS Redshift
* **Data Lake / Staging Source**: AWS S3 (Assumed, modify if different)
* **Programming Language**: Python 3.x
* **Database Interaction**: SQLAlchemy (potentially used by Airflow hooks/operators), Psycopg2 (Redshift connector)
* **Infrastructure**: AWS (IAM Roles/Users, S3, Redshift)

## Prerequisites

* Python 3.x installed.
* An operational Apache Airflow instance (local or managed).
* AWS Account with:
    * An AWS Redshift Cluster configured.
    * An S3 bucket containing the source data.
    * AWS IAM User/Role with appropriate permissions for S3 access and Redshift operations (e.g., `AmazonS3ReadOnlyAccess`, `AmazonRedshiftDataFullAccess` or more granular permissions).
* Configured AWS Credentials accessible by Airflow (e.g., via IAM Role for EC2/ECS, or configured AWS Connection in Airflow).

## Setup Instructions

1.  **Clone the Repository**:
    ```bash
    git clone <your-repository-url>
    cd <your-repository-directory>
    ```

2.  **Set up a Virtual Environment (Recommended)**:
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```

3.  **Install Dependencies**:
    Install the required Python libraries:
    ```bash
    pip install -r requirements.txt
    ```
    *(Ensure `requirements.txt` includes `apache-airflow`, `apache-airflow-providers-amazon`, `psycopg2-binary`, `SQLAlchemy`, etc.)*

4.  **Configure Airflow Connections**:
    In the Airflow UI (`Admin -> Connections`), configure the following:
    * **AWS Connection**:
        * `Conn Id`: `aws_credentials` (or your preferred ID, ensure it matches the DAG)
        * `Conn Type`: `Amazon Web Services`
        * Configure using AWS Access Key ID / Secret Access Key *or* leave fields blank if using IAM roles for EC2/ECS. Specify the appropriate AWS Region.
    * **Redshift Connection**:
        * `Conn Id`: `redshift` (or your preferred ID, ensure it matches the DAG)
        * `Conn Type`: `Postgres` (Redshift uses the Postgres protocol)
        * `Host`: Your Redshift cluster endpoint URL.
        * `Schema`: Your target database name in Redshift.
        * `Login`: Your Redshift master username or a dedicated user.
        * `Password`: The password for the Redshift user.
        * `Port`: Your Redshift cluster port (default is 5439).

5.  **Configure Airflow Variables (Optional)**:
    If your DAG uses Airflow Variables (e.g., for S3 bucket names, prefixes, table names), set them in the Airflow UI (`Admin -> Variables`):
    * `s3_bucket`: Name of the S3 bucket holding source data.
    * `s3_log_key_prefix`: S3 prefix for log data (if applicable).
    * `s3_song_key_prefix`: S3 prefix for song data (if applicable).
    * *(Add any other variables your DAG requires)*

6.  **Create Redshift Tables (If not handled by operators)**:
    Ensure the necessary staging, dimension, and fact tables are created in your Redshift cluster. You might have a separate SQL script for this (`create_tables.sql`) or the operators might handle `CREATE TABLE IF NOT EXISTS` logic. Document this process here.
    
    *Example:*
    ```sql
    -- Connect to your Redshift cluster using a SQL client and run:
    -- CREATE TABLE public.staging_events (...);
    -- CREATE TABLE public.staging_songs (...);
    -- CREATE TABLE public.songplays (...);
    -- ... etc. for all tables
    ```

8.  **Place Project Files in Airflow Directory**:
    Copy or link the DAG file (`final_project.py`), the `operators/` directory, and the `helpers/` directory into your Airflow environment's designated folders:
    * DAG file -> `AIRFLOW_HOME/dags/`
    * `operators/` directory -> `AIRFLOW_HOME/plugins/operators/`
    * `helpers/` directory -> `AIRFLOW_HOME/plugins/helpers/`
    *(Note: Custom operators and helpers belong in the `plugins` directory for Airflow to discover them)*

9.  **Enable the DAG**:
    * Start the Airflow webserver and scheduler:
        ```bash
        airflow webserver -p 8080
        airflow scheduler
        ```
    * Open the Airflow UI in your browser (usually `http://localhost:8080`).
    * Find your DAG (e.g., `data_pipeline_dag`) and toggle it to `On`.

## Usage

* Once enabled, the DAG will run based on its defined `schedule_interval`.
* You can manually trigger a DAG run from the Airflow UI.
* Monitor the pipeline's progress, view logs, and check task statuses through the Airflow UI's Graph View, Tree View, etc.

## File Structure
```
.
├── dags/                     # Airflow DAG files
│   └── final_project.py      # Main DAG definition file
├── plugins/                  # Airflow plugins (custom operators, hooks, helpers)
│   ├── operators/            # Custom Operator classes
│   │   ├── init.py
│   │   ├── stage_redshift.py # StageToRedshiftOperator implementation
│   │   ├── load_fact.py      # LoadFactOperator implementation
│   │   ├── load_dimension.py # LoadDimensionOperator implementation
│   │   └── data_quality.py   # DataQualityOperator implementation
│   └── helpers/              # Helper modules
│       ├── init.py
│       └── sql_queries.py    # Module containing SQL query templates/constants
├── requirements.txt          # Python package dependencies
├── README.md                 # Project documentation (this file)
└── (Optional: create_tables.sql) # SQL script to create Redshift tables
└── (Optional: .airflowignore)   # Specify files/dirs for Airflow scheduler to ignore
```











