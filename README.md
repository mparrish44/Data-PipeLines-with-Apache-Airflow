
# Data Pipeline with Airflow

## Overview

This project sets up an **Apache Airflow** DAG to manage a data pipeline that extracts, transforms, and loads (ETL) data from a source system into a data warehouse (Redshift). It utilizes several custom operators to stage data, load fact and dimension tables, and perform data quality checks.

## Features

- **Airflow DAG**: The central workflow that automates the ETL pipeline.
- **Custom Operators**:
  - `StageToRedshiftOperator`: Stages data into Redshift from the source.
  - `LoadFactOperator`: Loads fact tables into Redshift.
  - `LoadDimensionOperator`: Loads dimension tables into Redshift.
  - `DataQualityOperator`: Ensures that the data meets quality standards.
- **Helper Functions**: SQL queries and variable management for seamless integration.

## Technologies

- **Apache Airflow**: Orchestration of the ETL workflow.
- **Python**: Primary programming language used for the DAG and custom operators.
- **PostgreSQL**: For managing intermediate data storage.
- **Redshift**: Data warehouse used for staging and final data storage.
- **SQLAlchemy**: For SQL queries and connection management.

## Setup

1. **Install Dependencies**:
   Install the required Python libraries using `pip`:
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure Airflow**:
   - Set up your Airflow instance and configure the necessary connections for PostgreSQL and Redshift.
   - Place your credentials and connection details in Airflow’s connection UI or `airflow.cfg`.

3. **Set Airflow Variables**:
   Ensure that you have defined the necessary variables in Airflow to access your database credentials and configurations.

4. **Run the DAG**:
   Start the Airflow webserver and scheduler to run the DAG:
   ```bash
   airflow webserver
   airflow scheduler
   ```

5. **Monitoring**:
   You can monitor the pipeline’s progress through the Airflow web interface at `http://localhost:8080`.

## File Structure

- `final_project.py`: The main DAG file that includes the setup of the pipeline and operators.
- `operators/`: Custom operator classes for staging, loading, and data quality.
- `helpers/`: Helper functions and SQL queries used in the DAG.
- `requirements.txt`: List of Python dependencies required for the project.
- `README.md`: Documentation for the project.

## License

This project is licensed under the MIT License.
