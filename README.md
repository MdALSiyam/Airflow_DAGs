# Apache Airflow ETL & Data Pipelines

A robust, enterprise-ready collection of Apache Airflow DAGs and Python scripts for automated **ETL workflows**, **database synchronization**, and **remote file processing** across FTP, SFTP, Oracle, and PostgreSQL.

---

## Detailed Repository Structure

| File Name | Pipeline Type | Key Functionality & Description |
| :--- | :--- | :--- |
| sample_data_upload_process.py | FTP/SFTP $\rightarrow$ Postgres | Automates daily file acquisition from remote locations, performs PostgreSQL bulk copy (COPY), and updates tracking metadata. |
| Example_Test_Data_Sync.py | Oracle $\rightarrow$ SFTP | Fetches dynamic records from Oracle DB, handles BLOB/binary conversion safely, generates timestamped CSVs, and pushes to SFTP. |
| exchange_rate_pipeline.py | REST API $\rightarrow$ Clean $\rightarrow$ Alert | Downloads ECB exchange rates via REST API, triggers data cleaning, and sends automated success notifications via Email. |
| clean_data.py | Data Cleansing | Helper script invoked by `exchange_rate_pipeline.py` to fill missing values (NULL handling) and organize outputs in date-partitioned paths. |
| Import_csv_from_sftp.py | FTP/SFTP $\rightarrow$ Local | Checks for file presence on remote FTP servers using ftplib and downloads specified daily reports. |
| ftp_download_with_progress.py | FTP Sync | Features chunked binary file downloads with customized logging callback mechanisms for live progress tracking. |
| passingValueBetweenAction.py | DAG Architecture | Demonstrates multi-stage task pipelines utilizing Airflow XCom for dynamic state and file path sharing. |
| passingValueActionV2.py | Logging Template | Lightweight DAG template for standardized task logging and execution tracking. |
| OracleConnection.py | Utility / Test | Validates connection dependencies and executes simple daily workflow checks. |
| welcome_dag.py | Starter | Basic DAG for initial system testing, scheduler sanity checks, and logging verification. |

---

## Prerequisites & Setup Guide

### 1. Python Environment Dependencies
Install the required provider packages in your Airflow virtual environment:

```bash
pip install \
  pandas \
  psycopg2-binary \
  requests \
  apache-airflow-providers-oracle \
  apache-airflow-providers-postgres \
  apache-airflow-providers-sftp \
  apache-airflow-providers-ftp
