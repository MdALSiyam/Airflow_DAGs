# **Apache Airflow ETL & Data Pipelines**

A robust, enterprise-ready collection of Apache Airflow DAGs and Python scripts for automated ETL workflows, database synchronization, remote file processing, and connectivity tests across FTP, SFTP, SMB, Oracle, and PostgreSQL.

---

## Detailed Repository Structure

| File Name | Pipeline Type | Key Functionality & Description |
| :--- | :--- | :--- |
| sample_data_upload_process.py | FTP/SFTP to Postgres | Automates daily file acquisition from remote locations using ftplib/SFTPHook, performs PostgreSQL bulk copy (COPY), and updates tracking metadata in AIRFLOW_DATA_UPLOAD_TRACK. |
| GET_PUSH_DUMP_DATA.py | SMB to Oracle / SFTP | Fetches daily CSV dump files from SMB shares, cleans and validates geographic data (retailer lat/long) using Pandas, performs bulk insert into Oracle DB, and mirrors backup copies to SFTP. |
| Dump_Data_Scheduler.py | SMB to Oracle / SFTP | Scheduled pipeline to pull retail location data from SMB, execute TRUNCATE_RETAILER_LATLONG_BULK procedure, bulk load clean records into Oracle, and sync to remote SFTP. |
| smb_csv_transfer.py | SMB to SFTP | Downloads CSV files in chunked batches from SMB servers, validates and processes data via Pandas temporary files, and uploads directly to target SFTP locations. |
| sftp_to_local_transfer.py | SFTP to Local Storage | Daily automated task that connects to remote SFTP servers, verifies directory contents, detects new or updated CSV files, and downloads them to local Airflow storage. |
| remote_server_connection.py | Utility / Diagnostic | Diagnostic DAG to validate SMB server reachability, test common file-sharing ports (445, 139, 135), check credential formats, and display connection summary logs. |
| Example_Test_Data_Sync.py | Oracle to SFTP | Fetches dynamic records from Oracle DB, handles BLOB/binary conversion safely, generates timestamped CSVs, and pushes to SFTP. |
| exchange_rate_pipeline.py | REST API to Clean to Alert | Downloads ECB exchange rates via REST API, triggers data cleaning, and sends automated success notifications via Email. |
| clean_data.py | Data Cleansing | Helper script invoked by exchange_rate_pipeline.py to fill missing values (NULL handling) and organize outputs in date-partitioned paths. |
| Import_csv_from_sftp.py | FTP/SFTP to Local | Checks for file presence on remote FTP servers using ftplib and downloads specified daily reports. |
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
pip install pandas psycopg2-binary requests pysmb \
  apache-airflow-providers-oracle \
  apache-airflow-providers-postgres \
  apache-airflow-providers-sftp \
  apache-airflow-providers-ftp
