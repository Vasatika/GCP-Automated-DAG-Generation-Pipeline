# 🏥 Healthcare Data Pipeline Automation (GCP + Airflow)

This project showcases a dynamic, production-ready data ingestion and transformation pipeline built using **Google Cloud Platform**, **Cloud Composer (Airflow)**, **BigQuery**, and **Python**. It automates the ingestion of **sample healthcare data** generated via a custom tool called **Data Beacon**, supporting both **File-to-BigQuery** and **BigQuery-to-BigQuery** pipelines.

---

## 🚨 Problem Statement

Manual ingestion of structured data (like healthcare records) is time-consuming, hard to scale, and often error-prone. Organizations need reusable, parameterized pipelines that can validate schema, apply transformations, and handle errors automatically — especially in production environments.

---

## ✅ Solution Overview

- 🔁 **Dynamic DAG Generation** using Python + Jinja2 templates  
- 🧪 **Synthetic Data via Data Beacon** to simulate real-world healthcare ingestion  
- 🔁 **File-to-BQ & BQ-to-BQ support** via config-based branching  
- 🧹 **Post-processing logic** to archive/move failed files  
- 🚨 **Error handling** via `PythonOperator` and Cloud Monitoring  
- ⏱ **Batch and full-load support** with partition logic  

---

## 💡 What is Data Beacon?

**Data Beacon** is a synthetic data generator written in Python. It creates realistic, anonymized healthcare datasets for safe testing and validation of ingestion pipelines. Each run of Data Beacon:
- Generates CSV files with timestamped names
- Simulates vitals, demographics, visit dates, etc.
- Adds partition and ingestion metadata fields
- Uploads directly to GCS `inbound/` folder

> This tool ensures a consistent data structure and variability to rigorously test pipeline behavior without compromising privacy.

---

## 🧰 Technologies Used

| Tool/Service | Purpose |
|--------------|---------|
| **Cloud Composer (Airflow)** | Workflow orchestration and DAG scheduling |
| **BigQuery** | Scalable serverless data warehousing |
| **GCS (Cloud Storage)** | File ingestion and archival |
| **Python** | DAG automation, file movement logic, schema handling |
| **Pandas** | Data wrangling and type inference |
| **Jinja2** | Templating for dynamic DAG generation |
| **Airflow Operators** | GCSToBigQueryOperator, BigQueryInsertJobOperator, PythonOperator |


# 🔧 How It Works

## 1. Generate Sample Healthcare Data (Data Beacon)

I created a synthetic data generation script inside the `data_generation` folder to simulate real-world healthcare ingestion.

You can run the script `healthcare_data_generator.py` to generate a CSV file with realistic patient data. The file name includes a timestamp for uniqueness (e.g., `healthcare_patients_2025-05-05-03.csv`). This file is automatically uploaded to a Google Cloud Storage bucket under the `inbound/` folder.

The generated data includes fields like patient demographics, admission/discharge dates, vitals, and timestamp columns for partitioning and audit purposes.

---

## 2. Define Pipeline Behavior Using JSON Configs

The `json_configs` folder contains JSON files that control how each DAG should behave.

Each JSON config defines:

- The DAG ID  
- Whether the ingestion type is `file-to-bq` or `bq-to-bq`  
- GCS source paths (with support for wildcards like `*.csv`)  
- BigQuery dataset, staging table, and final table  
- Write mode (e.g., overwrite or append)  
- Scheduling, catchup behavior, retries, and ownership  
- Optional schema and partitioning logic

You can add or modify these JSON files to create new pipelines dynamically.

---

## 3. Dynamically Generate DAGs

Inside the `dag_generation` folder, there's a script called `generate_dags.py`.

When executed, this script:

- Reads every JSON file inside `json_configs`  
- Uses Jinja2 templating to inject parameters into a DAG structure  
- Outputs ready-to-deploy DAG `.py` files (in your Composer bucket or locally)

This removes the need to hardcode DAGs for each data pipeline.

---

## 4. Deploy to Cloud Composer

Once the DAGs are generated, they can be uploaded to your Cloud Composer environment (Airflow). The composer will automatically recognize new DAG files once placed in the Composer bucket under the `dags/` folder.

You can manually trigger DAG runs through the Airflow UI or wait for the next scheduled execution.

---

## 5. File Organization Logic

Each DAG includes success and failure callbacks that organize files after ingestion:

- On success: CSV files are moved from `inbound/` to `archival/`  
- On failure: Files are moved to a `failed/` folder for debugging  

This logic is implemented using Airflow’s PythonOperator and GCS APIs. It ensures traceability and prevents re-processing of the same files.

---

# ⚠️ Error Handling & Debugging Notes

Some issues faced during development were caused by:

- GCP free-tier trial limits (Composer resource caps, SendGrid email blocks)  
- Timing issues in Composer’s DAG sync  
- Schema detection failures when `autodetect` was turned off, but no schema was provided  
- GCS wildcard paths not resolving due to incorrect bucket or prefix usage

To resolve these:

- I replaced the `EmailOperator` with `PythonOperator` for success/failure fallback  
- Verified Composer logs and used GCS `list_blobs()` to confirm file paths  
- Used ChatGPT to troubleshoot Composer errors, Airflow parsing issues, and schema mismatches

---

# 🧠 Key Technical Learnings

- Building reusable, parameterized DAGs with Jinja2 templates  
- Structuring pipelines for both file-to-BigQuery and BigQuery-to-BigQuery ingestion  
- Using Python to move files in GCS post-DAG execution  
- Applying partitioning and timestamp strategies for scalable data ingestion  
- Replacing brittle email-based alerts with logging and GCP-native alerting tools  
- Organizing code and configs to automate DAG creation and deployment

---

# 👩‍💻 Author

**Vasatika Ghadiyaram**  
Data Analyst | GCP Developer | Business Analyst 
Education: MS in Business Analytics, Northeastern University  
LinkedIn: https://www.linkedin.com/in/vasatikaghadiyaram/
