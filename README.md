# RDS-to-Snowflake ETL: A Lakehouse Pipeline

![architecture_diagram_](https://github.com/user-attachments/assets/4ba4e92a-a487-4a1a-a9f0-e3331883c9e0)

## 📌 Overview

This project implements a **modern, scalable ETL pipeline** that ingests data from **AWS RDS** (MySQL & PostgreSQL), processes it through a **Lakehouse architecture** using **AWS Glue** and **Amazon S3**, and loads it into **Snowflake** for analytics and reporting.

The pipeline follows the **Bronze → Silver → Snowflake** layered approach:

- **Bronze** – Raw data preservation.
- **Silver** – Cleaned & analytics-ready datasets.
- **Snowflake** – Cloud data warehouse for BI.

The entire workflow is **fully orchestrated with Apache Airflow (MWAA)** for automation and reliability.

---

## 🏗 Architecture Flow

1. **Data Source**  
   - AWS RDS (MySQL, PostgreSQL) as the source systems.
   - Data resides inside a VPC for secure access.

2. **Crawling & Cataloging**  
   - AWS Glue Crawlers connect to RDS instances to discover schema and metadata.
   - The crawlers populate the AWS Glue Data Catalog, making the data queryable.

4. **Raw Data Ingestion (Bronze)**  
   - AWS Glue Jobs (PySpark) extract the raw data from RDS and store it in S3 as the Bronze layer (raw, unprocessed data).

5. **Data Transformation (Silver)**  
   - Additional AWS Glue Jobs clean, standardize, and enrich Bronze data into the Silver layer.

6. **Loading into Snowflake**  
   - Processed Silver data is loaded into Snowflake Warehouse for BI and advanced analytics.

7. **Ad-hoc Querying**  
   - Amazon Athena provides ad-hoc querying capabilities directly on the S3 Data Lake.

8. **Workflow Orchestration**  
   - Apache Airflow (MWAA) schedules and automates the entire pipeline.

---

## 🗂 Data Lake Layers

| Layer  | Description |
|--------|-------------|
| Bronze | Raw, unprocessed data from RDS for archival & reprocessing. |
| Silver | Cleaned, transformed data optimized for analytics. |

---

## ☁ Services Used

- **AWS RDS** (MySQL, PostgreSQL) — Data source  
- **AWS Glue** — Data cataloging & transformation (PySpark)  
- **Amazon S3** — Data Lake storage (Bronze/Silver)  
- **Amazon Athena** — Querying S3 datasets  
- **Snowflake** — Cloud data warehouse  
- **Amazon MWAA (Apache Airflow)** — Workflow orchestration  
- **VPC** — Secure networking  

---

## 🔄 Pipeline Steps

1. **Extract** – Glue Crawlers scan RDS schema, update Data Catalog.  
2. **Transform** – Glue (PySpark) cleans, joins, aggregates data.  
3. **Load** – Silver data saved to S3 & loaded into Snowflake.

---

## ✅ Advantages

- **Scalable** – Serverless Glue jobs for big data processing.  
- **Cost-Effective** – Pay-per-query Athena + on-demand Glue jobs.  
- **Secure** – IAM & VPC-based access control.  
- **Flexible** – Supports both Athena & Snowflake analytics.

---

## 💡 Example Use Cases

- Cloud migration from on-prem relational databases.  
- Building secure & scalable data lakes.  
- Enabling BI analytics with Snowflake.

---

## 🚀 Getting Started

1. Provision RDS in a secure VPC.  
2. Configure AWS Glue connections to RDS.  
3. Create Crawlers for schema discovery.  
4. Build Glue ETL Jobs for Bronze & Silver layers.  
5. Set up MWAA DAGs for orchestration.  
6. Connect Athena & Snowflake for analytics.


