# 🏥 HealthCare Claim Anomaly Detection

## 📌 Overview

This project presents a complete data engineering pipeline to detect anomalies in healthcare claims data using a modern data lakehouse architecture. It follows a **Bronze → Silver → Gold** data layer structure using **AWS services**, **Redshift**, and **dashboards for analytics**.

We worked with three real-world healthcare datasets:
- `claims.csv` – ~250,000 records
- `patients.csv` – ~52,000 records
- `providers.json` – ~5,800 records

---

## 🧱 Architecture: Bronze - Silver - Gold Layered Pipeline

```
Raw S3 (Bronze) 
     ↓
Preprocessing (Glue + RDS) → S3 (Silver)
     ↓
Redshift Data Warehouse (Gold)
     ↓
Business Analysis & Dashboards
```

---

## 🗃️ Datasets

| Dataset       | Format | Record Count | Description                      |
|---------------|--------|--------------|----------------------------------|
| claims.csv    | CSV    | ~250,000     | Insurance claims and procedures  |
| patients.csv  | CSV    | ~52,000      | Patient demographics and history |
| providers.json| JSON   | ~5,800       | Provider credentials and details |

---

## 🥉 Bronze Layer (Raw Storage)

- All raw files are uploaded to **Amazon S3** in `s3://<bucket-name>/bronze/`
- Structure:
  ```
  s3://<bucket-name>/bronze/
      ├── claims.csv
      ├── patients.csv
      └── providers.json
  ```

---

## 🥈 Silver Layer (Preprocessed Data)

### 🔧 Preprocessing Techniques Used

| Dataset       | Method                  | Tool Used       |
|---------------|--------------------------|------------------|
| `claims.csv`  | Visual data mapping, cleaning | AWS Glue Studio |
| `providers.json`| Script-based parsing, flattening | AWS Glue Script Jobs (PySpark) |
| `patients.csv`| SQL-based cleaning (e.g., null handling, type casting) | Amazon RDS (PostgreSQL) |

All cleaned data is stored in:
```
s3://<bucket-name>/silver/
    ├── claims_cleaned/
    ├── patients_cleaned/
    └── providers_cleaned/
```

---

## 🥇 Gold Layer (Data Warehouse & Analytics)

### 🔄 Loading to Redshift

- Data from **Silver Layer** is ingested into **Amazon Redshift**
- Database: `healthcare_cp`
- Schema: `hcp`

| Table Name       | Description                  |
|------------------|------------------------------|
| `claims_data`    | Cleaned claim details        |
| `patients_data`  | Preprocessed patient info    |
| `providers_data` | Flattened provider metadata  |

---

## 📊 Business Queries & Dashboards

### 🔍 Real-World Business Queries

- **Top 10 procedures by volume and revenue**
- **Claims anomalies by provider**
- **Most common diagnoses by region**
- **Average claim amount by location**
- **High-risk patients with repeated claims**
- **Revenue generated per provider**
- **Claim trend analysis over time**
- **Diagnosis code vs procedure code mismatch**
- **Claims without matching provider or patient**
- **Location-wise claim fraud rate**

> All queries were executed in **Amazon Redshift**.

### 📈 Dashboards

- Built interactive dashboards with **Amazon QuickSight / Streamlit / Power BI (choose as applicable)**.
- Charts include:
  - Claim trend over months
  - Top 10 providers by claim count
  - Pie chart: Procedure code distribution
  - Heatmap: Claims by location and diagnosis

---

## 🚀 Technologies & Tools Used

| Category         | Tools                         |
|------------------|-------------------------------|
| Cloud Storage    | Amazon S3                     |
| ETL              | AWS Glue Studio, AWS Glue Scripts, Amazon RDS |
| Data Warehouse   | Amazon Redshift               |
| Data Analytics   | SQL, Redshift Queries         |
| Visualization    | Amazon QuickSight / Power BI / Streamlit |
| Programming Lang | Python (for scripts)          |

---

## 🧼 Anomaly Detection Logic (Overview)

Some common anomaly detection patterns we used:
- **Claim amount deviating from procedure average**
- **Claims with missing or unknown diagnosis codes**
- **Providers with abnormally high claim submissions**
- **Patients submitting claims across multiple regions**
- **Duplicate claims within a short period**

---

## 📁 Project Structure (For GitHub)

```
healthcare-claim-anomaly-detection/
│
├── data/
│   ├── bronze/
│   ├── silver/
│
├── glue_jobs/
│   ├── providers_etl_script.py
│
├── sql_queries/
│   ├── analysis_queries.sql
│
├── dashboards/
│   ├── dashboard_screenshots/
│   ├── dashboard_code/
│
├── README.md
├── LICENSE
└── requirements.txt
```

---

## ✅ How to Reproduce

1. Upload raw files to S3 `bronze` layer.
2. Run AWS Glue jobs and RDS scripts for preprocessing.
3. Output saved to `silver` S3 layer.
4. Use Redshift COPY commands to ingest from Silver → Redshift tables.
5. Run SQL queries from `sql_queries/analysis_queries.sql`.
6. Visualize with chosen dashboard tool.

---

<!-- ## 📬 Contact

For questions or collaboration, reach out to:

**Gowri Swetha**  
Email: your_email@example.com  
LinkedIn: [linkedin.com/in/your-profile](https://linkedin.com/in/your-profile) -->

---
<!-- 
## 📄 License

This project is licensed under the [MIT License](./LICENSE). -->
