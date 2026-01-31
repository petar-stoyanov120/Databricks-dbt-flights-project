# ✈️ End-to-End Flight Data Engineering Pipeline  
**Databricks | Medallion Architecture | Incremental Processing**

---

## 📌 Project Overview
This project demonstrates an **end-to-end data engineering pipeline** built on **Databricks**, following the **Medallion Architecture** pattern.

The pipeline ingests raw flight booking data, processes it incrementally through **Bronze, Silver, and Gold layers**, and produces **analytics-ready fact and dimension tables**.

> ⚠️ **Note:**  
> `dbt` is planned for the analytics layer but **has not yet been integrated** into the repository.

---

## 🏗️ High-Level Architecture
The data flows through the following stages:

1. **Raw Zone**
   - CSV files landed in **Databricks Volumes**

2. **Bronze Layer**
   - Incremental ingestion using **Databricks Autoloader**
   - Implemented via PySpark structured streaming

3. **Silver Layer**
   - Data cleansing and validation using  
     **Lakeflow Declarative Pipelines (DLT)**
   - Enforces data quality expectations

4. **Gold Layer**
   - Star Schema modeling (Facts & Dimensions)
   - Implements **Slowly Changing Dimensions (SCD Type 1)**

5. **Analytics Layer (Planned)**
   - Business transformations using **dbt Cloud**

---

## 🧰 Technical Stack

### Platform
- **Databricks (Free Edition)**

### Storage & Governance
- **Delta Lake**
- **Unity Catalog**
- **Databricks Volumes**

### Processing
- **PySpark**
- **Spark Structured Streaming**

### Orchestration
- **Databricks Jobs**
- **Lakeflow (DLT)**

### Analytics (Planned)
- **dbt Cloud**
- **Databricks SQL Warehouse**

---

## 🧠 Key Engineering Concepts Demonstrated

### 🔹 Medallion Architecture
- **Bronze:** Raw, incremental ingestion  
- **Silver:** Cleaned and validated datasets  
- **Gold:** Business-ready fact & dimension tables  

### 🔹 Incremental Processing
- Autoloader-based ingestion
- Efficient handling of new and changed data

### 🔹 Dynamic Pipeline Design
- Parameter-driven PySpark pipelines
- Reusable builders for dimensions and facts
- Avoids static, one-off notebooks

### 🔹 Data Quality Enforcement
- DLT expectations to:
  - Drop malformed records
  - Enforce schema and null checks

### 🔹 SCD Type 1
- Overwrites dimension records on change
- Maintains a single, current version of truth

---

## 📂 Repository Structure

```text
Databricks&DBT End-To-End project/
│
├── SILVER_DLT_PIPELINE/
│   └── (DLT pipeline notebooks & logic)
│
├── BronzeLayer.py
├── GOLD_FACT.py
├── Gold_Dims.py
├── Setup.py
├── SrcParameters.py
│
├── dim_airports.csv
├── dim_airports_increment.csv
├── dim_airports_scd.csv
│
├── dim_flights.csv
├── dim_flights_increment.csv
├── dim_flights_scd.csv
│
├── dim_passengers.csv
├── dim_passengers_increment.csv
├── dim_passengers_scd.csv
│
├── fact_bookings.csv
│
└── README.md
