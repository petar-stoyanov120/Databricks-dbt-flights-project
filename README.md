# ✈️ End-to-End Flight Data Engineering Pipeline  
**Databricks • dbt • Medallion Architecture**

## 📌 Project Overview
This project demonstrates an **automated, incremental data engineering pipeline** built on **Databricks**, following the **Medallion Architecture**.

The pipeline processes raw flight booking data through **Bronze, Silver, and Gold layers**, culminating in a **production-ready Gold layer** modeled as a **Star Schema** and consumed through **dbt Cloud**.

---

## 🏗️ High-Level Architecture
The data flows through the following stages:

1. **Raw Zone**
   - CSV files landed in **Databricks Volumes**

2. **Bronze Layer**
   - Incremental ingestion using **Databricks Autoloader**

3. **Silver Layer**
   - Data cleaning and quality enforcement via  
     **Lakeflow Declarative Pipelines (DLT)**

4. **Gold Layer**
   - Dynamic dimensional modeling (**Star Schema**)
   - Implements **Slowly Changing Dimensions (SCD Type 1)**

5. **Analytics Layer**
   - Business logic transformations managed in **dbt Cloud**

---

## 🧰 Technical Stack

### Platform
- **Databricks (Free Edition)**

### Storage & Governance
- **Unity Catalog**
- **Delta Lake**
- **Databricks Volumes**

### Processing
- **PySpark**
- **Spark Structured Streaming**

### Orchestration & Workflow
- **Databricks Jobs** (Control Flow & Looping)
- **Lakeflow**

### Transformation & Modeling
- **dbt Cloud**
- **Databricks SQL Warehouse**

---

## 🧠 Key Engineering Concepts Demonstrated

### 🔹 Medallion Architecture
Structured progression of data:
- **Bronze** → Raw, incremental ingestion  
- **Silver** → Cleaned, validated datasets  
- **Gold** → Business-ready dimensional models  

### 🔹 Incremental Processing
- Efficient ingestion using **Databricks Autoloader**
- Processes data in an **“available-once”** pattern

### 🔹 Dynamic Pipeline Design
- Reusable **Python builders** for facts and dimensions
- Parameter-driven pipelines instead of static notebooks

### 🔹 Data Quality Enforcement
- **DLT Expectations** to:
  - Drop malformed records
  - Flag data quality issues early

### 🔹 SCD Type 1
- Dynamic handling of dimension updates
- Ensures a **single version of the truth**

---

## 🚀 Future Improvements
- ✅ Add **dbt Tests** for schema & referential integrity
- ⚡ Explore **Liquid Clustering** or **Partition Pruning** in Gold
- 🔄 Integrate **CI/CD pipelines** for dbt deployments

---

## 📂 Repository Structure (Optional)
```text
.
├── bronze/
├── silver/
├── gold/
├── dbt/
├── jobs/
└── README.md
