End-to-End Flight Data Engineering Pipeline (Databricks & dbt)
Project Overview
This project demonstrates an automated, incremental data pipeline built on Databricks following the Medallion Architecture. It processes raw flight booking data through three stages of refinement, culminating in a production-ready Gold layer modeled as a star schema.
High-Level Architecture
The data flow follows these stages:
1. Raw Zone: CSV files landed in Databricks Volumes.
2. Bronze Layer: Incremental ingestion via Databricks Autoloader.
3. Silver Layer: Data cleaning and quality enforcement using Lakeflow Declarative Pipelines (DLT).
4. Gold Layer: A dynamic dimensional model (Star Schema) implementing SCD Type 1.
5. Analytics: Business logic transformations managed via dbt Cloud.
Technical Stack
• Platform: Databricks (Free Edition).
• Storage & Governance: Unity Catalog, Delta Lake, Databricks Volumes.
• Processing: PySpark, Spark Structured Streaming.
• Workflow: Databricks Jobs (Control Flow/Looping), Lakeflow.
• Transformation & Modeling: dbt Cloud, SQL Warehouse.
Key Engineering Concepts Demonstrated
• Medallion Architecture: Organizing data into Bronze (raw), Silver (cleaned), and Gold (modeled) tables.
• Incremental Processing: Using Autoloader for efficient, "available once" data ingestion.
• Dynamic Pipeline Design: Utilizing Python functions and parameters to build reusable "builders" for dimensions and facts rather than static, single-use notebooks.
• Data Quality: Implementing DLT Expectations to drop or flag malformed records.
• SCD Type 1: Dynamically managing updates to dimension tables to ensure a single version of the truth.
Future Improvements
• Implement dbt Tests for automated schema and referential integrity validation.
• Explore Liquid Clustering or Partition Pruning for performance optimization in the Gold layer.
• Integrate a CI/CD pipeline for dbt model deployments.
