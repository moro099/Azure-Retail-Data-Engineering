#  Azure Retail Data Engineering Platform

> End-to-End Azure Data Engineering Project using Azure Data Factory, Azure Databricks, ADLS Gen2, Delta Lake, Azure Synapse Serverless SQL, Power BI, GitHub and Azure DevOps CI.![Azure](https://img.shields.io/badge/Azure-0078D4?style=for-the-badge&logo=microsoft-azure&logoColor=white)

![ADF](https://img.shields.io/badge/Azure_Data_Factory-0078D4?style=for-the-badge)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Synapse](https://img.shields.io/badge/Synapse-0078D4?style=for-the-badge)
![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=powerbi)
![Azure DevOps](https://img.shields.io/badge/Azure_DevOps-0078D7?style=for-the-badge&logo=azure-devops)

An end-to-end Azure Data Engineering project built on the Medallion Architecture (Bronze → Silver → Gold) to ingest, transform, and analyze retail sales data. This project demonstrates real-world data engineering concepts including metadata-driven ingestion, watermark-based incremental loading, event & schedule triggers, Azure Synapse analytics, Power BI dashboards, and Azure DevOps CI.

# Project Overview
This solution simulates a production-ready Azure Data Engineering platform using the AdventureWorks retail dataset.

The platform ingests data from multiple sources (GitHub, Azure SQL, Azure Blob), stores raw data in Azure Data Lake Storage, performs transformations using Azure Databricks, creates business-ready datasets in the Gold layer, exposes them through Azure Synapse Serverless SQL, and visualizes business insights in Power BI.

The project also integrates GitHub with Azure DevOps to automate Continuous Integration (CI), ensuring code quality and reliability.

# Architecture
(Add your architecture diagram here)


+---------------------------+          +---------------------------+          +---------------------------+
|        SOURCES            |          |        INGESTION          |          |        STORAGE            |
+---------------------------+          +---------------------------+          +---------------------------+
|  GitHub CSV Files         | -------> |  Azure Data Factory       | -------> |  ADLS Gen2 (Bronze)       |
|  Azure SQL Database       | -------> |  • Metadata-Driven Pipe   |          |  Raw Data (Parquet)       |
|  Azure Blob Storage       | -------> |  • Incremental Pipeline   |          |                           |
+---------------------------+          |  • Schedule/Event Trig    |          +---------------------------+
                                       +---------------------------+                     |
                                                                                         ▼
                                       +---------------------------+          +---------------------------+
                                       |     TRANSFORMATION        |          |        STORAGE            |
                                       +---------------------------+          +---------------------------+
                                       |  Azure Databricks         | -------> |  ADLS Gen2 (Silver)       |
                                       |  Data Cleaning & Val.     |          |  Cleaned Data (Delta)     |
                                       |                           |          |                           |
                                       +---------------------------+          +---------------------------+
                                                                                         |
                                                                                         ▼
                                       +---------------------------+          +---------------------------+
                                       |     TRANSFORMATION        |          |        STORAGE            |
                                       +---------------------------+          +---------------------------+
                                       |  Azure Databricks         | -------> |  ADLS Gen2 (Gold)         |
                                       |  Business Aggregations    |          |  Business Ready (Delta)   |
                                       |                           |          |                           |
                                       +---------------------------+          +---------------------------+
                                                                                         |
                                       +---------------------------+          +---------------------------+
                                       |     ANALYTICS & REPORTS   |          |        OUTPUT             |
                                       +---------------------------+          +---------------------------+
                                       |  Azure Synapse Serverless | -------> |  Power BI Dashboard       |
                                       |  (SQL Views)              |          |  Executive KPIs & Insights |
                                       +---------------------------+          +---------------------------+

+---------------------------+          +---------------------------+
|    CI / CD               |          |   VERSION CONTROL         |
+---------------------------+          +---------------------------+
|  GitHub                  | -------> |  Azure DevOps CI Pipeline |
|  (Source Control)        |          |  (Automated Validation)   |
+---------------------------+          +---------------------------+
⚙️ Technology Stack
Service	Purpose
Azure Data Factory	Data ingestion, orchestration, and pipeline automation.
Azure SQL Database	Source system for transactional data.
GitHub	Version control for code and source CSV files.
Azure Blob Storage	Source system.
ADLS Gen2	Storage for all three layers (Bronze, Silver, Gold).
Azure Databricks	Distributed data transformation and processing (PySpark).
Delta Lake	Optimized storage format with ACID transactions
Azure Synapse Serverless SQL	Analytics layer for querying Gold layer data.
Power BI	Business intelligence and interactive dashboarding.
Azure DevOps	Continuous Integration (CI) pipeline.
📂 Project Structure
text
Azure-Retail-Data-Engineering/
│
├── 📁 ADF/                             # Azure Data Factory assets
│   ├── pipelines/                      # JSON pipeline definitions
│   
│                    
│
├── 📁 Databricks/                      # Azure Databricks notebooks
│   ├── notebooks/                      # .ipynb or .py files for transformations
│   │   ├── 1_bronze_to_silver.py
│   │   └── 2_silver_to_gold.py
│                      
│
├── 📁 SQL/                             # SQL scripts
│   └── synapse_views.sql               # Serverless SQL view definitions
│
├── 📁 PowerBI/                         # Power BI reports
│   └── retail_dashboard.pbix
│
├── 📁 Architecture/                    # Diagrams and documentation
│   └── architecture-diagram.png
│
├── 📄 azure-pipelines.yml              # Azure DevOps CI pipeline definition
└── 📄 README.md                        # Project overview (this file)
# Data Pipeline Workflow
1️. Data Ingestion
Azure Data Factory orchestrates data ingestion from three distinct sources:

GitHub CSV Files:  customer, subcategory,category and sales territory

Azure SQL Database: Transactional  tables.

Another Resource group (Azure Blob Storage): Products Data

2️. Bronze Layer
Raw data is ingested directly into ADLS Gen2 as Parquet files.

No transformations are applied, ensuring an immutable audit trail.

Historical data is retained for reprocessing and troubleshooting.

3️. Silver Layer (Databricks)
Azure Databricks performs the following operations using PySpark and Delta Lake:

 Azure Databricks performs:

• Data Cleaning
• Duplicate Removal
• Data Type Conversion
• String Standardization
• Null Validation
• Delta MERGE (Incremental Processing)

4️. Gold Layer (Databricks)
 Business-ready Fact and Dimension tables:

FactSales

DimProduct 

DimCustomer

DimTerritory  

5️. Analytics Layer (Synapse Serverless SQL)
Azure Synapse Serverless SQL exposes the Gold layer data through external views

Secure data access for Power BI and other consuming applications.

6️. Reporting (Power BI)
Power BI dashboards provide interactive visualizations of business KPIs, including:

Executive Snapshot: Top-line metrics (Revenue, Orders, Customers).

Sales Trends: Monthly, quarterly, and yearly analysis.

Product Performance: Best-selling products and categories.

Territory Insights: Revenue and market share by region.

# Key Features
Multi-Source Ingestion: Combines data from structured (SQL) and unstructured (Blob/GitHub) sources.

>Incremental Processing

Incremental loading was implemented using watermark-based ingestion in Azure Data Factory and Delta Lake MERGE operations in Azure Databricks.

• Azure SQL source uses watermark (ModifiedDate) based incremental ingestion.

• SalesOrderHeader and SalesOrderDetail are incrementally updated using Delta Lake MERGE.

• Existing records are updated while new records are inserted into the Silver layer.

>Metadata-Driven ADF Pipelines: Dynamic pipelines reduce maintenance and increase scalability.

>Event & Schedule Triggers: Real-time (event) and scheduled (batch) orchestration.

>Medallion Architecture: Standard, industry-proven data design pattern.

> Delta Lake Processing

• Delta format storage

• MERGE (UPSERT)

• ACID transactions

Synapse Serverless SQL Views: Cost-effective analytics layer without a dedicated SQL pool.

Interactive Power BI Dashboard

Azure DevOps CI Pipeline: Automated validation ensures production-grade code.

# Dashboard Highlights
The Power BI dashboard includes the following sections:

.. Executive Overview..
Total Revenue: Overall sales revenue.

Total Orders: Count of all orders.

Total Customers: Unique customer count.

Sales LY (Last Year): Sales revenue from the previous year.

Sales YoY %: Year-over-Year growth percentage.

Sales Trends Analysis

Sales By Category: Breakdown of revenue by product category.

Top 10 products by Revenue


#.Business Insights


Top 10 Products by Quantity sold

Sales YOY % by month

Sales Table

# Continuous Integration (CI)
The project integrates GitHub with Azure DevOps to automate CI.

Whenever code is pushed to the main branch, the CI pipeline (azure-pipelines.yml) automatically performs the following validations to ensure code integrity:

Checks out the latest source code.

Validates ADF pipeline JSON files against Azure Data Factory schemas.

Verifies SQL scripts required for the project.

Verifies Databricks notebook syntax.

Checks for the existence of Power BI report files.

Validates the project folder structure.

Publishes build artifacts for subsequent deployment.

This ensures that only validated code is merged, significantly reducing deployment failures.

# Business Outcomes
Automated end-to-end retail data ingestion.

Enabled scalable analytics using Medallion Architecture.

Built centralized reporting using Synapse Serverless SQL and Power BI.

Automated CI Validation 

# Future Enhancements
Continuous Deployment (CD): Automate deployment to higher environments.

Databricks Asset Bundles: Standardize and automate Databricks job deployment.

Monitoring & Alerting: Implement logs, metrics, and alerting using Azure Monitor.

Security and Governance: By Unity Catalog

Data Quality Framework: Integrate Great Expectations for automated data validation.

## 📌 Project Highlights

- 3 Data Sources
- Metadata-Driven Pipeline
- Watermark Incremental Loading
- Event Trigger
- Schedule Trigger
- Bronze → Silver → Gold Architecture
- Delta MERGE (UPSERT)
- Synapse Serverless SQL
- Power BI Dashboard
- Azure DevOps CI



👨‍💻 Author
Ketan Dubey

⭐ If you find this project useful, please consider giving it a star!

