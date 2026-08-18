# Adventure Works – Azure Data Engineering & Power BI Analytics

## Project overview

This project implements an end-to-end Adventure Works data engineering and analytics workflow using Azure Data Factory (ADF), Azure Databricks, Delta Lake and Power BI.

The solution processes four Adventure Works CSV extracts through a Bronze → Silver → Gold workflow and exposes the resulting business metrics through an interactive Power BI dashboard.

The orchestration is parameterised so the same Databricks transformation notebook can process multiple entities instead of maintaining separate transformation logic for each dataset.

## Architecture

```text
Adventure Works CSV files
          |
          v
      ADLS Gen2
       Bronze
          |
          v
 Azure Data Factory
   Master Pipeline
          |
          v
 Bronze → Silver
          |
          v
 Azure Databricks
   nb_transform
          |
          v
   Silver Delta tables
          |
          v
 Silver → Gold
          |
          v
     Gold layer
          |
          v
      Power BI
  Sales & Product Analysis
```

## Technologies

- Azure Data Factory
- Azure Data Lake Storage Gen2
- Azure Databricks
- Apache Spark / PySpark
- Delta Lake
- Power BI
- Python
- GitHub

## Source entities

| File | Entity | Primary key |
|---|---|---|
| `sales_order_header.csv` | `sales_order_header` | `SalesOrderID` |
| `sales_order_details.csv` | `sales_order_detail` | `SalesOrderDetailID` |
| `product.csv` | `product` | `ProductID` |
| `customer.csv` | `customer` | `CustomerID` |

## Azure Data Factory orchestration

The ADF solution contains three pipeline definitions:

1. `pl_adventureworks_master` — master orchestration pipeline.
2. `pl_bronze_to_silver` — orchestrates Bronze-to-Silver processing.
3. `pl_silver_to_gold` — handles the Silver-to-Gold stage.

The master pipeline executes the Bronze-to-Silver stage and then the Silver-to-Gold stage.

The Bronze-to-Silver pipeline uses a `ForEach` activity and parameterised values so that the same Databricks notebook can process the four source entities.

### Pipeline parameters

The Bronze-to-Silver process uses:

- `p_load_date`
- `p_bronze_container`
- `p_silver_container`
- `p_file_list`

Each item in `p_file_list` supplies:

- `file_name`
- `entity_name`
- `primary_key`

These values are passed from ADF to the Databricks notebook.

## Databricks transformation

The reusable `nb_transform.py` notebook:

1. Receives ADF parameters through Databricks widgets.
2. Reads the parameterised Bronze CSV path.
3. Removes exact duplicate rows.
4. Removes rows with null primary keys.
5. Trims string columns.
6. Applies entity-specific data types.
7. Adds `ingestion_timestamp` and `load_date`.
8. Writes the processed data to Silver Delta tables.
9. Uses Delta `MERGE` when the target Silver table already exists.

The notebook is designed to support all four entities through parameters rather than duplicated notebooks.

## Gold layer and Power BI

The completed pipeline continues from Silver to Gold and feeds the Power BI analytics layer.

The Power BI report contains two main pages:

### Sales Overview

The Sales Overview page includes:

- Total Revenue
- Total Orders
- Total Customers
- Total Units Sold
- Monthly Revenue Trend
- Annual Revenue
- Top 10 Products by Revenue
- Top 10 Customers by Revenue
- Year filtering

### Product Analysis

The Product Analysis page provides product-level analysis, including:

- Product performance
- Revenue
- Units Sold
- Product Revenue vs Units Sold

The dashboard was tested with interactive filtering to confirm that the relevant KPIs and visuals respond correctly.

## Power BI report

[Open the Power BI report](https://app.powerbi.com/groups/me/reports/4e35ecd3-bf06-44a8-85c8-f43cc120738c/3ced75801d6cccaaca2e?experience=power-bi)

> Access to this report may require the appropriate Power BI account/permissions.

## Repository structure

```text
adventureworks_project2_github_docs/
│
├── adf/
│   ├── pl_adventureworks_master.json
│   ├── pl_bronze_to_silver.json
│   └── pl_silver_to_gold.json
│
├── nb_transform.py
│
├── README.md
│
└── evidence/
    ├── ADF_Master_Pipeline_Success.png
    ├── databricks_nb_transform_with_outputs.html
    ├── PowerBI_Sales_Overview.png
    └── PowerBI_Product_Analysis.png
```

## Evidence

The `evidence/` folder contains execution and dashboard evidence for the completed solution.

### ADF

`ADF_Master_Pipeline_Success.png` shows a successful master pipeline execution, including successful Bronze-to-Silver and Silver-to-Gold stages.

### Databricks

`databricks_nb_transform_with_outputs.html` contains the exported Databricks notebook with its execution outputs.

### Power BI

The Power BI screenshots demonstrate the completed Sales Overview and Product Analysis pages.

## Security

Secrets must not be committed to source control.

The Databricks notebook retrieves storage credentials through a Databricks secret scope rather than hard-coding credentials in the source code.

Environment-specific values and credentials should be supplied through the appropriate Azure/Databricks configuration when deploying the solution.

## Reproducibility

The repository contains:

- ADF pipeline JSON definitions for the orchestration layer.
- The reusable Databricks transformation notebook source.
- Documentation describing the architecture and processing flow.
- Evidence of successful execution and the final Power BI analytics layer.

Environment-specific configuration such as storage account names, workspace identifiers, credentials and other deployment settings should be configured separately when reproducing the solution.

## Project outcome

The completed solution demonstrates an end-to-end cloud data engineering workflow:

**CSV source → ADLS Gen2 Bronze → ADF orchestration → Databricks transformation → Silver Delta → Gold layer → Power BI analytics**

The project combines data ingestion/orchestration, parameterised Spark transformation, Delta Lake processing and interactive business intelligence into a single workflow.
