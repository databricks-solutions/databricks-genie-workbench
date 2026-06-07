# Demo Data Notebooks

These Databricks notebooks generate fictional Unity Catalog datasets for Genie Workbench demos and evaluation. They are useful when users want to try Create Agent, IQ Scan, Quick Fix, or Auto-Optimize but do not have a ready dataset.

Run them from a Databricks workspace. They are Databricks notebook source files and are not intended for local execution.

## Required Widgets

Each notebook defines the same setup widgets:

| Widget | Required | Default | Description |
|---|---:|---|---|
| `catalog` | Yes | empty | Unity Catalog where the demo schema will be created. There is no safe default. |
| `schema` | No | dataset-specific | Schema/database name for the generated tables and metric views. |
| `overwrite_existing` | No | `false` | When `false`, writes fail if a table already exists. Set to `true` to recreate existing demo tables. |

The notebooks create the schema if it does not exist, then write Delta tables, add table and column comments, register primary/foreign key constraints, and create metric views.

## Permissions

The user running a notebook needs:

- `USE CATALOG` on the selected catalog
- `CREATE SCHEMA` on the selected catalog
- Permission to create Delta tables, constraints, comments, and views in the generated schema
- A Databricks Runtime or SQL warehouse/runtime combination that supports Unity Catalog metric views

Metric views use Databricks `CREATE VIEW ... WITH METRICS LANGUAGE YAML`. See the Databricks docs for [CREATE VIEW](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-view) and the [metric view YAML reference](https://docs.databricks.com/aws/en/business-semantics/metric-views/yaml-reference).

## Available Datasets

| Notebook | Default schema | Tables | Metric views | Story |
|---|---|---|---|---|
| `generate_banking_data.py` | `horizon_bank` | `products`, `branches`, `customers`, `accounts`, `transactions`, `service_requests` | `mv_banking_transactions`, `mv_customer_health`, `mv_service_quality` | Bank customers shift toward digital channels while a service outage drives complaint spikes and lower satisfaction. |
| `generate_hospital_readmission_data.py` | `hospital_readmission` | `patients`, `hospitals`, `encounters`, `care_transitions`, `claims`, `readmissions` | `mv_readmission_quality`, `mv_claims_cost`, `mv_care_transitions` | Discharge follow-up, diagnosis mix, and weekend discharges affect 30-day readmission risk and claims cost. |
| `generate_retail_apparel_data.py` | `retail_apparel` | `products`, `stores`, `customers`, `inventory_snapshots`, `sales`, `returns` | `mv_retail_sales`, `mv_inventory_health`, `mv_retail_returns` | Holiday demand, online growth, clearance activity, stockouts, and returns shape retail revenue and margin. |
| `generate_saas_churn_data.py` | `saas_churn` | `accounts`, `subscriptions`, `product_usage`, `support_tickets`, `invoices`, `churn_events` | `mv_subscription_revenue`, `mv_product_usage`, `mv_churn_risk` | Product adoption, support severity, billing behavior, and utilization explain SaaS churn and ARR loss. |
| `generate_talent_advisory_data.py` | `talent_advisory` | Workforce source tables plus curated marts for planning, hiring, retention, mobility, compensation, and succession | `mv_workforce_planning`, `mv_hiring_funnel`, `mv_retention_engagement`, `mv_internal_mobility`, `mv_comp_performance`, `mv_succession_planning` | Workforce planning connects engagement, mobility, compensation, hiring, and succession coverage across business units. |
| `generate_wind_turbine_maintenance_data.py` | `wind_turbine_maintenance` | `wind_farms`, `turbines`, `components`, `sensor_readings`, `maintenance_events`, `failure_events` | `mv_turbine_performance`, `mv_maintenance_reliability`, `mv_failure_events` | Sensor anomalies, icing, preventive maintenance, and model differences drive turbine failures and downtime. |

## Recommended Flow

1. Open one notebook in a Databricks workspace.
2. Set `catalog` to a Unity Catalog where you can create schemas and tables.
3. Optionally change `schema`.
4. Leave `overwrite_existing` as `false` for the first run.
5. Run the notebook from the top.
6. In Genie Workbench, create or optimize a Genie space that references the generated tables or metric views.

To refresh a demo dataset, rerun the notebook with `overwrite_existing` set to `true`.
