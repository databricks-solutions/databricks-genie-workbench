# Demo Data Notebooks

These Databricks notebooks generate fictional Unity Catalog datasets for Genie Workbench demos and evaluation. They are useful when users want to try Create Agent, IQ Scan, Quick Fix, or Auto-Optimize but do not have a ready dataset.

Run them from a Databricks workspace. They are Databricks notebook source files and are not intended for local execution.

## Configuration Variables

Each notebook has the same editable configuration variables near the top:

| Variable | Required | Default | Description |
|---|---:|---|---|
| `CATALOG` | Yes | empty | Unity Catalog where the demo schema will be created. There is no safe default. |
| `SCHEMA` | No | dataset-specific | Schema/database name for the generated tables. |
| `OVERWRITE_EXISTING` | No | `False` | When `False`, writes fail if a table already exists. Set to `True` to recreate existing demo tables. |

The notebooks create the schema if it does not exist, then write Delta tables.

## Permissions

The user running a notebook needs:

- `USE CATALOG` on the selected catalog
- `CREATE SCHEMA` on the selected catalog
- Permission to create Delta tables in the generated schema

## Available Datasets

| Notebook | Default schema | Tables | Story |
|---|---|---|---|
| `generate_banking_data.py` | `horizon_bank` | `products`, `branches`, `customers`, `accounts`, `transactions`, `service_requests` | Bank customers shift toward digital channels while a service outage drives complaint spikes and lower satisfaction. |
| `generate_hospital_readmission_data.py` | `hospital_readmission` | `patients`, `hospitals`, `encounters`, `care_transitions`, `claims`, `readmissions` | Discharge follow-up, diagnosis mix, and weekend discharges affect 30-day readmission risk and claims cost. |
| `generate_retail_apparel_data.py` | `retail_apparel` | `products`, `stores`, `customers`, `inventory_snapshots`, `sales`, `returns` | Holiday demand, online growth, clearance activity, stockouts, and returns shape retail revenue and margin. |
| `generate_saas_churn_data.py` | `saas_churn` | `accounts`, `subscriptions`, `product_usage`, `support_tickets`, `invoices`, `churn_events` | Product adoption, support severity, billing behavior, and utilization explain SaaS churn and ARR loss. |
| `generate_talent_advisory_data.py` | `talent_advisory` | Workforce source tables plus curated marts for planning, hiring, retention, mobility, compensation, and succession | Workforce planning connects engagement, mobility, compensation, hiring, and succession coverage across business units. |
| `generate_wind_turbine_maintenance_data.py` | `wind_turbine_maintenance` | `wind_farms`, `turbines`, `components`, `sensor_readings`, `maintenance_events`, `failure_events` | Sensor anomalies, icing, preventive maintenance, and model differences drive turbine failures and downtime. |

## Recommended Flow

1. Open one notebook in a Databricks workspace.
2. Set `CATALOG` to a Unity Catalog where you can create schemas and tables.
3. Optionally change `SCHEMA`.
4. Leave `OVERWRITE_EXISTING` as `False` for the first run.
5. Run the notebook from the top.
6. In Genie Workbench, create or optimize a Genie Agent that references the generated tables.

To refresh a demo dataset, rerun the notebook with `OVERWRITE_EXISTING` set to `True`.
