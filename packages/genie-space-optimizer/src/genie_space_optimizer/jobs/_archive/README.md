# Archived Notebooks

These notebooks are **not part of the active 6-task optimization DAG** and are
**not uploaded to the Databricks workspace**. They are kept here for historical
reference or manual ad-hoc testing only.

## Contents

| File | Description |
|------|-------------|
| `run_optimization.py` | Legacy single-task notebook that ran the entire optimization pipeline in one process. Superseded by the 6-task DAG (`run_preflight` → `run_baseline` → `run_enrichment` → `run_lever_loop` → `run_finalize` → `run_deploy`). |
