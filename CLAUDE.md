# Genie Workbench

## Project Overview

Genie Workbench is a Databricks App that acts as a quality control and optimization platform for Genie Space administrators. It helps builders understand why their Genie Space isn't performing well and fix it.

- **Backend:** Python (FastAPI), deployed as a Databricks App
- **Frontend:** React/TypeScript (Vite)
- **Storage:** Lakebase (with in-memory fallback for local dev)
- **Tracing:** Optional MLflow integration

## GenieRX Specification

The GenieRX spec (`docs/genierx-spec.md`) defines the core analysis and recommendation framework used throughout this project. **Always consult it when working on analysis, scoring, or recommendation features.**

Key concepts from the spec:

- **Authoritative Facts** — raw data from systems of record, safe to surface directly
- **Canonical Metrics** — governed KPIs with stable definitions and cross-team agreement
- **Heuristic Signals** — derived fields with subjective thresholds; must always carry caveats

When implementing or modifying any analyzer, scorer, or recommender logic, ensure field classifications align with this taxonomy. Heuristic signals must never be presented as authoritative facts in Genie answers.

## Key Documentation

- `docs/genierx-spec.md` — GenieRX analyzer/recommender specification
- `docs/genie-space-schema.md` — Genie space schema reference
- `docs/checklist-by-schema.md` — Analysis checklist organized by schema section
- `CUJ.md` — Core user journeys and product analysis

## Development

```bash
# Backend (from repo root)
uv run start-server

# Frontend
cd frontend && npm run dev
```

Frontend runs at `localhost:5173`, proxies API calls to backend at `localhost:8000`.
