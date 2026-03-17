# 01 -- Introduction

[Back to Index](00-index.md)

---

## Purpose

The Genie Space Optimizer is a full-stack Databricks App that automatically improves the quality of [Genie Space](https://docs.databricks.com/en/genie/index.html) answers. It evaluates a Genie Space configuration against benchmark questions, identifies quality gaps across 7 dimensions, and applies targeted metadata patches through 5 optimization levers -- all with full rollback support.

Instead of manually tuning table descriptions, column aliases, join specifications, and Genie instructions through trial and error, the optimizer uses an LLM-driven adaptive loop that:

1. Generates benchmark questions covering the space's data assets
2. Scores Genie responses with 9 specialized judges
3. Clusters failures by root cause and priority-ranks them
4. Applies targeted patches and evaluates through a 3-gate quality check
5. Reflects on outcomes and iterates until convergence or diminishing returns

The result is a measurably better Genie Space configuration that can be applied with one click or rolled back entirely.

---

## Scope

This documentation covers:

- **Architecture** -- system components, data flows, and technology stack
- **Optimization Pipeline** -- end-to-end walkthrough of every stage from preflight through finalize
- **Evaluation System** -- benchmark generation, the 9-judge scoring framework, and MLflow integration
- **Optimization Levers** -- what each lever optimizes and how preparatory stages work
- **State Management** -- Delta tables, run lifecycle, and provenance tracking
- **API** -- all endpoints for both UI and programmatic (headless) access
- **Permissions** -- authentication model, required grants, and the advisor-only security design
- **Deployment** -- how to build, deploy, and configure the app
- **Operations** -- monitoring, troubleshooting, and diagnostic procedures

---

## Audience

| Audience | Relevant Sections |
|----------|-------------------|
| **Genie Space owners** who want to improve answer quality | [03 Pipeline](03-optimization-pipeline.md), [07 API](07-api-reference.md), [08 Permissions](08-permissions-and-security.md) |
| **Workspace admins** setting up and managing the app | [08 Permissions](08-permissions-and-security.md), [09 Deployment](09-deployment-guide.md), [10 Operations](10-operations-guide.md) |
| **Developers** contributing to the codebase | [02 Architecture](02-architecture-overview.md), [04 Evaluation](04-evaluation-and-scoring.md), [05 Levers](05-optimization-levers.md), [06 State](06-state-management.md) |
| **CI/CD integrators** using the headless API | [07 API Reference](07-api-reference.md) (Programmatic Trigger section) |

---

## Prerequisites

### Databricks Workspace

- Unity Catalog enabled
- A SQL Warehouse (serverless recommended) with `CAN_USE` permission
- At least one Genie Space with tables configured
- Permission to create Databricks Apps and Jobs
- Foundation Model API access (the `databricks-claude-opus-4-6` endpoint must be available)

### Development Tools (for local development only)

| Tool | Version | Purpose |
|------|---------|---------|
| Python | 3.11+ | Backend runtime |
| [uv](https://docs.astral.sh/uv/) | latest | Python package manager |
| [bun](https://bun.sh/) | latest | JavaScript runtime |
| [apx](https://github.com/databricks-solutions/apx) | latest | Full-stack app framework |
| [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/index.html) | latest | Deployment and workspace management |

---

## Technology Stack

| Layer | Technology |
|-------|------------|
| **Backend** | Python 3.11+, FastAPI, Pydantic, Uvicorn |
| **Frontend** | React 19, TypeScript, TanStack Router and Query, Vite |
| **UI Components** | shadcn/ui, Radix UI, Tailwind CSS 4 |
| **Data** | Delta Lake (state), PostgreSQL (Lakebase), Unity Catalog |
| **ML/AI** | MLflow 3.4+, Claude Opus 4.6 (via Databricks Foundation Model API), MLflow GenAI scorers |
| **Infrastructure** | Databricks Apps, Databricks Jobs, SQL Warehouse |
| **SDK** | Databricks SDK 0.40+, Databricks Connect 15+ |
| **Build** | apx, uv (Python), bun (JavaScript) |

---

## Success Criteria

The optimizer targets the following quality thresholds. If all are met, the run converges:

| Dimension | Target |
|-----------|--------|
| Syntax Validity | 98% |
| Schema Accuracy | 95% |
| Logical Accuracy | 90% |
| Semantic Equivalence | 90% |
| Completeness | 90% |
| Result Correctness | 85% |
| Asset Routing | 95% |
| Repeatability | 90% |

These thresholds are configurable in `src/genie_space_optimizer/common/config.py`. See [Appendix A](appendices/A-configuration-reference.md) for all tunable parameters.

---

Next: [02 -- Architecture Overview](02-architecture-overview.md)
