# Genie Workbench Documentation

Genie Workbench is a unified developer tool for creating, scoring, and optimizing Databricks Genie Spaces. It is deployed as a Databricks App (FastAPI backend + React/Vite frontend) with On-Behalf-Of authentication, Lakebase persistence, and a benchmark-driven optimization pipeline.

## Table of Contents

| # | Document | Description |
|---|----------|-------------|
| 01 | [Introduction](01-introduction.md) | What Genie Workbench is, who it's for, key concepts, feature overview |
| 02 | [Architecture Overview](02-architecture-overview.md) | Backend, frontend, GSO package, data flows, SSE streaming |
| 03 | [Authentication & Permissions](03-authentication-and-permissions.md) | OBO vs SP dual auth model, trigger flow, GRANT statements |
| 04 | [Create Agent](04-create-agent.md) | Multi-turn LLM agent for building Genie Spaces from scratch |
| 05 | [IQ Scanner](05-iq-scanner.md) | Rule-based quality scoring: 12 checks, 3 maturity tiers |
| 06 | [Fix Agent](06-fix-agent.md) | Scan-to-patch pipeline: findings to JSON patches to Genie API |
| 07 | [Auto-Optimize (GSO)](07-auto-optimize.md) | Benchmark-driven optimization: 6-stage DAG, levers, gates |
| 08 | [Deployment Guide](08-deployment-guide.md) | Local terminal installer, Databricks notebook installer, Lakebase setup, configuration reference |
| 09 | [Operations Guide](09-operations-guide.md) | Lakebase management, MLflow, monitoring, GSO job ops |

### Appendices

| # | Document | Description |
|---|----------|-------------|
| A | [API Reference](appendices/A-api-reference.md) | All API endpoints with auth identity and purpose |
| B | [Troubleshooting](appendices/B-troubleshooting.md) | Common issues, causes, and fixes |
| C | [Environment Variables](appendices/C-environment-variables.md) | Full reference for app.yaml, .env.deploy, and notebook widget variable flow |

## Quick Reference

```bash
# Local terminal install (first time)
./scripts/install.sh

# Local terminal deploy (subsequent)
./scripts/deploy.sh

# Code-only update
./scripts/deploy.sh --update

# Tear down
./scripts/deploy.sh --destroy
```

For the Databricks-native install path, clone the repo into a Databricks Git folder and run `notebooks/install.py`.

## Start Here

- **Genie Space developer** — Read [Introduction](01-introduction.md), then [Create Agent](04-create-agent.md) and [IQ Scanner](05-iq-scanner.md)
- **Workspace admin** — Read [Deployment Guide](08-deployment-guide.md), then [Authentication & Permissions](03-authentication-and-permissions.md)
- **Contributor** — Read [Architecture Overview](02-architecture-overview.md), then the feature doc for the area you're modifying
