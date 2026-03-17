# Genie Space Optimizer -- Documentation Index

> Automatically optimize [Genie Spaces](https://docs.databricks.com/en/genie/index.html) through iterative, LLM-driven metadata improvement with full rollback support.

**Last updated:** 2026-03-06 | **Version:** See [CHANGELOG](../../CHANGELOG.md)

---

## Quick Navigation

| Document | Description |
|----------|-------------|
| [01 -- Introduction](01-introduction.md) | Purpose, scope, audience, prerequisites, success criteria |
| [02 -- Architecture Overview](02-architecture-overview.md) | System architecture, component inventory, data flows |
| [03 -- Optimization Pipeline](03-optimization-pipeline.md) | End-to-end optimizer deep-dive (preflight through finalize) |
| [04 -- Evaluation and Scoring](04-evaluation-and-scoring.md) | 9 judges, 7 quality dimensions, benchmark generation, MLflow |
| [05 -- Optimization Levers](05-optimization-levers.md) | 5 levers and 4 preparatory stages in detail |
| [06 -- State Management](06-state-management.md) | Delta tables, run lifecycle, provenance, ASI results |
| [07 -- API Reference](07-api-reference.md) | All endpoints, UI trigger, headless API, CI/CD patterns |
| [08 -- Permissions and Security](08-permissions-and-security.md) | OBO auth, UC grants, Genie Space ACLs, advisor model |
| [09 -- Deployment Guide](09-deployment-guide.md) | `make deploy`, `databricks.yml`, environment variables |
| [10 -- Operations Guide](10-operations-guide.md) | Monitoring, reconciliation, diagnostic queries, logs |
| [Appendix A -- Configuration Reference](appendices/A-configuration-reference.md) | All tunable parameters with defaults |
| [Appendix B -- Troubleshooting](appendices/B-troubleshooting.md) | Error-solution matrix, convergence reasons |
| [Appendix C -- References](appendices/C-references.md) | Glossary, external links |

---

## Key Statistics

| Metric | Value |
|--------|-------|
| Pipeline stages | 6 (preflight, baseline eval, enrichment, lever loop, finalize, deploy) |
| Preparatory stages | 4 (prompt matching, description enrichment, join discovery, instruction seeding) |
| Optimization levers | 5 (tables/columns, metric views, TVFs, joins, instructions) |
| Quality dimensions | 7 + response quality + arbiter = 9 judges |
| Default benchmark count | 20 questions per evaluation |
| Max adaptive iterations | 3 (configurable) |
| Delta state tables | 8 |
| API endpoints | 16 |

---

## Architecture at a Glance

```
Browser (React)  --API-->  FastAPI Backend  --triggers-->  Databricks Job (5-task DAG)
                             |                               |
                             +- OBO user token               +- preflight
                             +- SP fallback                  +- baseline_eval
                             +- Serverless Spark             +- lever_loop
                             +- Delta state tables           +- finalize
                                                             +- deploy (optional)
```

---

## Getting Started

- **First time?** Start with the [Quickstart Guide](../../QUICKSTART.md)
- **Deploying?** See [09 -- Deployment Guide](09-deployment-guide.md)
- **Running an optimization?** See [03 -- Optimization Pipeline](03-optimization-pipeline.md)
- **Using the API?** See [07 -- API Reference](07-api-reference.md)
- **Debugging a failure?** See [Appendix B -- Troubleshooting](appendices/B-troubleshooting.md)
- **Setting up permissions?** See [08 -- Permissions and Security](08-permissions-and-security.md)

---

## Related Documents

| File | Location | Purpose |
|------|----------|---------|
| [README](../../README.md) | Root | Project overview with concise summaries |
| [Quickstart](../../QUICKSTART.md) | Root | Get running in 15 minutes |
| [E2E Testing Guide](../../E2E_TESTING_GUIDE.md) | Root | Complete testing playbook |
| [Changelog](../../CHANGELOG.md) | Root | Version history |
| [Code Review](../../CODE_REVIEW.md) | Root | Architecture review and refactoring plan |
