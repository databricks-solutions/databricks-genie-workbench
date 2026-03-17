# Appendix C -- References

[Back to Index](../00-index.md) | Previous: [Appendix B -- Troubleshooting](B-troubleshooting.md)

---

## Glossary

| Term | Definition |
|------|-----------|
| **Adaptive Strategist** | LLM-based component that analyzes failure clusters and produces exactly one action group per iteration |
| **Action Group (AG)** | A set of related patches targeting a single root cause, produced by the adaptive strategist |
| **Apply Mode** | Controls where patches are written: `genie_config` (Genie Space only), `uc_artifact` (Unity Catalog), or `both` |
| **Arbiter** | Tiebreaker LLM judge that determines whether Genie or the benchmark expected SQL is correct when results disagree |
| **ASI** | Automated Structured Investigation -- structured failure metadata from LLM judges (failure_type, blame_set, counterfactual_fix) |
| **Benchmark** | An evaluation question with expected SQL, used to measure Genie Space quality |
| **Confirmation Double-Run** | Two independent evaluations averaged during the full gate to smooth Genie's non-determinism |
| **Convergence** | When all quality thresholds are met, ending the optimization loop |
| **Delta State Tables** | 8 Delta tables storing all optimization state (runs, stages, iterations, patches, ASI, provenance, queued patches, flagged questions) |
| **DO NOT RETRY List** | (failure_type, blame_set, lever) tuples that were tried and rolled back; prevents repeated failed approaches |
| **Entity Matching** | Genie feature that builds value dictionaries for fuzzy string matching |
| **Format Assistance** | Genie feature that shows exact value formats for columns |
| **Gate** | Quality check after patch application: slice gate, P0 gate, and full gate |
| **Genie Space** | A Databricks Genie AI assistant configured with tables, instructions, and metadata to answer natural language queries |
| **Hard Failure** | A genuine error where arbiter verdict is `ground_truth_correct` or `neither_correct` |
| **Instruction Slot Budget** | 100-slot limit enforced by the Genie API (example SQLs + functions + text instructions + descriptions) |
| **Lever** | One of 5 optimization dimensions: tables/columns, metric views, TVFs, joins, instructions |
| **LoggedModel** | MLflow model version representing a specific Genie Space configuration with associated scores |
| **Noise Floor** | Minimum score improvement (5 points) required to accept a patch as meaningful rather than cosmetic |
| **OBO** | On-Behalf-Of authentication -- the app acts with the logged-in user's identity and permissions |
| **Patch** | A specific change to the Genie Space configuration with an action, rollback command, and risk level |
| **Preflight** | First pipeline stage: validates config, collects UC metadata, generates benchmarks |
| **Provenance** | End-to-end traceability from judge verdicts through failure clusters, proposals, patches, to gate outcomes |
| **Quarantine** | Excluding a benchmark question from the accuracy denominator after exhausting automated fix attempts |
| **Reflection Buffer** | History of prior iteration outcomes (accepted/rolled-back, score deltas) shown to the strategist |
| **Repeatability** | Measure of SQL consistency when the same question is asked multiple times |
| **Service Principal (SP)** | The app's own identity in the Databricks workspace |
| **Slice Gate** | Quick regression check evaluating only questions affected by the current patch |
| **Soft Signal** | Best-practice opportunity where arbiter says Genie is correct but individual judges still flagged issues |
| **Temporal Date Resolution** | Automatic rewriting of relative date references ("this year", "last quarter") in benchmark SQL |

---

## External Links

### Databricks Documentation

- [Genie Spaces](https://docs.databricks.com/en/genie/index.html) -- what Genie Spaces are and how they work
- [Databricks Apps](https://docs.databricks.com/en/dev-tools/databricks-apps/index.html) -- building and deploying apps
- [Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/index.html) -- data governance and metadata
- [Foundation Model APIs](https://docs.databricks.com/en/machine-learning/model-serving/score-foundation-models.html) -- LLM endpoints
- [MLflow on Databricks](https://docs.databricks.com/en/mlflow/index.html) -- experiment tracking
- [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/index.html) -- command-line interface
- [Databricks SDK for Python](https://docs.databricks.com/en/dev-tools/sdk-python.html) -- Python SDK

### Tools and Frameworks

- [apx](https://github.com/databricks-solutions/apx) -- full-stack Databricks App framework
- [FastAPI](https://fastapi.tiangolo.com/) -- Python web framework
- [TanStack Router](https://tanstack.com/router) -- file-based React routing
- [TanStack Query](https://tanstack.com/query) -- data fetching and caching
- [shadcn/ui](https://ui.shadcn.com/) -- React UI components
- [Tailwind CSS](https://tailwindcss.com/) -- utility-first CSS framework
- [uv](https://docs.astral.sh/uv/) -- Python package manager
- [bun](https://bun.sh/) -- JavaScript runtime

### Documentation Standards

- [Diataxis Documentation Framework](https://diataxis.fr/) -- tutorials, how-to guides, reference, explanation
- [Good Docs Project](https://thegooddocsproject.dev/) -- templates and best practices
- [AgentSkills.io](https://agentskills.io/specification) -- agent skill specification (influenced Lever 5 holistic prompts)

---

## Project Links

| Resource | Location |
|----------|----------|
| Source code | `src/genie_space_optimizer/` |
| Configuration | `src/genie_space_optimizer/common/config.py` |
| Deployment config | `databricks.yml`, `app.yml`, `Makefile` |
| Grant script | `resources/grant_app_uc_permissions.py` |
| Tests | `tests/` |
| Changelog | `CHANGELOG.md` |
| Code Review | `CODE_REVIEW.md` |

---

## Document Metadata

| Field | Value |
|-------|-------|
| Documentation set | `docs/genie-space-optimizer-design/` |
| Created | 2026-03-06 |
| Template | [documentation-organization skill](../../documentation-organization/SKILL.md) Mode 2 |
| Quality checklist | [quality-checklist.md](../../documentation-organization/references/quality-checklist.md) |
