---
sidebar_position: 2
description: "Deterministic 12-check quality scoring with three maturity tiers."
---

# IQ Scanner

The IQ Scanner is a **deterministic, rule-based** quality assessment engine for Genie Agent configurations. It evaluates 12 binary checks, assigns a maturity tier, and produces actionable findings with recommended next steps.

Unlike [Auto-Optimize](/docs/features/auto-optimize), which runs a benchmark-driven job against the live Agent, the scanner runs instantly with no LLM calls — it inspects the `serialized_space` JSON directly. It is the only analysis path in the Workbench.

### Unity Catalog Enrichment

Before scoring, `scan_space()` fetches table and column descriptions from Unity Catalog via `WorkspaceClient.tables.get()` and merges them into the agent config. This means checks 2 (table descriptions) and 3 (column descriptions) reflect metadata that exists in UC even if not inlined in the Genie Agent config. Existing inline descriptions are never overwritten. If UC metadata is unavailable (permissions, network), the scan continues with config-only data.

## Scoring Model

### Score: 0–12

Each of the 12 checks is worth 1 point. A check either passes (1 point) or fails (0 points). The total score ranges from 0 to 12.

### Maturity Tiers

| Tier | Criteria | Meaning |
|------|----------|---------|
| <span className="badge badge--success">Trusted</span> | All 12 checks pass | Agent is fully configured and has proven accuracy |
| <span className="badge badge--warning">Ready to Optimize</span> | Checks 1–10 pass (config complete) | Configuration is solid; ready for benchmark-driven optimization |
| <span className="badge badge--danger">Not Ready</span> | Any of checks 1–10 fail | Configuration gaps need to be addressed first |

The first 10 checks evaluate configuration quality. The last 2 checks evaluate optimization results — you must run Auto-Optimize to pass them.

## The 12 Checks

### Configuration Checks (1–10)

| # | Check | Pass Criteria | On Failure |
|---|-------|--------------|------------|
| 1 | **Agent description** | Top-level Agent description present and meaningful (≥30 chars, ≥5 words) | "Missing or placeholder agent description" |
| 2 | **Table descriptions** | ≥80% of tables have descriptions | Finding + next step to add descriptions |
| 3 | **Column descriptions** | ≥50% of columns have descriptions | Finding + next step to add descriptions |
| 4 | **Text instructions (>50 chars)** | Present and >50 characters total | Finding to add business context instructions |
| 5 | **Join specifications** | At least 1 join spec when multiple ordinary tables are configured; metric views do not require join specs | Finding to add join specs |
| 6 | **Data source count 1–12** | Between 1 and 12 tables + metric views | Finding to reduce data sources or use multi-room architecture |
| 7 | **SQL guidance artifacts** | At least 1 of: SQL function, expression, measure, filter, or example SQL | "No SQL guidance artifacts configured" |
| 8 | **Entity/format matching** | At least 1 column with entity matching or format assistance | Finding to enable on categorical/date/number columns |
| 9 | **10+ benchmark questions** | At least 10 benchmark questions | Finding to add benchmarks |
| 10 | **Column visibility / noise control** | Not (≥20 visible columns **and** ≥30% of them look internal/noisy) | Finding to hide noisy internal, audit, raw, and opaque technical columns |

:::note
Checks 1, 7, and 10 replaced earlier versions of this table. There is no longer a
standalone "data sources exist" check (an empty Agent fails check 6), and example
SQLs and SQL snippets are now scored together as one **SQL guidance artifacts**
check rather than as two separate checks.
:::

### Optimization Checks (11–12)

| # | Check | Pass Criteria | On Failure |
|---|-------|--------------|------------|
| 11 | **Optimization workflow completed** | A terminal optimization run exists (`CONVERGED`, `STALLED`, `MAX_ITERATIONS`, or `APPLIED`) | "Agent has not been through the optimization workflow" |
| 12 | **Optimization accuracy ≥ 85%** | Best accuracy from optimization is ≥ 0.85 | "Optimization accuracy is X% — target ≥ 85%" |

## Severity Levels

Each check has a severity beyond pass/fail:

| Severity | Meaning |
|----------|---------|
| `pass` | Check passed cleanly |
| `warning` | Check passed but with advisory guidance (e.g., table descriptions at 90% — aim for 100%) |
| `fail` | Check failed — a finding is generated |

Warnings do not reduce the score but provide improvement suggestions.

## Output Structure

The scanner returns:

```json
{
  "score": 8,
  "total": 12,
  "maturity": "Not Ready",
  "checks": [
    {"label": "Agent description", "passed": true, "detail": "142 chars", "severity": "pass"},
    ...
  ],
  "findings": ["No join specifications for multi-table agent", ...],
  "next_steps": ["Add join specifications to help Genie correctly join your tables", ...],
  "warnings": ["Instructions total 2,500 chars — keep under 2,000", ...],
  "warning_next_steps": ["Restructure text instructions for optimal LLM context usage", ...],
  "scanned_at": "2026-04-08T12:00:00+00:00"
}
```

- **`findings`** and **`next_steps`** come from failed checks — they tell you which configuration gaps to address and how.
- **`warnings`** and **`warning_next_steps`** come from warning-severity checks — advisory guidance that doesn't block maturity progression.
- Both lists are capped at 8 items.

## Advisory Warnings

Beyond the 12 scored checks, the scanner emits additional warnings for edge cases:

| Condition | Warning |
|-----------|---------|
| Agent description under 100 chars | "Add domain, audience, and scope details" |
| Column descriptions at 50–80% | "Higher coverage improves SQL generation accuracy" |
| No column synonyms defined | "Add synonyms for columns with abbreviated or technical names" |
| Text instructions > 2,000 chars | "Keep under 2,000 to avoid pushing out higher-value SQL context" |
| SQL patterns in text instructions | "Move to Example SQLs or SQL Expressions" (structure-aware detection, not a keyword regex) |
| Join specs fewer than `tables − 1` | "Relationship coverage may be incomplete" |
| Data source count 9–12 | "Consider focused agents for broad domains" |
| Missing `usage_guidance` on >50% of example SQLs | "Add descriptions of when each example should be applied" |
| Missing measures or filters in SQL snippets | "Add missing SQL snippet types for better coverage" |
| Benchmark questions 10–19 | "Add more for broader coverage" |
| Entity matching columns > 100 | "Approaching 120/agent limit" (>120: excess is ignored) |
| Row-level security on tables with entity matching | "Entity matching is silently disabled for these" |
| ≥20 visible columns with ≥15% noisy | "Review noisy internal columns" |
| A single table exposes >75 visible columns | Names the table's visible-column count |

## Integration with Auto-Optimize

Checks 11 and 12 evaluate optimization results. The scanner reads from two sources concurrently:

1. **Lakebase** `optimization_runs` table — legacy/simple optimization records
2. **GSO Delta tables** (`genie_opt_runs`) — Auto-Optimize pipeline runs, with fallback from Lakebase synced tables to direct Delta queries via SP

The scanner normalizes accuracy values (GSO stores 0–100, scanner expects 0.0–1.0) and uses the best accuracy across both sources.

Only terminal GSO run statuses count: `CONVERGED`, `STALLED`, `MAX_ITERATIONS`, and `APPLIED`. In-progress runs are ignored, as are:

- `FAILED` / `CANCELLED` — `best_accuracy` is absent or unreliable.
- `DISCARDED` — a discard reverts the live config, so the run's accuracy no longer describes what is deployed; the score falls back to baseline instead.

`APPLIED` **is** counted: applying an optimization flips the run status to `APPLIED` while preserving `best_accuracy`, and the applied champion is the live config, so its measured accuracy is what the header should report.

## Persistence

Scan results are persisted to Lakebase (table: `scan_results`) with:
- `space_id`, `score`, `maturity`
- `breakdown` (JSONB with full checks, warnings)
- `findings`, `next_steps`
- `scanned_at` timestamp

Historical scans are available via `GET /api/spaces/{id}/history`.

## Source Files

- `packages/genie-space-optimizer/src/genie_space_optimizer/iq_scan/scoring.py` — the scoring rules (`calculate_score`, `get_maturity_label`, `CONFIG_CHECK_COUNT`), extracted from the backend so the GSO optimizer preflight shares one source of truth
- `backend/services/scanner.py` — backend-specific IO: UC metadata enrichment, Lakebase persistence, and the async `scan_space()` orchestration
- `backend/routers/spaces.py` — `POST /api/spaces/{id}/scan` endpoint
- `backend/services/lakebase.py` — persistence
- `backend/services/gso_lakebase.py` — GSO run data for checks 11–12

## Related Documentation

- [Auto-Optimize](/docs/features/auto-optimize) — the optimization pipeline that satisfies checks 11–12
- [Introduction](/docs/getting-started/introduction) — how the scanner fits in the feature workflow
