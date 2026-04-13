# IQ Scanner

The IQ Scanner is a **deterministic, rule-based** quality assessment engine for Genie Space configurations. It evaluates 12 binary checks, assigns a maturity tier, and produces actionable findings that feed Quick Fix.

Unlike the LLM-based analysis tools, the scanner runs instantly with no LLM calls — it inspects the `serialized_space` JSON directly.

### Unity Catalog Enrichment

Before scoring, `scan_space()` fetches table and column descriptions from Unity Catalog via `WorkspaceClient.tables.get()` and merges them into the space config. This means checks 2 (table descriptions) and 3 (column descriptions) reflect metadata that exists in UC even if not inlined in the Genie Space config. Existing inline descriptions are never overwritten. If UC metadata is unavailable (permissions, network), the scan continues with config-only data.

## Scoring Model

### Score: 0–12

Each of the 12 checks is worth 1 point. A check either passes (1 point) or fails (0 points). The total score ranges from 0 to 12.

### Maturity Tiers

| Tier | Criteria | Meaning |
|------|----------|---------|
| **Trusted** | All 12 checks pass | Space is fully configured and has proven accuracy |
| **Ready to Optimize** | Checks 1–10 pass (config complete) | Configuration is solid; ready for benchmark-driven optimization |
| **Not Ready** | Any of checks 1–10 fail | Configuration gaps need to be addressed first |

The first 10 checks evaluate configuration quality. The last 2 checks evaluate optimization results — you must run Auto-Optimize to pass them.

## The 12 Checks

### Configuration Checks (1–10)

| # | Check | Pass Criteria | On Failure |
|---|-------|--------------|------------|
| 1 | **Data sources exist** | At least 1 table or metric view configured | "No tables or metric views configured" |
| 2 | **Table descriptions** | ≥80% of tables have descriptions | Finding + next step to add descriptions |
| 3 | **Column descriptions** | ≥50% of columns have descriptions | Finding + next step to add descriptions |
| 4 | **Text instructions** | Present and >50 characters total | Finding to add business context instructions |
| 5 | **Join specifications** | At least 1 join spec (for multi-source spaces) | Finding to add join specs |
| 6 | **Data source count 1–12** | Between 1 and 12 tables + metric views | Finding to reduce data sources or use multi-room architecture |
| 7 | **8+ example SQLs** | At least 8 example question-SQL pairs | Finding to add more examples |
| 8 | **SQL snippets** | At least 1 function, expression, measure, or filter | Finding to add SQL snippets |
| 9 | **Entity/format matching** | At least 1 column with entity matching or format assistance | Finding to enable on categorical/date/number columns |
| 10 | **10+ benchmark questions** | At least 10 benchmark questions | Finding to add benchmarks |

### Optimization Checks (11–12)

| # | Check | Pass Criteria | On Failure |
|---|-------|--------------|------------|
| 11 | **Optimization workflow completed** | A terminal optimization run exists (`CONVERGED`, `STALLED`, or `MAX_ITERATIONS`) | "Space has not been through the optimization workflow" |
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
    {"label": "Data sources exist", "passed": true, "detail": "5 table(s) configured", "severity": "pass"},
    ...
  ],
  "findings": ["No join specifications for multi-source space", ...],
  "next_steps": ["Add join specifications to help Genie correctly join your tables", ...],
  "warnings": ["Instructions total 2,500 chars — keep under 2,000", ...],
  "warning_next_steps": ["Restructure text instructions for optimal LLM context usage", ...],
  "scanned_at": "2026-04-08T12:00:00+00:00"
}
```

- **`findings`** and **`next_steps`** come from failed checks — these are the inputs for the [Quick Fix](06-fix-agent.md).
- **`warnings`** and **`warning_next_steps`** come from warning-severity checks — advisory guidance that doesn't block maturity progression.
- Both lists are capped at 8 items.

## Advisory Warnings

Beyond the 12 scored checks, the scanner emits additional warnings for edge cases:

| Condition | Warning |
|-----------|---------|
| Column descriptions at 50–80% | "Higher coverage improves SQL generation accuracy" |
| No column synonyms defined | "Add synonyms for columns with abbreviated or technical names" |
| Text instructions > 2,000 chars | "Keep under 2,000 to avoid pushing out higher-value SQL context" |
| SQL patterns in text instructions | "Move to Example SQLs or SQL Expressions" |
| Data source count 9–12 | "Consider splitting into focused rooms for >8 data sources" |
| Example SQLs 8–14 | "10-15 is the sweet spot for largest accuracy jump" |
| Missing `usage_guidance` on >50% of example SQLs | "Add descriptions of when each example should be applied" |
| Missing measures or filters in SQL snippets | "Add missing SQL snippet types for better coverage" |
| Entity matching columns > 100 | "Approaching 120/space limit" |
| Row-level security on tables with entity matching | "Entity matching is silently disabled for these" |

## Integration with Auto-Optimize

Checks 11 and 12 evaluate optimization results. The scanner reads from two sources concurrently:

1. **Lakebase** `optimization_runs` table — legacy/simple optimization records
2. **GSO Delta tables** (`genie_opt_runs`) — Auto-Optimize pipeline runs, with fallback from Lakebase synced tables to direct Delta queries via SP

The scanner normalizes accuracy values (GSO stores 0–100, scanner expects 0.0–1.0) and uses the best accuracy across both sources.

Only terminal GSO run statuses count: `CONVERGED`, `STALLED`, `MAX_ITERATIONS`. In-progress or failed runs are ignored.

## Persistence

Scan results are persisted to Lakebase (table: `scan_results`) with:
- `space_id`, `score`, `maturity`
- `breakdown` (JSONB with full checks, warnings)
- `findings`, `next_steps`
- `scanned_at` timestamp

Historical scans are available via `GET /api/spaces/{id}/history`.

## Source Files

- `backend/services/scanner.py` — scoring engine
- `backend/routers/spaces.py` — `POST /api/spaces/{id}/scan` endpoint
- `backend/services/lakebase.py` — persistence
- `backend/services/gso_lakebase.py` — GSO run data for checks 11–12

## Related Documentation

- [Quick Fix](06-fix-agent.md) — automatically fixes findings from the scanner
- [Auto-Optimize](07-auto-optimize.md) — the optimization pipeline that satisfies checks 11–12
- [Introduction](01-introduction.md) — how the scanner fits in the feature workflow
