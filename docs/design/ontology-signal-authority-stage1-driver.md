# Ontology Signal Authority — Stage 1 (Popularity) · Goal-Mode driver (MV-D93/D94)

> **One stage per run, on the `ontology` branch.** Build spec: `ontology-signal-authority-build.md`
> §3 (authoritative). This driver only turns on the **0.40 usage factor** the ranker already
> reserves but never sources. Additive, read-only, no new table/column/dependency. STOP before
> deploy — a human runs the deploy-verify gate.

## Why now
MV-D59 (the §10 eval/trust harness) is **deploy-verified green** (precision 1.00 / F1 0.865 on the
surfaced airline estate, `8c6af04e`), so the hard order-gate that blocked this track is lifted.
Stage 1 is the highest-leverage, smallest change: `usage_signals()` returns `{}` today, so the
highest-weight blend factor is *always absent*, holding well-founded proposals below their earned
tier via the coverage-cap.

## Grounded facts (probed 2026-09-15, `fevm-serverless`)
- Seam: `run_ontology_materialize.usage_signals(allowlist)` (stub → `{}`, ~L930) →
  `materialize._gather_usage` (~L454, already threads into `rank.RankSignals.usage`) → `rank.blend`.
- The job reads via `spark.sql` (`_rows`/`_rows_safe`, ~L286/L623), running as the MV-D50 run_as
  user — so `system.access.table_lineage` is directly readable; no SP grant dance needed here.
- `system.access.table_lineage` columns (confirmed): `source_table_full_name`,
  `source_table_catalog/schema/name`, `created_by`, `event_time`, `statement_id`.
- Live volume for the airline catalog (30d): **42,168 reads · 559 tables · 9 users** → Stage 1 will
  produce a real, non-empty usage map.

## Testability seam (do this)
Put the *pure* normalization in the wheel so it is unit-testable and I/O-free: add
`normalize_usage(rows) -> dict[str, float]` to a small pure module (e.g. `ontology/usage.py`, or
next to the blend in `rank.py`). The job's `usage_signals` only *fetches* rows via `_rows_safe`
and calls the pure normalizer. `rank.py`'s blend arithmetic stays untouched (MV-D35).

---

## GOAL PROMPT (paste verbatim into Goal Mode)

Implement **Stage 1 (Popularity)** of `docs/design/ontology-signal-authority-build.md` §3 on the
`ontology` branch. Turn on the dormant 0.40 usage factor. Additive, read-only, offline-green,
STOP before deploy.

BUILD A — pure normalizer (wheel, unit-tested):
- Add `normalize_usage(rows: list[dict]) -> dict[str, float]` to `ontology/usage.py` (new, pure,
  I/O-free) or `rank.py`. Input rows are `{fqn, reads, users}`. Compute a combined demand score
  (blend `reads` with a `users` breadth term), then **percentile-rank** it over the in-scope
  tables → `[0,1]` (robust to query-volume right-skew). Return a **sorted** `{fqn: float}`.
- Honest-gap (MV-D43): tables with zero reads are **omitted** (absent, never `0.0`); empty/failed
  input ⇒ `{}`. Deterministic (sorted inputs, no wall-clock).

BUILD B — job reader (`run_ontology_materialize.usage_signals`):
- Replace the `return {}` stub. On empty allowlist ⇒ `{}` (byte-identical to today). Else read
  `system.access.table_lineage` via `_rows_safe` over a trailing 30-day window, per in-scope
  catalog: group by `lower(source_table_full_name)` → `COUNT(*) AS reads`,
  `COUNT(DISTINCT created_by) AS users`; filter `source_table_full_name IS NOT NULL`. Pass the rows
  to `normalize_usage`. Any read failure degrades to `{}` (MV-D43) — never raises, never a zero map.

BUILD C — tests:
- Unit (normalizer): synthetic `reads`/`users` rows → sorted `{fqn: float}`, all values in `[0,1]`,
  busiest table at the top percentile, zero-read tables omitted, empty input ⇒ `{}`.
- Blend: a proposal whose anchor now carries a usage entry has `factors.usage.present == true` and
  a **higher `evidence_coverage`** than the same proposal without usage (coverage rises when usage
  joins governance+centrality) — assert the tier lifts where earned.
- A raising/parse-failing reader path returns `{}` (degrade-not-hang).

GUARDRAILS (hard):
- Do NOT change `FACTOR_WEIGHTS`, `blend`, `coverage_cap`, `tier_of`, `confidence_band`, or any
  firewall (MV-D35) — this feeds better inputs, it does not re-weight or re-tier.
- No new Delta table/column (MV-D49); no new dependency (MV-D45) — `uv.lock`/`package-lock.json`
  byte-identical; read-only, no governed-tag write (MV-D26). Deterministic (MV-D82).
- Live path stays default-correct: an env without lineage grants degrades to `{}` = today's bytes.

ACCEPTANCE (offline): `./scripts/test.sh` green; targeted rank/materialize/usage tests green;
`uv.lock` + `frontend/package-lock.json` untouched; only wheel files changed. Then STOP.

---

## Deploy-verify gate (human, after offline-green)
`SKIP_FRONTEND_BUILD=1 DATABRICKS_CONFIG_PROFILE=fevm-serverless ./scripts/deploy.sh --update`,
then `run-now` the materialize job with `catalog=serverless_stable_6t92c3_catalog`,
`catalog_allowlist=["serverless_stable_6t92c3_catalog"]`, `industry_alignment_enabled=true`,
`industry_alignment_reference_model=airline` (~19 min). Confirm:
- `evidence.rank.factors.usage.present == true` on queried surfaced anchors; `evidence_coverage`
  and tiers lift where earned.
- The MV-D59 harness P/R/F is **flat-or-up** vs the `db43149…` baseline (precision 1.00 / F1 0.865)
  — capture the `genie_ont_eval` row before re-running so `compare_reports` has a before/after.
