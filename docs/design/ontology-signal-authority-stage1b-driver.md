# Ontology Signal Authority — Stage 1b (usage is rank-only, not a surfacing gate) · Goal-Mode driver (MV-D94)

> **One run, on the `ontology` branch.** Follows Stage 1 (`8b121aa7`, local/unpushed), which wired
> the 0.40 usage factor but **regressed the §10 harness** (precision 1.00→0.426, F1 0.865→0.556) by
> letting popularity *admit* clusters to the surfaced set. This fix keeps usage in the blend for
> ranking but stops it from being the reason a cluster surfaces. Additive, wheel-only, read-only.
> STOP before deploy — a human runs the deploy-verify gate.

## Why (root cause, verified live 2026-09-15, run `ea011db…`)
`rank.blend` is a **coverage-normalized weighted average** of `usage(0.40) × centrality(0.35) ×
governance(0.25)`, and `_score_row` sets `evidence["surfaced"] = (rank["tier"] is not None and not
blocked)` where `tier = tier_of(score)`. So **the surfacing gate is the score threshold**. Turning
on usage raised the *average* for heavily-queried but ungoverned tables — `Migration`, the GSO's own
`Cost Attribution`/`Genie Space Optimizer` tables, `Wanderbricks/Bakehouse/Dev-Demo` datasets (all
`usage≈0.9–1.0`, `governance=0.2` ungoverned) — from ~20 to ~69, pushing them over the threshold.
Popularity is orthogonal to business-domain legitimacy; it must **rank** the legitimate set, never
**admit** to it. The legit airline domains surface on `governance=1.0`; the junk only ever scored
low. (The MV-D59 harness caught this — first real catch. Pre-Stage-1 surfaced-scoped baseline:
precision 1.00 / F1 0.865, run `db4314937b4f4d68986af334e2e0c77b`.)

## The fix (principle)
**Usage cannot be the sole reason a cluster surfaces.** A cluster surfaces only if it clears the
threshold on its **non-usage** evidence (centrality + governance); usage still rides the full blend
for the *displayed* tier/score/coverage (ranking). This reuses the existing `tier_of`/`blend`
primitives over a factor subset — it does **not** re-weight or re-tier (MV-D35): the displayed tier
is unchanged; only the boolean `surfaced` gate gets a usage-independent second condition.

---

## GOAL PROMPT (paste verbatim into Goal Mode)

Fix Stage 1 of `docs/design/ontology-signal-authority-build.md` so the usage factor is **rank-only,
never a surfacing gate**, on the `ontology` branch. Wheel-only, additive, read-only, offline-green,
STOP before deploy.

ROOT CAUSE: `rank.blend` is a coverage-normalized weighted average; `_score_row` sets
`evidence["surfaced"] = (rank["tier"] is not None and not blocked)`, so score (which now includes the
0.40 usage factor) is the surfacing gate. Heavily-queried ungoverned junk (Migration, the GSO's own
cost tables, dev/demo datasets: usage≈1.0, governance=0.2) now clears the threshold and surfaces,
crashing harness precision 1.00→0.426 / F1 0.865→0.556.

BUILD A — pure surfacing predicate (`rank.py`):
- Add a pure helper `_surfacing_ok(factors: dict) -> bool`: recompute a score over the **non-usage**
  present factors only (centrality, governance) using the SAME formula as `blend`
  (`100 * sum(w*value for present non-usage factors) / sum(w for present non-usage factors)`;
  0 if no non-usage factor present), then return `transforms.tier_of(score_non_usage) is not None`.
  Reuse `FACTOR_WEIGHTS`; do NOT change `blend`, `FACTOR_WEIGHTS`, `coverage_cap`, `tier_of`, or any
  firewall (MV-D35). A cluster whose ONLY evidence is usage ⇒ non-usage score 0 ⇒ tier None ⇒ not a
  surfacing basis (honest-gap, MV-D43).
- In `_score_row`, change the surfaced assignment to also require the predicate:
  `evidence["surfaced"] = bool(rank["tier"] is not None and not blocked and _surfacing_ok(rank["factors"]))`.
  Record `rank["surface_basis"] = "non_usage_evidence"` for traceability. The DISPLAYED
  `rank["tier"]`/`score`/`evidence_coverage`/`factors` keep the full usage-inclusive blend (ranking).

BUILD B — tests (`test_ontology_rank.py`):
- A cluster with only `usage` present (governance/centrality absent) does NOT surface even at
  usage=1.0 (was the regression).
- A governed domain (`governance` present) surfaces with AND without usage; its displayed tier still
  rises when usage is added (usage ranks, does not gate).
- A domain with `centrality` present surfaces on structural evidence (no usage needed).
- `_surfacing_ok` unit cases: only-usage⇒False; governance-only⇒True; centrality-only⇒True;
  no-factors⇒False. Deterministic.

GUARDRAILS: do NOT touch `FACTOR_WEIGHTS`/`blend`/`coverage_cap`/`tier_of`/firewalls (MV-D35); no new
Delta table/column (MV-D49); no new dependency (MV-D45) — `uv.lock`/`package-lock.json` byte-identical;
read-only, no governed-tag write (MV-D26); deterministic (MV-D82). Only `rank.py` + its test change.

ACCEPTANCE (offline): `./scripts/test.sh` green; targeted rank/usage/materialize tests green;
lockfiles untouched. Then STOP.

---

## Deploy-verify gate (human, after offline-green)
Capture the current `genie_ont_eval` row first (post-Stage-1 regression: precision 0.426 / F1 0.556,
run `ea011db…`) so `compare_reports` has a before/after. Then
`SKIP_FRONTEND_BUILD=1 DATABRICKS_CONFIG_PROFILE=fevm-serverless ./scripts/deploy.sh --update`,
`run-now` the materialize job with `catalog=serverless_stable_6t92c3_catalog`,
`catalog_allowlist=["serverless_stable_6t92c3_catalog"]`, `industry_alignment_enabled=true`,
`industry_alignment_reference_model=airline`. Confirm:
- The junk (`Migration`, `Genie Space Optimizer`, `Cost Attribution`, `Wanderbricks/Bakehouse/Dev`)
  is gone from the surfaced set; surfaced count returns toward ~16.
- `evidence.rank.factors.usage.present == true` still holds on surfaced domains (usage ranks them);
  the displayed tier ordering reflects popularity.
- Harness **precision ≥ ~1.0 and F1 ≥ 0.865** (recover to/above the `db4314…` baseline) — this is the
  merge gate. If it clears, push `ontology` and mark Stage 1 deploy-verified.
