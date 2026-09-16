# Ontology Eval — curated/governed domains are ground truth, never false positives · Goal-Mode driver (MV-D59)

> **One run, on the `ontology` branch.** A harness-precision refinement, wheel-only, additive,
> read-only. It unblocks the Stage 1 (MV-D93/D94) merge: Stage 1b is deploy-verified
> (precision 0.944, F1 0.85, junk gone, usage ranks all domains low→medium) but the harness
> penalizes ONE human-governed domain (`Traceability`) purely because the *industry* reference
> model doesn't list it. STOP before deploy — a human runs the re-baseline gate.

## Why (verified live 2026-09-15, run `979e5c6…`)
`compute_precision_recall_f1` counts every discovered domain with no industry-reference alignment
as a **false positive** (`fp = unmatched_discovered`). But `Traceability` is a *human-governed*
domain (`evidence.rank.factors.governance.present=true, value=1.0`; its member is a governed
`…mande_gold.traceability_document`). It surfaces because it's governed (MV-D53: curated wins),
not because of usage (0.265). Penalizing a customer-governed domain for being absent from a
generic industry taxonomy is wrong — a governed domain is ground truth by human declaration.

**This does NOT make precision meaningless on a governed estate.** Junk is *ungoverned*: Stage 1's
regression surfaced `Migration` / the GSO's own cost tables / dev-demo datasets, all with
`governance.value≈0.2` (ungoverned). Those STILL count as false positives. The refinement only
reclassifies **governed/curated** unmatched domains (value ≥ 0.6) as legitimate; ungoverned
unmatched clusters remain FPs, so precision still catches junk. Recall stays pure industry-coverage
(a curated-unmatched domain covers no reference domain, so it must NOT inflate recall).

---

## GOAL PROMPT (paste verbatim into Goal Mode)

Refine `ontology/eval_harness.py::compute_precision_recall_f1` so a CURATED/GOVERNED discovered
domain that has no industry-reference match is treated as ground truth (a true positive for
precision), NOT a false positive — on the `ontology` branch. Wheel-only, additive, read-only,
offline-green, STOP before deploy.

WHY: today `fp = unmatched_discovered`, so a human-governed domain absent from the generic industry
model (e.g. `Traceability`, governance value 1.0) wrongly drags precision down (0.944 on run
979e5c6). Junk is ungoverned (governance≈0.2) and must stay a false positive.

BUILD A — eval_harness.py:
- Add a pure helper `_is_curated(domain: dict) -> bool`: read
  `_load_evidence(domain).get("rank",{}).get("factors",{}).get("governance",{})`; return
  `bool(gov.get("present")) and float(gov.get("value") or 0.0) >= _CURATED_GOV_MIN`. Define module
  constant `_CURATED_GOV_MIN = 0.6` with a comment: this is the rank ladder's `curated` rung
  (`rank._GOVERNANCE_VALUE = {governed:1.0, curated:0.6, ungoverned:0.2}`) — governed+curated pass,
  ungoverned (0.2) does not. Do not import rank (avoid a cycle); pin the literal + comment.
- In `compute_precision_recall_f1`, after computing `unmatched_discovered`:
  `curated_unmatched = {d for d in unmatched_discovered if _is_curated(discovered_by_id[d])}`.
  Set `fp = len(unmatched_discovered - curated_unmatched)` (ungoverned-unmatched only).
  Precision legitimacy numerator = matched + curated: `legit = tp + len(curated_unmatched)`;
  `precision = legit / (legit + fp) if (legit + fp) > 0 else 0.0`.
  **Recall is UNCHANGED**: `recall = tp / (tp + fn)` where `tp = len(matched_discovered)` and
  `fn = len(unmatched_reference)` — a curated-unmatched domain covers no reference domain, so it
  must NOT enter recall. Recompute f1 from the new precision + unchanged recall.
- In the per-domain `match_statuses` loop, a domain in `curated_unmatched` gets
  `match_type = "curated"` (the others keep `exact`/`extra` as today).

BUILD B — test_ontology_eval_harness.py:
- A governed-unmatched domain (governance present, value 1.0) does NOT count as FP: precision stays
  high; its `match_type == "curated"`.
- An ungoverned-unmatched domain (governance value 0.2, or governance absent) DOES count as FP:
  precision drops (junk still caught).
- Recall is identical with and without a curated-unmatched domain (it never inflates recall).
- A matched domain still scores exactly as before (no behavior change on aligned domains).
- Deterministic; degrade-safe: a domain with no evidence/governance is treated as not-curated (FP if
  unmatched — conservative).

GUARDRAILS: only `eval_harness.py` + its test change. Do NOT alter `compute_structural_health`,
`compute_llm_sanity`, the spot-review builder, `assemble_eval_report` scoping (MV-D59 surfaced-only),
`report_to_dict/json`, `eval_report_to_row`, or the DDL. No new Delta table/column (MV-D49); no new
dependency (MV-D45) — `uv.lock`/`package-lock.json` byte-identical; read-only (MV-D26); deterministic
(MV-D82). Recall formula MUST stay unchanged.

ACCEPTANCE (offline): `./scripts/test.sh` green; targeted eval-harness + materialize tests green;
lockfiles untouched. Then STOP.

---

## Deploy-verify gate (human, after offline-green)
Capture the current `genie_ont_eval` row first (Stage 1b: precision 0.944 / F1 0.85, run `979e5c6…`)
for `compare_reports`. Then `SKIP_FRONTEND_BUILD=1 DATABRICKS_CONFIG_PROFILE=fevm-serverless
./scripts/deploy.sh --update`, `run-now` the materialize job with
`catalog=serverless_stable_6t92c3_catalog`, `catalog_allowlist=["serverless_stable_6t92c3_catalog"]`,
`industry_alignment_enabled=true`, `industry_alignment_reference_model=airline`. Confirm:
- `Traceability` now carries `match_type="curated"` and is no longer a false positive.
- **Precision climbs to ~1.0, F1 to/above 0.865**, recall unchanged (~0.77) — the honest recovery.
- Junk is still absent (no ungoverned unmatched cluster surfaced). If it clears, push `ontology` and
  mark Stage 1 (MV-D93/D94) + this refinement deploy-verified.
