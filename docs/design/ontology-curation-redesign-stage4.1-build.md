# Ontology — Curation Redesign · Stage 4.1 build spec (coded columns + Page-attachment gate)

Follow-up to **Stage 4 (Pages)**. Stage-4.1a (warehouse wiring) already landed and is
deploy-verified: threading `warehouse_id` into the materialize job turned on
`measure_signals`, and `page_count` went **0 → 480** on the airline estate. This spec
covers **4.1b** — the two things the live yield showed are still missing.

- **Spec umbrella:** `docs/design/ontology-curation-redesign-build.md` (§8 Pages; honor
  §11–§14). This file is the 4.1 addendum.
- **Decisions:** `docs/design/mv-advisor-playbook.md` — new **MV-D63** (coded-column
  batch feed), **MV-D64** (Page-attachment gate). Honor MV-D35 / D43 / D45 / D49 / D50 /
  D55 / D57 / D61 / D62.
- **Grain:** Metastore (MV-D49). Reads run as the job `run_as` identity (MV-D50).

## 1. Problem — what the Stage-4.1a live yield proved

Run `714995083781872` (fevm-serverless, airline catalog), read from `genie_ont_pages`:

| Cut | Count |
|---|---|
| Total Pages | 480 |
| by trigger | **measure = 480** (0 comment, 0 taxonomy) |
| by archetype | Routing 436 · Guardrail 28 · Disambiguation 16 |
| certify | **false = 480** (nothing certify-eligible) |
| surfaced=false, unattached | 288 (correctly hidden) |
| **surfaced=true, attached** | **151** (good) |
| **surfaced=true, unattached** | **41** (defect — shown with empty `domain_id`) |

Two gaps:

1. **Comments/Taxonomy still produce nothing.** Every Page is `trigger=measure`. The
   comment→`[Taxonomy]` path and the bare coded-column `[Taxonomy]` path both require
   `ColumnSignal`s, but the batch reader returns `[]`:

   ```python
   # jobs/run_ontology_materialize.py — SparkSystemTableReader.coded_column_signals
   def coded_column_signals(self, allowlist): 
       """... value-profiling is not read here ... degrades to []"""
       return []
   ```

   With no coded columns, `_taxonomy_detector` and the comment detector's Taxonomy branch
   can never fire, and no measure ever gains a second independent artifact → **every Page
   is single-artifact → `certify=false`** (`corroboration()==1`, gate MV-D35).

2. **41 surfaced Pages have no domain.** Pages skip the legitimacy + diffuseness gates
   (rank.py), so a Page surfaces iff `tier is not None and not blocked` — even when
   `_resolve_home_domain` returned `""` (its source tables are in no domain's members).
   These 41 show in the UI with an empty `domain_id`.

## 2. Goals

- **4.1b-coded:** feed real low-cardinality coded columns in batch, **bounded** (prefilter
  → capped `approx_count_distinct`), so `[Taxonomy]` Pages, the comment→`[Taxonomy]`
  trigger, and measure⊕column corroboration (→ some `certify=true`) all light up.
- **4.1b-gate:** a **Page-attachment gate** — a Page whose `domain_id` is empty or points
  to a non-surfaced Domain is **kept but `surfaced=false`** with an "attach to a domain"
  hint (mirrors `_apply_legitimacy_gate` / `_apply_diffuseness_gate`). Closes the 41.

Non-goals (deferred): **4.1c** comment-primary standalone Pages (a lone comment with no
coded/measure signal still mines nothing — "no signal → nothing" stays). Genie-history
trigger stays dormant.

## 3. Design

### 3.1 Coded-column reader (MV-D63) — `coded_column_signals`, reuse don't fork
Replace the `[]` stub with a **bounded** two-pass read:

1. **Prefilter (metadata-only, cheap).** Reuse the same allowlist-scoped, denylist-filtered
   `information_schema.columns` read `comment_signals` already issues (`column_name`,
   `data_type`, `comment`). Keep a column as a *candidate* when it is `STRING`/`CHAR`/
   small-`INT` **and** looks coded — name matches `_code|_status|_type|_category|_flag|
   _cd|_ind` (suffix or whole word) **or** its comment reads enum-like (contains `one of`,
   `values:`, `codes`, or a comma list). Purely deterministic; no warehouse yet.
2. **Profile only the candidates, capped.** For at most `coded_column_max_columns`
   candidates/run (config, default 300; deterministic order = sorted FQN), reuse
   `optimization.wide_schema_profile` work-item builders (`approx_count_distinct` +
   value-list `_value_list_item`) against `self._warehouse_id` (already threaded in 4.1a).
   Keep a column iff `distinct <= coded_column_max_cardinality` (default =
   `pages._TAXONOMY_MAX_CARDINALITY`). Collect up to ~24 distinct values.
3. **Governed flag.** `governed=True` when the column carries a governed tag **or** a CHECK
   constraint enumerating its values (metadata read, best-effort); else `False`.
4. Emit `pages.ColumnSignal(table_fqn, column, comment, distinct_values, governed,
   agent_fqns=(), domain_id=<schema-home, provenance only>)`. **Any failure at any step
   degrades to `[]`** (MV-D43) — never fail the run. `self._warehouse_id==""` ⇒ skip
   profiling → `[]` (same posture as measures).

No change to `pages.py` detectors — they already consume `ColumnSignal` (`_taxonomy_detector`,
comment detector's `coded_by_fqn` branch). This is a **reader-only** change.

### 3.2 Page-attachment gate (MV-D64) — `rank.py`
Add `_apply_page_attachment_gate(row, surfaced_domain_ids)` called from `score_proposals`
for `kind=="page"`, **after** `_score_row` sets the tentative `evidence["surfaced"]`:

- Compute `surfaced_domain_ids` = `{d["domain_id"] for d in domain_rows if
  d["evidence"].surfaced}` (the Domains that cleared their own gates this run).
- If a Page's `domain_id` is `""` **or** not in `surfaced_domain_ids` → set
  `evidence["surfaced"]=False` and `evidence["surfaced_reason"]="page not attached to a
  surfaced domain"`. Keep the row (metastore re-MERGE, MV-D49) — never delete.
- Config `page_require_domain` (default `True`) gates the whole rule so it can be disabled.

Ordering: Domains are scored first (they already are), so `surfaced_domain_ids` is known
before the Page pass. Pure/in-place, idempotent, no new column (rides `evidence`).

### 3.3 Config (MV-D57 surface)
Add to `OntologySettings` + job params + widgets (all with in-code defaults, MV-D43):
`coded_column_max_columns=300`, `coded_column_max_cardinality=<_TAXONOMY_MAX_CARDINALITY>`,
`page_require_domain=true`. Round-trip old rows to defaults (same pattern as Stage 3/3.2).

## 4. Data-model impact
None. No new tables/columns. `distinct_values`/`governed`/`surfaced`/`surfaced_reason`
ride existing `evidence` JSON. DDL unchanged (MV-D49).

## 5. Guardrails / invariants
- **Additive-only.** No new API model, route, or frontend frame. No governed-tag write.
- **Reuse, don't fork.** Profiling = `wide_schema_profile` builders; comments/columns read
  = the existing `_per_catalog` + `filter_denylisted_schemas` path; detectors unchanged.
- **Bounded cost.** Prefilter before any `SELECT`; hard per-run column cap; single value
  ceiling. No full-estate profiling sweep.
- **Degrade-not-hang (MV-D43).** Every new read wrapped; failure ⇒ `[]` / rule-skip, run
  still `succeeded`.
- **Grain + auth.** Metastore keys; warehouse DESCRIBE/profile runs as job `run_as`
  (MV-D50) — that identity needs `CAN_USE` on the warehouse (already true for the default
  run_as user).
- **Exact pins.** No new dependency.

## 6. Acceptance (offline — the agent's job; stops before deploy)
- `test_ontology_materialize` / new reader test: prefilter keeps coded-looking columns and
  drops free-text/high-cardinality; cap honored (≤ N, deterministic order); warehouse-less
  ⇒ `[]`; a profiling exception ⇒ `[]` and run still succeeds.
- `test_ontology_pages`: a governed coded column ⇒ certify-eligible `[Taxonomy]`; a comment
  term on a coded column ⇒ comment-trigger `[Taxonomy]`; a measure + a coded column on the
  **same canonical concept** ⇒ `corroboration()==2` ⇒ certify-eligible.
- `test_ontology_rank`: a Page with empty `domain_id` ⇒ `surfaced=false` +
  `surfaced_reason`; a Page attached to a surfaced Domain ⇒ `surfaced=true`; a Page
  attached to a **non-surfaced** Domain ⇒ `surfaced=false`; `page_require_domain=false`
  disables the rule.
- `./scripts/test.sh` green; wheel unit suite green.

## 7. Deploy-verify (human-gated, after offline green)
Deploy (`SKIP_FRONTEND_BUILD=1 ./scripts/deploy.sh --update`, fevm-serverless), trigger
scoped to `["serverless_stable_6t92c3_catalog"]`, then confirm on `genie_ont_pages`:
- `Taxonomy > 0` and `trigger IN (comment, measure, <coded>)` includes non-measure Pages.
- `certify=true` count `> 0` (measure⊕column corroboration).
- **0** rows with `surfaced=true AND (domain_id="" OR domain_id NOT IN surfaced domains)`.
- Airline Taxonomy Pages decode real coded columns (spot-check `distinct_values`).

## 8. Risks / mitigations
- **Warehouse cost from profiling** → prefilter + hard cap + value ceiling; cap is config.
- **Prefilter misses a code list** (ungoverned, oddly named) → acceptable for 4.1b; a full
  sweep is a later opt-in (declined here for cost).
- **Over-hiding Pages** if a legitimate Domain didn't surface → the gate keeps the row
  (not deleted) and `page_require_domain` can be flipped off; revisit with Domain gates.

## 9. Live verification (Stage-4.1b deploy-verify) — LANDED

Deploy: `SKIP_FRONTEND_BUILD=1 ./scripts/deploy.sh --update` (fevm-serverless), ontology
job `529504954941024` redeployed with the 4.1b wheel (commit `d85ed3b5`). Run
`934403918953760`, `catalog_allowlist=["serverless_stable_6t92c3_catalog"]` → **SUCCESS**
(~20 min; the added coded-column profiling pass lengthened it). Ledger: 140 domains,
**page_count 480 → 641**. Read from `genie_ont_pages`:

| Check (from §7) | Result | |
|---|---|---|
| `[Taxonomy] > 0`, non-measure trigger fires | **161 Taxonomy** (0 → 161) | ✅ coded-column trigger lit |
| **0** surfaced-but-unattached | **0** (was 41) | ✅ MV-D64 gate closed all orphans |
| Taxonomy Pages decode real coded columns | `account_status→[active]`, `ad_status→[active]`, `accrual_status→[posted]`, `accrual_source_type→[flight]` … all attached to real domains | ✅ |
| `certify=true > 0` | **0** | ⚠️ **by design** (below) |

Archetype mix: Routing 436 · **Taxonomy 161** · Guardrail 28 · Disambiguation 16.

**Corroboration — the real 4.1b goal — works.** The `certify` line was a proxy for
measure⊕column corroboration, and that engine is now on: **591 of 641 Pages are at
corroboration ≥ 2** (corr=2 → 477, corr=3 → 61, up to corr=12), versus all-`corr=1` in
Stage-4.1a. Coded columns collapse onto the same canonical concepts as the measures.

**Why `certify=0` is by design, not a defect.** `certify` (pages.py:988) =
`certify_shape AND corroborated AND syn_ok AND not conflict AND llm_ok`. Everything is
satisfied **except `llm_ok`** — all 641 Pages are `body_source=stub`. `default_page_drafter`
does `from backend.services.llm_utils import call_serving_endpoint`, but **`backend` is not
on the job cluster** (the job env installs only the `genie_space_optimizer` wheel), so the
import raises → drafter returns `""` → deterministic stub → `llm_ok=False` → `certify=false`.
The drafter docstring states this intent explicitly ("stays importable on a job cluster
without `backend`… falls back to the deterministic stub + `certify=false`", MV-D43).
Certification is an **app-process / interactive** concern, not batch. Separately, all 161
Taxonomy are `governed=false` — correct for this estate (the demo columns carry no governed
tags / CHECK enums).

**Verdict:** the four 4.1b deliverables (coded-column trigger, real decodes, attachment
gate, measure⊕column corroboration) all landed and verified live. The `certify=true>0`
line in §7 was written against the wrong process — unachievable in batch without porting an
LLM client into the wheel, which MV-D43 deliberately avoids. Batch emits
corroboration-complete, attached, decoded Pages that are intentionally **uncertified**;
certification happens on the app path (or a future **Stage-4.1c** wheel-native drafter).
