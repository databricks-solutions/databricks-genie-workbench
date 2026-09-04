# Ontology — Stage 4.1 Goal-Mode driver (coded columns + Page-attachment gate)

Copy-paste launcher for **Stage 4.1b** with a long-running agent (Claude Code / Cursor
Goal Mode). Run on the **`ontology`** branch **after Stage 4 + Stage-4.1a (warehouse
wiring) have landed** (commit `246b3983`). Additive-only: a reader-only change plus one
new rank gate — no new route, frame, or DDL. The agent produces offline code + green
tests and **stops before deploy**.

- **Spec (source of truth):** `docs/design/implemented/ontology-curation-redesign-stage4.1-build.md`
  (honor the umbrella `ontology-curation-redesign-build.md` §8, §11–§14)
- **Decisions:** `docs/design/mv-advisor-playbook.md` — **MV-D63** (coded-column batch
  feed), **MV-D64** (Page-attachment gate); honor MV-D35 / D43 / D45 / D49 / D50 / D57.
- **Live evidence:** build §1 (run `714995083781872`: 480 measure Pages, 0 comment/
  taxonomy, 480 `certify=false`, 41 surfaced-but-unattached).

---

## Driver prompt (paste verbatim)

GOAL: Stage 4.1b — feed bounded coded columns in batch + add a Page-attachment gate.
Additive-only. Offline code + green tests; STOP before deploy.

SPEC (source of truth): docs/design/implemented/ontology-curation-redesign-stage4.1-build.md (umbrella
…-build.md §8, §11–§14). DECISIONS: mv-advisor-playbook.md MV-D63, MV-D64; honor
MV-D35/D43/D49/D50/D57. Read before coding.

CONTEXT: 4.1a landed — the job threads warehouse_id (SparkSystemTableReader._warehouse_id).
Live: 480 Pages, ALL trigger=measure, ALL certify=false; 41 surfaced with empty domain_id.
Cause: coded_column_signals returns [] so [Taxonomy]/comment triggers never fire and no
concept gets a 2nd artifact; Pages skip attachment checks so orphans surface.

BUILD A — coded columns (reader-only), jobs/run_ontology_materialize.py
SparkSystemTableReader.coded_column_signals: replace `return []` with a BOUNDED two-pass
read. Pass 1 (metadata, cheap): reuse the SAME allowlist-scoped, denylist-filtered
information_schema.columns read comment_signals uses (column_name, data_type, comment); keep
a candidate when type is STRING/CHAR/small-INT AND it looks coded (name matches
_code|_status|_type|_category|_flag|_cd|_ind OR comment enum-like: "one of"/"values:"/
"codes"/comma list). Pass 2 (profile, capped): for ≤ coded_column_max_columns (default 300,
sorted-FQN), REUSE optimization/wide_schema_profile builders (approx_count_distinct +
value-list) against self._warehouse_id; keep iff distinct ≤ coded_column_max_cardinality
(default = pages._TAXONOMY_MAX_CARDINALITY), collect ≤24 distinct_values. governed=True iff
governed tag OR CHECK-constraint enum (metadata, best-effort) else False. Emit
pages.ColumnSignal(table_fqn, column, comment, distinct_values, governed, agent_fqns=(),
domain_id=schema-home). ANY failure ⇒ [] (MV-D43); _warehouse_id=="" ⇒ []. Do NOT touch
pages.py detectors (they already consume ColumnSignal).

BUILD B — Page-attachment gate (MV-D64), ontology/rank.py: add
_apply_page_attachment_gate(row, surfaced_domain_ids), called from score_proposals for
kind=="page" AFTER _score_row (Domains scored first). surfaced_domain_ids = domain_ids whose
evidence surfaced is true. If a Page's domain_id is "" OR not in that set ⇒ set
evidence["surfaced"]=False + evidence["surfaced_reason"]="page not attached to a surfaced
domain"; KEEP the row (never delete). Guard with config page_require_domain (default True).
Pure/in-place.

BUILD C — config (MV-D57): add coded_column_max_columns=300,
coded_column_max_cardinality=<_TAXONOMY_MAX_CARDINALITY>, page_require_domain=true to
OntologySettings + job params + widgets, each with in-code defaults; round-trip old rows to
defaults (Stage-3/3.2 pattern).

HARD GUARDRAILS: additive-only — NO new API model/route/frame, NO governed-tag write, NO new
DDL/column (distinct_values/governed/surfaced/surfaced_reason ride evidence JSON, MV-D49),
NO new dependency. Reuse wide_schema_profile (don't fork). Bounded cost: prefilter before any
SELECT, hard cap, value ceiling. Degrade-not-hang (MV-D43): wrap every new read, failure ⇒
[]/skip, run still succeeds. Reads run as job run_as (MV-D50).

ACCEPTANCE (offline): test_ontology_materialize — prefilter keeps coded / drops free-text +
high-cardinality; cap honored (≤N, deterministic); warehouse-less ⇒ []; profiling exception
⇒ [] + run succeeds. test_ontology_pages — governed coded col ⇒ certify-eligible [Taxonomy];
comment term on coded col ⇒ comment [Taxonomy]; measure+coded col same concept ⇒
corroboration==2 ⇒ certify-eligible. test_ontology_rank — empty domain_id ⇒ surfaced=false +
reason; attached-to-surfaced ⇒ true; attached-to-non-surfaced ⇒ false; page_require_domain=
false disables. ./scripts/test.sh + wheel unit suite green.

WORKFLOW: branch `ontology`. Do NOT deploy, run the job, or touch UC governance. When
offline-green, STOP and report the diff + test summary; a human runs deploy-verify (§7).

---

## After the run (human-gated)
Deploy `SKIP_FRONTEND_BUILD=1 ./scripts/deploy.sh --update` (fevm-serverless), trigger the
job with `catalog_allowlist=["serverless_stable_6t92c3_catalog"]`, then verify per build §7:
Taxonomy > 0, non-measure triggers present, `certify=true` > 0, and **0** surfaced-but-
unattached Pages.
