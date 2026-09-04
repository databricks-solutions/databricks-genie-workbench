# Ontology — Stage 4.1d Goal-Mode driver (Step 2: `body_source` preservation across re-materialize)

Copy-paste launcher for **Stage 4.1d Step 2** with a long-running agent. Run on the
**`ontology`** branch **after Step 1 landed** (deterministic certify + capped super-sure
auto-draft). Step 1 introduced `evidence.body_source ∈ {stub, llm_auto}`; Steps 3–4 will add
curator sources `{llm_ondemand, llm_bulk, human}`. Step 2 makes those curator bodies
**durable** — the batch snapshot MERGE must stop clobbering them every refresh. Additive;
offline code + green tests; **stops before deploy**.

- **Spec:** `docs/design/ontology-curation-redesign-stage4.1d-build.md` §3.2, §4, §5, §6
  (umbrella `…-build.md` §7.2). **Decisions:** `mv-advisor-playbook.md` — MV-D66; honor
  MV-D26/D43/D49.
- **Seam:** `ontology/ddl.build_snapshot_merge_sql` (the generic snapshot MERGE, already
  parameterized with `delete_unmatched`), `materialize.SparkSnapshotWriter.merge`,
  `materialize._project_to_schema`. `genie_ont_pages` has real `body`/`certify` columns and
  an `evidence` JSON STRING column — `body_source`/`facts_hash` ride `evidence` (no DDL).

---

## Driver prompt (paste verbatim)

GOAL: Stage 4.1d Step 2 — preserve curator-authored Page bodies across batch
re-materialize. The snapshot MERGE currently overwrites `body` every run; curator bodies
(Steps 3–4: llm_ondemand/llm_bulk/human) must survive. Batch-owned bodies (stub/llm_auto)
still refresh. Additive. Offline code + green tests; STOP before deploy.

SPEC: docs/design/ontology-curation-redesign-stage4.1d-build.md §3.2/§4/§5/§6 (umbrella
…-build.md §7.2). DECISIONS: mv-advisor-playbook.md MV-D66; honor MV-D26/D43/D49. Read
first. Steps 1 (done) and 3–4 (app routes) are NOT in scope here.

CONTEXT: ddl.build_snapshot_merge_sql emits `WHEN MATCHED THEN UPDATE SET <all update_cols>`
unconditionally, so a re-run overwrites `body`. genie_ont_pages has real body/certify cols +
an evidence JSON col. Step 1 already writes evidence.body_source (stub|llm_auto). A stable
page_id means the same Page re-MERGEs onto the same target row.

BUILD A — facts_hash (pages.py): when finalizing a Page, compute a deterministic
`facts_hash` over spec.facts() (the prose inputs) and store it in evidence. This lets Step 2
detect when a preserved body has gone stale.

BUILD B — conditional preserve in the MERGE (ddl.build_snapshot_merge_sql): add two OPTIONAL
params `preserve_cols: list[str] = ()` and `preserve_when: str = ""`. When both are set,
emit, BEFORE the general WHEN MATCHED clause, a guarded clause:
`WHEN MATCHED AND {preserve_when} THEN UPDATE SET <update_cols MINUS preserve_cols>` — i.e.
refresh everything except the preserved cols for rows matching the predicate; the existing
`WHEN MATCHED THEN UPDATE SET <all>` handles the rest. Keep delete_unmatched behavior and
the metastore-scoped delete unchanged. Purely additive: unset params ⇒ byte-identical SQL.

BUILD C — wire pages preservation (materialize.SparkSnapshotWriter.merge): for the
genie_ont_pages table ONLY, pass preserve_cols=["body"] and
preserve_when="get_json_object(t.evidence,'$.body_source') IN
('llm_ondemand','llm_bulk','human')". Also, so a preserved body keeps its own source marker,
include the target's body_source in what's preserved — since body_source lives inside the
evidence JSON, preserve `evidence` for curator rows too BUT re-stamp evidence.body_stale by
comparing the incoming facts_hash to the stored one (if different ⇒ body_stale=true). Keep
_project_to_schema (evidence stays; ad-hoc keys dropped). Other tables unchanged (no
preserve params) so their MERGE is untouched.

HARD GUARDRAILS: additive — NO new table/column/DDL (body_source/facts_hash/body_stale ride
evidence JSON, MV-D49), NO API/route/frontend, NO governed-tag write, NO new dependency.
Unset preserve params ⇒ identical SQL for every non-pages table. Metastore keys + scoped
delete unchanged. Degrade-not-hang (MV-D43).

ACCEPTANCE (offline): test_ontology_ddl — with preserve params, the MERGE has the guarded
WHEN MATCHED AND <predicate> clause updating all-but-body first, then the general clause;
WITHOUT them the SQL is byte-identical to today. test_ontology_materialize — a page row
whose stored evidence.body_source is llm_ondemand/llm_bulk/human keeps its prior body after a
re-merge that carries a new stub body; a stub/llm_auto row is refreshed normally; a preserved
row whose incoming facts_hash differs gets evidence.body_stale=true. ./scripts/test.sh +
wheel suite green.

WORKFLOW: branch `ontology`. Do NOT deploy or run the job. When offline-green, STOP and
report the diff + test summary; a human runs deploy-verify.

---

## After the run (human-gated)
Deploy `SKIP_FRONTEND_BUILD=1 ./scripts/deploy.sh --update` (fevm-serverless), trigger scoped
to `["serverless_stable_6t92c3_catalog"]`. Since no curator bodies exist yet (Steps 3–4 land
later), the acceptance here is **no regression** — re-materialize still refreshes stub /
llm_auto bodies and the run succeeds. Full preservation is exercised once Step 3 writes an
`llm_ondemand` body and a subsequent refresh leaves it intact.
