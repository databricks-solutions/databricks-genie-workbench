# Ontology Map — Typed Estate Assets · Stage 3 Goal-Mode driver

Copy-paste launcher for a long-running agent. Run on the **`ontology`** branch **after Stage 2
landed**. Stage 3 puts **Lakeview Dashboards** on the map: each dashboard becomes a `dashboard`
node, **placed by its APPLIED governed tag** (read from the `entity-tag-assignments` API) and
edged to the tables/MVs its datasets query. **Wheel + reader. Additive. Offline + green tests.
STOPs before deploy.**

- **Spec:** `docs/design/ontology-map-typed-assets-build.md` §2.5, §5, §6, §7 (MV-D92).
  **Decisions:** `mv-advisor-playbook.md` — MV-D92; honor MV-D26/D43/D49/D82.
- **Seams:** `jobs/run_ontology_materialize.py` (reader — extend `entity_tag_assignments(...)`
  to the `dashboards` entity via `w.lakeview.list()` + per-dashboard
  `list_tag_assignments("dashboards", id)`; add `dashboard_scopes` from dataset lineage);
  `ontology/graph.build_signal_graph` (add an OPTIONAL `dashboard_scopes` kwarg mirroring
  `agent_scopes` — `dashboard:<id>` nodes + `dashboard_scope` edges); `ontology/materialize.py`
  (feed dashboard tag rows into the domain assembly; pass `dashboard_scopes=`);
  `ontology/layout.py` (`_label_for_node` dashboard branch, `_verb_of` `dashboard_scope`→"reads",
  §6 fallback). Renderer FROZEN. Stage 2 already built the `geniespaces` reader — mirror it.

---

## Driver prompt (paste verbatim)

GOAL: Ontology Map Stage 3 — put Lakeview Dashboards on the map. Each dashboard becomes a
dashboard node PLACED BY ITS APPLIED GOVERNED TAG (entity-tag-assignments API, not
information_schema) and edged to the tables/MVs its datasets query. Wheel+reader, additive,
offline; STOP before deploy.

SPEC: docs/design/ontology-map-typed-assets-build.md §2.5/§5/§6/§7. DECISIONS:
mv-advisor-playbook.md MV-D92; honor MV-D26/D43/D49/D82. Read first. Stages 1 (typed assets) +
2 (agents, incl. the geniespaces entity-tag reader) are landed — mirror the Stage-2 wiring.

CONTEXT: No dashboard reader, kwarg, or kind exists yet. 30 Lakeview dashboards exist
(w.lakeview.list()). Governed tags DO apply to dashboards but the assignment lives behind GET
/api/2.0/entity-tag-assignments/dashboards/{id}/tags (SDK
w.workspace_entity_tag_assignments.list_tag_assignments; entity_type "dashboards"), NOT
information_schema. Reuse Stage 2's geniespaces entity-tag reader + assemble path, and mirror
its agent_scopes kwarg wiring. Frontend already types dashboard — DO NOT touch it.

BUILD A — DOMAIN (primary), reader entity_tag_assignments("dashboards"): enumerate dashboards
via w.lakeview.list(); per dashboard call list_tag_assignments("dashboards", id) (SDK, or REST
w.api_client.do("GET", f"/api/2.0/entity-tag-assignments/dashboards/{id}/tags") if the pinned
databricks-sdk==0.117.0 lacks the method — NO dependency bump). Return rows {tag_name,
tag_value, member_id: f"dashboard:{id}"}. Domain-key selection is identical to Stage 2: keep a
tag_key ONLY if transforms.classify_tag(...) (MV-D51) = aboutness (drop facets certified,
contains_synthetic); do NOT use system.tags.governed_tags. Degrade to [] on failure (MV-D43). In
materialize, feed the kept rows into the SAME transforms.assemble_tag_graph path as tables/agents
⇒ a tagged dashboard lands in its domain with origin=applied (usually the value-less top-level
key, e.g. "Alaska Airlines Operations"). Sorted/deterministic.

BUILD B — READ EDGES (secondary): add an OPTIONAL dashboard_scopes kwarg to build_signal_graph
mirroring agent_scopes (emit dashboard:<id> nodes + dashboard_scope edges; UNSET ⇒
byte-identical graph, MV-D43). Reader dashboard_scopes(allowlist) -> {id->sorted[fqn]} from
dataset lineage (system.access.table_lineage or serialized datasets; degrade to {}); materialize
passes it. In layout add a _label_for_node dashboard branch (name) + _verb_of dashboard_scope ->
"reads".

BUILD C — FALLBACK (optional, §6): an UNTAGGED dashboard may borrow the MODAL top-domain of its
dashboard_scopes assets (via node_domain_id; ties by domain id), emitted origin=proposed (never
applied); empty scope ⇒ ungrouped. Skip if it complicates the applied slice. NO tag written.

HARD GUARDRAILS: additive — NO frontend, NO backend/route, NO new table/column (blob-only,
MV-D49), NO governed-tag WRITE (read-only, MV-D26), NO new dependency (REST fallback keeps the
SDK pin). Unset dashboard_scopes ⇒ byte-identical graph for every existing caller/test.
Degrade-not-hang on Beta/permission failure (MV-D43). Deterministic (MV-D82).

ACCEPTANCE (offline): wheel unit tests — a dashboards TagAssignment{tag_key=<domain>, tag_value}
maps through assemble_tag_graph so the dashboard:<id> node lands in that Domain/sub-domain with
origin=applied; build_signal_graph with dashboard_scopes emits dashboard:<id> nodes +
dashboard_scope edges and is byte-identical WITHOUT it; layout labels dashboards with the
"reads" verb + name; an untagged dashboard is ungrouped (or origin=proposed if Build C shipped);
the reader returns [] when list_tag_assignments raises. ./scripts/test.sh + wheel suite green.
Additive-only diff; package-lock untouched.

WORKFLOW: branch `ontology`. Do NOT deploy or run the job. When offline-green, STOP and report
the diff + test summary; a human runs the §8 deploy-verify gate.

---

## After the run (human-gated)
`SKIP_FRONTEND_BUILD=1 ./scripts/deploy.sh --update` (fevm-serverless), trigger the materialize
job scoped to `["serverless_stable_6t92c3_catalog"]`. Eyeball `genie_ont_graph_snapshot.assets`:
`dashboard` node count ≈ 30; each **tagged** dashboard sits in its governed-tag domain
(`origin=applied`, solid) with `dashboard_scope` read edges; untagged ⇒ ungrouped or dashed
`proposed`. This closes the Typed Estate Assets build.
