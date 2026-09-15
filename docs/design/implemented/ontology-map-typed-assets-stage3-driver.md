# Ontology Map — Typed Estate Assets · Stage 3 Goal-Mode driver

Copy-paste launcher for a long-running agent. Run on the **`ontology`** branch **after Stage 2
landed**. Stage 3 puts **Lakeview Dashboards** on the map: each dashboard becomes a `dashboard`
node, **placed by its APPLIED governed tag** (read from the `entity-tag-assignments` API) and
edged to the tables/MVs its datasets query. **Wheel + reader. Additive. Offline + green tests.
STOPs before deploy.**

- **Spec:** `docs/design/ontology-map-typed-assets-build.md` §2.5, §5, §6, §7 (MV-D92).
  **Decisions:** `mv-advisor-playbook.md` — MV-D92; honor MV-D26/D43/D49/D82.
- **Seams (mirror Stage 2's as-built symbols, commit `2841fe99`):**
  `jobs/run_ontology_materialize.py` (extend `entity_tag_assignments(entity_type)` to the
  `dashboards` entity via `w.lakeview.list()` + per-dashboard
  `list_tag_assignments("dashboards", id)`, filtered by `transforms.is_domain_entity_tag`; add
  `dashboard_scopes` from dataset lineage); `ontology/materialize.py` — **reuse**
  `agent_domain_placement(entity_rows, proposals)` (generic on `member_id`) via
  `_gather_entity_tags(reader, "dashboards")`, merging `{**asset_domain, **agent_domain,
  **dashboard_domain}` into the layout node→domain map — **not** `assemble_tag_graph` (the cluster
  drops tag-only members, MV-D52); `ontology/graph.build_signal_graph` (OPTIONAL
  `dashboard_scopes`/`dashboard_names` kwargs mirroring `agent_scopes`/`agent_names` —
  `dashboard:<id>` nodes + `dashboard_scope` edges); `ontology/layout.py` (`_label_for_node`
  dashboard branch, `_verb_of` `dashboard_scope`→"reads", §6 fallback). Renderer FROZEN.

---

## Driver prompt (paste verbatim)

GOAL: Ontology Map Stage 3 — put Lakeview Dashboards on the map. Each dashboard becomes a
dashboard node PLACED BY ITS APPLIED GOVERNED TAG (entity-tag-assignments API, not
information_schema) and edged to the tables/MVs its datasets query. Wheel+reader, additive,
offline; STOP before deploy.

SPEC: docs/design/ontology-map-typed-assets-build.md §2.5/§5/§6/§7. DECISIONS:
mv-advisor-playbook.md MV-D92; honor MV-D26/D43/D49/D82. Read first. Stages 1 + 2 are landed —
mirror the Stage-2 wiring.

CONTEXT: 30 Lakeview dashboards exist; no dashboard reader/kwarg/kind exists yet. Governed tags
DO apply to dashboards but the assignment lives behind the
entity-tag-assignments API (entity_type "dashboards"), NOT information_schema. MIRROR Stage 2's
as-built wiring (the geniespaces entity_tag_assignments reader, is_domain_entity_tag,
agent_domain_placement, the agent_scopes/agent_names kwargs) — Stage 2 placed agents by a
POST-CLUSTER ATTACH, NOT assemble_tag_graph (MV-D52). Frontend already types dashboard — DO NOT
touch it.

BUILD A — DOMAIN (primary), reader entity_tag_assignments("dashboards"): enumerate via
w.lakeview.list(); per dashboard call list_tag_assignments("dashboards", id) (SDK, or REST
w.api_client.do("GET", f"/api/2.0/entity-tag-assignments/dashboards/{id}/tags") if the pin lacks
it — NO dep bump). Return {tag_name, tag_value, member_id: f"dashboard:{id}"} (same shape as
geniespaces). Keep a tag_key ONLY if transforms.is_domain_entity_tag(...) (Stage-2 guard: drops
certified/contains_synthetic + system.*/class.*/sap.* tags); do NOT use
system.tags.governed_tags. PLACEMENT — do NOT rebuild assemble_tag_graph (the
cluster engine drops tag-only members, MV-D52): REUSE
materialize.agent_domain_placement(entity_rows, proposals) AS-IS — generic on member_id, so
dashboard:<id> works unchanged (matches each tag to its reuse/reassign proposal → domain_id;
scope-reconcile out-of-scope⇒ungrouped + slash→sub / value-less→top nesting are internal). In
materialize call _gather_entity_tags(reader, "dashboards") → agent_domain_placement(...) and merge
{**asset_domain, **agent_domain, **dashboard_domain}. Degrade to [] on failure (MV-D43).
Sorted/deterministic.

BUILD B — READ EDGES (secondary): add an OPTIONAL dashboard_scopes kwarg to build_signal_graph
mirroring agent_scopes (emit dashboard:<id> nodes + dashboard_scope edges; UNSET ⇒
byte-identical graph, MV-D43). Reader dashboard_scopes(allowlist) -> {id->sorted[fqn]} from
dataset lineage (degrade to {}); materialize passes it (mirror agent_scopes/agent_names). In
layout add a _label_for_node dashboard branch (name) + _verb_of dashboard_scope -> "reads".

BUILD C — FALLBACK (optional, §6): an UNTAGGED dashboard may borrow the MODAL top-domain of its
dashboard_scopes assets (origin=proposed; empty scope ⇒ ungrouped). NO tag written.

HARD GUARDRAILS: additive — NO frontend, NO backend/route, NO new table/column (blob-only,
MV-D49), NO governed-tag WRITE (read-only, MV-D26), NO new dependency (REST fallback keeps the
SDK pin). Unset dashboard_scopes ⇒ byte-identical graph for every existing caller/test.
Degrade-not-hang on permission failure (MV-D43). Deterministic (MV-D82).

ACCEPTANCE (offline): wheel unit tests — a dashboards entity-tag row whose key = a reuse/reassign
proposal's governed tag places dashboard:<id> in that Domain/sub-domain via agent_domain_placement
(reused) ⇒ origin=applied; an out-of-scope-domain tag ⇒ ungrouped; build_signal_graph with
dashboard_scopes emits dashboard:<id> + dashboard_scope edges, byte-identical WITHOUT it; layout
labels dashboards with the "reads" verb + name; the reader returns [] when list_tag_assignments
raises. ./scripts/test.sh + wheel suite green. Additive-only diff; package-lock untouched.

WORKFLOW: branch `ontology`. Do NOT deploy or run the job. When offline-green, STOP and report
the diff + test summary; a human runs the §8 deploy-verify gate.

---

## After the run (human-gated)
`SKIP_FRONTEND_BUILD=1 ./scripts/deploy.sh --update` (fevm-serverless), trigger the materialize
job scoped to `["serverless_stable_6t92c3_catalog"]`. Eyeball `genie_ont_graph_snapshot.assets`:
`dashboard` node count ≈ 30; each **tagged** dashboard sits in its governed-tag domain
(`origin=applied`, solid) with `dashboard_scope` read edges; untagged ⇒ ungrouped or dashed
`proposed`. This closes the Typed Estate Assets build.
