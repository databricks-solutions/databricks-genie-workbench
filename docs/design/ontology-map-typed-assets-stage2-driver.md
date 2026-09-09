# Ontology Map — Typed Estate Assets · Stage 2 Goal-Mode driver

Copy-paste launcher for a long-running agent. Run on the **`ontology`** branch **after Stage 1
landed**. Stage 2 puts **Genie Agents** on the map: each Genie space becomes an `agent` node,
**placed by its APPLIED governed tag** (read from the `entity-tag-assignments` API, NOT
`information_schema`) and edged to the tables/MVs it reads. **Wheel + reader. Additive. Offline
+ green tests. STOPs before deploy.**

- **Spec:** `docs/design/ontology-map-typed-assets-build.md` §2.5, §4, §6, §7 (MV-D91/D92).
  **Decisions:** `mv-advisor-playbook.md` — MV-D91, MV-D92; honor MV-D26/D43/D49/D82.
- **§2.5 probe: CAPTURED** (see "probe verdict" below) — the domain tag lands on the
  `entity-tag-assignments` surface for `geniespaces`+`dashboards`; the captured `tag_key` list
  tunes the aboutness filter + in-snapshot reconcile. Only open gate: `run_as`-headless read.
- **Seams:** `jobs/run_ontology_materialize.py` (reader — add `entity_tag_assignments(...)`
  enumerating `list_spaces` + per-space `list_tag_assignments("geniespaces", id)`, and
  `agent_scopes`); `ontology/transforms.assemble_tag_graph` (map entity-tag rows like table
  members); `ontology/materialize.py` (feed entity-tag rows into the domain assembly; pass
  `agent_scopes=`); `ontology/graph.build_signal_graph` (already emits `agent:<id>` +
  `agent_scope`); `ontology/layout._label_for_node` (agent name). Renderer FROZEN.

---

## Driver prompt (paste verbatim)

GOAL: Ontology Map Stage 2 — put Genie Agents on the map. Each Genie space becomes an agent
node PLACED BY ITS APPLIED GOVERNED TAG (read from the entity-tag-assignments API, not
information_schema) and edged to the tables/MVs it reads. Wheel+reader, additive, offline; STOP
before deploy.

SPEC: docs/design/ontology-map-typed-assets-build.md §2.5/§4/§6/§7. DECISIONS:
mv-advisor-playbook.md MV-D91, MV-D92; honor MV-D26/D43/D49/D82. Read first. Stage 1 landed.

CONTEXT: Governed tags DO apply to Genie spaces, but the assignment lives behind the
entity-tag-assignments API (w.workspace_entity_tag_assignments.list_tag_assignments, entity_type
"geniespaces"), NOT information_schema — why the materializer never saw it. build_signal_graph
already accepts agent_scopes={id->[fqn]} (emits agent:<id> + agent_scope edges) but materialize
never passes it. Frontend already types agent — DO NOT touch it.

BUILD A — DOMAIN (primary), reader entity_tag_assignments("geniespaces"): enumerate spaces via
list_spaces(w); per space call w.workspace_entity_tag_assignments.list_tag_assignments(
"geniespaces", id). If the pinned databricks-sdk==0.117.0 lacks the method, call REST directly
via w.api_client.do("GET", f"/api/2.0/entity-tag-assignments/geniespaces/{id}/tags") — NO
dependency bump. Return rows shaped like assignments(): {tag_name, tag_value, member_id:
f"agent:{id}"}. Keep a tag_key ONLY if transforms.classify_tag(...) (MV-D51) = aboutness — drop
facets (certified, contains_synthetic) AND system.*/class.*/sap.*-prefixed system tags; do NOT
source the allowlist from system.tags.governed_tags (unreliable — empty despite governed tags
existing). SCOPE RECONCILE (required): keep an entity tag ONLY if its top-level domain key
already exists in THIS run's table-derived domain_meta (workspace tags are not catalog-scoped);
an out-of-scope domain ⇒ agent ungrouped, never fabricate a domain. The tag is usually the
VALUE-LESS TOP-LEVEL domain key ⇒ top-level placement; a slash key / mvm_subdomain=value nests a
sub-domain like a table. Degrade to [] on any
API/permission failure (MV-D43). Sorted/deterministic. In materialize, feed the kept rows into
the SAME transforms.assemble_tag_graph path tables use so a tagged agent lands in its domain with
origin=applied.

BUILD B — READ EDGES (secondary), reader agent_scopes(allowlist) -> {id->sorted[fqn]} reusing
the create/scan space->tables resolution (degrade to {}); in materialize pass
agent_scopes=reader.agent_scopes(allowlist) into build_signal_graph (emits agent:<id> +
agent_scope edges — the relational overlay, NOT the domain mechanism). Ensure
layout._label_for_node labels an agent with the space display name (thread name onto the node
payload if missing — additive).

BUILD C — FALLBACK (optional, §6): an UNTAGGED agent may borrow the MODAL top-domain of its
agent_scopes assets (origin=proposed; empty scope ⇒ ungrouped). Skip if it complicates the
applied slice. NO tag written.

HARD GUARDRAILS: additive — NO frontend, NO backend/route, NO new table/column (blob-only,
MV-D49), NO governed-tag WRITE (read-only, MV-D26), NO new dependency (REST fallback keeps the
SDK pin). Unset/empty agent_scopes ⇒ byte-identical graph. Degrade-not-hang on permission
failure (MV-D43). Deterministic (MV-D82).

ACCEPTANCE (offline): wheel unit tests — a geniespaces TagAssignment maps through
assemble_tag_graph so agent:<id> lands in its Domain/sub-domain origin=applied; an
out-of-scope-domain tag ⇒ ungrouped; build_signal_graph with agent_scopes emits agent:<id> +
agent_scope edges, byte-identical without them; layout labels agents with the space name; reader
returns [] when list_tag_assignments raises. ./scripts/test.sh + wheel suite green; additive-only
diff; package-lock untouched.

WORKFLOW: branch `ontology`. Do NOT deploy or run the job. When offline-green, STOP and report
the diff + test summary (incl. any §2.5 probe rows you were given); a human runs deploy-verify.

---

## §2.5 probe verdict (CAPTURED — `fevm-serverless`, read-only)
**Ran** against `fevm-serverless` (interactive; a `run_as`-headless re-run is the only remaining
gate — see risk below). The domain governed tag **does** land on the `entity-tag-assignments`
surface for both `geniespaces` and `dashboards`, confirming the applied path. Captured `tag_key`
shapes (feed the filter, do not hard-code):
- **In-scope aboutness (KEEP):** `Alaska Airlines Commercial`, `Alaska Airlines Operations`
  (value-less top-level keys) and value-carrying `Field Operations=fieldops`.
- **Slash sub-domain keys (KEEP, nest like a table):** `Operations1/maintenance`,
  `fuels_pricing/loyalty`.
- **Out-of-scope domains (DROP via in-snapshot reconcile):** `SupplyChain`, `Horizon M&E`,
  `fuels_pricing` — workspace tags are not catalog-scoped, so these appear even though they are
  not in the airline snapshot; attach only when the top-level domain exists in `domain_meta`.
- **System/facet tags (DROP):** `system.certification_status=certified`, `certified`,
  `contains_synthetic` — guard on the `system.*`/`class.*`/`sap.*` prefix + `classify_tag`.

**Remaining risk = access under `run_as` (MV-D50), not presence.** If the job identity lacks the
`tags` scope / entity read, Build A still ships (degrades to `[]`) and placement falls to the §6
`proposed` fallback until the grant lands — close this during Stage 1.

## After the run (human-gated)
`SKIP_FRONTEND_BUILD=1 ./scripts/deploy.sh --update` (fevm-serverless), trigger the materialize
job scoped to `["serverless_stable_6t92c3_catalog"]`. Eyeball `genie_ont_graph_snapshot.assets`:
`agent` node count ≈ 17; each **tagged** space sits in its governed-tag domain (`origin=applied`,
solid) with `agent_scope` read edges; untagged ⇒ ungrouped or dashed `proposed`. Then Stage 3.
