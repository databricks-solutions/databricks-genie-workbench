# Ontology Map — Typed Estate Assets · Stage 2 Goal-Mode driver

Copy-paste launcher for a long-running agent. Run on the **`ontology`** branch **after Stage 1
landed**. Stage 2 puts **Genie Agents** on the map: each Genie space becomes an `agent` node,
**placed by its APPLIED governed tag** (read from the `entity-tag-assignments` API, NOT
`information_schema`) and edged to the tables/MVs it reads. **Wheel + reader. Additive. Offline
+ green tests. STOPs before deploy.**

- **Spec:** `docs/design/ontology-map-typed-assets-build.md` §2.5, §4, §6, §7 (MV-D91/D92).
  **Decisions:** `mv-advisor-playbook.md` — MV-D91, MV-D92; honor MV-D26/D43/D49/D82.
- **Before you build (human, §2.5 probe):** the reader is correct + safe to build regardless,
  but a human should run the one-space/one-dashboard probe to confirm the domain tag lands on
  the `entity-tag-assignments` surface and tune the domain tag-key filter.
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

CONTEXT: Governed tags DO apply to Genie spaces, but the assignment lives behind GET
/api/2.0/entity-tag-assignments/{entity_type}/{entity_id}/tags (SDK
w.workspace_entity_tag_assignments.list_tag_assignments; entity_type "geniespaces"), NOT
system.information_schema.*_tags — which is why the materializer never saw it. reader.agents()
lists 17 spaces (list_spaces, each has id) but is taxonomy-count only. build_signal_graph
already accepts agent_scopes={id->[fqn]} and emits agent:<id> nodes + agent_scope edges, but
materialize never passes it. Frontend already types agent — DO NOT touch frontend.

BUILD A — DOMAIN (primary), reader entity_tag_assignments("geniespaces"): enumerate spaces via
list_spaces(w); per space call w.workspace_entity_tag_assignments.list_tag_assignments(
"geniespaces", id). If the pinned databricks-sdk==0.117.0 lacks the method, call REST directly
via w.api_client.do("GET", f"/api/2.0/entity-tag-assignments/geniespaces/{id}/tags") — NO
dependency bump. Return rows shaped like assignments(): {tag_name, tag_value, member_id:
f"agent:{id}"}; filter to the domain governed-tag key(s) tables use. Degrade to [] on any
API/permission failure (MV-D43). Sorted/deterministic. In materialize, feed these rows into the
SAME transforms.assemble_tag_graph path table assignments use so a tagged agent lands in its
Domain/sub-domain with origin=applied.

BUILD B — READ EDGES (secondary), reader agent_scopes(allowlist) -> {id->sorted[fqn]} reusing
the create/scan space->tables resolution (degrade to {}); in materialize pass
agent_scopes=reader.agent_scopes(allowlist) into build_signal_graph (emits agent:<id> +
agent_scope edges — the relational overlay, NOT the domain mechanism). Ensure
layout._label_for_node labels an agent with the space display name (thread name onto the node
payload if missing — additive).

BUILD C — FALLBACK (optional, §6): an UNTAGGED agent may borrow the MODAL top-domain of its
agent_scopes assets (via node_domain_id; ties by domain id), emitted origin=proposed (never
applied). Empty scope ⇒ ungrouped. If this complicates the applied slice, skip it — ungrouped
is honest. NO tag written.

HARD GUARDRAILS: additive — NO frontend, NO backend/route, NO new table/column (blob-only,
MV-D49), NO governed-tag WRITE (read-only, MV-D26), NO new dependency (REST fallback keeps the
SDK pin). Unset/empty agent_scopes ⇒ byte-identical graph. Degrade-not-hang on Beta/permission
failure (MV-D43). Deterministic (MV-D82).

ACCEPTANCE (offline): wheel unit tests — a geniespaces TagAssignment{tag_key=<domain>,
tag_value} maps through assemble_tag_graph so the agent:<id> node lands in that
Domain/sub-domain with origin=applied; build_signal_graph with agent_scopes emits agent:<id>
nodes + agent_scope edges and is byte-identical without them; layout labels agents with the
space name; an untagged agent is ungrouped (or origin=proposed if Build C shipped); the reader
returns [] when list_tag_assignments raises. ./scripts/test.sh + wheel suite green.
Additive-only diff; package-lock untouched.

WORKFLOW: branch `ontology`. Do NOT deploy or run the job. When offline-green, STOP and report
the diff + test summary (incl. any §2.5 probe rows you were given); a human runs deploy-verify.

---

## Before the run (human-gated §2.5 probe — read-only, no writes)
Run as the job's `run_as` identity (MV-D50): pick one space id (`list_spaces`) + one dashboard
id (`w.lakeview.list()`), call `list_tag_assignments("geniespaces"/"dashboards", id)` (SDK or
REST `api_client.do`), and confirm the returned `tag_key`/`tag_value` carry the domain governed
tag. Paste the rows into the run notes so Build A's tag-key filter matches reality. If the API
is unavailable (Beta off / missing `tags` scope), Build A still ships (degrades to `[]`) and
placement falls to the §6 `proposed` fallback until the grant lands.

## After the run (human-gated)
`SKIP_FRONTEND_BUILD=1 ./scripts/deploy.sh --update` (fevm-serverless), trigger the materialize
job scoped to `["serverless_stable_6t92c3_catalog"]`. Eyeball `genie_ont_graph_snapshot.assets`:
`agent` node count ≈ 17; each **tagged** space sits in its governed-tag domain (`origin=applied`,
solid) with `agent_scope` read edges; untagged ⇒ ungrouped or dashed `proposed`. Then Stage 3.
