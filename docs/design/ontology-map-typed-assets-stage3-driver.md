# Ontology Map — Typed Estate Assets · Stage 3 Goal-Mode driver

Copy-paste launcher for a long-running agent. Run on the **`ontology`** branch **after Stage 2
landed**. Stage 3 puts **Lakeview Dashboards** on the map: each dashboard becomes a `dashboard`
node edged to the tables/MVs its datasets query, placed in the domain it most draws from.
**Wheel + reader. Additive. Offline + green tests. STOPs before deploy.**

- **Spec:** `docs/design/ontology-map-typed-assets-build.md` §5, §6, §7 (MV-D92).
  **Decisions:** `mv-advisor-playbook.md` — MV-D92; honor MV-D26/D43/D49/D82.
- **Seams:** `jobs/run_ontology_materialize.py` (reader — add `dashboard_scopes` from dataset
  lineage); `ontology/graph.build_signal_graph` (add an OPTIONAL `dashboard_scopes` kwarg
  mirroring `agent_scopes` — emit `dashboard:<id>` nodes + `dashboard_scope` edges);
  `ontology/materialize.py` (pass it through); `ontology/layout.py` (`_label_for_node`
  dashboard branch, `_verb_of` `dashboard_scope`→"reads", §6 placement). Renderer FROZEN.

---

## Driver prompt (paste verbatim)

GOAL: Ontology Map Stage 3 — put Lakeview Dashboards on the map. Each dashboard becomes a
dashboard node edged to the tables/MVs its datasets query, placed in the domain it most draws
from. Wheel+reader, additive, offline; STOP before deploy.

SPEC: docs/design/ontology-map-typed-assets-build.md §5/§6/§7. DECISIONS: mv-advisor-playbook.md
MV-D92; honor MV-D26/D43/D49/D82. Read first. Stages 1 (typed assets) + 2 (agents) are landed;
mirror the agent_scopes wiring.

CONTEXT: There is NO dashboard reader, NO dashboard_scopes kwarg, and NO dashboard node kind in
the wheel today. 30 Lakeview dashboards exist. Dashboards are NOT UC relations and cannot carry
UC table tags, so they get their domain by DERIVED LINEAGE, never a tag write (MV-D92).
build_signal_graph already has an agent_scopes kwarg emitting agent:<id> nodes + agent_scope
edges — mirror that exactly for dashboards. Frontend already types dashboard — DO NOT touch it.

BUILD A (reader in run_ontology_materialize): add dashboard_scopes(allowlist) ->
{dashboard_id -> sorted [table_fqn,...]} = the tables/MVs each dashboard's datasets query,
from dataset lineage (system.access.table_lineage filtered to the dashboard's queries, or the
serialized dashboard datasets). Degrade to {} on failure (MV-D43). Deterministic (sorted).

BUILD B (graph.build_signal_graph): add an OPTIONAL dashboard_scopes kwarg mirroring
agent_scopes: for each dashboard emit a dashboard:<id> node and a dashboard_scope edge to each
read asset. UNSET kwarg ⇒ byte-identical graph (MV-D43). Then in materialize pass
dashboard_scopes=reader.dashboard_scopes(allowlist).

BUILD C (layout.py): add a dashboard branch to _label_for_node (label = dashboard display
name; thread onto node payload — additive) and a _verb_of branch dashboard_scope -> "reads".
Apply §6 DOMAIN PLACEMENT identically to agents: a dashboard inherits the MODAL top-domain of
its scoped assets (via node_domain_id → top domain; ties by domain id); empty/all-untagged
scope ⇒ UNGROUPED, never guessed. Placement only; NO tag proposed or written.

HARD GUARDRAILS: additive — NO frontend, NO backend/route, NO new table/column (blob-only,
MV-D49), NO governed-tag write (MV-D26), NO new dependency. Unset dashboard_scopes ⇒
byte-identical graph for every existing caller/test. Read-only. Degrade-not-hang (MV-D43).
Deterministic (MV-D82).

ACCEPTANCE (offline): wheel unit tests — build_signal_graph with dashboard_scopes emits
dashboard:<id> nodes + dashboard_scope edges; WITHOUT it the graph is byte-identical to today;
layout emits them as dashboard display nodes with a name label and the "reads" verb; a
dashboard whose scope is one domain lands there, a tie resolves by domain id, an empty-scope
dashboard is ungrouped. ./scripts/test.sh + wheel suite green. Additive-only diff; package-lock
untouched.

WORKFLOW: branch `ontology`. Do NOT deploy or run the job. When offline-green, STOP and report
the diff + test summary; a human runs the §8 deploy-verify gate.

---

## After the run (human-gated)
`SKIP_FRONTEND_BUILD=1 ./scripts/deploy.sh --update` (fevm-serverless), trigger the materialize
job scoped to `["serverless_stable_6t92c3_catalog"]`. Eyeball `genie_ont_graph_snapshot.assets`:
`dashboard` node count ≈ 30, each edged (`dashboard_scope`) to its datasets' tables and sitting
inside a domain (or ungrouped). This closes the Typed Estate Assets build.
