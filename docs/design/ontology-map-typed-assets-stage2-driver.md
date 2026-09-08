# Ontology Map — Typed Estate Assets · Stage 2 Goal-Mode driver

Copy-paste launcher for a long-running agent. Run on the **`ontology`** branch **after Stage 1
landed**. Stage 2 puts **Genie Agents** on the map: each Genie space becomes an `agent` node
edged to the tables/MVs it reads, placed in the domain it most draws from. **Wheel + reader.
Additive. Offline + green tests. STOPs before deploy.**

- **Spec:** `docs/design/ontology-map-typed-assets-build.md` §4, §6, §7 (MV-D91/D92).
  **Decisions:** `mv-advisor-playbook.md` — MV-D91, MV-D92; honor MV-D26/D43/D49/D82.
- **Seams:** `jobs/run_ontology_materialize.py` (reader — add `agent_scopes`, reuse the
  space→tables resolution the create/scan path already uses); `ontology/materialize.py`
  (pass `agent_scopes=` into `graph.build_signal_graph`); `ontology/graph.build_signal_graph`
  (already accepts `agent_scopes`, already emits `agent:<id>` + `agent_scope` edges);
  `ontology/layout.py` (`_label_for_node` agent branch + §6 domain placement). Renderer FROZEN.

---

## Driver prompt (paste verbatim)

GOAL: Ontology Map Stage 2 — put Genie Agents on the map. Each Genie space becomes an agent
node edged to the tables/MVs it reads, placed in the domain it most draws from. Wheel+reader,
additive, offline; STOP before deploy.

SPEC: docs/design/ontology-map-typed-assets-build.md §4/§6/§7. DECISIONS: mv-advisor-playbook.md
MV-D91, MV-D92; honor MV-D26/D43/D49/D82. Read first. Stage 1 (typed assets) is already landed.

CONTEXT: graph.build_signal_graph ALREADY accepts an agent_scopes={agent_id -> [fqn,...]} kwarg
and emits agent:<id> nodes + agent_scope edges — but materialize NEVER passes it, so agents
never render (the reader's agents() is taxonomy-count only). 17 Genie spaces exist. Genie
spaces are NOT UC relations and cannot carry UC table tags (governed_tags is empty), so they
get their domain by DERIVED LINEAGE, never a tag write (MV-D92). Frontend already types agent —
DO NOT touch frontend.

BUILD A (reader in run_ontology_materialize): add agent_scopes(allowlist) ->
{agent_id -> sorted [table_fqn,...]} = the tables/MVs each Genie space reads. Reuse the
existing space→tables resolution (the create/scan path already resolves a space's tables);
degrade to {} for a space that exposes none, and to {} overall on failure (MV-D43).
Deterministic (sorted).

BUILD B (materialize): pass agent_scopes=reader.agent_scopes(allowlist) into
graph.build_signal_graph(...) alongside the existing mv_membership/join_key/schema_affinity
args. Do not change any other arg.

BUILD C (layout.py): ensure _label_for_node labels an agent node with the space display name
(thread the name onto the node payload if missing — additive). Apply §6 DOMAIN PLACEMENT: an
agent inherits the MODAL top-domain of the assets in its scope (look up each scoped asset's
domain via node_domain_id, walk to the top domain, take the most common; ties broken by domain
id). An agent with an empty or all-untagged scope stays UNGROUPED (renders under Estate root) —
never guessed. Placement only; NO tag proposed or written.

HARD GUARDRAILS: additive — NO frontend, NO backend/route, NO new table/column (blob-only,
MV-D49), NO governed-tag write (MV-D26), NO new dependency. Unset/empty agent_scopes ⇒
byte-identical graph. Read-only. Degrade-not-hang (MV-D43). Deterministic (MV-D82).

ACCEPTANCE (offline): wheel unit tests — build_signal_graph with agent_scopes emits agent:<id>
nodes + agent_scope edges; layout emits them as agent display nodes carrying a name label; an
agent whose scope is all-tagged to one domain lands in that domain; a tie resolves
deterministically by domain id; an empty-scope agent is ungrouped. build_signal_graph WITHOUT
agent_scopes is byte-identical to today. ./scripts/test.sh + wheel suite green. Additive-only
diff; package-lock untouched.

WORKFLOW: branch `ontology`. Do NOT deploy or run the job. When offline-green, STOP and report
the diff + test summary; a human runs the §8 deploy-verify gate.

---

## After the run (human-gated)
`SKIP_FRONTEND_BUILD=1 ./scripts/deploy.sh --update` (fevm-serverless), trigger the materialize
job scoped to `["serverless_stable_6t92c3_catalog"]`. Eyeball `genie_ont_graph_snapshot.assets`:
`agent` node count ≈ 17, each edged (`agent_scope`) to its read tables and sitting inside a
domain (or ungrouped if its scope is untagged). Then proceed to Stage 3.
