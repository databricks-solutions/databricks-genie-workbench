# Ontology Map — Typed Estate Assets · Stage 1 Goal-Mode driver

Copy-paste launcher for a long-running agent. Run on the **`ontology`** branch. Stage 1 makes
the map show **typed display-assets, not tag plumbing**: exclude `tag`/`schema`/`mv` hubs from
`assets.nodes`, type tagged Metric Views as `metric_view` (so Measures expand), and dedupe the
MV double-emit. **Wheel-only. Additive. Offline + green tests. STOPs before deploy.**

- **Spec:** `docs/design/ontology-map-typed-assets-build.md` §2, §3, §7 (MV-D89/D90).
  **Decisions:** `mv-advisor-playbook.md` — MV-D89, MV-D90; honor MV-D26/D43/D49/D82.
- **Seams:** `ontology/layout.py` (`build_graph_snapshot` — the `nodes → asset_nodes` loop
  + the `TOP_N_BY_CENTRALITY` cap); `ontology/transforms.py` (`assemble_tag_graph` — the
  `asset_type="table"` member hardcode); `jobs/run_ontology_materialize.py` (reader — add an
  `information_schema.tables` type map, allowlist-scoped). Renderer is FROZEN (frontend
  `typeForKind`/`NodeType` already type `metric_view`).

---

## Driver prompt (paste verbatim)

GOAL: Ontology Map Stage 1 — the estate map must show TYPED display-assets, not tag plumbing.
(1) Exclude tag/schema/mv HUB nodes from assets.nodes; (2) type tagged Metric Views as
metric_view; (3) dedupe the MV double-emit so Measures expand. Wheel-only, additive, offline;
STOP before deploy.

SPEC: docs/design/ontology-map-typed-assets-build.md §2/§3/§7. DECISIONS:
mv-advisor-playbook.md MV-D89, MV-D90; honor MV-D26/D43/D49/D82. Read first.

CONTEXT: layout.build_graph_snapshot iterates signal_graph["nodes"] and emits EVERY node into
asset_nodes with its kind, so tag:/schema:/mv: hubs render as display assets and win the
TOP_N_BY_CENTRALITY=2000 cap by degree, crowding real assets to ~94. transforms.
assemble_tag_graph hardcodes member asset_type="table", so the 24 tagged Metric Views render
as tables and their Measures never appear. A tagged MV also appears as the structural mv:<fqn>
measure-hub → double-emit. Frontend typeForKind already maps metric_view/agent/dashboard — DO
NOT touch frontend.

BUILD A (layout.py): when building asset_nodes from nodes, include ONLY display kinds
{table, metric_view, agent, dashboard, measure}; skip tag:/schema:/mv: hubs. Keep those hubs
in the structural paths the function already uses (node_domain_id domain resolution,
mv_membership edges, _top_of_asset) — only the DISPLAY projection narrows. The cap then ranks
real assets. Deterministic.

BUILD B (transforms.assemble_tag_graph + run_ontology_materialize reader): replace the
asset_type="table" hardcode with a type from an OPTIONAL asset-type map arg
{fqn -> table_type}; table_type=='METRIC_VIEW' ⇒ metric_view, else table; absent map ⇒ table
(byte-identical to today). In the reader, add an allowlist-scoped
system.information_schema.tables query returning {fqn -> table_type} and thread it into
assemble_tag_graph. Degrade to {} on query failure (MV-D43).

BUILD C (layout.py dedupe): a tagged MV can emit as asset:<fqn> (now metric_view) AND as the
mv:<fqn> hub. Fold the hub into the typed metric_view node of the same FQN: keep exactly one
display node, and REATTACH the hub's mv_membership edges to that node id so Measures still
expand. Deterministic (sorted).

HARD GUARDRAILS: wheel-only — NO frontend, NO backend/route, NO new table/column (blob-only,
MV-D49), NO governed-tag write (MV-D26), NO new dependency. Absent asset-type map ⇒
byte-identical assemble_tag_graph output. Domain rollups + membership edges unchanged.
Degrade-not-hang (MV-D43). Deterministic (MV-D82).

ACCEPTANCE (offline): wheel unit tests — (i) assets.nodes contains NO tag/schema/mv kind;
(ii) a tagged MV emits exactly ONE metric_view node retaining its mv_membership edges;
(iii) assemble_tag_graph types a METRIC_VIEW member as metric_view, others as table, and is
byte-identical with the map absent; (iv) with >2000 display assets the cap keeps display
assets (no hubs). ./scripts/test.sh + wheel suite green. Additive-only diff; package-lock
untouched.

WORKFLOW: branch `ontology`. Do NOT deploy or run the job. When offline-green, STOP and report
the diff + test summary; a human runs the §8 deploy-verify gate.

---

## After the run (human-gated)
`SKIP_FRONTEND_BUILD=1 ./scripts/deploy.sh --update` (fevm-serverless), trigger the materialize
job scoped to `["serverless_stable_6t92c3_catalog"]`. Eyeball `genie_ont_graph_snapshot.assets`:
zero `tag`/`schema`/`mv` node kinds; `metric_view` count ≈ 24 (the tagged MVs); expanding an MV
in the UI reveals its Measures. Then proceed to Stage 2.
