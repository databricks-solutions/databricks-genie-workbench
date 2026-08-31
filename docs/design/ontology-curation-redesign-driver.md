# Ontology — Curation Redesign · Stage 1 Goal-Mode driver

Copy-paste launcher for **Stage 1 of the curation redesign** (signals-first grouping
— make Domains business areas, not governed tags) with a long-running agent (Claude
Code / Cursor Goal Mode). Run it on the **`ontology`** branch, atop the shipped
17d/17e/re-grain/17f/17g spine. The prompt is bounded to the **offline-verifiable**
wheel + job-reader slice; it stops before deploy.

- **Spec (source of truth):** `docs/design/ontology-curation-redesign-build.md`
  (build §5 + §11–§14; §Appendix A is the live signal inventory that justifies it)
- **Decisions register:** `docs/design/mv-advisor-playbook.md`
  (MV-D51 / D52 / D53 / D60; honor MV-D43 / D45 / D49 / D50)
- **Design context:** `docs/design/ontology-engine-architecture.md` §3–§6
- **Baseline (do NOT regress):** 17g + 17f + re-grain + 3b + 3a specs
- **Visual contract:** none — Stage 1 is wheel + job reader only; no frame changes
- **Project rules:** `AGENTS.md`

Acceptance is **offline for the code, deploy-gated for verification** — the agent
builds the engine + tests green, then STOPS; a human runs `./scripts/deploy.sh
--update`, clicks **Refresh ontology**, and confirms the airline business domains
(Revenue, Maintenance, Loyalty, Reservation, Route, Fleet, Passenger) now surface
from FK/MV/naming signals and facet tags no longer appear as Domains.

---

## Driver prompt (paste verbatim)

```text
GOAL: Build the OFFLINE Stage 1 of the ontology curation redesign — signals-first
grouping. Make Domains BUSINESS AREAS discovered from strong signals, NOT governed
tags. Branch: ontology, atop shipped 17d/17e/re-grain/17f/17g. Wheel + job reader
ONLY; NO backend/route/TS change, NO new settings column, NO new dependency.

SPEC (follow §5 + §11-§14): docs/design/ontology-curation-redesign-build.md
DECISIONS: mv-advisor-playbook.md (MV-D51/52/53/60; honor MV-D43/45/49/50)
DESIGN: ontology-engine-architecture.md §3-§6
BASELINE (no regress): 17g + 17f + re-grain + 3b + 3a
RULES: AGENTS.md (read first).

REUSE, DON'T FORK:
  - ontology/graph.py build_signal_graph — ADD edge kinds join_key (FK/PK + shared
    join-column proxy), mv_membership, schema_affinity; keep existing kinds and the
    (graph, lineage_edges)-only call BYTE-IDENTICAL (new inputs are opt-in kwargs).
  - jobs/run_ontology_materialize.py — read FK/PK from information_schema
    (referential_constraints + key_column_usage + constraint_column_usage), shared
    join-columns from information_schema.columns, MV membership from the EXISTING
    estate_metric_view_yamls reader (17f), schema/name stems from information_schema.
    Scope by the MV-D42 catalog allowlist; read as OBO/run_as (MV-D50); degrade to
    empty on any missing grant (MV-D43).
  - ontology/transforms.py — facet-vs-aboutness classifier: a shipped default facet
    pattern list (constants HERE; Stage 3 lifts to settings) + cardinality/enum
    backstops; optional INJECTED LLM tiebreaker that DEGRADES. Stamp a plain reason
    per grouping into evidence.
  - ontology/cluster.py — rules-first grouping in precedence: curated domain tag ->
    FK-connected component -> MV membership -> shared schema; Leiden (leidenalg,
    RETAINED) runs ONLY on the unresolved remainder. Demote tag_assignment 5.0 ->
    ~lineage/2; a tag NEVER solo-creates a domain.
  - ontology/er.py — map-not-merge across contexts (same-as/role-of/related);
    canonical-id scheme UNCHANGED.

HARD GUARDRAILS:
  - Add NO new API model/route/frontend/TS; NO new settings column (defaults are
    in-code constants); all response contracts byte-identical.
  - NO SET/UNSET/CREATE GOVERNED TAG, NO manage_uc_tags, NO web_search — anywhere.
  - Metastore grain (MV-D49) unchanged; reads OBO-default (MV-D50); degrade-not-hang
    (MV-D43); NO new dependency (MV-D45) — uv.lock UNTOUCHED, uv lock --check green.
  - Deterministic + offline wheel logic; fixed Leiden seed. Do NOT pull forward
    Stage 2 (sub-domain boundaries), Stage 3 (gates/config/why), Stage 4 (Pages),
    the §9 alignment, or the §10 eval harness.
  - NO DEPLOY (no deploy.sh / bundle deploy / uvicorn / npm dev / live job run).

ACCEPTANCE (all true before done): ./scripts/test.sh green covering — FK/PK +
shared-join extraction from a fixture information_schema; join_key layer populated;
facet classifier routes the seed denylist (contains_synthetic/data_tier/
certification/controlled_placeholder/governance/demo*/reference/'domain'/*_team) to
FACET and keeps Revenue/Maintenance ABOUTNESS; rules-first produces FK-connected +
schema-named domains with NO tag present; tag-only input no longer makes a
single-asset domain; Leiden runs ONLY on the remainder; evidence carries a plain
per-assignment reason; map-not-merge keeps two contexts distinct; contract-frozen
(routes + OntologyRefreshStatus / taxonomy / tag-lens byte-identical); firewall
unchanged. npm run lint + tsc clean; uv lock --check green; §5.5 reads true.

WORKFLOW: graph.py (edge kinds) -> run_ontology_materialize.py (information_schema +
  MV YAML readers) -> transforms.py (facet classifier + reasons) -> cluster.py
  (rules-first + tag demotion + Leiden remainder) -> er.py (map-not-merge) -> tests.
  Run ./scripts/test.sh after each slice. STOP and ask if a spec detail is ambiguous
  or a guardrail would be crossed.
```

---

## After the run (human-gated — the agent must not do these)

```bash
git diff --stat            # expect: packages/.../ontology/{graph,transforms,cluster,er}.py,
                           #   packages/.../jobs/run_ontology_materialize.py, packages/.../tests/unit/**
./scripts/test.sh          # re-confirm green
cd frontend && npm run lint && npm run build && cd ..
uv lock --check            # UNCHANGED — no dependency touched (MV-D45)

git add packages/genie-space-optimizer
git commit -m "feat(ontology): Stage 1 signals-first grouping — FK/MV/naming signals, facet split, rules-first (MV-D51/52/53/60)"
git push -u origin ontology
```

Then **you** run the deploy-and-verify gate:

```bash
./scripts/deploy.sh --update   # rebuilds the wheel + redeploys the job (no dep change)
# In the live app: Settings → confirm the airline catalog allowlist is set, then
#   Ontology → Refresh ontology. When the materialize job finishes, confirm:
#   1. Facet/demo tags (Contains Synthetic, Data Tier, Certification, 'Domain') are
#      NO LONGER Domains.
#   2. Airline business domains (Revenue, Maintenance, Loyalty, Reservation, Route,
#      Fleet, Passenger) surface, grouped by FK-connected component / metric view /
#      shared schema — each with a plain reason in its evidence.
#   3. No single-asset tag "domains" remain.
# Query to eyeball:
#   SELECT name, tag_decision, round(score,2) s FROM <gso_catalog>.genie_space_optimizer.genie_ont_domains
#   WHERE parent_id IS NULL ORDER BY s DESC;
```

Stage 1 is offline and deterministic, but the `information_schema` reads, the MV-YAML
membership, the Delta write, and the synced mirror can only be validated in a deployed
app — which is why the offline slice stops here. Stage 2 (sub-domain boundaries)
builds on this grouping next.
