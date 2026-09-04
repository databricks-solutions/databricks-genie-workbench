# Ontology — Phase 3b Goal-Mode driver

Copy-paste launcher for building **Phase 3b of the Ontology page** (domain /
sub-domain clustering) with a long-running agent (Claude Code / Cursor Goal Mode).
Run it on the **`ontology`** branch, on top of the shipped Phase-1/2/3a spine. The
prompt is bounded so the agent builds only the **offline-verifiable** slice and stops
before the live materialize.

- **Spec (source of truth):** `docs/design/implemented/ontology-phase3b-build.md`
- **Phase-3a baseline (already shipped):** `docs/design/implemented/ontology-phase3a-build.md`
- **Earlier baselines:** `docs/design/implemented/ontology-phase2-build.md` + `ontology-phase1-build.md`
- **Design context:** `docs/design/ontology-engine-architecture.md` (§5 — the **L4
  clustering engine** subsection)
- **Decisions register:** `docs/design/mv-advisor-playbook.md` (Prompt 17e; MV-D35 /
  D37 / D39 / D40 / D45)
- **Visual contract:** **no frontend** — the `17.0d` Domain draft is served in 17g;
  17e writes proposals to Delta/mirror only
- **Project rules:** `AGENTS.md` (+ the dependency policy — `leidenalg` + `igraph` are exact-pinned)

Like the prior phases, Phase-3b acceptance is **offline for the code** but
**deploy-gated for verification**. Unlike 17d there is **no UI change** — proposals
are verified by querying `genie_ont_domains`/`_members`, not the page. This is also
the **first** ontology phase to add dependencies (`leidenalg` + `igraph`), so
`uv.lock` changes.

---

## Driver prompt (paste verbatim)

```text
GOAL: Build the OFFLINE slice of Phase 3b — cluster 17d's canonical entities into a
two-level Domain -> Sub-Domain tree with reuse/create/reassign tag decisions, emitted
as PROPOSAL rows into genie_ont_domains + genie_ont_members (+ mirror). Branch:
ontology, atop shipped Phase-1/2/3a.

SPEC (§1-§12): ontology-phase3b-build.md. BASELINE (no regress): phase3a+2+1. DESIGN:
ontology-engine-architecture.md §5 (L4). DECISIONS: mv-advisor-playbook.md 17e
(MV-D35/D37/D39/D40/D45). RULES: AGENTS.md + dep policy. VISUAL: NONE (17.0d in 17g).

REUSE, DON'T FORK:
  - graph.py build_signal_graph — fused heterograph is the INPUT: project edge kinds into
    leidenalg multiplex LAYERS (one/kind), don't collapse/rebuild. Cluster over CANONICAL
    refs (genie_ont_identity).
  - materialize.py run_materialize builds+discards the graph; feed it to cluster.py, then
    MERGE domains+members via ddl.py build_snapshot_merge_sql — NO new DDL.
  - llm_utils.call_serving_endpoint — ONLY LLM path, NAMING only; lazy-import + degrade (17d).

HARD GUARDRAILS:
  - ALGORITHM: Leiden via leidenalg (NOT Louvain — guarantees CONNECTED communities). CPM:
    coarse gamma=Domains, finer subgraph gamma=Sub-Domains. Multiplex, never fused scalar.
  - Add NO API model/route/frontend/TS. Phase-1/2/3a contracts byte-identical;
    taxonomy/tags routes unchanged.
  - WRITE only genie_ont_domains + genie_ont_members (+ run-ledger domain_count); NEVER
    pages/consents/suppressions (stay EMPTY — 17f/17g).
  - NO SET/UNSET TAG, NO CREATE GOVERNED TAG, NO manage_uc_tags, NO web_search.
    tag_decision is a PROPOSAL FIELD ('reuse'|'create'|'reassign'), never a UC write.
  - DETERMINISTIC: fixed leidenalg seed (set_rng_seed) + fixed n_iterations/beta + stable
    tie-break; domain_id = fingerprint of SORTED canonical members. MERGE incl. NOT-MATCHED-
    BY-SOURCE DELETE (workspace-scoped). Re-run MUST yield identical domains/members.
  - SOFT-SEED: seed governed-tag nodes via leidenalg initial_membership (STRONG, SOFT prior
    — NOT is_membership_fixed; a node may move on strong graph disagreement); agent_scope=
    seed; cost NOT a layer (L6). Bind: seed held->reuse; no tag->create; seed broken past
    REASSIGN_MARGIN->reassign w/ conflict {existing_tag,moved_members,margin}, a PROPOSAL for
    17g — NEVER auto-switch; below margin->reuse. No dup tag.
  - Degrade, never block (MV-D43): naming-LLM down -> anchor names, run succeeds; clustering
    additive-LAST, must NOT corrupt earlier snapshots.
  - Do NOT pull forward §12: no Page miners (17f); no ranking/score (NULL/0.0); no /drafts,
    frontend, suppression FILTER, or reassign ADJUDICATION (17g); no Context Pack (P4); no
    SET TAG (P5).
  - Do NOT enable Lakebase Search; NO similarity code (lakebase_* only in similarity.py).
  - DEPENDENCY (§10): add leidenalg==<exact> + igraph==<exact> to GSO pyproject; uv lock;
    regen requirements.txt; commit all 3; uv lock --check green. NO other dep; don't edit uv.lock.
  - NO DEPLOY: no deploy.sh/bundle deploy/uvicorn/npm dev/live job run.

ACCEPTANCE (all true before done):
  - ./scripts/test.sh green (see §11): contract-frozen; two-level tree
    (Commercial->{Sales,Marketing}, orders=anchor); connectedness; multiplex + soft-seed
    (initial_membership, NOT fixed); reuse/create/reassign (conflict fixture ->
    reassign + conflict block + NO tag write; below margin -> reuse); determinism/idempotency
    (stable domain_id, no dups); naming-degrade + LeakageOracle; firewall (domains/members
    only; pages/consents/suppressions never; lakebase_* in similarity.py). uv lock --check green.
  - npm run lint + tsc clean. §12 "Offline done" true.

WORKFLOW: pyproject + uv lock (leidenalg+igraph) -> cluster.py (multiplex/soft-seeded
  Leiden-CPM/recursive-split/centrality/naming/bind) -> materialize.py (canonical map +
  MERGE domains/members) -> firewall/test updates. Run ./scripts/test.sh per slice; stop
  if ambiguous or a guardrail is crossed.
```

---

## After the run (human-gated — the agent must not do these)

```bash
git diff --stat            # expect: packages/.../ontology/{cluster,materialize,ddl}.py,
                           #   packages/genie-space-optimizer/pyproject.toml, uv.lock, requirements.txt,
                           #   backend/tests/test_ontology_firewall.py, packages/.../tests/unit/
./scripts/test.sh          # re-confirm green
cd frontend && npm run lint && npm run build && cd ..
uv lock --check            # leidenalg + igraph locked, lockfile consistent

git add packages/genie-space-optimizer uv.lock requirements.txt backend/tests
git commit -m "feat(ontology): Phase 3b domain/sub-domain clustering + proposals (MV-D39 Leiden/leidenalg)"
git push -u origin ontology
```

Then **you** run the deploy-and-verify gate:

```bash
./scripts/deploy.sh --update   # builds the wheel (now incl. leidenalg+igraph), redeploys the job
# In the live app: click "Refresh ontology" to run the materialize job, then query
#   SELECT * FROM <gso_catalog>.<gso_schema>.genie_ont_domains  (and _members)
# Confirm sensible Domain -> Sub-Domain proposals with reuse/create decisions and
# evidence. NOTE: there is NO UI change in 17e — the drafts render in 17g. Re-run once
# to confirm idempotency (identical domain_ids, no duplicate rows).
```

Clustering runs offline and deterministically, but the SP reads, the `leidenalg`/`igraph`
install on the serverless job, the Delta write, and the synced mirror can only be
validated in a deployed app — which is why Phase 3b's offline slice stops here.
