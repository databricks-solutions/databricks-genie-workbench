# Ontology — Phase 3a Goal-Mode driver

Copy-paste launcher for building **Phase 3a of the Ontology page** (signal-graph
completion + ER/dedupe engine) with a long-running agent (Claude Code / Cursor Goal
Mode). Run it on the **`ontology`** branch, on top of the shipped Phase-1 spine and
Phase-2 batch path. The prompt is bounded so the agent builds only the
**offline-verifiable** slice and stops at the **irreversible** Lakebase Search gate.

- **Spec (source of truth):** `docs/design/implemented/ontology-phase3a-build.md`
- **Phase-2 baseline (already shipped):** `docs/design/implemented/ontology-phase2-build.md`
- **Phase-1 baseline (already shipped):** `docs/design/implemented/ontology-phase1-build.md`
- **Design context:** `docs/design/ontology-engine-architecture.md` (§3–§4 signal
  graph, the **L2 graph builder** subsection, the **L3 ER/dedupe engine** subsection)
- **Decisions register:** `docs/design/mv-advisor-playbook.md` (Prompt 17d; MV-D35 /
  D37 / D39 / D40 / D45)
- **Visual contract:** **no new frames** — the Tags lens (`17.0c`) renders richer
  collisions through the **unchanged** `TagLens` contract; drafts are 17g
- **Project rules:** `AGENTS.md`

Like Phase 2, Phase-3a acceptance is **offline for the code** but **deploy-gated for
verification** — and the deploy gate here is special: enabling Lakebase Search on the
app's Autoscaling Lakebase is **beta and IRREVERSIBLE**. The agent builds the engine
behind the similarity interface (in-process cosine fallback), green-tests it offline,
then **stops**; a human enables Lakebase Search and runs the first live materialize.

---

## Driver prompt (paste verbatim)

```text
GOAL: Build the OFFLINE slice of Phase 3a — complete the fused signal graph, then the
ER/dedupe engine (block -> score -> adjudicate -> gate -> PII firewall) resolving
tags/measures/MVs/Agents into canonical entities + an identity map. Work only on the
current branch (ontology), atop the shipped Phase-1/2.

SPEC (follow §1-§12): docs/design/implemented/ontology-phase3a-build.md
BASELINE (do NOT regress): ontology-phase2-build.md + ontology-phase1-build.md
DESIGN: ontology-engine-architecture.md (§3-§4; L2 + L3 subsections)
DECISIONS: mv-advisor-playbook.md (Prompt 17d; MV-D35/D37/D39/D40/D45)
VISUAL: no new frames; Tags lens 17.0c stays on the UNCHANGED TagLens contract.
RULES: AGENTS.md (read first).

REUSE, DON'T FORK:
  - optimization/leakage.py — get_embedding (GTE) for embedding score; LeakageOracle
    extended to tag-name PII. mv_scoring.py — EmbeddingClient Protocol + GTE
    L2-normalize caveat; dedup_gate thresholds for the confidence gate.
  - llm_utils.call_serving_endpoint (+ model_catalog.validate_chat_model) — the ONLY
    LLM path, near-tie adjudication only.
  - ontology/ddl.py — Delta DDL + idempotent MERGE builder for genie_ont_identity.
  - ontology/materialize.py — L3 runs INSIDE it; NO new job task.
  - gso_lakebase.py + Phase-2 mirror.py — identity map rides the EXISTING mirror
    reader; do NOT invent a new read path.

HARD GUARDRAILS:
  - Add NO new API model, NO new route. Phase-1/2 contracts (TagLens, TagCollision,
    OntologyRefreshStatus) stay byte-identical — enrich collision CONTENT not shape.
    No TS changes.
  - Similarity behind ONE interface (similarity.py), two backends; DEFAULT in-process
    cosine + edit/BM25. Author LakebaseSearchBackend but do NOT enable Lakebase Search
    (beta, IRREVERSIBLE) and do NOT run it live. lakebase_vector/lakebase_text tokens
    ONLY in similarity.py.
  - WRITE only genie_ont_identity + genie_ont_tag_graph.dedupe_verdicts. NEVER write
    domains/members/pages/consents/suppressions (stay EMPTY — 17e/17f).
  - NO SET TAG, NO CREATE GOVERNED TAG, NO manage_uc_tags — anywhere.
  - Idempotent: derived canonical_id (fingerprint of sorted members) + MERGE incl.
    NOT MATCHED BY SOURCE DELETE scoped to workspace_id. Re-run MUST NOT duplicate.
  - Degrade, never block (MV-D43): if similarity/LLM is down, fall back / skip
    escalation and still write exact/string verdicts; the job does not fail.
  - Adjudicate ONLY the near-tie band; never LLM-score every pair.
  - Do NOT pull forward §12: no clustering (17e), no domain/page proposals, no
    /drafts route or frontend (17g), no external context/web_search, no SET TAG.
  - NO DEPLOY, NO ENABLE: no deploy.sh, no bundle deploy, no uvicorn/npm dev, no live
    job run, and above all NO enabling Lakebase Search. uv.lock UNTOUCHED (igraph is 17e).

ACCEPTANCE (all true before done):
  - ./scripts/test.sh green, incl: contract-frozen guard; blocking recall; string-vs-
    embedding catch (order_revenue~orders_revenue; "net revenue"~"revenue after
    discount"; headcount NOT~revenue); adjudication called ONLY on the near-tie band
    (mock call_serving_endpoint); backend-selection degrade PARITY (fake-Lakebase ==
    in-process on the fixture); PII firewall rejects a PII-echoing tag name; identity
    idempotency (stable canonical_id, no dups, NOT-MATCHED-BY-SOURCE delete); UPDATED
    firewall (lakebase_vector/lakebase_text ONLY in similarity.py, web_search still
    banned, proposal tables still never written).
  - cd frontend && npm run lint passes; tsc clean. §12 "Offline done" reads true.

WORKFLOW: graph.py -> similarity.py (interface + both backends) -> er.py
  (block/score/adjudicate/gate/PII) -> transforms.py identity-map assembly -> ddl.py
  (+genie_ont_identity) -> materialize.py wiring -> firewall/test updates -> backend
  dedupe.py mirror read (shape unchanged). Run ./scripts/test.sh after each slice.
  Stop and ask if a spec detail is ambiguous or a guardrail would be crossed.
```

---

## After the run (human-gated — the agent must not do these)

```bash
git diff --stat            # expect: packages/.../genie_space_optimizer/ontology/{similarity,er,graph,transforms,ddl,materialize}.py,
                           #   backend/ontology/services/dedupe.py, scripts/setup_synced_tables.py, scripts/deploy_lib/, backend/tests/
./scripts/test.sh          # re-confirm green
cd frontend && npm run lint && npm run build && cd ..

git add packages/genie-space-optimizer backend/ontology \
        scripts/setup_synced_tables.py scripts/deploy_lib backend/tests
git commit -m "feat(ontology): Phase 3a signal graph + ER/dedupe engine + identity map (MV-D40, in-process fallback)"
git push -u origin ontology
```

Then **you** run the deploy-and-verify gate (note the irreversible step):

```bash
# 1. IRREVERSIBLE, out-of-band, with the account team: enable Lakebase Search
#    (lakebase_vector + lakebase_text) on the app's Autoscaling Lakebase. Beta;
#    restarts computes. This is the Phase-3 gate — never scripted, never the agent.
# 2. Flip settings.lakebase_search_enabled = true.
./scripts/deploy.sh --update      # registers genie_ont_identity as a synced table, redeploys
# 3. In the live app: click "Refresh ontology" → confirm the ontology_materialize job
#    runs L3, the LakebaseSearchBackend returns ANN + BM25 neighbors, genie_ont_identity
#    populates Delta + the synced mirror, and the Tags lens collisions MATCH the offline
#    in-process verdicts (production parity). Re-run once to confirm idempotency.
```

The GTE endpoint, SP system-table reads, the Delta write, and the Lakebase Search
index can only be validated in a deployed app — and enabling Lakebase Search is
irreversible — which is why Phase 3a's offline slice stops here.
