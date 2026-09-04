# Ontology — Phase 3c Goal-Mode driver

Copy-paste launcher for building **Phase 3c of the Ontology page** (L5 Page miners)
with a long-running agent (Claude Code / Cursor Goal Mode). Run it on the **`ontology`**
branch, on top of the shipped Phase-1/2/3a/3b spine **and the metastore re-grain**
(`ontology-regrain-build.md`, MV-D49). The prompt is bounded so the agent builds only
the **offline-verifiable** slice and stops before the live materialize.

- **Spec (source of truth):** `docs/design/implemented/ontology-phase3c-build.md`
- **Re-grain prerequisite (must land first):** `docs/design/implemented/ontology-regrain-build.md`
- **Phase-3b baseline (already shipped):** `docs/design/implemented/ontology-phase3b-build.md`
- **Earlier baselines:** `ontology-phase3a-build.md` (the **17d identity map** 17f
  anchors on) + `phase2` + `phase1`
- **Design context:** `docs/design/ontology-engine-architecture.md` (§5 — the **L5
  Page-miners** subsection; §7 — the **metastore grain**) + `page-archetypes.md` +
  `genie-retrieval-notes.md`
- **Decisions register:** `docs/design/mv-advisor-playbook.md` (Prompt 17f; MV-D49 /
  D27 / D35 / D37 / D43 / D45)
- **Visual contract:** **no frontend** — the `17.0e` Page draft is served in 17g; 17f
  writes Page proposals to Delta/mirror only
- **Project rules:** `AGENTS.md`

Like the prior phases, Phase-3c acceptance is **offline for the code** but
**deploy-gated for verification**. As in 17e there is **no UI change** — Pages are
verified by querying `genie_ont_pages`, not the page. This phase adds **no dependency**
(MV-D45), so `uv.lock` is untouched.

---

## Driver prompt (paste verbatim)

```text
GOAL: OFFLINE slice of Phase 3c — mine PAGES per CANONICAL CONCEPT (17d genie_ont_identity), NOT per
artifact: resolve each signal to its canonical_id, AGGREGATE all artifacts across the
METASTORE (incl. across sub-domains), gate on CORROBORATION, draft archetype bodies, validate vs
retrieval gates, emit PROPOSAL rows into genie_ont_pages(+mirror). Branch: ontology.

SPEC(§1-§12): ontology-phase3c-build.md. PREREQ: ontology-regrain-build.md (MV-D49 MUST land first).
BASELINE: phase3b+3a+2+1. DESIGN: ontology-engine-architecture.md §5+§7, page-archetypes.md,
genie-retrieval-notes.md. DECISIONS: playbook 17f (MV-D49/D27/D35/D37/D43/D45). RULES: AGENTS.md.
VISUAL: NONE (17.0e in 17g).

REUSE, DON'T FORK:
  - genie_ont_identity (17d canonical_id) = the ANCHOR: group signals by concept, count corroboration;
    page_id concept-derived, NOT artifact/domain.
  - genie_ont_domains(parent_id NOT NULL)+members+graph.py = INPUT; don't re-cluster.
  - mv_fingerprint(CONFLICT)->[Disambiguation]; mv_scoring->[Routing]/[Guardrail]: reuse, NOT new comparator.
  - leakage.LeakageOracle EXTEND for Page bodies; er.pii_reject for PII; genie_client.run_genie_query =
    ask_genie routing validation (degrade); llm_utils.call_serving_endpoint = ONLY LLM path, body prose
    only, lazy+degrade.
  - materialize.py: mine AFTER cluster MERGE, MERGE pages via ddl.py. NO new DDL.

HARD GUARDRAILS:
  - GRAIN (MV-D49): genie_ont_pages keyed (metastore_id,page_id); MERGE delete metastore-scoped;
    workspace_id provenance NEVER a key.
  - CANONICAL CONCEPT: detectors per canonical_id; same-concept artifacts (even across sub-domains)
    collapse to ONE Page; Sources aggregate ALL.
  - CORROBORATION (MV-D35): >=2 indep artifacts=full+certify-eligible; 1=low-conf+certify=false (surface,
    never drop/auto-certify). [Disambiguation] >=2 by construction.
  - DETERMINISTIC DETECTORS, LLM PROSE ONLY: archetype/source_fqns/certify/page_id deterministic; LLM
    writes body prose only. page_id=pg_<fp(canonical_id,archetype,sorted canonical ids)>, NOT domain_id,
    NOT prose. MERGE metastore-scoped NOT-MATCHED-BY-SOURCE DELETE; re-run identical.
  - IDENTIFIER GATE: every backticked id + Source FQN must EXIST in members; invented->FAIL.
  - RETRIEVAL GATES (§6): synonyms>=3/4 classes (else low-conf+certify=false); chunk-safe; specificity
    (>=1 backticked id/formula/section); Sources+Related STRUCTURAL (not prose, MV-D27).
  - CONTRADICTION read-only vs text_instructions -> hit=downgrade to CONFLICT (17g), NEVER write back.
    Page dedupe best-effort name/synonym (no Page read API).
  - Add NO API model/route/frontend/TS; 1/2/3a/3b contracts byte-identical. WRITE only genie_ont_pages
    (+ledger page_count); NEVER consents/suppressions. NO SET/UNSET/CREATE GOVERNED TAG, NO manage_uc_tags,
    NO web_search, NO Agent-instruction write (MV-D27).
  - Degrade never block (MV-D43): LLM down->stub+certify=false; ask_genie down->unvalidated; mining
    additive-LAST, never corrupt earlier snapshots.
  - NO Lakebase Search/new similarity backend (lakebase_* only in similarity.py). NO NEW DEPENDENCY
    (MV-D45): uv.lock untouched. Do NOT pull forward §12 (advisories/ranking/serving=17g; Context Pack=P4;
    SET TAG=P5). NO DEPLOY.

ACCEPTANCE: ./scripts/test.sh green over ALL §11 cases — esp. canonical-concept keying (cross-sub-domain
collapse to one Page; page_id from canonical_id), corroboration gate, metastore grain (metastore_id key
+ scoped delete); + frozen contracts, gates, contradiction->CONFLICT, certify, degrade,
determinism, firewall, additive-safety. uv lock --check + npm lint + tsc clean; §12 done.

WORKFLOW: pages.py (canonical_id resolve -> aggregate+corroboration -> detectors -> LLM draft -> gates ->
  dedupe -> page_id) -> leakage.py (page-body scan) -> materialize.py (mine after cluster, MERGE pages
  metastore-scoped) -> ddl.py firewall set + setup_synced_tables.py + tests. ./scripts/test.sh per slice;
  stop if ambiguous.
```

---

## After the run (human-gated — the agent must not do these)

```bash
git diff --stat            # expect: packages/.../ontology/{pages,materialize,ddl,transforms}.py,
                           #   packages/.../optimization/leakage.py, scripts/setup_synced_tables.py,
                           #   backend/tests/test_ontology_firewall.py, packages/.../tests/unit/
./scripts/test.sh          # re-confirm green
cd frontend && npm run lint && npm run build && cd ..
uv lock --check            # UNCHANGED — no new dependency (MV-D45)

git add packages/genie-space-optimizer scripts backend/tests
git commit -m "feat(ontology): Phase 3c L5 Page miners -> canonical-concept, corroboration-gated, metastore-grain genie_ont_pages proposals (MV-D49/D27/D35)"
git push -u origin ontology
```

Then **you** run the deploy-and-verify gate:

```bash
./scripts/deploy.sh --update   # rebuilds the wheel + redeploys the job (no new dep)
# In the live app: click "Refresh ontology" to run the materialize job, then query
#   SELECT metastore_id, count(*) FROM <gso_catalog>.<gso_schema>.genie_ont_pages GROUP BY 1
# Confirm ONE Page per canonical concept at metastore grain (no per-artifact/per-workspace
# duplicates), each with copy-ready Related/Sources aggregating all corroborating artifacts,
# corroboration >=2 -> certify-eligible. NOTE: no UI change in 17f — drafts render in 17g.
# Re-run once to confirm idempotency (identical concept page_ids, no duplicate rows).
```

Page mining runs offline and deterministically, but the SP reads, the drafting LLM +
`run_genie_query`, the Delta write, and the synced mirror can only be validated in a
deployed app — which is why Phase 3c's offline slice stops here.
