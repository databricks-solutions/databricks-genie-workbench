# Ontology — Phase 3d Goal-Mode driver

Copy-paste launcher for building **Phase 3d of the Ontology page** (L6 rank & trust
gate + the FIRST serving of drafts) with a long-running agent (Claude Code / Cursor
Goal Mode). Run it on the **`ontology`** branch, on top of the shipped
Phase-1/2/3a/3b/3c spine **and the metastore re-grain** (`ontology-regrain-build.md`,
MV-D49). This phase adds the frontend, so acceptance runs **both** the Python suite
and the frontend vitest. It ends at the **STOP proposal-quality checkpoint** — the
agent must not proceed to 17h/17i.

- **Spec (source of truth):** `docs/design/ontology-phase3d-build.md`
- **Re-grain prerequisite (must land first):** `docs/design/ontology-regrain-build.md`
- **Baselines (already shipped):** `ontology-phase3c-build.md` (Pages) +
  `ontology-phase3b-build.md` (Domains + `reassign`/`conflict`) + `3a` + `2` + `1`
- **Design context:** `docs/design/ontology-engine-architecture.md` (§5 — the **L6
  rank & trust** + **L7/L8 serving** subsections) + the `17.0d` / `17.0e` mockups
- **Decisions register:** `docs/design/mv-advisor-playbook.md` (Prompt 17g; MV-D49 /
  D35 / D26 / D23 / D37 / D38 / D43 / D27)
- **Visual contract:** the `17.0d` (Domain draft) + `17.0e` (Page draft) frames — this
  is the FIRST ontology phase with a UI change since Phase 1
- **Project rules:** `AGENTS.md`

Phase-3d code acceptance is **offline** (pytest + vitest); the ranked scores, the
mirror read, and the OBO ledger write are **deploy-gated** for verification. This
phase adds **no dependency** (MV-D45), so `uv.lock` is untouched.

---

## Driver prompt (paste verbatim)

```text
GOAL: OFFLINE slice of Phase 3d — L6 RANK & TRUST gate + FIRST serving of drafts. (1) WHEEL: score every
Domain/Page proposal by usage x lineage-centrality x governance (MV-D35), firewall (PII on tag names, policy,
dormant provenance hook), read suppressions, persist score+surfaced. (2) BACKEND+FRONTEND: serve
ranked drafts (GET /drafts) + record human decisions (POST /decision) into the ledger; render 17.0d/17.0e
zero-burden cards. NO SET TAG, NO apply. Branch: ontology.

SPEC(§1-§12): ontology-phase3d-build.md. PREREQ: ontology-regrain-build.md (MV-D49 lands FIRST).
BASELINE: 3c(Pages)+3b(Domains+reassign)+3a+2+1. DESIGN: ontology-engine-architecture.md §5 (L6+L7/L8),
17.0d/17.0e mockups. DECISIONS: playbook 17g (MV-D49/D35/D26/D23/D37/D38/D43/D27). RULES: AGENTS.md.

REUSE, DON'T FORK:
  - SCORE = mv_scoring blend+thresholds+coverage cap; graph.py centrality + L2 usage/cost = inputs; pure,
    LLM-free. transforms.py = the ONE home for tier_of()+thresholds (wheel rank.py AND serve).
  - leakage.LeakageOracle EXTEND for tag NAMES (er.pii_reject shape); ONE oracle.
  - mirror.py = draft reads (read_domain_drafts/read_page_drafts, surfaced-only) gated like taxonomy;
    gso_lakebase warehouse = OBO ledger write.
  - materialize.py: rank additive-LAST after 17f Page MERGE; re-MERGE domains/pages with score+surfaced.

HARD GUARDRAILS:
  - GRAIN (MV-D49): score/serve/ledger keyed metastore_id; MERGE delete metastore-scoped; workspace_id
    provenance, NEVER a key.
  - MV-D35: score RANKS (demand/importance), NEVER "confidence"; card leads with EvidenceChips+tier, NEVER a
    rendered NN%. Cap: corroborated > single-signal.
  - FIREWALLS (ALL to surface): PII on tag_key/tag_value -> BLOCK; policy propose-only; provenance ladder =
    DORMANT no-op + a "T3 never outranks T0" test (17h owns it). Blocked=dropped+reported.
  - LEDGER (MV-D26): WHEEL READS genie_ont_suppressions (SELECT only) -> surfaced=false; wheel has NO
    MERGE/INSERT/UPDATE on the ledger + NO reference to consents. BACKEND (OBO) = ONLY writer: approve +
    reassign_accept -> consents; dismiss + reassign_reject -> suppressions. Idempotent on
    (metastore_id,kind,proposal_id); decided_by=OBO email; rejected reassign stays suppressed on re-run.
  - surfaced flag in evidence JSON (NO new column/DDL/table).
  - ROUTES: add ONLY drafts.py (GET /drafts + POST /decision), register in main.py. NO apply route, NO
    SET/UNSET/CREATE GOVERNED TAG, NO manage_uc_tags, NO web_search.
  - ZERO-BURDEN (MV-D23): cards prop-driven (backend assembles why/reason/chips); rendered copy has NONE of:
    SET TAG, MERGE, metastore_id, workspace_id, genie_ont_, system.tags, SQL warehouse, provenance tier,
    Lakebase, mirror, L6. Apply-for-me DISABLED (17i).
  - CONTRACTS: Phase-1/2/3a-c models byte-identical; new models append-only; types.ts mirrors 1:1.
  - MV-D43 degrade: mirror/warehouse fail -> empty OntologyDrafts(source="cold"), never 500. Rank additive-LAST,
    never corrupts snapshots. Do NOT pull forward 17h or 17i (apply). NO NEW DEP (MV-D45): uv.lock untouched.
    NO DEPLOY. STOP at checkpoint.

ACCEPTANCE: ./scripts/test.sh green over ALL §11 pytest cases (ranking+cap, PII/policy firewall, dormant
provenance seam, suppression idempotency incl. rejected reassign, decision route incl. OBO email + no-SET-TAG,
metastore grain, frozen contracts, router-verb allowlist POST only drafts.py+refresh.py, wheel reads-not-writes
ledger, degrade). cd frontend && npm run test green on the zero-burden render. uv lock --check + npm
lint+build+tsc; §12 done.

WORKFLOW: transforms.py -> rank.py (score+firewalls+mark_surfaced) -> leakage.py -> materialize.py (rank
  after Page MERGE, re-MERGE) -> models.py -> mirror.py (draft readers) -> decisions.py (OBO ledger MERGE) ->
  drafts.py (GET/POST)+main.py -> types.ts+api.ts + DomainDraftCard/PageDraftCard/DraftsView + OntologyPage
  drafts tab -> draftCards.test.tsx -> firewall test updates. Test per slice; stop if ambiguous.
```

---

## After the run (human-gated — the agent must not do these)

```bash
git diff --stat            # expect: packages/.../ontology/{rank,transforms,materialize}.py,
                           #   packages/.../optimization/leakage.py,
                           #   backend/ontology/{models.py,routers/drafts.py,services/{mirror,decisions}.py,main.py},
                           #   frontend/src/ontology/{api,types}.ts + components/{DomainDraftCard,PageDraftCard,DraftsView}.tsx
                           #   + OntologyPage.tsx + __tests__/draftCards.test.tsx,
                           #   backend/tests/test_ontology_firewall.py, packages/.../tests/unit/
./scripts/test.sh          # re-confirm green
cd frontend && npm run test && npm run lint && npm run build && cd ..
uv lock --check            # UNCHANGED — no new dependency (MV-D45)

git add packages/genie-space-optimizer backend frontend
git commit -m "feat(ontology): Phase 3d L6 rank & trust gate + serve drafts -> ranked/firewalled proposals, OBO consent/suppression ledger, 17.0d/e zero-burden cards (MV-D49/D35/D26/D23)"
git push -u origin ontology
```

Then **you** run the deploy-and-verify gate:

```bash
./scripts/deploy.sh --update   # rebuilds the wheel + redeploys the job + app (no new dep)
# In the live app: click "Refresh ontology" to run the materialize job, then open the
#   new Drafts tab. Confirm proposals are ranked HIGH -> LOW with evidence chips (no "NN%"),
#   17.0d shows new-vs-reuse-vs-reassign with the conflict tag + "why", Apply-for-me disabled,
#   17.0e leads with the reason + Synonyms + Related/Sources + certify.
# Dismiss a proposal, re-run Refresh, confirm it does NOT resurface (MV-D26). Reject a reassign,
#   confirm it stays suppressed. Verify NOTHING is written to Unity Catalog (no SET TAG).
```

This is the **STOP checkpoint**: review proposal quality (ranking, firewalls, zero-burden
copy) with a human before Phase 4 (17h, external context) or Phase 5 (17i, the apply).
