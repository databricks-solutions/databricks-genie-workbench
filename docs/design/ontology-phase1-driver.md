# Ontology — Phase 1 Goal-Mode driver

Copy-paste launcher for building **Phase 1 of the Ontology page** (the read-only
spine) with a long-running agent (Claude Code / Cursor Goal Mode). Run it on the
**`ontology`** branch. The prompt is bounded so the agent builds only the
offline-verifiable slice and stops at the deploy gate.

- **Spec (source of truth):** `docs/design/ontology-phase1-build.md`
- **Design context:** `docs/design/ontology-engine-architecture.md`
- **Decisions register:** `docs/design/mv-advisor-playbook.md` (MV-D36–D46)
- **Visual contract:** `docs/design/mockups/17.0a/b/c-*.html` (+ the source
  components in `frontend/src/components/auto-optimize/mockups/OntologyPageMockups.tsx`)
- **Project rules:** `AGENTS.md`

Phase 1 acceptance is **fully offline** (`./scripts/test.sh` + `npm run lint` +
`tsc`), so this run needs no workspace. Everything harder (batch, Lakebase Search,
external context, `SET TAG` apply) is a later phase and is called out in the
spec's §12 — the prompt forbids pulling any of it forward.

---

## Driver prompt (paste verbatim)

```text
GOAL: Build Phase 1 of the Ontology page — the read-only spine — exactly as
specified. Work only on the current branch (ontology).

SPEC (source of truth, follow section-by-section):
  docs/design/ontology-phase1-build.md   ← §1 scope, §3 layout, §4 contracts,
  §5 TS mirrors, §6 routes, §7 DDL, §8 readers, §9 frontend, §10 grants, §11 tests, §12 DoD
DESIGN CONTEXT: docs/design/ontology-engine-architecture.md
DECISIONS: docs/design/mv-advisor-playbook.md (MV-D36–D46)
VISUAL CONTRACT: docs/design/mockups/17.0a/b/c-*.html — build fresh components in
  frontend/src/ontology/ that match these frames. You MAY read
  frontend/src/components/auto-optimize/mockups/OntologyPageMockups.tsx for pixel
  reference, but do NOT import the mockup scaffold into the production page.
PROJECT RULES: AGENTS.md (read it first).

REUSE, DON'T FORK:
  backend/watch/services/system_tables.py  (SP reads + TTL cache + permission-error shape)
  backend/services/auth.py                 (get_workspace_client OBO / get_service_principal_client SP)
  backend/services/lakebase.py             (_ensure_schema, is_available, in-memory fallback)
  backend/main.py                          (register the ontology router the way watch is registered)
  frontend/src/App.tsx                     (add the admin-gated top-level nav, lazy-loaded)
  frontend/src/lib/api.ts                  (model frontend/src/ontology/api.ts on THIS —
                                            frontend/src/watch/api.ts does NOT exist in this repo)
  scripts/grant_permissions.py             (add the SELECT system.tags.governed_tags grant)

HARD GUARDRAILS (do not violate):
  - Read-only only. The ONLY write is PUT /api/ontology/settings (writes our config, never UC).
  - Create ONLY the genie_ont_settings table (§7). Do NOT create any other genie_ont_* table.
  - Do NOT pull forward anything in §12 (no proposal engine, no embeddings/clustering,
    no Lakebase Search, no external context / web search, no SET TAG apply).
  - No deploy. Do NOT run uvicorn or npm run dev. Do NOT run scripts/deploy.sh. Do NOT
    enable Lakebase Search (irreversible).
  - Keep Pydantic (§4) and TypeScript (§5) mirrors 1:1.

ACCEPTANCE (all must be true before you declare done):
  - ./scripts/test.sh is green, including a read-only firewall test asserting no route
    imports a SET TAG / CREATE GOVERNED TAG / manage_uc_tags write path (§11).
  - Frames 17.0a (preflight banner), 17.0b (taxonomy), 17.0c (tags lens) are wired to
    live data with the zero-burden copy from the visual contract.
  - cd frontend && npm run lint passes; tsc is clean.
  - The §12 Definition of Done reads true.

WORKFLOW: backend contracts → services → routers → main.py wiring → frontend types →
api → page + nav → grants → tests. Run ./scripts/test.sh after each backend slice.
Stop and ask if a spec detail is ambiguous or if any guardrail would have to be crossed.
```

---

## After the run (human-gated — the agent must not do these)

```bash
git diff --stat                         # expect: backend/ontology/, frontend/src/ontology/, App.tsx, grant_permissions.py, backend/tests/
./scripts/test.sh                       # re-confirm green
cd frontend && npm run lint && npm run build && cd ..

git add backend/ontology frontend/src/ontology frontend/src/App.tsx \
        scripts/grant_permissions.py backend/main.py backend/tests
git commit -m "feat(ontology): Phase 1 read-only spine — preflight, inventory, taxonomy, tags lens (MV-D36/D37/D43)"
git push -u origin ontology
```

Then **you** run the deploy-and-verify gate (`./scripts/deploy.sh --update` → test in
the live Databricks App). OBO auth, Lakebase, and system-table grants can only be
validated in a deployed app — that boundary is why Phase 1 stops here.
