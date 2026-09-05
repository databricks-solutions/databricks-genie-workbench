# Ontology — Phase 5 (17i) Goal-Mode driver

## ⚙️ Parallel-build lane header — Lane A (READ FIRST)

You run in an **isolated git worktree** off the `ontology` HEAD (`isolation: worktree`,
`worktree.baseRef: "head"`). Sibling lanes edit the repo concurrently. Keep merges clean:

- **OWNS (edit freely):** `backend/ontology/services/apply.py` (new statement builder + OBO
  execute), `backend/ontology/routers/apply.py` (**pre-seeded empty seam — fill it**),
  `backend/ontology/routers/preflight.py` (membership_write probe),
  `frontend/src/ontology/components/{ApplyPreview.tsx (new),DomainDraftCard.tsx}`,
  `frontend/src/ontology/__tests__/applyPreview.test.tsx`, apply cases under
  `packages/genie-space-optimizer/tests/unit/`.
- **SHARED — append your block at the END, never reflow existing lines:**
  `packages/genie-space-optimizer/src/genie_space_optimizer/ontology/ddl.py`
  (`APPLY_TABLES` + `genie_ont_applied` DDL), `backend/ontology/models.py` (`Apply*`),
  `frontend/src/ontology/{api.ts,types.ts}` (`Apply*` mirrors),
  `backend/ontology/services/mirror.py` (add an approved-consents read fn),
  `backend/tests/test_ontology_firewall.py` (append the single-writer-carve assertions).
- **OFF-LIMITS (do NOT touch):** `backend/main.py` + `backend/ontology/routers/__init__.py`
  — the `apply`/`graph` routers are **already imported + registered** by the pre-seed carve,
  so the prompt's "register in main.py" is **already done**; only fill `routers/apply.py`.
  Also never touch `docs/design/mv-advisor-playbook.md`, nor sibling-owned `ontology/layout.py`,
  `ontology/pages.py`, `routers/graph.py`.
- **MERGE-ORDER:** `§10,§9 (docs) → 4.1d-Step2 (B) → 3e (C) → Phase5 (A)`. **You are A — merged last.**
- Offline only (pytest + vitest). No deploy, no job run, no live governed-tag write. Commit on
  your worktree branch, report diff + test summary, then STOP.

---

Copy-paste launcher for building **Phase 5 of the Ontology page** (L9 — the single
consented `SET TAG` apply) with a long-running agent (Claude Code / Cursor Goal Mode).
Run it on the **`ontology`** branch, on top of the shipped 17g drafts+decision ledger
**and** the metastore re-grain (MV-D49). **Trust prereq satisfied:** the batch engine is
complete through Stage 4.1f (MV-D68) — proposals are fast, deterministic, and `certify`
works, so the apply consumes a trustworthy `state='approved'` consent set. This phase adds
a UI change, so acceptance runs **both** the Python suite and the frontend vitest. It ends at the **STOP apply-safety
checkpoint** — the agent must not run the live deploy or proceed to 17j.

- **Spec (source of truth):** `docs/design/ontology-phase5-apply-build.md`
- **Ledger prerequisite (already shipped):** `docs/design/implemented/ontology-phase3d-build.md`
  (17g — `genie_ont_consents` `state='approved'` is the apply's input)
- **Re-grain prerequisite (already shipped — MV-D49):** `docs/design/implemented/ontology-regrain-build.md`
- **Design context:** `docs/design/ontology-engine-architecture.md` §5 — the **L9
  apply** subsection — + the `17.0d` "Apply for me → Preview changes" mockup
- **Decisions register:** `docs/design/mv-advisor-playbook.md` (Prompt 17i; MV-D37 /
  D49 / D26 / D50 / D23 / D27 / D43 / D45)
- **Governed-tag reference (MUST read before writing the statement builder):**
  `https://docs.databricks.com/aws/en/uc-semantics/domains` +
  `https://docs.databricks.com/aws/en/data-governance/unity-catalog/tags` — the exact
  `CREATE GOVERNED TAG` / `SET TAG` / `UNSET TAG` syntax + the `{parent}/{child}`
  sub-domain convention (per `AGENTS.md`'s References rule).
- **Project rules:** `AGENTS.md`

Phase-5 code acceptance is **offline** (pytest + vitest); the live governed-tag write is
**deploy-gated**. This phase adds **no dependency** (MV-D45), so `uv.lock` is untouched.

---

## Driver prompt (paste verbatim)

```text
GOAL: OFFLINE slice of Phase 5 (17i) — the ONE consented SET TAG apply (L9), the subsystem's ONLY write
path. Flow: dry-run -> preview diff -> consent -> execute SET TAG (OBO) -> audit. DEFAULT OFF. Branch:
ontology.

SPEC(§1-§12): ontology-phase5-apply-build.md. PREREQ (SHIPPED): implemented/ontology-phase3d-build.md (17g consents),
implemented/ontology-regrain-build.md (MV-D49). DESIGN: ontology-engine-architecture.md §5 (L9), 17.0d mockup.
DECISIONS: playbook 17i (MV-D37/D49/D26/D50/D23/D27/D43/D45). RULES: AGENTS.md. READ (Refs rule):
Databricks governed-tags + domains docs for exact CREATE GOVERNED TAG / SET TAG / UNSET TAG syntax.

REUSE, DON'T FORK:
  - require_obo_workspace_client() (auth.py) = write client; read_principal_id for attribution.
  - decisions.py (17g) OBO SQL-warehouse seam = genie_ont_applied MERGE + consent approved->applied flip.
  - mirror.py = read approved consents + surfaced members/domains (metastore grain).
  - ONE statement builder in services/apply.py used by preview AND execute AND the copy-ready card.

WRITE SHAPES (from state='approved' genie_ont_consents, kind domain/subdomain/reassign; NEVER page):
  - create -> CREATE GOVERNED TAG (tag_key; sub-domain value={parent}/{child}) THEN SET TAG members.
  - reuse  -> SET TAG (tag_key=tag_value) per surfaced genie_ont_members (domain_id=proposal_id).
  - approved reassign -> UNSET conflict_tag THEN SET new per moved member.
  Pages consents -> SKIP (copy-ready-only, MV-D27).

HARD GUARDRAILS:
  - SINGLE-WRITER CARVE: set tag / unset tag / create governed tag in EXACTLY
    backend/ontology/services/apply.py, forbidden elsewhere. manage_uc_tags / alter+drop governed tag
    forbidden EVERYWHERE incl. apply.py. POSITIVE test: apply.py DOES carry the write path.
  - GRAIN (MV-D49): consents/members/genie_ont_applied keyed metastore_id; MERGE delete metastore-scoped;
    workspace_id provenance. apply_id=ap_<sha256(proposal_id|shape|target_fqn|tag_value)>.
  - OBO (MV-D50): UC write AND audit/consent-flip under require_obo_workspace_client (NEVER SP); applied_by
    =OBO email; no OBO context -> degrade, no silent SP widening.
  - CONSENT GATE: execute requires confirm=true AND the preview's plan_hash (recompute; 409 on mismatch).
    17g approval is NOT authorization to write.
  - DRY-RUN writes NOTHING (no UC write, no audit row). Execute per-statement fail-soft: PERMISSION_DENIED
    -> state='failed' + copy-ready; others still run; NEVER 500 (MV-D43).
  - IDEMPOTENT (MV-D26): consume approved consent -> flip applied; re-run = no-op.
  - ROUTES: add ONLY routers/apply.py (POST /apply/preview + /apply/execute) + register in main.py. NO other
    write route. Preflight membership_write tier -> real OBO grant probe (ok/blocked+lines).
  - ZERO-BURDEN (MV-D23): card prop-driven; rendered copy has NONE of: SET TAG, UNSET, CREATE GOVERNED TAG,
    MERGE, metastore_id, workspace_id, genie_ont_, plan_hash, OBO, SQL warehouse, Lakebase, mirror.
  - genie_ont_applied DDL in wheel ddl.py (APPLY_TABLES, empty); batch job NEVER writes it; materialize.py +
    run_as UNCHANGED.
  - CONTRACTS: Phase-1/2/3a-d models byte-identical; new models append-only; types.ts mirrors 1:1.
  - NO NEW DEP (MV-D45): uv.lock untouched. NO DEPLOY. STOP.

ACCEPTANCE: ./scripts/test.sh green over ALL §11 pytest cases (dry-run-writes-nothing, 3 write shapes,
pages-excluded, consent+plan_hash gate, audit-row + idempotent flip, missing-grant->copy-ready, metastore
grain, OBO attribution, single-writer carve + positive guard + POST allowlist + applied absent from
materialize, frozen contracts, degrade). cd frontend && npm run test (diff render). uv lock --check + npm
lint+build+tsc; §12 done.

WORKFLOW: ddl.py -> models.py -> services/apply.py (builder + OBO execute) -> routers/apply.py + main.py ->
  preflight.py (membership_write probe) -> types.ts + api.ts -> ApplyPreview.tsx + DomainDraftCard (enable
  Apply-for-me) -> applyPreview.test.tsx -> firewall test. Stop if ambiguous.
```

---

## After the run (human-gated — the agent must not do these)

```bash
git diff --stat            # expect: packages/.../ontology/ddl.py,
                           #   backend/ontology/{models.py,routers/apply.py,routers/preflight.py,
                           #     services/apply.py,main.py},
                           #   frontend/src/ontology/{api,types}.ts + components/{ApplyPreview,DomainDraftCard}.tsx
                           #   + __tests__/applyPreview.test.tsx,
                           #   backend/tests/test_ontology_firewall.py, packages/.../tests/unit/
./scripts/test.sh          # re-confirm green
cd frontend && npm run test && npm run lint && npm run build && cd ..
uv lock --check            # UNCHANGED — no new dependency (MV-D45)

git add packages/genie-space-optimizer backend frontend
git commit -m "feat(ontology): Phase 5 (17i) L9 consented SET TAG apply -> dry-run preview + plan_hash/confirm gate, OBO governed-tag membership write, genie_ont_applied audit, single-writer carve (MV-D37/D49/D26/D50)"
git push -u origin ontology
```

Then **you** run the deploy-and-verify gate:

```bash
./scripts/deploy.sh --update   # rebuilds the wheel + redeploys the job + app (no new dep)
# In the live app: approve a domain draft (17g), then click "Apply for me → Preview changes".
#   Confirm the diff shows adds/moves + any blocked-copy-ready items with the exact grant.
#   Click "Apply these changes"; confirm the governed tag/membership lands live in Unity Catalog
#   (SET TAG attributed to YOU via OBO), an audit row exists, and the consent flips to applied.
#   Re-preview: the applied proposal is gone (idempotent). Verify NOTHING else was written
#   (no Page/card/Agent write). An account without the write grant sees copy-ready, never a hang.
```

This is the **STOP checkpoint**: review apply-safety (dry-run purity, consent gate,
OBO attribution, single-writer carve, degrade-to-copy-ready) with a human before 17j
(hardening + E2E + undo).
