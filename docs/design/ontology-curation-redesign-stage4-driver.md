# Ontology — Curation Redesign · Stage 4 Goal-Mode driver

Copy-paste launcher for **Stage 4 of the curation redesign** (Pages — the last
redesign stage) with a long-running agent (Claude Code / Cursor Goal Mode). Run it on
the **`ontology`** branch **after Stage 1 + Stage 2 + Stage 3 have landed and been
deploy-verified**. Like Stage 3 it touches backend + frontend, but strictly
**additively** — new wheel signal types, new draft fields, no new route or frame. The
Page engine already exists (Phase 3c); Stage 4 **broadens its triggers, fixes
attachment, and adds a per-asset "why."** The offline code + tests are the agent's
job; it stops before deploy.

- **Spec (source of truth):** `docs/design/ontology-curation-redesign-build.md` (§8;
  honor §11–§14)
- **Live evidence (this estate):** `docs/design/ontology-signal-inventory-findings.md`
  — the airline estate's governed metric views, low-cardinality coded columns, and
  table/column comments are the trigger surface Stage 4 mines.
- **Decisions register:** `docs/design/mv-advisor-playbook.md` (MV-D55; honor
  MV-D27 / D35 / D43 / D45 / D49 / D50)
- **Design context:** `docs/design/ontology-engine-architecture.md` §5 (Page miners),
  plus `page-archetypes.md` and `genie-retrieval-notes.md`
- **Baseline (do NOT regress):** Stage 1 + Stage 2 + Stage 3 + 3c Pages + 17g + 17f +
  re-grain + 3b + 3a
- **Visual contract:** additive only — enrich `PageDraftCard` (per-asset "why" under
  Sources/Related) and reuse `mirror._assemble_page_draft` / `_page_chips`; no new
  frame/route
- **Project rules:** `AGENTS.md`

**Post-3c reality (verified against the code — reuse, don't re-add):** `pages.py`
already carries concept-anchoring (17d `canonical_id` via `er.canonical_id_of` /
`token_set_sig`), the eight archetypes + `[Prefix]` titles, `MeasureSignal` /
`ColumnSignal`, the corroboration gate (`_CORROBORATION_FULL = 2`), the retrieval
gates (`identifier_gate` / synonyms / chunk-safe / specificity / read-only
contradiction), `home_domain()`, and a concept-anchored `page_id`. `mv_fingerprint` is
the ONLY measure comparator and `similarity.keyword_score` the ONLY dedupe scorer.
Backend `mirror.read_page_drafts` / `_assemble_page_draft` / `_page_chips` and
`PageDraftCard.tsx` already render Page drafts; the job already wires
`default_page_drafter()` + best-effort measure reads. Stage 4 **extends each — it
invents no new comparator, similarity backend, table, or route.**

Acceptance is **offline for the code, deploy-gated for verification**: the agent builds
+ tests green, then STOPS; a human runs `./scripts/deploy.sh --update`, refreshes, and
confirms broadened Pages surface, each attaches to its source-majority domain, and
each Source/Related carries a one-line "why this asset."

---

## Driver prompt (paste verbatim)

```text
GOAL: Build Stage 4 of the ontology curation redesign — PAGES. Broaden the trigger
surface, attach each Page to its source-majority domain, and give every asset a
one-line "why." Branch: ontology, atop LANDED Stage 1+2+3 + 3c Pages. ADDITIVE,
read-only, offline.

SPEC (§8; honor §11-§14): docs/design/ontology-curation-redesign-build.md
DECISIONS: mv-advisor-playbook.md (MV-D55; honor MV-D27/35/43/45/49/50)
LIVE EVIDENCE: ontology-signal-inventory-findings.md (airline MVs, coded cols, comments).
BASELINE (no regress): Stage 1+2+3 + 3c Pages + 17g/17f/re-grain/3b/3a. RULES: AGENTS.md.

BUILD A — WHEEL triggers (ontology/pages.py; pure/deterministic/offline; REUSE
concept-anchoring + er + mv_fingerprint + similarity — NO new comparator/backend):
  - Add frozen CommentSignal (business term in a table/column COMMENT) + HistorySignal
    (recurring Genie-history disambiguation), mirroring Measure/ColumnSignal (fqn,
    domain_id, agent_fqns, comment). Each resolves to canonical_id via
    er.canonical_id_of(token_set_sig(...)) — SAME identity scheme.
  - Detectors: corroborated comment-term -> [Taxonomy]/[Disambiguation]; recurring
    history disambiguation -> [Disambiguation]. Corroboration-gated like measures/cols
    (>=2 artifacts -> certify-eligible; 1 -> low-conf + certify=false). Retrieval gates
    (synonyms, chunk-safe, specificity, read-only contradiction) UNCHANGED. Empty reads
    -> mine nothing.
  - Attachment: home_domain() picks the domain of the MAJORITY of the Page's SOURCE
    TABLES (not signal.domain_id counts); deterministic sorted tie-break. page_id stays
    concept-anchored (canonical_id|archetype|sorted keys) — NEVER domain_id/body.
  - Per-asset why: each Source (backing MV/table) + Related (agent) carries a one-line
    deterministic, evidence-derived "why this asset" (additive field).

BUILD B — JOB reads (jobs/run_ontology_materialize.py; best-effort, MV-D43): extend the
page-signal reader to ALSO gather table/column COMMENTs (information_schema, allowlist-
scoped) -> CommentSignal, and recurring disambiguations from Genie history if available
-> HistorySignal. Missing grant/absent source -> mine zero (no raise). No new profiling.

BUILD C — PRESENTATION (contracts frozen; additive only):
  - materialize.build_page_rows carries the per-asset why in the EXISTING evidence JSON
    (prefer no new Delta column; if unavoidable, ddl ADD-COLUMN-IF-NOT-EXISTS).
  - mirror._assemble_page_draft/_page_chips surface the why + broadened provenance
    (additive); types.ts mirrors; PageDraftCard.tsx renders "why this asset" under
    Sources/Related — reuse the card; NO new route/frame.

HARD GUARDRAILS:
  - Read-only: NO SET/UNSET/CREATE TAG, NO Agent-instruction write (MV-D27), NO
    manage_uc_tags, NO web_search; wheel writes no ledger; contradiction gate read-only.
  - page_id concept-anchored + deterministic; drafter writes PROSE only, never the id;
    absent/raising drafter -> stub + certify=false.
  - Metastore grain (MV-D49); OBO reads (MV-D50); NO new dependency (MV-D45) — uv.lock
    UNTOUCHED (git status). NO new similarity backend/comparator.
  - Response keys byte-identical (additive only). Do NOT change Stage 1/2/3 grouping/
    gates; do NOT pull forward §9 alignment or §10. NO DEPLOY.

ACCEPTANCE (./scripts/test.sh green): comment-term w/ >=2 artifacts -> certify-eligible
Page; single-artifact concept -> low-conf + certify=false; history disambiguation ->
[Disambiguation] Page; attachment == majority source-table domain (asserted); page_id
stable as body/home change; every Source/Related has a why; contradiction read-only;
contract-frozen (routes + OntologyRefreshStatus/taxonomy/tag-lens/drafts keys); firewall
unchanged. npm run test + lint + tsc clean; uv.lock untouched.

WORKFLOW: pages.py -> materialize.build_page_rows -> job reader ->
  mirror._assemble_page_draft -> types/PageDraftCard. ./scripts/test.sh per slice.
  STOP and ask if ambiguous or a guardrail would break.
```

---

## After the run (human-gated — the agent must not do these)

```bash
git diff --stat            # expect: packages/.../ontology/pages.py,
                           #   packages/.../ontology/materialize.py,
                           #   packages/.../jobs/run_ontology_materialize.py,
                           #   backend/ontology/services/mirror.py, backend/ontology/models.py (if a field added),
                           #   frontend/src/ontology/{types.ts,components/PageDraftCard.tsx},
                           #   packages/.../tests/unit/test_ontology_pages.py, frontend/**/*.test.tsx
./scripts/test.sh          # re-confirm green
cd frontend && npm run test && npm run lint && npm run build && cd ..
git status --porcelain uv.lock   # UNCHANGED (MV-D45; uv lock --check fails structurally here)

git add packages/genie-space-optimizer backend frontend
git commit -m "feat(ontology): Stage 4 Pages — broadened triggers (comments + Genie history), source-majority attachment, per-asset why (MV-D55)"
git push -u origin ontology
```

Then **you** run the deploy-and-verify gate:

```bash
./scripts/deploy.sh --update   # rebuilds frontend + wheel + redeploys the job (no dep change)
# In the live app: Ontology → Refresh ontology (keep the airline catalog allowlist set).
#   When the materialize job finishes, confirm:
#   1. Pages surface from more than metric-view measures — a coded-column /
#      comment-term concept backed by >=2 artifacts appears as a certify-eligible Page.
#   2. Each Page attaches to the domain of the MAJORITY of its source tables (not a
#      stray single-signal home).
#   3. Each Source and Related asset shows a one-line "why this asset."
#   4. A single-artifact concept is present but low-confidence (certify=false), not
#      hidden — and no Page names a governed tag or writes an Agent instruction.
# Query to eyeball:
#   SELECT title,
#          get_json_object(evidence,'$.corroboration') AS corr,
#          certify
#   FROM <gso_catalog>.genie_space_optimizer.genie_ont_pages
#   ORDER BY certify DESC, corr DESC;
```

Stage 4 is offline and deterministic, but the broadened reads (comments, Genie
history), the Delta write, and the synced mirror can only be validated in a deployed
app — which is why the offline slice stops here. Stage 4 is the last curation-redesign
stage; §9 industry alignment folds into Phase 4 (17h) and §10 the eval harness
(MV-D59) follows.
