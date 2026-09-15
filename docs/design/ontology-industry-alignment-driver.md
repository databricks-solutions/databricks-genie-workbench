# Ontology — Industry-reference alignment Goal-Mode driver (MV-D58, 17h)

## Status / positioning (READ FIRST)

**BUILD-READY — dependencies met (2026-09-15).** The Phase-4 Context Pack seam
(17h Stage B) is **landed + deploy-verified** and Stage-4.1c (MV-D65, wheel-native
LLM client) is built, so the two things §9 waited on now exist. §9 of the
curation-redesign build spec (`ontology-curation-redesign-build.md`) is now the
**authoritative** behavior source (expanded 2026-09-15); honor it exactly.

**Why now (live finding motivating this build).** A 2026-09-15 re-verify of the
Phase-4 naming prior showed `rank.apply_context_prior`'s token-overlap match
mistargets: pack names landed on suppressed dev/migration clusters while the estate's
16 **surfaced** domains are all **curated governed tags** (zero surfaced `create`
clusters), so the prior only ever produced noisy `applied=false` corroboration. The
prior was hardened (surfaced-gate + ≥2-token match), but the durable fix is §9's
typed, confidence-gated correspondence + gap detection — which is what this build
delivers. Build order of record is the **Ontology Build Queue** in
`mv-advisor-playbook.md`.

Because it is future work, this driver carries **no parallel-build lane header** — it
is not competing with sibling worktrees today. When it is scheduled, re-confirm the
OWNS/OFF-LIMITS surface below against the then-current tree before editing.

---

## Spec (source of truth)

- **Primary:** `docs/design/ontology-curation-redesign-build.md` **§9
  Industry-reference alignment · MV-D58** — the authoritative behavior. Honor it
  exactly; invent nothing beyond it.
- **Supporting (same spec):** §2 goals/principles (*"align, don't conform"* —
  MV-D58; *"map, don't merge"* — MV-D60), §4 architecture (alignment sits between
  rank+why and the metastore MERGE), §7 config surface
  (`industry_alignment={enabled,reference_model}`), §10 eval harness (MV-D59
  consumes the aligned reference), §11 data-model impact (`genie_ont_alignment`,
  evidence-JSON-preferred), §12 guardrails/invariants.
- **Design source of truth:** `ontology-engine-architecture.md` §6 (External
  context / enrichment tier, MV-D38) — the provenance ladder (T0 > T1 > T2 > T3),
  the "naming/description/hypothesis layer, never structural truth" rule, the
  L4→L5 naming + L5 gap-check plug-points, and the two user touch-points (opt-in
  toggle + optional low-confidence confirm).
- **Decisions register:** `mv-advisor-playbook.md` — MV-D58 (this build), depending
  on MV-D38 (provenance firewall/ladder), MV-D44 (toggle off by default), MV-D60
  (map-not-merge across contexts), MV-D59 (eval harness). Inherit MV-D43 / MV-D45 /
  MV-D49 / MV-D50 unchanged. Do **not** open or edit the playbook from this build.

---

## What industry alignment must build (derived from §9)

1. **Load the matching Vibe industry model.** Given the run's resolved industry
   (company → NAICS/GICS, already a Context-Pack input), load the corresponding
   **Vibe industry model**: its domain taxonomy + ontology JSON. This is a **T2
   industry-canonical** reference (architecture §6) — vocabulary and structure only.
2. **Hybrid match, in three ordered passes:** (a) **string** match discovered
   domains against reference domain names/synonyms; (b) **embedding** match on
   **seed anchors** to catch renamed-but-equivalent domains; (c) **structural
   propagation** across the FK/join graph so a matched anchor pulls its connected
   neighbours; then (d) a **semantic sanity** pass that rejects matches that are
   lexically/embedding-close but structurally incoherent. Deterministic ordering,
   fixed embedding seed anchors — same estate + same reference ⇒ same result.
3. **Emit typed correspondences.** Every discovered-domain ↔ reference-domain link
   is one of the fixed relation types: **`exact` / `narrower` / `broader` /
   `derived` / `not-equivalent`**. No untyped or free-text relation.
4. **Emit gap-domain hypotheses.** Where the reference model has a domain the
   estate has none for, emit a **hypothesis** ("industry has *Loyalty*, estate has
   none") — ranked **below** every evidence-backed proposal, **never auto-created**.
5. **Provenance-gate everything (T2/T3).** Alignment may **suggest and name** but
   **never outranks a T0 / curated fact** — the `rank.py` provenance ladder decides
   (T0 internal-verified > T1 company-official > T2 industry-canonical > T3
   web-inferred). A naming hint never overrides membership, a measure definition, a
   lineage fact, or a curator's decision.
6. **Fold into the Phase-4 / 17h Context Pack + firewall (MV-D38).** Alignment
   consumes the versioned, cached, per-company Context Pack as a read-only prior and
   emits `Provenanced<T>` leaves (tier + source + as-of date) so L6 ranking treats
   pack facts and graph facts uniformly. It respects the same firewall (PII on tag
   names; no structural writes).
7. **Toggle off by default (MV-D44).** The feature is gated by the additive config
   `industry_alignment={enabled, reference_model}` (§7). `enabled=false` (default)
   ⇒ the run behaves byte-identically to today — no alignment leaves, no gap
   hypotheses, no naming influence.
8. **Persist additively (MV-D49, §11).** Alignment relations + gap hypotheses ride
   the existing **`evidence` JSON** where possible; a per-run/per-domain
   **`genie_ont_alignment`** table is optional-additive only (metastore-keyed, CDF
   on, no retired columns, no key/grain change).
9. **Feed the eval harness (MV-D59, §10).** Emit the aligned reference in the shape
   the offline harness reads to compute precision/recall/F of discovered domains vs
   the reference. (Building the harness itself is §10 / a separate driver — this
   build only ensures the aligned-reference output exists and is stable.)

---

## Decisions to honor

- **MV-D58 — align, don't conform.** Alignment renames and hypothesizes; it does
  **not** reshape the estate to fit the reference. Structure comes from the graph.
- **MV-D38 — provenance ladder / firewall.** T0 > T1 > T2 > T3; T2/T3 alignment
  never touches membership, measures, lineage, or certification; PII firewall on
  tag names holds.
- **MV-D44 — off by default.** `industry_alignment.enabled=false` ⇒ no behavior
  change; the toggle is opt-in.
- **MV-D60 — map, don't merge.** Cross-context equivalence is expressed as a typed
  correspondence (`exact`/`narrower`/…), never by fusing two contexts' identities.
- **MV-D59 — eval-gated.** The aligned reference is the harness's ground truth; this
  output must be stable and reproducible so §10 can gate later threshold changes.
- **Inherited, unchanged:** MV-D43 (degrade-not-hang: a missing/failed reference
  model degrades to no-alignment, never blocks the run), MV-D45 (no new dependency —
  `uv.lock` untouched), MV-D49 (metastore grain; additive DDL only), MV-D50
  (OBO-default reads; config via idempotent `ADD COLUMN IF NOT EXISTS`).

---

## OWNS / OFF-LIMITS (prospective — re-confirm at schedule time)

**OWNS (the alignment surface):**
- Wheel engine: a new alignment module under
  `packages/genie-space-optimizer/src/genie_space_optimizer/ontology/` (e.g.
  `alignment.py`) plus the L4→L5 **naming + gap-check** wiring in `rank.py`
  (alignment leaves enter ranking as provenanced priors, never outranking T0).
- Optional additive table `genie_ont_alignment` in `ontology/ddl.py`
  (metastore-keyed, CDF on) **only if** the `evidence` JSON cannot carry the typed
  relations + gap hypotheses; prefer `evidence`.
- Config: `industry_alignment={enabled, reference_model}` in
  `backend/ontology/services/ont_settings.py` (idempotent `ADD COLUMN IF NOT
  EXISTS`), mirrored in `frontend/src/ontology/**` `types.ts` and surfaced in
  `SettingsForm` as the single opt-in toggle (+ optional low-confidence confirm).
- New offline unit tests for the alignment module and the config round-trip.

**OFF-LIMITS (do NOT touch):**
- The **apply path** (17i / `apply.py`) — alignment is read-only; no
  SET/UNSET/CREATE GOVERNED TAG, no `manage_uc_tags`, no `web_search` here.
- **Structural stages** — `graph.py`, `cluster.py`, Leiden, FK/join edge weights,
  the metastore re-grain (MV-D49). Alignment never changes membership.
- The **Context Pack builder / Phase-4 seam itself** if it is owned by a sibling
  Phase-4 build — this driver *consumes* the pack, it does not author it.
- The **eval harness** (§10 / MV-D59) — separate driver.
- `docs/design/mv-advisor-playbook.md`, and any earlier frozen route/response
  contract.

---

## Acceptance criteria (offline)

- **Off-by-default parity:** with `industry_alignment.enabled=false` (the default),
  a materialize run is **byte-identical** to today — no alignment leaves, no gap
  hypotheses, no naming change, no new rows.
- **Typed relations only:** every emitted correspondence carries exactly one of
  `exact` / `narrower` / `broader` / `derived` / `not-equivalent`; no untyped link.
- **Gap hypotheses ranked below evidence:** a reference-only domain surfaces as a
  hypothesis that ranks **below** every graph-backed proposal and is **never
  auto-created** as a domain.
- **Provenance ladder enforced:** a fixture where a T2/T3 alignment name conflicts
  with a T0 lineage fact or a curated tag ⇒ the T0/curated fact **wins** (name +
  identity); the alignment becomes corroborating `evidence`, not a rival.
- **Determinism:** same estate + same `reference_model` + fixed embedding seed
  anchors ⇒ identical relations, hypotheses, and ordering across runs.
- **Degrade-not-hang:** a missing / unloadable / empty reference model ⇒ the run
  completes with alignment silently skipped (logged), never blocked (MV-D43).
- **Additive persistence:** no key/grain change, no retired columns; alignment data
  rides `evidence` JSON (or the optional additive `genie_ont_alignment` table only).
- **Harness-ready output:** the aligned reference is emitted in the shape §10's
  offline harness reads (discovered ↔ reference correspondence set) and is stable.
- **Green:** `./scripts/test.sh` (backend + GSO suites); `npm run lint` + `tsc`
  clean on the touched frontend config.

---

## Driver prompt (paste verbatim when scheduled)

GOAL: Ontology industry-reference alignment (MV-D58, §9). Given a run's resolved
industry, load the matching Vibe industry model (domain taxonomy + ontology JSON) and
hybrid-match it against the discovered domains (string → embedding seed anchors →
structural propagation → semantic sanity), emitting TYPED correspondences
(exact/narrower/broader/derived/not-equivalent) + gap-domain hypotheses. Provenance-
gated (T2/T3): may suggest and name, NEVER outranks a T0/curated fact (rank.py
ladder). Folds into the Phase-4/17h Context Pack + firewall (MV-D38). Toggle off by
default (MV-D44). Additive persistence (MV-D49).

SPEC: docs/design/ontology-curation-redesign-build.md §9 (primary), §2/§4/§7/§11/§12;
ontology-engine-architecture.md §6 (provenance ladder, plug-points). DECISIONS:
mv-advisor-playbook.md MV-D58 depending on MV-D38/D44/D60/D59; honor MV-D43/D45/D49/
D50. Read first. The Context Pack builder (Phase-4) and the eval harness (§10/MV-D59)
are NOT in scope here — this build consumes the pack and emits the aligned reference.

CONTEXT: Membership comes from the graph and is fixed before alignment runs —
alignment is a naming/description/hypothesis layer only. rank.py already runs the
L4→L5 naming + L5 gap-check plug-points and the provenance ladder. Config
industry_alignment={enabled,reference_model} lives in ont_settings (idempotent ADD
COLUMN IF NOT EXISTS), mirrored to types.ts + SettingsForm. Relations ride the
evidence JSON where possible.

BUILD A — alignment module (new ontology/alignment.py): load the reference model for
`reference_model`; run the four ordered passes (string, embedding-seed-anchor,
structural propagation across FK/join edges, semantic sanity); emit typed
correspondences + gap hypotheses as Provenanced<T> leaves (tier T2/T3 + source +
as-of). Deterministic; fixed seed anchors; degrade to no-op on missing/empty model.

BUILD B — rank wiring (rank.py): feed alignment leaves into L4→L5 naming and the L5
gap-check as provenanced priors — never outranking T0/curated (the existing ladder
enforces this); gap hypotheses rank below every evidence-backed proposal and are
never auto-created.

BUILD C — config + UI seam (ont_settings.py + types.ts + SettingsForm): add
industry_alignment={enabled:false default, reference_model} via idempotent ADD COLUMN
IF NOT EXISTS; mirror the type; surface the single opt-in toggle (+ optional low-
confidence confirm). enabled=false ⇒ byte-identical run.

BUILD D — persistence (evidence-first; ddl.py only if needed): store relations + gap
hypotheses in the evidence JSON; add the optional additive genie_ont_alignment table
ONLY if evidence cannot carry them (metastore-keyed, CDF on, no retired cols, no
key/grain change). Emit the aligned reference in the shape §10's harness reads.

HARD GUARDRAILS: read-only — NO SET/UNSET/CREATE GOVERNED TAG, no manage_uc_tags, no
web_search, no apply-path (17i) edit. NO structural change (no graph/cluster/Leiden/
edge-weight edits) — alignment never changes membership. NO new dependency (uv.lock
untouched, MV-D45). Additive DDL only (MV-D49). OBO-default reads (MV-D50). Degrade-
not-hang (MV-D43). Off by default (MV-D44). Do NOT edit mv-advisor-playbook.md.

ACCEPTANCE (offline): enabled=false ⇒ byte-identical materialize run (no leaves, no
hypotheses, no new rows). Every correspondence is exactly one of the five typed
relations. Gap hypotheses rank below evidence-backed proposals,
never auto-created. A T2/T3 name conflicting with a T0 lineage fact / curated tag ⇒
T0/curated wins, alignment becomes corroborating evidence. Same estate + same
reference_model + fixed seeds ⇒ identical output. Missing/empty model ⇒ run completes,
alignment skipped. ./scripts/test.sh + GSO suite green; npm run lint + tsc clean.

WORKFLOW: branch `ontology` (schedule against the Ontology Build Queue). Do NOT
deploy or run the job. When offline-green, STOP and report the diff + test summary;
a human runs deploy-verify.

---

## STOP before deploy (human-gated)

Stop when offline code + tests are green. Report the diff + `./scripts/test.sh`
summary. Do **not** deploy, run the materialize job, or merge to `ontology`. A human
runs deploy-verify: deploy `SKIP_FRONTEND_BUILD=1 ./scripts/deploy.sh --update`
(fevm-serverless), enable `industry_alignment` with a real `reference_model`, trigger
scoped to `["serverless_stable_6t92c3_catalog"]`, and confirm — against the live
airline estate — that discovered domains (Revenue, Maintenance, Loyalty, Reservation,
Route, Fleet, Passenger) pick up business-language names + typed correspondences, gap
hypotheses appear ranked below evidence-backed proposals, and no T0/curated fact is
outranked. With the toggle off, confirm **no regression** vs the current run.
