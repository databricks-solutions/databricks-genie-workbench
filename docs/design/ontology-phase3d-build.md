# Ontology — Phase 3d: rank & trust gate + serve the drafts (L6) — build spec

**Status:** build-ready (offline slice) · **STOP checkpoint after this phase.**
**Owner directive:** MV-D49 (grain = metastore; Domains/Sub-Domains/Pages are
account-level artifacts), MV-D35 (evidence-first — the score *ranks*, it is never a
"confidence in correctness"), MV-D23 (prop-driven, zero-burden cards), MV-D26
(consent/suppression ledger — a resolved proposal never resurfaces), MV-D37
(governed-tag substrate; MV/Agent advisories are first-class), inheriting the shipped
17d/17e/17f spine (see `mv-advisor-playbook.md`, Prompt 17g). **Design source of
truth:** `ontology-engine-architecture.md` §5 — the **L6 rank & trust gate**
subsection (score → firewall → display) and the **L7 persistence** / **L8 serving**
subsections — plus the `17.0d` (Domain draft) and `17.0e` (Page draft) mockups.
**Builds on:** `ontology-regrain-build.md` (the metastore re-grain, MV-D49 — L6 is
keyed by `metastore_id` from the start) **and** `ontology-phase3b-build.md` (the L4
Domain → Sub-Domain proposals + `reassign`/`conflict` rows in `genie_ont_domains`)
**and** `ontology-phase3c-build.md` (the L5 Page proposals in `genie_ont_pages`).

This is the **sixth** proposal-engine deliverable (after the re-grain) and the **first
phase that serves proposals to the page and records human decisions**. It does two
jobs. (1) In the wheel: **rank** every proposal (Domain / Sub-Domain / Page) by
`usage × lineage-centrality × governance` and pass it through the **trust firewalls**
(PII on proposed tag names, tag-policy conformance, and a *dormant* MV-D38 provenance
ladder), persisting the `score` and dropping/suppressing anything unsafe or
sub-threshold. (2) In the backend + frontend: **serve** the ranked drafts (`GET
/api/ontology/drafts`) and let a human **adjudicate** them (`POST
/api/ontology/decision`) — approve or dismiss, *including adjudicating an 17e
`reassign`* — recording the outcome in `genie_ont_consents` / `genie_ont_suppressions`
so a re-run **skips what a curator resolved** (MV-D26). It renders the `17.0d` / `17.0e`
zero-burden cards. It **still applies nothing to Unity Catalog** — there is **no `SET
TAG`, no apply route** (that is 17i). The ledger writes are app-state Delta rows
(human-attributed under OBO), **not** governance mutations.

> **The one-line contract:** 17e/17f decided *what to propose*. Phase 3d decides
> *what a reviewer sees first and what a reviewer's decision means*. Facts lead
> (MV-D35): the blended `score` **orders** the list into HIGH / MEDIUM / LOW tiers
> (coverage-capped so a single signal never outranks a corroborated one) and the card
> leads with evidence chips — never a rendered "NN% confidence." A **dismiss** writes
> a suppression that no re-run resurfaces; a **rejected `reassign`** stays suppressed
> (MV-D26). The one write is to the ledger; nothing touches UC until 17i.

---

## 1. Scope

### In (Phase 3d)

- **L6 rank & trust gate** (`ontology/rank.py`, new — in the GSO wheel), run as an
  **additive-last** materialize step **after** the 17f Page MERGE, over the metastore's
  Domain / Sub-Domain (`genie_ont_domains`) and Page (`genie_ont_pages`) proposals:
  1. **Score (`usage × lineage-centrality × governance`, MV-D35).** Generalize the
     MV-advisor's LYDS blend (`mv_scoring`) from single-MV proposals to every estate
     candidate. Three deterministic factors, each already available upstream:
     - **usage** — demand/cost from the L2 signals (`system.query.history` /
       `system.billing.usage` already read by the batch): how much the anchoring
       assets are asked about (and cost).
     - **lineage-centrality** — degree / betweenness on the fused lineage subgraph,
       **precomputed in L2/L4** (`graph.py`) — the load-bearing spine outranks a leaf.
     - **governance** — governed > curated > ungoverned (the traffic-light ladder),
       read from `genie_ont_tag_graph` / certification coverage.
     Blend → a **tier** (HIGH / MEDIUM / LOW) with a **sub-threshold `suppress`**, reusing
     the `mv_scoring` thresholds **and the evidence-coverage cap** (a single-signal
     opinion cannot outrank a corroborated finding — dovetails with 17f corroboration).
  2. **Firewalls (a candidate must pass ALL to *surface*):**
     - **PII firewall on proposed tag names** — extend `leakage.LeakageOracle` to tag
       *names* (governed tag names replicate globally in plaintext, so a Domain /
       Sub-Domain proposal whose `tag_key` / `tag_value` **name** would leak PII — an
       email, an id — is **blocked**, not surfaced). Reuse the `er.pii_reject` shape.
     - **Policy conformance** — propose-only. Any candidate whose evidence implies an
       instruction / card / Page *write* (there should be none) is rejected at the gate.
     - **Provenance ladder (MV-D38) — SCAFFOLDED, DORMANT.** A pure hook that today is
       a no-op (all estate signal is T0 structural); it becomes load-bearing when 17h
       lands (T0 internal-verified > T1 company > T2 industry > T3 web). Leave the seam
       + a passing "T3 hint never outranks a T0 fact" unit test; do **not** read
       external context here.
  3. **Persist ranked + suppressed (L7).** Write the numeric `score` onto the existing
     `score` column of `genie_ont_domains` / `genie_ont_pages`. **Read the
     `genie_ont_suppressions` ledger** (written by the backend in a prior run) and mark
     ledger-dismissed + blocked + sub-threshold proposals as **not surfaced** (a
     `surfaced BOOLEAN` written into the proposal `evidence` JSON, not a new column) so
     the serve route excludes them and the run report counts them. The wheel **reads**
     the ledger; it **never writes** `genie_ont_consents` / `genie_ont_suppressions`.
- **Serving (backend, mirror-first, degrade-not-hang):**
  - `GET /api/ontology/drafts` → the ranked, surfaced Domain / Sub-Domain + Page
    proposals **including 17e `reassign` / `conflict` rows**, read from the Lakebase
    mirror (Delta-via-warehouse today, synced later — the `mirror.py` seam), tiered and
    ordered, ledger-resolved rows excluded. Empty / cold / enrichment-failed → an empty,
    typed payload (never a 500).
  - `POST /api/ontology/decision` → the human adjudication. `approve` records a row in
    `genie_ont_consents` (state `approved` — the pin 17i's apply will consume; **no UC
    write now**); `dismiss` and a **rejected `reassign`** record a row in
    `genie_ont_suppressions` (MV-D26 — never resurfaced); an **approved `reassign`**
    records a consent that pins the human's call for the next 17e soft-seed run. Written
    under **OBO** (attributed to the deciding human) as `genie_ont_*` Delta rows via the
    SQL warehouse. **No apply route.**
  - New Pydantic models (drafts + decision) mirrored 1:1 in `types.ts`; the
    Phase-1/2/3a-c models are **frozen** (byte-identical).
- **Frontend (prop-driven cards — the MV-D23 obligation):** a new **Drafts** tab in
  `OntologyPage.tsx` rendering:
  - **`17.0d` Domain draft card** — leads with the recommendation, a plain
    **new-vs-reuse-vs-reassign** line + **"Why we're suggesting this"** (for a
    `reassign`: the tag it conflicts with + the evidence), Proposed Sub-Domains,
    Member-asset chips, **Apply-for-me [disabled until 17i]** and a **do-it-yourself**
    checklist + a **Copy for Discover** (client-side clipboard) button, plus
    **Approve** / **Dismiss** actions and, for a `reassign`, **Keep current** /
    **Accept reassignment**.
  - **`17.0e` Page draft card** — leads with the reason, **prominent Synonyms**,
    Related / Sources chips, a **certify** recommendation, and copy + checklist.
  - **enrichment-failed** and **empty** variants.
  All copy is **zero-burden** — no DDL, grants, system-table names, or backend jargon.
- **Firewall + contract tests** extended (§11), including the router-verb allowlist,
  the wheel read-not-write ledger invariant, and a **vitest** zero-burden-copy render
  test.

### Out (deferred — do NOT pull forward)

- **The apply (`SET TAG` / `CREATE GOVERNED TAG` / `UNSET`+`SET` for a reassignment) —
  17i (Phase 5).** 17.0d's "Apply for me → Preview changes" stays **disabled**.
- **External context / Context Pack / provenance-ladder enforcement — 17h (Phase 4).**
  The ladder hook is a dormant no-op; **no `web_search`, no Context Sources.**
- **New similarity backend / Lakebase Search** beyond `similarity.py` (MV-D40/D45).
- **New dependency (MV-D45)** — `uv.lock` is untouched; scoring reuses `mv_scoring` +
  `graph.py`; no clustering/embedding library is added.
- **Any new `genie_ont_*` table** — every table already exists (17d/17f DDL). No DDL.

---

## 2. Decisions honored

| Decision | How Phase 3d honors it |
|---|---|
| **MV-D49** grain = metastore | Score, serve, and the ledger are all keyed by `metastore_id`; `workspace_id` is provenance only, never a key. Drafts read the mirror at metastore grain. |
| **MV-D35** evidence-first | The blend is a *demand/importance ranking* signal; the card leads with evidence chips + a tier, never a rendered percent. Coverage cap: a corroborated finding outranks a single signal. |
| **MV-D26** suppression ledger | `dismiss` / rejected `reassign` → `genie_ont_suppressions`; the wheel reads it so a re-run never resurfaces a resolved proposal. |
| **MV-D23** prop-driven zero-burden | Cards receive fully-assembled props; no data assembly in the component; no backend jargon (guarded by a vitest render test). |
| **MV-D37** governed-tag substrate | MV/Agent advisories (from 17f) are ranked and served alongside Domain/Page drafts; the substrate is governed tags. |
| **MV-D38** provenance ladder | Scaffolded as a dormant no-op hook + a unit test; only 17h makes it read external context. |
| **MV-D43** degrade-not-hang | Mirror-first read with a live fallback; every serve failure returns a typed empty payload, never a 500 or a hang. |
| **MV-D27** Page-only, propose-only | No Agent-instruction write; the only write is the app-state ledger; **no `SET TAG`.** |
| **MV-D50** OBO-first / `run_as` | The batch scoring reads system-table signals as the job's `run_as` identity (not necessarily the app SP); the decision route writes the ledger under **OBO** (`decided_by` = the deciding human). |

---

## 3. Subsystem layout (files touched)

**Wheel (`packages/genie-space-optimizer/src/genie_space_optimizer/ontology/`):**

| File | Change |
|---|---|
| `rank.py` | **NEW** — the L6 gate: `score_proposals()` (blend + tier + coverage cap, pure), the firewall stack (`pii_name_reject` via `LeakageOracle`, `policy_conform`, `provenance_ladder` dormant hook), and `mark_surfaced()` (reads the suppression ledger, marks blocked/sub-threshold/dismissed). Pure transforms live in `transforms.py`. |
| `transforms.py` | **EXTEND** — `tier_of(score)` + threshold constants + `coverage_cap()` (the ONE source of thresholds, imported by both `rank.py` and — mirrored — the backend serve tiering). |
| `materialize.py` | **EXTEND** — additive-last step: after the Page MERGE, run `rank.score_proposals` + `rank.mark_surfaced` (reading `genie_ont_suppressions`), then re-MERGE `genie_ont_domains`/`_pages` with the `score` + `surfaced` evidence set. Reads the ledger; **never** writes consents/suppressions. |
| `leakage.py` (optimization) | **EXTEND** — a tag-*name* PII rule (transpose the MV-D8 comment-echo rule); one oracle, no second scanner. |

**Backend (`backend/ontology/`):**

| File | Change |
|---|---|
| `models.py` | **EXTEND (append-only)** — `OntologyDrafts`, `DomainDraft`, `PageDraft`, `ReassignDraft`, `EvidenceChip`, `DraftTier`, `DecisionRequest`, `DecisionResponse`. Phase-1/2/3a-c models frozen. |
| `routers/drafts.py` | **NEW** — `GET /api/ontology/drafts` + `POST /api/ontology/decision`. The only new router; the only new POST. |
| `services/mirror.py` | **EXTEND** — `read_domain_drafts(metastore_id)` + `read_page_drafts(metastore_id)` (surfaced-only, tiered), mirroring the existing reader seam. |
| `services/decisions.py` | **NEW** — `record_decision()` (OBO): idempotent MERGE into `genie_ont_consents` / `genie_ont_suppressions` via the SQL warehouse; `decided_by` = OBO email. |
| `main.py` | register the `drafts` router next to the other ontology routers. |

**Frontend (`frontend/src/ontology/`):**

| File | Change |
|---|---|
| `components/DomainDraftCard.tsx` | **NEW** — `17.0d`, prop-driven. |
| `components/PageDraftCard.tsx` | **NEW** — `17.0e`, prop-driven. |
| `components/DraftsView.tsx` | **NEW** — lists drafts by tier; owns the decision-action calls + optimistic removal. |
| `OntologyPage.tsx` | **EXTEND** — add the `drafts` tab (admin-gated, mirror-fresh gated, like `taxonomy`). |
| `api.ts` | **EXTEND** — `getDrafts()` + `postDecision()`. |
| `types.ts` | **EXTEND** — TS mirrors of the new models. |
| `__tests__/draftCards.test.tsx` | **NEW (vitest)** — renders the cards from a fixture; asserts zero-burden copy present + backend-jargon tokens absent. |

---

## 4. Contracts (the new models)

```python
DraftTier = Literal["high", "medium", "low"]            # sub-threshold never served
DecisionKind = Literal["domain", "subdomain", "page", "reassign"]
DecisionAction = Literal["approve", "dismiss", "reassign_accept", "reassign_reject"]

class EvidenceChip(BaseModel):
    label: str            # plain-language, e.g. "41% of Genie questions"
    kind: Literal["usage", "centrality", "governance", "corroboration", "conflict"]

class DomainDraft(BaseModel):
    proposal_id: str      # = domain_id (concept/fingerprint-derived, metastore-stable)
    kind: Literal["domain", "subdomain", "reassign"]
    name: str
    description: str
    tag_decision: Literal["create", "reuse", "reassign"]
    conflict_tag: str | None = None      # for reassign: the tag it conflicts with
    subdomains: list[str] = []
    members: list[MemberAsset] = []
    why: str                              # "Why we're suggesting this" (zero-burden)
    evidence: list[EvidenceChip] = []
    tier: DraftTier

class PageDraft(BaseModel):
    proposal_id: str      # = page_id (canonical-concept-derived, 17f)
    archetype: Literal["Routing", "Disambiguation", "Guardrail", "Taxonomy"]
    title: str
    reason: str                           # leads the card
    body: str
    synonyms: list[str] = []
    related_fqns: list[str] = []
    source_fqns: list[str] = []
    certify: bool
    evidence: list[EvidenceChip] = []
    tier: DraftTier

class OntologyDrafts(BaseModel):
    domains: list[DomainDraft] = []
    pages: list[PageDraft] = []
    source: Literal["mirror", "live", "cold"]
    as_of: str

class DecisionRequest(BaseModel):
    kind: DecisionKind
    proposal_id: str
    action: DecisionAction

class DecisionResponse(BaseModel):
    ok: bool
    recorded: Literal["consent", "suppression"]
    as_of: str
```

`types.ts` mirrors these exactly (the repo's Pydantic ↔ TS rule).

---

## 5. The rank & trust gate (wheel)

- **Pure + deterministic.** `score_proposals(candidates, signals) -> scored` reads only
  what the batch already computed (usage, centrality, governance). No LLM, no I/O. Same
  inputs → same scores → same tiers → same order. `tier_of` + thresholds live in
  `transforms.py` (the single source, mirrored by the backend serve layer).
- **Coverage cap.** Reuse `mv_scoring`'s evidence-coverage cap so a proposal backed by
  one weak signal cannot outrank a corroborated one (dovetails with 17f corroboration).
- **Firewalls (ALL must pass to surface).** `pii_name_reject` (LeakageOracle on
  `tag_key`/`tag_value`), `policy_conform` (propose-only), `provenance_ladder` (dormant
  no-op returning "pass" today). A blocked candidate is **dropped from the surfaced set**
  and counted in the run report — never served, never persisted as surfaced.
- **Ledger read (idempotency).** `mark_surfaced` reads `genie_ont_suppressions` for the
  metastore and marks any `(kind, proposal_id)` the human already dismissed as
  `surfaced=false` — so the next serve never shows it (MV-D26). The wheel issues a
  read-only `SELECT`; it holds **no MERGE/INSERT/UPDATE** against the ledger tables.

---

## 6. Serving (backend)

- **`GET /api/ontology/drafts`** — mirror-first (`refresh.mirror_is_fresh`), like
  `taxonomy`: read `genie_ont_domains` + `genie_ont_pages` (surfaced-only) from the
  mirror, tier via `transforms.tier_of`, order HIGH → LOW, assemble `EvidenceChip`s +
  the zero-burden `why`/`reason` strings **server-side** (MV-D23 — the card assembles
  nothing). Include 17e `reassign`/`conflict` rows as `kind="reassign"` with
  `conflict_tag`. Any failure → `OntologyDrafts(source="cold", ...)` empty payload.
- **`POST /api/ontology/decision`** — OBO. Resolve `metastore_id` (the re-grain
  resolver), then `decisions.record_decision`:
  - `approve` → MERGE `genie_ont_consents` (state `approved`).
  - `dismiss` / `reassign_reject` → MERGE `genie_ont_suppressions`.
  - `reassign_accept` → MERGE `genie_ont_consents` (pins the reassignment for 17e).
  Idempotent on `(metastore_id, kind, proposal_id)`; `decided_by` = OBO email;
  `decided_at` = now. Returns `DecisionResponse`. **No `SET TAG`.**
- **Firewall note.** `drafts.py` is the **only** new router and the **only** new POST.
  It writes `genie_ont_*` Delta app-state rows — **not** governed tags. The router-verb
  test (§11) is extended to allow POST in `drafts.py` while still forbidding PATCH /
  DELETE and any governed-tag write anywhere.

---

## 7. Persistence & grain (MV-D49)

- **Scores** land on the existing `score DOUBLE` column of `genie_ont_domains` /
  `genie_ont_pages`; the `surfaced` flag lives in each row's `evidence` JSON (no new
  column, no new DDL). Keys are `(metastore_id, domain_id)` / `(metastore_id, page_id)`.
- **Ledger** — `genie_ont_consents` / `genie_ont_suppressions`, keyed
  `(metastore_id, proposal_kind, proposal_id)` **after the re-grain** (the re-grain
  re-keys these from `workspace_id`). `workspace_id` stays as provenance only.
- **Mirror** — the ledger and scored proposals mirror through the existing `mirror.py`
  seam; no new synced table beyond registering the (already-provisioned) proposal
  tables in `scripts/setup_synced_tables.py` if not already present.

---

## 8. Batch job (materialize) ordering

1. …existing snapshot + identity (17a-d) + cluster MERGE (17e) + Page mine + MERGE (17f)…
2. **NEW (17g, additive-last):** `rank.score_proposals` over the just-written Domain /
   Page rows → firewalls → `rank.mark_surfaced` (read `genie_ont_suppressions`) →
   re-MERGE `genie_ont_domains` / `genie_ont_pages` with `score` + `surfaced` evidence.
   The re-MERGE source MUST carry the **full** metastore proposal set (every row 17e/17f
   just wrote), so the metastore-scoped `WHEN NOT MATCHED BY SOURCE ... DELETE` prunes
   nothing it shouldn't — a partial source would wrongly delete unranked proposals.
3. Update the `genie_ont_runs` header counts (surfaced / suppressed / blocked). Ranking
   is additive — it never corrupts earlier snapshots and is safe to re-run.

---

## 9. Grants / deploy

No new grants. Scoring reads the same L2 signals the batch already reads (as the job's
`run_as` identity, MV-D50 — not necessarily the app SP). The decision route writes the
ledger under **OBO** via the existing `SQL_WAREHOUSE_ID` (the same warehouse the mirror
reads). Deploy is the existing `./scripts/deploy.sh --update` (wheel rebuild + job +
app). No new dependency (`uv.lock` unchanged).

---

## 10. Zero-burden copy (the MV-D23 obligation)

The cards render **exactly** the `17.0d` / `17.0e` register: recommendation-first,
plain new-vs-reuse-vs-reassign, "Why we're suggesting this," prominent Synonyms
(Pages), Related / Sources chips, certify, a do-it-yourself checklist, and a
Copy-for-Discover button. **Forbidden in rendered copy** (guarded by the vitest test):
`SET TAG`, `MERGE`, `metastore_id`, `workspace_id`, `genie_ont_`, `system.tags`,
`SQL warehouse`, `provenance tier`, `Lakebase`, `mirror`, `L6`. The reviewer sees the
recommendation and the evidence — never the machinery.

---

## 11. Tests (offline — `./scripts/test.sh` + `cd frontend && npm run test`)

**Wheel / backend (pytest):**

1. **Ranking order** — a fixture of mixed proposals ranks HIGH → LOW by
   `usage × centrality × governance`; the coverage cap keeps a corroborated finding
   above a single-signal one; `tier_of` thresholds are stable.
2. **PII / policy firewall** — a Domain proposal whose `tag_key`/`tag_value` name
   embeds an email/id is **blocked** (dropped from surfaced, counted in the report); a
   propose-only violation is rejected.
3. **Provenance ladder (dormant)** — the hook is a no-op today; a "T3 hint never
   outranks a T0 fact" test pins the seam for 17h.
4. **Suppression idempotency (MV-D26)** — with a `genie_ont_suppressions` row present,
   `mark_surfaced` sets `surfaced=false`; a re-run does not resurface it; a rejected
   `reassign` stays suppressed.
5. **Decision route** — `approve` → consents; `dismiss`/`reassign_reject` →
   suppressions; `reassign_accept` → consents; idempotent on the derived key;
   `decided_by` is the OBO email; **no `SET TAG`**.
6. **Metastore grain (MV-D49)** — scores/ledger keyed by `metastore_id`; MERGE delete
   metastore-scoped; `workspace_id` provenance only.
7. **Frozen contracts** — Phase-1/2/3a-c models byte-identical; the new models are
   append-only.
8. **Firewall extensions** — (a) router-verb test allows POST in `drafts.py`, still
   forbids PATCH/DELETE + governed-tag writes anywhere; (b) the wheel **reads** but
   **never writes** `genie_ont_consents`/`genie_ont_suppressions` (no MERGE/INSERT/
   UPDATE against them in `materialize.py`/`rank.py`); consents absent from the wheel
   entirely; no `web_search`; Lakebase-search tokens still confined to `similarity.py`.
9. **Degrade-not-hang** — a mirror/warehouse failure yields an empty typed payload.

**Frontend (vitest):**

10. **Zero-burden render** — `DomainDraftCard` (create / reuse / reassign variants) and
    `PageDraftCard` render the `17.0d`/`17.0e` copy from a fixture and contain **none**
    of the forbidden jargon tokens (§10); the reassign card shows the conflict tag + the
    evidence; Apply-for-me is disabled.

---

## 12. Definition of done

- `rank.py` + `transforms.py` extensions + the additive materialize step score and
  firewall every Domain/Page proposal at **metastore grain**, persist `score` +
  `surfaced`, and read (never write) the suppression ledger — idempotent, deterministic.
- `GET /api/ontology/drafts` serves ranked, surfaced, tiered drafts (incl. 17e
  `reassign`/`conflict`) mirror-first with a degrade-not-hang empty payload; `POST
  /api/ontology/decision` records consent/suppression under OBO with **no UC write**.
- The `17.0d`/`17.0e` cards render prop-driven zero-burden copy in a new Drafts tab;
  Apply-for-me is disabled (17i).
- `./scripts/test.sh` green over all §11 pytest cases; `cd frontend && npm run test`
  green on the vitest render test; `npm run lint` + `npm run build` + `tsc` clean;
  `uv lock --check` unchanged.
- **STOP** at the proposal-quality review checkpoint — do not proceed to 17h/17i.
