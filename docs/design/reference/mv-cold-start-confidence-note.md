# Confidence for evidence-poor spaces, and the cold-start quality question — research + design note (Prompt 15.7, decides MV-D32)

**Status: APPROVED — reviewer sign-off recorded; MV-D32 is DECIDED in the
playbook.** The research and the position on MV-D32(2) below stand as the
decision: **(1)** coverage-aware display and **(3)** cross-surface enrichment
shipped as-implemented; **(2)** the cold-start LLM-judgment signal is **deferred
per §5** (does not ship on this branch; a separately-labeled schema-validated
axis if it ever does). Both shipped parts leave the LYDS blend arithmetic and
MV-D15's availability honesty byte-untouched. The pre-planned follow-up that §2
item 2 noted — persist `uncapped_tier`/`tier_capped_by_coverage` and split
surfacing on them — is **now built as Prompt 15.7b** (see §2's updated note).

Read `MV-D32` in the playbook first. The one-line frame: the 34%-on-a-strong-
proposal defect is a **display conflation** before it is a scoring problem, and
the blend is not open for change.

---

## 1. The defect, precisely

Second smoke run: a real proposal governing **5 measures recurring across 18
curated queries** surfaced at **34% / LOW**, and the user asked the two
questions this prompt answers — "why so low, and can we improve without queries,
since tables are sometimes fresh?"

The displayed number conflates two things MV-D15 keeps separate:

- **How strong the available evidence is** — the renormalized blend
  (`mv_scoring.blended_score` divides the weighted sum by
  `evidence_coverage`, so the scale stays 0–100 over whatever was measured).
- **How much evidence is available** — `evidence_coverage`, the summed weight
  of the signals that actually ran (MV-D15).

On a fresh table, lineage (**L**, weight 0.35) and usage/demand (**D**, weight
0.15) are **structurally absent** — there is no lineage graph and no query
history to read. When the semantic producer (**S**, 0.20) is also unavailable
(no embedding endpoint), only curated-SQL recurrence (**Y**, 0.30) is left, and
the coverage cap (`capped_tier`) holds the tier at or below **LOW** no matter
how strong Y is. So "34% / LOW" is *mostly a statement about coverage*, not
about the proposal being doubtful. Rendering the bare blend as "confidence"
tells the user their strongest candidate is weak — which is false, and is the
exact honesty gap MV-D15 was written to avoid but that the **display** never
picked up.

---

## 2. What ships now (MV-D32(1) + (3)) — display and assembly only

**(1) Coverage-aware display.** The surfaced number is unchanged — the blend is
byte-untouched — but every card now carries a one-line **evidence-basis
caption** derived from `score_components.statuses` (the per-signal
COMPUTED/EMPTY/UNAVAILABLE map that already rides on every proposal):

- L and D both UNAVAILABLE → *"Based on curated SQL only — no usage history
  yet."*
- D and L available → *"Backed by usage history and lineage."*
- mixed → assembled from what ran.

So evidence-poor is *presented as* evidence-poor, not as low quality. The raw
blend, the weights, and `evidence_coverage` stay in `score_components` for the
debugging user (unchanged). Implemented as `confidenceDisplay` in
`frontend/src/components/auto-optimize/mvFormat.ts`; pure assembly, no LLM.

**(3) Cross-surface enrichment made visible.** The advisor upserts the *same*
candidate by fingerprint across surfaces: an IQ scan seeds it from curated SQL
(Y) and the semantic match (S); a later GSO run adds generated-SQL recurrence,
lineage (L) and usage/demand (D) — signals a cold scan **structurally cannot
produce**. So a COMPUTED D or L, or a non-empty `query_history_statement_ids`,
is *proof* the proposal grew beyond the initial scan. The card surfaces this as
"Evidence grew beyond the initial scan: +generated-SQL recurrence, +usage
signals, +lineage" (`evidenceGrowth` in `mvFormat.ts`). This is honest
assembly: the claim is falsifiable ("these signals cannot come from a scan"),
not a fabricated stored-snapshot delta. It shows nothing for a scan-only
proposal, so the line appears only when there is genuine cross-surface growth.

**A deliberate scoping note on (3).** The playbook's example phrasing is
"evidence grew in run <n>". A precise *run-numbered* delta ("+3 recurrence since
run 7") would need a versioned per-run evidence snapshot the candidate row does
not keep today — that is new machinery, and MV-D32(3) is explicitly "assembly,
not new machinery". The shipped surfacing therefore states the growth by its
*structural provenance* (signals a scan cannot yield) rather than by a stored
diff. Recorded here so the phrasing reads as a decision, not an omission.

**Tier re-examination (MV-D32(1)'s mandate).** MV-D30 surfaces MEDIUM+ by
default and hides explicit LOW behind a disclosure. Two candidate changes were
weighed and **rejected**:

1. *Move the thresholds.* Rejected: the blend is byte-untouched by constraint,
   and lowering `MV_TIER_*_MIN` would re-tier every space, not just cold ones —
   it would make MV-D30's "MEDIUM+" mean something new everywhere to fix a
   display problem in one place.
2. *Promote coverage-capped-high proposals into the primary list.* A proposal
   whose *uncapped* tier is MEDIUM+ but which coverage capped to LOW is exactly
   the "strong candidate buried" case. Rejected **for the display-only prompt
   (15.7)** because `uncapped_tier`/`tier_capped_by_coverage` are computed in
   `mv_scoring` (`to_payload`) but were **not persisted as candidate columns**,
   so the panel could not see them without an additive migration + exposure-
   matrix classification — out of scope for a display-only prompt, and the
   caption already closes the *honesty* gap the user hit (they saw the proposal;
   the number just lied about it).

Conclusion (15.7): thresholds and MV-D30 surfacing stand unchanged; the caption
is the fix.

> **Update — Prompt 15.7b (built).** The pre-planned follow-up below is now
> shipped, because the coverage-cap × MEDIUM+-default composition makes it
> load-bearing rather than optional: a genuinely-strong cold-start proposal
> (strong Y, L/D UNAVAILABLE) is capped to LOW and, under the 15.7 split, would
> sit behind the MV-D30 disclosure — reintroducing the "strong candidate
> buried" defect the caption only half-closed. 15.7b persists `uncapped_tier` +
> `tier_capped_by_coverage` additively (CREATE DDL + `ADDITIVE_COLUMN_MIGRATIONS`,
> both `wh_*`/Spark writers, exposure-matrix SERVED rows, MV-D21 written-column
> pin extended) and splits on them: a coverage-capped MEDIUM+ proposal joins the
> **default** list wearing a distinct **"Strong (evidence-limited)"** badge with
> the §2 caption — never a bare LOW, never behind the disclosure. Plain LOW
> (uncapped LOW included) stays behind the disclosure per MV-D30. The
> Recommended-badge ranking (15.6) orders a capped-strong proposal by its
> *uncapped* tier; the caption carries the honesty. Legacy rows (no persisted
> uncapped fields) fall back to the 15.7 tier-only split unchanged.

---

## 3. How the industry generates a semantic layer without usage history

The question behind MV-D32(2) — "can we improve without queries, since tables
are sometimes fresh?" — has a clear industry answer: **the mainstream semantic-
layer tools do not use query recurrence to author metrics at all.** They are
**schema-first**: metrics, dimensions, entities and join paths are declared
against the physical schema and its relationships.

- **dbt Semantic Layer / MetricFlow (latest spec).** Semantic models are
  annotations *inline on the model's columns* — `entity:` and `dimension:` sit
  on the columns, and `metrics:` (simple metrics with `agg` + `expr`, plus
  cross-model ratio/derived/cumulative/conversion) are authored declaratively.
  Nothing about the definition reads query history; the metric exists because a
  human (or a generator) declared it over the schema. (docs.getdbt.com, latest
  metrics spec.)
- **Snowflake Semantic Views.** Schema-level objects that define logical
  tables, **relationships**, facts, dimensions and metrics directly over the
  physical schema. Relationship *type* (1:1, N:1) is **inferred from the data
  and primary-key definitions**, not from usage. Cortex Analyst can even
  bootstrap a semantic view from a YAML spec generated off the schema.
  (docs.snowflake.com, semantic-view YAML spec.)
- **Cube / AtScale (universal semantic layers).** The same idea lifted above
  the warehouse: metric definitions, join paths and hierarchies declared once
  over the schema and served to every tool. AtScale explicitly *ingests*
  Snowflake Semantic Views as a foundation — the declared schema model is the
  source of truth, not observed queries.

The pattern is **deterministic ontology + (increasingly) LLM decomposition**:
the ontology (tables, columns, keys, declared relationships) is deterministic
and comes from the schema; an LLM's role, where used, is to *decompose a
business question into that ontology* or to *draft candidate metric
definitions* from column names/types/comments — always structurally validated
against the real schema before anything is trusted. This is direct evidence
that a **schema-first route can be credible when usage history does not exist
yet** — which is precisely the fresh-table case.

---

## 4. What an LLM-grounded quality judgment would add for fresh tables

Concretely, for a space whose tables are fresh (no corpus, no history, no
lineage), a judgment signal would work like this:

- **Inputs (all already reachable on this platform):** table + column metadata
  (names, types, comments), lightweight **profiling** (distinct counts, null
  rates, sample values, candidate keys — the same profiling GSO already runs,
  MV-D14), and any **curated context** (example questions, instructions) the
  space carries.
- **Model path:** the workbench's own model serving via
  `llm_utils` / `validate_chat_model` (the same OpenAI-compatible endpoint
  selection the Create Agent and Auto-Optimize use), so it inherits model
  choice, tracing and the availability contract.
- **Output, structurally validated like 17b's drafts:** a bounded, schema-
  grounded judgment — "these columns look like a governable measure of X on
  grain G" — that is *checked against the real schema* (columns exist, types
  aggregate, keys support the grain) before it can contribute anything. A
  judgment that references a column that does not exist is discarded, exactly
  as 17b discards an enrichment whose structure fails validation.

What it would add: a **fresh table would stop presenting as barren**. Today a
schema-only space is honestly `EMPTY` (MV-D25 — no corpus, no candidates). A
judgment signal could offer *candidate* measures with a clearly-bounded,
clearly-labeled confidence of a different kind.

---

## 5. Position on MV-D32(2) — the judgment signal does NOT ship on this branch

**Decision (proposed, for sign-off): defer the cold-start LLM-judgment signal.
It does not ship on the MV Advisor branch, and if it ever ships it must be a
separately-labeled signal that never shares a scale with recurrence.** Three
grounds:

1. **It collides head-on with MV-D25, which this branch does not own.** MV-D25
   records that schema-only suggestion is *owned by the create-agent branch,
   after Prompt 16, and is NOT decided here* — and by construction: the sole
   candidate producer is `candidate_from_measure` over a `FingerprintRecurrence`
   (`mv_advisor.py:646`). Every candidate on this branch is a measure that
   **recurred in SQL somebody wrote**, and a recurrence-backed proposal makes a
   specific, load-bearing promise (MV-D8): *"people already compute this."* A
   judgment-derived proposal makes a fundamentally weaker promise — *"a model
   thinks you might want this"* — and letting it enter through the advisor's
   scoring path would quietly retire MV-D8's pre-trusted premise. That is the
   create-agent branch's decision to make (SCHEMA_DERIVED provenance, MV-D25),
   and building it here would pre-empt it.

2. **The scale-sharing rule is non-negotiable and hard to honor inside one
   number.** MV-D32(2)'s own constraint: *a judgment-backed score never
   silently shares a scale with a recurrence-backed one.* A model's "this looks
   like a good measure" and the LYDS blend's "this recurred 18 times" are
   different epistemic objects. If a judgment signal ever ships, it must be a
   **new, separately-labeled axis** (its own provenance = SCHEMA_DERIVED, its
   own caption, never folded into the L/Y/S/D blend or the 0–100 confidence the
   card shows), so a user is never shown a schema-guess and a usage-fact wearing
   the same badge. The coverage-aware caption shipped in §2 is the groundwork:
   it already teaches the card to *say what the number is based on*, which is
   the surface a second axis would plug into.

3. **The honest display fix already answers the user's real question.** The
   user's "why so low?" is answered by the caption (evidence-poor, not weak),
   and "can we improve without queries?" is answered truthfully today by MV-D25
   + the cross-surface enrichment surfacing (§2.3): the number **rises by
   construction** as a GSO run adds generated-SQL recurrence and full L/D, and
   the card now says so. That closes the felt problem without taking on a new
   scoring signal whose ownership sits on another branch.

**Relationship to MV-D25 and Prompt 18, stated without pre-empting either.** If
the create-agent branch decides (per MV-D25) to author schema-derived metric
views, and Prompt 18's profiling route lands, then a cold-start judgment signal
becomes a natural *joint* deliverable of those two — SCHEMA_DERIVED provenance
from MV-D25, profiling inputs from Prompt 18, structural validation from 17b's
pattern. This note takes no position on *their* timing or design; it only fixes
the contract that binds any future signal: **separately labeled, schema-
validated, never scale-shared with recurrence.**

---

## 6. Checkpoint

This note ends here, per the Prompt 15.7 body. Shipped alongside it (display +
assembly only, blend untouched): the coverage-aware confidence caption and the
cross-surface enrichment line, with tests. **Deferred per §5 (approved):** the
cold-start LLM-judgment signal — MV-D32(2) does not ship on this branch and, if
ever, only as a separately-labeled schema-validated axis under the
scale-sharing rule. On sign-off MV-D32 was recorded **DECIDED** in the playbook
with (1) and (3) as-implemented and (2) deferred.

The one follow-up this note pre-planned (§2 item 2) — persisting
`uncapped_tier`/`tier_capped_by_coverage` and splitting surfacing on them — is
built as **Prompt 15.7b**, whose scope is persistence + presentation only
(still no blend change, still no new scoring signal): the two fields already
exist on `ScoredProposal.to_payload`; 15.7b only carries them to the candidate
row and the panel.
