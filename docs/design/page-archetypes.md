# Page archetypes and format standard (SEED — Prompt 17.0 completes this)

> Status: seed committed at the Prompt 17 redraft so the archetype vocabulary
> has one home from day one. **Prompt 17.0 owns this file** — it completes the
> template, the per-archetype instruction-vs-Page-only defaults, and the
> worked examples, and it may restructure everything below the table. Until
> 17.0 runs, this file is the vocabulary, not the standard.

A Page is a governed, authoritative definition of a business concept in
Databricks Discover — the human-modeled layer of the Genie Ontology. Genie One
prioritizes a Page's definition over context it infers automatically and cites
the Page in its answer. Pages have **no public create/update API** (UI-created,
Genie Code assist in the Page editor), which is why the workbench emits
copy-ready drafts rather than writing Pages directly.

## The eight archetypes

| Archetype | Title prefix | What it does |
|---|---|---|
| Disambiguation | `[Disambiguation]` | Same English phrase, several valid answers — pick which grain/count/role |
| Method selection | `[Method]` | Several correct metrics; pick by the question, not habit |
| Metric routing | `[Routing]` | Map NL questions to the metric view + measure |
| Status / enum decode | `[Taxonomy]` | Decode codes, buckets, glossaries |
| Guardrail / non-additivity | `[Guardrail]` | Never average rates; recompute from numerator/denominator |
| Default assumptions | `[Defaults]` | What to assume when the question is silent |
| Comparability / break | `[Rule]` | A rule that makes periods or universes incomparable |
| Cross-domain traversal | `[Cross-domain]` | Shared spines and join keys across domains |

## How Genie consumes what we write (normative — see `genie-retrieval-notes.md`)

Pages and instructions are auto-extracted into **chunked ontology snippets**
ranked by semantic similarity × an **authority score** (source type,
corroboration, specificity), searched BEFORE any table schema is read.
Certified Pages sit at the top of Genie's trust stack and win conflicts
absolutely; Pages outrank space instructions, which outrank metric views.
Four writing rules follow, and the 17b validator enforces the first three:

1. **Chunk-safe**: every rule sentence stands alone — it names the metric,
   table, or measure it governs IN the sentence, never relying on the title
   or a prior bullet for context.
2. **Specific**: exact backticked identifiers and literal formulas — vague
   guidance ("be careful with rates") scores low authority and loses.
3. **Synonym-covered**: ≥3 synonyms spanning the four classes — industry
   acronyms, casual phrasings, internal jargon, abbreviation variants.
   Synonyms are retrieval, not metadata: without them the page is invisible
   to the search that decides whether it is read at all.
4. **Non-contradicting**: a draft is checked against the space's existing
   instructions for conflicts before it ships — a contradicted low-authority
   snippet loses silently, which is worse than not shipping.

## Format template (to be completed by 17.0)

- **Title**: `[Archetype] Name` — the prefix is part of the format, not decoration.
- **Header fields**: Domain, Owner, Synonyms (per rule 3 above), Description
  (one line).
- **Definition**: the load-bearing body. Answers the archetype's question using
  the space's OWN identifiers (tables, columns, measures — backticked). An
  identifier that does not exist in the space fails validation. Written
  chunk-safe (rule 1) and in the snippet type the extractor rewards for the
  archetype (mapping table in `genie-retrieval-notes.md`).
- **Rules**: explicit never-do-X warnings where the archetype carries them
  ("Never average pre-computed rates across periods") — these become the
  highest-value BUSINESS_LOGIC snippets.
- **Related**: cross-links to sibling pages ("grouping logic lives on Carrier
  Merger Lineage") — the demonstrator convention.
- **Certify recommendation**: the draft states whether this page should be
  marked Certified in the Discover UI (authoritative formulas: yes;
  informational context: no). Certified wins conflicts absolutely, so the
  recommendation is part of the deliverable, not an afterthought.
- **Recent context (informational, as of DATE)** — OPTIONAL. Present only when
  web enrichment succeeded. MUST end with: *"Informational context summarised
  by an LLM from public sources as of DATE. Not certified operational data."*
  MUST carry a Sources list. Absent entirely otherwise — never unlabeled,
  never unsourced. Never part of a certify-yes recommendation.
- **Lifecycle**: drafts are Draft until a human publishes in the Discover UI.

## Dual output — corroboration, not either/or

Each accepted suggestion may produce both:

1. **Instruction augmentation** (API-applied, consent-gated, OBO): a
   GSL-schema-compliant delta to `text_instructions[0]`.
2. **Page draft** (manual): copy-ready markdown in the format above, pasted
   into Discover by the user. Note the trust stack: the Page is the
   HIGHER-trust output; the instruction delta is the API-writable one.

Because corroboration raises authority, the TOP rules (identity, guardrail,
routing) appear in BOTH outputs with deliberately consistent wording — the
duplication is the point, not waste. Instruction space stays scarce (single
block, ~2000-char warning), so: Routing / Guardrail / Defaults /
Disambiguation earn instruction lines AND a Page; long Taxonomies are
Page-first with a one-line instruction pointer. 17.0 finalizes this mapping.
