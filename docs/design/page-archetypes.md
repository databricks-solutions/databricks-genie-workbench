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

## Format template (to be completed by 17.0)

- **Title**: `[Archetype] Name` — the prefix is part of the format, not decoration.
- **Header fields**: Domain, Owner, Synonyms, Description (one line).
- **Definition**: the load-bearing body. Answers the archetype's question using
  the space's OWN identifiers (tables, columns, measures — backticked). An
  identifier that does not exist in the space fails validation.
- **Recent context (informational, as of DATE)** — OPTIONAL. Present only when
  web enrichment succeeded. MUST end with: *"Informational context summarised
  by an LLM from public sources as of DATE. Not certified operational data."*
  MUST carry a Sources list. Absent entirely otherwise — never unlabeled,
  never unsourced.
- **Lifecycle**: drafts are Draft until a human publishes in the Discover UI.

## Dual output

Each accepted suggestion may produce both:

1. **Instruction augmentation** (API-applied, consent-gated, OBO): a
   GSL-schema-compliant delta to `text_instructions[0]`. Instruction space is
   scarce (single block, ~2000-char warning), so Routing / Guardrail /
   Defaults / Disambiguation earn instruction lines; long Taxonomies are
   Page-first with a one-line pointer. 17.0 finalizes this mapping.
2. **Page draft** (manual): copy-ready markdown in the format above, pasted
   into Discover by the user.
