# How Genie consumes curation — retrieval notes for the Ontology Pages track

> Provenance: distilled 2026-08-24 from a Genie-generated architecture
> self-description ("How Genie Works: Architecture, Retrieval, and Curation
> Best Practices", produced by Databricks Genie against a live workspace).
> Treat as observed behavior of the current system, not a versioned API
> contract — re-verify the load-bearing claims if this note is more than a
> quarter old. The Prompt 17.x bodies and `page-archetypes.md` cite this file;
> keep the three in sync.

## The loop (what a Page is FOR)

Genie is a multi-hop retrieval-reasoning agent, not a knowledge-graph walker:
parallel discovery (asset search + **ontology search** + user activity) →
inspect (schemas, dashboard SQL, space instructions, Pages) → write SQL from
curated patterns → verify → respond **with citations**. The ontology search
runs BEFORE any table schema is read — a Page that gets retrieved shapes the
SQL from the first token.

## Ontology snippets (the unit of retrieval)

Pages, space instructions, dashboard SQL, and notebooks are auto-extracted
into indexed **snippets**. Each carries:

- **Content** — the rule / formula / hint itself
- **Type** — `BUSINESS_LOGIC` | `METRIC_DEFINITION` | `JOIN_HINT` | `FILTER`
  | `TABLE_SEMANTICS` | `OTHER`
- **Source asset** and an **authority score**

Authority is driven by: **source type** (curated space instructions and Pages
outrank auto-extracted dashboard patterns), **corroboration** (highly similar
content in multiple independent assets scores higher), and **specificity**
(exact tables/columns/formulas outrank vague guidance). On conflict, the
higher-authority snippet wins.

**Consequences for anything we generate:**

1. **Chunk-safe writing.** A snippet is an extracted chunk — a rule must carry
   its meaning standing alone (name the metric/table IN the sentence; never
   rely on the page title or a previous bullet for context).
2. **Specificity is not style, it is ranking.** Backticked exact identifiers
   and literal formulas raise authority; "be careful with rates" is invisible.
3. **Corroboration is a feature — earned structurally, not by duplication.**
   Authority rises when a rule appears in independent assets. The advisor does
   NOT manufacture that by writing a second copy into the Agent's instructions
   (MV-D27, Page-only): a Page's **Sources** point at the metric view / tables /
   certified dashboard the formula derives from, and its **Related assets**
   point at the Genie Agent(s) it serves, so a Page that restates an existing
   dashboard-SQL or instruction pattern corroborates it by pointing at it — no
   duplicated prose the advisor owns and must keep in sync.
4. **Conflicts are lost silently.** A vague new rule that contradicts an
   existing higher-authority snippet simply loses. Drafts must be checked for
   contradiction against the space's existing instructions before they ship.

## The trust stack (ranked, highest first)

1. **CERTIFIED Pages** — absolute: Genie will not contradict them, and cites them
2. Non-certified Pages
3. Genie space instructions
4. Dashboard SQL (validated patterns)
5. Metric views (`MEASURE()` columns)
6. Auto-extracted ontology snippets
7. Table/column descriptions
8. Bare schema (guessing)

So: the Page draft is the **highest-trust** output the advisor can produce, and
under Page-only (MV-D27) it is the ONLY output — the advisor never writes the
lower-trust instruction block. A page worth writing is usually worth
recommending for certification (a manual UI step; certified wins absolutely on
conflict).

## Synonyms (retrieval-critical, not metadata)

Ontology search is semantic similarity over the user's phrasing. Without
synonyms, "What's our A14 rate?" misses a page titled "DOT On-Time
Definition". Coverage checklist per page: **industry acronyms** (A14, OTP,
RASM), **casual language** ("how late are we"), **internal jargon**, and
**abbreviation variants** ("on-time" / "on time" / "ontime").

## What schema alone can never answer (the archetypes' hunting ground)

Non-additivity of a rate; whether an entity name means one thing or a family;
denominator inclusion rules; structural-break periods to exclude; real vs
synthetic data; the business reason behind a standard filter. Every one is a
curation opportunity — and each maps onto an archetype in
`page-archetypes.md`.

## Failure modes → mitigations (Genie's own table)

| Failure | Cause | Mitigation |
|---|---|---|
| Right page not found | Missing synonyms | Add synonyms |
| Wrong page found | Ambiguous term, no domain scoping | Domain/subdomain tags |
| Outdated pattern used | Deprecated asset unmarked | Mark deprecation explicitly |
| Missed critical filter | Rule in no curated source | Space instructions |
| Wrong join | No join hint | Join hints in descriptions/ontology |

## Archetype → snippet-type mapping (write in the shape the extractor rewards)

| Archetype | Dominant snippet type(s) |
|---|---|
| [Disambiguation] | BUSINESS_LOGIC (identity rules) |
| [Method] | METRIC_DEFINITION |
| [Routing] | BUSINESS_LOGIC + METRIC_DEFINITION (NL → view + measure maps) |
| [Taxonomy] | TABLE_SEMANTICS / FILTER |
| [Guardrail] | BUSINESS_LOGIC ("never average pre-computed rates") |
| [Defaults] | FILTER / BUSINESS_LOGIC |
| [Rule] | FILTER (structural-break exclusions) |
| [Cross-domain] | JOIN_HINT |
