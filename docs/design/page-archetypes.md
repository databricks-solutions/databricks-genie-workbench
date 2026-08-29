# Ontology curation standard: Domains, Sub-Domains, Pages (Prompt 17.0)

> Status: completed at Prompt 17.0, RE-SCOPED by MV-D36 + MV-D37. The Ontology
> surface is a standalone, admin-gated, estate-wide page (not a per-Agent tab):
> it proposes a **Domain → Sub-Domain → Page** taxonomy across every Genie Agent
> in the workspace and every metric view in the account. This doc is the standard
> the 17.x track builds to — the governed-tag substrate, the archetype
> vocabulary, the format templates (Domain/Sub-Domain AND Page), the dedupe +
> permission contracts, and the detection framework. Its "How Genie consumes what
> we write" section is NORMATIVE and must stay consistent with
> `genie-retrieval-notes.md`; if the two ever disagree, fix them in the same
> commit (this pair is the track's rules-parity analogue).

The **Genie Ontology** has three governed layers, from coarse to fine: **Domains
→ Sub-Domains → Pages**. Domains and Sub-Domains are the *organization* layer;
Pages are the *definition* layer. The advisor proposes all three, plus advice on
the **metric views** and **Genie Agents** they organize.

A **Page** is a governed, authoritative definition of a business concept. Genie
One prioritizes a Page's definition over context it infers automatically and
cites the Page in its answer. Pages have **no public create/update API**
(UI-created, Genie Code assist in the Page editor), and **no read API** (so
Page-level dedupe is best-effort — name/synonym heuristics only). A Page carries
seven fields (`https://docs.databricks.com/aws/en/uc-semantics/pages`): **Domain,
Owner, Synonyms, Description, Page body, Related assets, Sources**; each Page
belongs to exactly one Domain or Sub-Domain.

A **Domain / Sub-Domain IS a governed tag** (MV-D37,
`https://docs.databricks.com/aws/en/uc-semantics/domains`): "to create a
'Marketing' domain you must create a 'Marketing' governed tag," and membership is
`SET TAG` on the asset. Sub-Domains use a `{parentDomainTag}/{subdomainName}`
convention (e.g. `Finance/Tax`), parent and child being independent tags.
Governed tags apply to tables, dashboards, metric views, **and Genie Agents**.
This substrate — not the Discover card — is what makes the ontology partly
writable and makes dedupe mandatory; see the next section.

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
   snippet loses silently, which is worse than not shipping. The check is
   READ-ONLY (Page-only — the advisor never writes those instructions).

**Corroboration, the Page-only way.** Authority rises when the same rule
appears in independent assets. The advisor does NOT manufacture that by writing
a second copy into the Agent's instructions (MV-D27). It earns corroboration
structurally: a Page's **Sources** point at the metric view, tables, and
certified dashboard/query the formula derives from, and its **Related assets**
point at the Genie Agent(s) the concept serves. A Page that restates a pattern
already present in the space's dashboard SQL or instructions corroborates that
pattern by pointing at it — no duplicated prose the advisor owns and must keep
in sync.

## Format template

- **Title**: `[Archetype] Name` — the prefix is part of the format, not
  decoration; it is how the archetype is read back.
- **Domain / subdomain**: where the Page is filed in Discover. The draft
  RECOMMENDS one; filing is a manual UI step.
- **Owner**: defaults to the person publishing the Page.
- **Synonyms** (rule 3 above): ≥3 across the four classes. Surfaced prominently
  in the draft — the user pastes them into the dedicated Discover field.
- **Description**: one line — the concept in a sentence.
- **Definition**: the load-bearing body. Answers the archetype's question using
  the space's OWN identifiers (tables, columns, measures — backticked). An
  identifier that does not exist in the space fails validation. Written
  chunk-safe (rule 1) and in the snippet type the extractor rewards for the
  archetype (mapping table below).
- **Rules**: explicit never-do-X warnings where the archetype carries them
  ("Never average pre-computed rates across periods") — these become the
  highest-value `BUSINESS_LOGIC` snippets.
- **Related assets**: Databricks assets that logically connect to, depend on,
  or contextualize the Page — **the Genie Agent(s) the concept serves**,
  sibling/parent/child Pages, and dependent metrics. This is where the
  concept↔Agent link lives; it is an asset reference, not a sentence in the
  body. Emitted as copy-ready identifiers (the Agent's space id/name).
- **Sources**: the assets that are the ORIGIN of the definition — the metric
  view definition, its source tables, any certified dashboard or query the
  formula derives from, plus the external links a Recent-context section cites.
  Emitted as copy-ready FQNs.
- **Certify recommendation**: the draft states whether this Page should be
  marked Certified in the Discover UI (authoritative formulas: yes;
  informational context: no). Certified wins conflicts absolutely, so the
  recommendation is part of the deliverable, not an afterthought.
- **Recent context (informational, as of DATE)** — OPTIONAL. Present only when
  web enrichment succeeded. MUST end with: *"Informational context summarised
  by an LLM from public sources as of DATE. Not certified operational data."*
  MUST carry a Sources list. Absent entirely otherwise — never unlabeled,
  never unsourced. Never part of a certify-yes recommendation.
- **Lifecycle**: drafts are Draft until a human publishes in the Discover UI.

## Output contract: propose-only default, one consented write (MV-D27 + MV-D37)

The advisor's DEFAULT everywhere is **propose-only / copy-ready**. There is
exactly ONE optional write it will ever offer: the consented `SET TAG`
domain/sub-domain **membership** apply (MV-D37), default OFF, dry-run-first,
gated on the curator's permissions and the MV consent rails. It never writes
Agent instructions, never writes Pages, never writes the Discover card. The
Page sub-contract below is unchanged.

### Page output (MV-D27)

Each accepted Page suggestion produces exactly ONE artifact: a **Page draft** in the
format above, copy-ready for paste into Discover. The advisor never writes the
Agent's `text_instructions` and never writes any live space config — there is
no instruction-augmentation half, no diff, no apply. Two facts drive this:

1. The Page is the strictly higher-trust output (certified Pages win conflicts
   absolutely; Pages outrank instructions). Writing the lower-trust copy would
   be the weaker half of a pair, competing for the scarce single instruction
   block (~2000-char warning) against what a human wrote.
2. The link the instruction copy was meant to provide — "this rule belongs to
   this Agent" — is already a first-class Page field. A Genie Agent is a
   Discover asset (`https://docs.databricks.com/aws/en/discover/discover-page`
   lists "tables, dashboards, and Genie Agents"), so the advisor lists the
   Agent under **Related assets**. No back-pointer prose is added to the body.

Because Pages also have no create/update API, the Page surface is
copy-ready-draft-only: the advisor hands the user the Page body plus the
Related/Sources identifiers to `@`-tag in the Page editor. The manual steps are
named plainly in the UI, never dressed up as an API.

## The governed-tag substrate — Domains & Sub-Domains (MV-D37)

A Domain is a governed tag; a Sub-Domain is a `{parent}/{child}` governed tag.
This yields **three write tiers**, only one of which the advisor may act on:

| Layer | Mechanism | Advisor behaviour |
|---|---|---|
| Domain/Sub-Domain **membership** (the tag + `SET TAG` on the asset) | Governed-tag DDL (`CREATE/ALTER/DROP GOVERNED TAG`, `SET/UNSET TAG` DBR 16.1+) | **Optional consented OBO write** (default OFF, dry-run-first) |
| Discover **card** (subtitle, description, publish, sections) | Discover UI | Copy-ready only |
| **Pages** | Discover UI (Genie Code assist) | Copy-ready only |

**Enumeration + governance reads.** Existing governed tags come from
`system.tags.governed_tags` (one row per governed key; `deleted_at IS NULL` for
live) and `SHOW GOVERNED TAGS [LIKE …]` (allowed values). Assignments come from
`system.information_schema.{catalog,schema,table,column}_tags` (OBO,
auto-filtered, no explicit grant), joined on `tag_name = tag_key` to mark
`is_governed`. Tags already acting as Domains/Sub-Domains are detectable by the
`/` convention plus Discover metadata. Certification uses the system
`certified`/`deprecated` governed tags.

**Guards.** Tag names are stored as plaintext and replicated globally — the PII
firewall applies to proposed tag KEYS and VALUES, not just Page bodies. Governed
tag policies constrain allowed values; a proposal must fit the policy or propose
a policy change explicitly.

## Governed-tag dedupe — reuse-vs-create (MV-D37)

Because a Domain proposal is really "reuse or create a governed tag," every
Domain/Sub-Domain proposal carries an explicit **reuse-vs-create** decision, run
BEFORE anything is surfaced:

1. **Block** by prefix / catalog / schema to bound comparisons.
2. **Score** the proposed name against every existing governed tag key by fuzzy
   similarity (case/plural/tokenization — `Sales` vs `sales` vs `Sales_`) AND
   embedding + keyword similarity (Lakebase Search: `lakebase_vector` cosine +
   `lakebase_text` BM25, on the existing Lakebase; degrades to in-process cosine).
3. **Adjudicate** near-ties with the LLM (company context in scope).
4. **Emit** one of: *reuse `Finance/Tax`*, *create `Finance/Audit`*, or *merge/
   alias* — never a silent duplicate.

The lens also surfaces **cleanup**, not just additions: orphan governed tags
(no Domain), near-empty Domains, and deprecated-but-still-assigned tags. The
same entity-resolution pass runs over {governed tags, Domains, measures, Pages,
Agents} so overlaps are caught across object types.

## Permission tiers (the preflight banner)

Full ontology visualization unlocks in five tiers; the banner is a
capability→permission MATRIX, each row ✓/✗ with a copy-ready grant. Read tiers
degrade gracefully; the write and enrichment tiers are never required to view.

| Capability | Exact permission | Identity |
|---|---|---|
| Metric-view + tag **inventory** | none — `system.information_schema` auto-filters | OBO |
| **Usage / lineage / cost ranking** | `USE CATALOG system` + `SELECT` on `system.access.audit`, `system.access.table_lineage`, `system.query.history`, `system.billing.usage` | SP |
| **Governed-tag graph** (dedupe) | `SELECT` on `system.tags.governed_tags` | SP |
| **Membership write** (optional apply) | `MANAGE DISCOVERY` (account/domain) + `ASSIGN` on each governed tag + `APPLY TAG` / `USE SCHEMA` / `USE CATALOG` | OBO |
| **Context sources** — external enrichment (optional, MV-D38 / MV-D46 / MV-D47) | `EXECUTE` on the enabled Unity AI Gateway MCP services (`system.ai.web_search`, You.com, Confluence / Drive / M365, internal Genie / SQL) — opt-in, default OFF; toggle disabled when no source is available | Batch |

## Domain / Sub-Domain draft format

```
Kind:          Domain            (or: Sub-Domain of `Commercial`)
Name:          Commercial
Tag decision:  CREATE governed tag `Commercial`   (or: REUSE existing `Commercial`)
Description:   Revenue-generating go-to-market: sales, marketing, partners.
Sub-Domains:   Sales, Marketing, Partnerships          (Domains only)

Member assets (SET TAG targets — @-tag or apply):
  - Genie Agent · Sales performance · 01ef9a2b3c4d5e6f
  - finance.sales.order_revenue            (metric view)
  - marketing.campaigns.channel_performance (metric view)

Why this grouping (evidence):
  3 Agents + 6 metric views share the `finance.sales` spine (lineage) and are
  41% of Genie query volume (30d, usage). Company context placed these under a
  Commercial domain rather than a generic "Sales" top level.

DDL preview (optional consented apply — default OFF, dry-run-first):
  CREATE GOVERNED TAG `Commercial`;
  ALTER TABLE finance.sales.order_revenue SET TAGS ('Commercial');
  -- …one SET TAG per member asset…

Manual path (Discover UI — card is UI-only):
  1. Create the Domain "Commercial" in Discover (select/create the tag).
  2. Add the 3 Sub-Domains.
  3. @-tag member Agents + metric views into the matching Sub-Domain.
  4. File the Page drafts under each Sub-Domain.
```

The block above is the **internal draft representation** (tag decision, DDL,
`SET TAG` targets) — the substrate the engine reasons over. It is **not** what
the curator reads. Applying the zero-burden principle from MV-D38 to the whole
curator surface: on the page, a Domain draft shows only the **recommendation**
(name, description, sub-domains, member assets), a plain-language **new-vs-reuse**
line ("New domain — we didn't find an existing one like this"), a **"Why we're
suggesting this"** reason, and a single simple choice — **Apply for me**
(preview-first; we run the tag/`SET TAG` machinery behind the scenes) or
**do-it-yourself** steps. No `CREATE GOVERNED TAG`, no `SET TAG`, no
`MANAGE DISCOVERY`, no system-table names appear in the curator draft; that
machinery lives in the admin **permission banner** and **Governed-Tags lens**,
not the draft. See mockup frames `17.0d` (Domain draft) and `17.0e` (Page draft,
which leads with the same "Why we're suggesting this" reason). Pages then sit
UNDER the Sub-Domains.

## Detection framework (17a inputs — signals × objects × methods)

Signals: **structural** (`information_schema` tables/columns/views, metric views
via `table_type='METRIC_VIEW'` + `view_definition` YAML, PK/FK join-key graph),
**lineage** (`system.access.table_lineage` / `column_lineage`), **usage**
(`system.query.history` co-occurrence, `system.access.audit`, `billing.usage`),
**governance** (`system.tags.governed_tags`, `information_schema.*_tags`),
**Agent context** (`list_spaces` + `serialized_space`), **semantic** (Vector
Search embeddings), **company prior** (Settings company name → industry taxonomy).

Methods:
- **Domains / Sub-Domains** — cluster the asset graph by catalog/schema prior +
  community detection (Louvain / label-propagation) on lineage + join-key edges
  + co-query co-occurrence + Agent-table-set priors + existing-tag reuse, named
  by LLM with the company prior. Sub-Domains = finer communities in a Domain.
- **Pages** — mine measures (routing), same-term conflicts (disambiguation),
  coded columns (taxonomy), non-additive rates (guardrail); dedupe best-effort
  (no Page read API).
- **Metric views** — gaps (recurring ungoverned aggregations), duplicates
  (overlapping measure definitions), quality (missing synonyms/comments).
- **Genie Agents** — infer domain from their tables' tags/lineage; flag overlap/
  consolidation; recommend tagging Agents into Domains.

Ranking is evidence-first (MV-D35): usage × lineage-centrality × governance
status, never a naked confidence %. All writes are dry-run/consent-gated.

## External context / enrichment (MV-D38)

External context (industry, competitor, company, Wall-Street filings, regulatory,
and Databricks industry data models) makes proposals domain-aware — but it is a
**naming/description/hypothesis PRIOR and informational overlay, NEVER structural
truth**. The graph (system tables) decides *membership*; the outside world only
decides *vocabulary* and informational overlay. This is the "Recent context"
contract generalized: labeled, sourced, dated, `certify-no`.

**Provenance ladder (higher tiers win — MV-D35):**

| Tier | Source | May influence | May NEVER touch |
|---|---|---|---|
| T0 Internal-verified | system tables, `information_schema`, `serialized_space` | membership, measures, certification | — (ground truth) |
| T1 Company-official | their filings / docs they provide | descriptions, synonyms, names, Recent-context | membership, measure definitions |
| T2 Industry-canonical | Databricks industry models, GICS/NAICS, standards | cluster naming, gap *hypotheses*, synonyms | anything structural, certification |
| T3 Web-inferred | competitors, news, general web | weak naming hints, dated Recent-context | names alone, any number, membership |

**Two plug-in points, nowhere else:** the cached, versioned, self-validated
**Context Pack** — an INTERNAL artifact, never a user surface — (resolved before
the run; steers Domain/Sub-Domain naming + gap hypotheses) and Page
**Recent-context** (informational overlay). **Zero user burden:** the engine does
the heavy lifting and the user only ever sees clean Domain/Sub-Domain/Page
suggestions; the whole external-context surface is one opt-in toggle + one
optional low-confidence confirm — no pack versions, tiers, or approvals. Rails:
structural firewall (external reaches only naming/description/synonym/gap
functions), no-unsourced-numbers (every financial/regulatory figure cites a URL +
as-of date or is dropped), recency decay on T3, confidence-gating (a
low-confidence company→industry map suppresses the canonical-domain gap-check),
and the opt-in **Context Sources** tier — a registry of Unity AI Gateway MCP
services (MV-D46 governed `system.ai.web_search` + fallback ladder; MV-D47
`{class, provenance_tier, influence}` registry, `EXECUTE`-granted + service-policy
governed; no source available → estate-only naming, nothing breaks). Full
mechanics: `ontology-engine-architecture.md` §6 / §6.1.

## Archetype → snippet type, asset emphasis, and certify default

The snippet type is what the extractor rewards (write in that shape); the asset
emphasis is which of Related/Sources carries the weight for that archetype; the
certify default follows the "formulas yes, context no" rule.

| Archetype | Dominant snippet type(s) | Related / Sources emphasis | Certify default |
|---|---|---|---|
| `[Disambiguation]` | `BUSINESS_LOGIC` (identity rules) | Related: the Agent + the sibling Pages the terms resolve to | Yes |
| `[Method]` | `METRIC_DEFINITION` | Sources: the metric views each method reads | Yes |
| `[Routing]` | `BUSINESS_LOGIC` + `METRIC_DEFINITION` (NL → view + measure) | Sources: the metric view + measure; Related: the Agent | Yes |
| `[Taxonomy]` | `TABLE_SEMANTICS` / `FILTER` | Sources: the coded table/column | No (unless a governed code list) |
| `[Guardrail]` | `BUSINESS_LOGIC` ("never average pre-computed rates") | Sources: the numerator/denominator measures | Yes |
| `[Defaults]` | `FILTER` / `BUSINESS_LOGIC` | Related: the Agent (whose default this is) | Yes |
| `[Rule]` | `FILTER` (structural-break exclusions) | Sources: the table/period the break lives in | Yes |
| `[Cross-domain]` | `JOIN_HINT` | Related: both domains' Agents/Pages; Sources: the shared spine table | Yes |

## Worked example — `[Routing]`

```
Title:        [Routing] Discounted revenue
Domain:       Finance / Sales
Owner:        (publisher)
Synonyms:     net revenue, revenue after discount, discounted sales,
              "how much did we actually make", disc rev
Description:  Revenue net of line-item discounts, the default "revenue" for
              the sales Agent.

Definition:
  For "revenue", "net revenue", or "how much did we make" over the sales
  Agent, answer from the governed metric view `finance.sales.order_revenue`
  using its `total_revenue` measure — never from a raw SUM over
  `finance.sales.order_items`. `total_revenue` is
  SUM(items.quantity * items.unit_price), evaluated inside the metric view
  so its join to `finance.sales.orders` and its discount handling stay
  consistent across every question.

Rules:
  - Route "revenue" for the sales Agent to
    `finance.sales.order_revenue.total_revenue`; do not hand-write a SUM
    over `finance.sales.order_items`.

Related assets:
  - Genie Agent · 01ef9a2b3c4d5e6f  (sales Agent — the concept serves it)
  - Page · [Guardrail] Rate measures are non-additive  (sibling)

Sources:
  - finance.sales.order_revenue        (metric view — the definition)
  - finance.sales.orders               (source table)
  - finance.sales.order_items          (source table)

Certify recommendation: YES — this is an authoritative routing formula;
certification makes Genie prefer it absolutely over any inferred SUM.
```

`Related assets` names the Agent — that IS the link back to it; the body says
nothing like "this came from the sales Agent." `Sources` names where the
formula lives. Both are copy-ready identifiers the user `@`-tags in the editor.

## Manual publish checklist (named plainly in the UI — Pages are UI-created)

1. Create the Page in Discover, filed under the recommended **domain /
   subdomain**.
2. Paste the **Synonyms** into the synonyms field (retrieval-critical).
3. Under **Related assets**, `@`-tag the listed Genie Agent(s) and sibling
   Pages.
4. Under **Sources**, `@`-tag the listed metric view and source tables.
5. If the draft recommends **Certify: YES**, mark the Page Certified — certified
   wins conflicts absolutely, the single strongest retrieval lever.
