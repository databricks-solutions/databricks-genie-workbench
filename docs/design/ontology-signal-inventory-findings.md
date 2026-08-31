# Ontology — Live signal-inventory findings (curation redesign evidence)

**Source:** read-only census of `serverless_stable_6t92c3_catalog` (metastore of the
`fevm-serverless` workspace), warehouse `41cfe645e10807a4`, run 2026-08-31 as the OBO
admin. **Purpose:** ground the Stage-1 facet classifier and the Stage-3
`domain_facet_denylist` / legitimacy defaults in real data, and give the deploy-verify
gate a concrete target. Feeds `ontology-curation-redesign-build.md` (§Appendix A,
§5.2, §7) and MV-D51 / D52 / D54 / D57.

> **Headline.** The estate now carries a **proper governed taxonomy that postdates the
> last materialize run** — 4 governed domains × 18 governed sub-domains (slash
> convention) — plus a clean, unambiguous facet set and a value-carrying
> `mvm_subdomain` sub-domain tag. The old "junk domains" (`Contains Synthetic`, `Data
> Tier`, a tag named `Domain`) were an artifact of running before this taxonomy
> existed AND of never filtering facets. Stage 1's rules-first grouping should now
> surface the real taxonomy, corroborated by 200 FKs and business-named schemas.

## 1. Aboutness — governed domains to REUSE (MV-D51/D53 precedence #1)

All active governed tags (`system.tags.governed_tags`), slash = sub-domain (MV-D54
explicit boundary #1). Assignment counts across all UC levels:

| Domain (governed tag) | assigns | Sub-domains (slash) |
|---|---|---|
| `Alaska Airlines Commercial` | 48 | Ticketing and Settlement (15), Fare and Pricing (11), Yield Management (9), Reservation (5), Loyalty (5), Passenger (3) |
| `Alaska Airlines Maintenance and Engineering` | 19 | Compliance and Airworthiness (8), Work Orders (8), Asset Registry (3) |
| `Alaska Airlines IFEC` | 15 | Reliability (6), Spares and Movement (4), Ground Windows (2), Configuration (2), Traceability (1) |
| `Alaska Airlines Operations` | 12 | Route (4), Flight (4), Airport (2), Fleet (2) |

These are the **reuse** targets: multi-word, title-cased, slashed, present in
`governed_tags`, and NOT in the facet set below. The classifier keeps them as
ABOUTNESS.

## 2. Facets — route OUT of domain candidacy (MV-D51/D57)

Clean, unambiguous. Recommended shipped `domain_facet_denylist` (patterns, not just
exact keys, so future estates are covered):

| Facet key (observed) | assigns | distinct values | Facet class |
|---|---|---|---|
| `certified` | 94 | 2 | certification |
| `contains_synthetic` | 89 | 2 | data-origin |
| `confidential` | 87 | 1 | sensitivity |
| `pii_financial` / `pii_name` / `pii_identifier` / `pii_phone` / `pii_dob` | 39/11/9/4/1 | 1 | PII |
| `restricted` | 29 | 1 | sensitivity |
| `reconciled` | 28 | 1 | quality/state |
| `semantic_layer` | 24 | 1 | data tier |
| `internal` | 2 | 1 | sensitivity |
| `system.certification_status` | 2 | 1 | certification |
| `self_ref_fk` | 4 | 1 | modeling marker |

**Recommended default denylist patterns:** `pii_*`, `contains_synthetic`,
`confidential`, `restricted`, `internal`, `certified`, `certification*`,
`*_status`, `reconciled`, `semantic_layer`, `data_tier`, `self_ref_fk`,
`controlled_placeholder`, `governance`, `open_reference`, `reference`, `demo*`,
`techsummit-*`, an exact bare `domain`, `*_team`. (The last block is the legacy junk
from the prior run; keep it so old estates stay clean.)

**Discriminator (for the classifier backstop, MV-D51 §5.2):** facets here are either
(a) matched by the denylist patterns, or (b) sensitivity/quality/PII semantics with
≤2 distinct values and no slash. Aboutness is the complement: title-cased business
nouns, often slashed, in `governed_tags`, not denylisted.

## 3. Value-carrying sub-domain hint — `mvm_subdomain` (MV-D54 boundary #2)

`mvm_subdomain` is a single tag whose **value** names the sub-domain — 79 assigns
across **14 distinct values** that mirror both the slash sub-tags and the
`airline_demo_mvm_*` schema names:

`ticket_settlement` (15), `fare_pricing` (11), `yield_management` (9),
`work_orders` (8), `compliance_planning` (8), `booking_management` (5),
`member_management` (5), `operational_performance` (4), `passenger_identity` (3),
`asset_registry` (3), `aircraft_registry` (2), `network_planning` (2),
`carrier_agreements` (2), `terminal_operations` (2).

So Stage 2 has **three** independent explicit sub-domain boundaries that agree:
slash sub-tags, the `mvm_subdomain` value tag, and the `airline_demo_mvm_*` schema
names — a strong, corroborated signal before any clustering.

## 4. Structural corroboration (from `ontology-curation-redesign-build.md` §Appendix A)

200 FK + 95 PK; **99 cross-schema FK edges** (domains = FK-connected components); 40
metric views; 10 `airline_demo_mvm_*` business-named schemas; shared join columns
(`aircraft_id`×14, `fare_id`×9, `route_id`×8, `pnr_id`×8, `ffp_member_id`×8). These
corroborate the governed taxonomy above rather than competing with it.

## 5. Implications per stage

- **Stage 1 (DONE + live-verified — see §5.1):** the heuristic classifier (patterns
  + ≤2-value/PII backstop) caught every facet key; facets no longer surface as
  Domains and the 4 Alaska Airlines domains surface with plain reasons. Fold §2 into
  the shipped `domain_facet_denylist` default in Stage 3.
- **Stage 2:** sub-domains come free from §1 slash sub-tags + §3 `mvm_subdomain` +
  schema-within-domain — Leiden is a fallback here, not the driver. Live run confirms
  the need: pre-Stage-2 sub-domains are raw schema names (`Dev … Metrics`), yet
  `Fare And Pricing` / `Ticketing And Settlement` already bind from the slash tags.
- **Stage 3:** ship §2 as the `domain_facet_denylist` default; the legitimacy bar
  (≥3 tables / ≥2 schemas / `require_connection`) prunes the shared-schema noise the
  live run exposed. Plus the **two §7 gates** the live run motivated: name
  dedup/qualification, and curated-tag-absorbs-its-FK-component.
- **Stage 4 (Pages):** disambiguation candidates likely where the same concept spans
  Commercial vs IFEC vs Operations (e.g., aircraft, route) — corroboration-gated.

## 5.1 Live verification (Stage 1 · 2026-08-31, deployed app)

`./scripts/deploy.sh --update` then `ontology_materialize` scoped to
`serverless_stable_6t92c3_catalog` → **SUCCEEDED** (1925 tags, 207 domains, 45
ungrouped). Verdict: **core thesis proven.**

- **Facets eliminated:** explicit check for the §2 facet keys as top-level domains →
  **zero rows** (was: `Contains Synthetic` 112, `Data Tier`, `Certification`, …).
- **Governed taxonomy surfaces:** `Alaska Airlines Commercial` (reuse, 48, *"grouped
  by curated domain tag"*), `Maintenance and Engineering` (reuse, 19), `IFEC` (reuse,
  15), `Operations` (reassign, 24). Top-level rule split: **32 FK-component / 26
  shared-schema / 3 curated-tag**; every domain carries an `evidence.reason`.
- **Two refinements exposed → folded into build §7:** (1) duplicate labels (`Cost
  Attribution` ×2, `Northpeak` ×2; `Dev … Metrics` sub ×14) → name dedup/qualification;
  (2) concept split — curated `Alaska Airlines Maintenance and Engineering` (19) vs FK
  `Airline Demo Mvm Maintenance` (46) → curated-tag absorbs its structural component.
- **Expected pre-Stage-3 noise:** scanning the whole shared catalog (many unrelated
  `e2e_real_*`/`migration`/`bakehouse` schemas) inflated the count via shared-schema;
  `domain_require_connection` + the legitimacy bar (Stage 3) prune it, and a Settings
  allowlist scoped to the airline catalogs avoids it. A bare button run correctly
  `skipped` on the empty allowlist (snapshot guard held).

## 6. Reproduce
Queries used (statements API via `databricks api post /api/2.0/sql/statements
--profile fevm-serverless`): `system.tags.governed_tags`; a UNION of
`information_schema.{catalog,schema,table,column}_tags` grouped by `tag_name` with
`count(*)` + `count(distinct tag_value)`; `mvm_subdomain` value breakdown from
`information_schema.table_tags`. All read-only.
