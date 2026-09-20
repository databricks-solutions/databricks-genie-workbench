# Ontology — Stage 4.1k: Related page↔page relevance ranking & cap · Goal-Mode driver

> **One stage per run, on the `ontology` branch.** Self-contained (no separate build spec).
> Additive, deterministic, membership-neutral. A quality refinement of Stage 4.1j (MV-D102) —
> it changes only `related_fqns` ordering/inclusion, never any domain/page/source set. **STOP
> before the live write** — the human deploy-verify gate (below) confirms bulk-archetype Pages no
> longer list a wall of arbitrary same-domain siblings. Proposed register line: **MV-D104**
> (page↔page Related ranking + relevance gate + separate sibling cap).

## Why now
Stage 4.1j (MV-D102) shipped graph-derived Related assets and page↔page relations. The live
deploy-verify surfaced a quality gap (backlog.md:201-205, follow-up **(a)**): on the bulk `Routing`
archetype, whose Pages' Sources are frequently NOT asset-graph nodes, the Pass-1 asset traversal is
empty, so the Related section fills with up to six **arbitrary, alphabetically-first same-domain
sibling Pages** — every one carrying the identical filler "Related page in the same area." The live
run recorded **641 sibling whys** across the estate. The asset-relation pass (`graph.related_assets`)
is already relevance-scored; only the page↔page pass is not. This stage brings the page↔page pass to
the same bar: rank siblings by real relatedness, cap bare siblings so they can't crowd out the
higher-signal rows, and drop zero-relevance filler when better siblings exist.

## Grounded facts (code, 2026-09-20)
- The page↔page pass lives in `_augment_related` **Pass 2** (`pages.py:1281-1326`). Siblings are
  `sorted(..., key=lambda d: d.title)` — **alphabetical, no relevance** (`pages.py:1298-1301`) — then
  appended up to the shared `max_related` cap (`pages.py:1250`, `:1310`), ahead-capped only by asset
  relations already added in Pass 1. Every sibling gets the constant `_SIBLING_PAGE_WHY`
  ("Related page in the same area.", `pages.py:1239`); linked-domain pages get `_LINKED_PAGE_WHY`
  (`:1240`).
- The relevance signal needed is already on the frozen `PageCandidate`: `source_fqns`
  (`pages.py:191`), `corroboration` (`:192`), `title`/`domain_id`/`page_id` — so a sibling's
  relatedness = size of its Source overlap with the anchor, no new input, no graph read.
- Pass 1 (`graph.related_assets`, `graph.py:443`) is ALREADY score-ranked (weight × KIND_PRIOR ×
  centrality) — this stage does NOT touch it; the asset rows stay first and unchanged.
- Gating precedent: the whole page↔page pass is already gated on `signal_graph is not None`
  (`_augment_related` runs only when threaded, `pages.py:1259-1270`), so `signal_graph=None` ⇒
  Pass 2 never runs ⇒ byte-identical agents-only (MV-D43).
- Membership neutrality is a pinned invariant: `test_related_augmentation_is_membership_neutral_eval_flat_or_up`
  (`test_ontology_pages.py:990`) asserts every `page_id`, `domain_id`, `source_fqns`, `certify`,
  `confidence`, `body` is identical with/without the graph — this stage must keep it green.

## Testability seam
Additive to ONE wheel file: `pages.py` `_augment_related` Pass 2 (a new pure `_rank_sibling_pages`
helper + a `max_sibling_pages` cap). No new dep, no new router, no schema change (`related_fqns`
stays `ARRAY<STRING>`, ddl.py:149). Deterministic (stable sort, explicit tie-breaks). Existing
Related/harness tests stay green; new tests pin the ranking, the cap, and the gate.

---

## GOAL PROMPT (paste verbatim into Goal Mode)

Implement **Stage 4.1k — Related page↔page relevance ranking & cap** on the `ontology` branch.
Additive, deterministic, membership-neutral, offline-green, no new dep, no new router, no schema
change. A refinement of Stage 4.1j (MV-D102). Proposed **MV-D104** (page↔page Related ranking +
relevance gate + separate sibling cap). This changes ONLY the ordering/inclusion of page↔page
entries in `related_fqns` and their `evidence.asset_why`; every domain/page/source membership set,
`certify`, `confidence`, and `body` stays identical.

BUILD A — pure sibling ranker (`pages.py`, above `_augment_related`): add
`_rank_sibling_pages(anchor, siblings) -> list[PageCandidate]` that scores each same-domain sibling
by RELEVANCE = `len(set(anchor.source_fqns) & set(sib.source_fqns))` (shared Source assets), and
returns them sorted `(-shared_sources, -corroboration, title)` — fully deterministic, alphabetical
only as the final tie-break. No graph read; uses fields already on `PageCandidate`.

BUILD B — cap + gate + reserve slots (`_augment_related` Pass 2, pages.py:1281-1326):
add a `max_sibling_pages: int = 2` kwarg (distinct from `max_related`). Replace the alphabetical
`siblings = sorted(..., key=d.title)` with `_rank_sibling_pages(cand, <same-domain siblings>)`.
Apply the RELEVANCE GATE: keep a bare same-domain sibling only if it shares ≥1 Source with the
anchor, EXCEPT always keep the single top-ranked sibling so a genuinely isolated Page still links
one (preserves `test_page_to_page_siblings_link_by_title`). Cap the kept bare siblings at
`max_sibling_pages`. Linked-domain pages (`_LINKED_PAGE_WHY`, the cross-domain, higher-signal
relation) keep PRIORITY over bare siblings and are NOT counted against `max_sibling_pages`; the
overall page↔page additions still respect `max_related`. Asset relations from Pass 1 remain first
and untouched.

BUILD C — determinism + default-safe: ordering within Related is stable and reproducible; when the
gate drops all-but-top siblings, the Page simply shows fewer page rows (no filler). `signal_graph=None`
⇒ Pass 2 does not run ⇒ byte-identical agents-only (MV-D43). Only real Pages are referenced (by
title, as today) — no invented FQN.

GUARDRAILS: additive; membership-neutral (related_fqns/asset_why ONLY); deterministic stable sort;
top-sibling floor keeps isolated-Page linking; no new dep/router; no schema change; DEFAULT-safe.

TESTS (wheel): `_rank_sibling_pages` orders by shared-source then corroboration then title; a bulk
domain (many same-domain Pages, no shared Sources) yields ≤`max_sibling_pages` sibling rows and does
NOT flood Related; a sibling sharing Sources outranks one that doesn't; the relevance gate drops
zero-overlap siblings when higher-overlap ones exist but keeps the top sibling for an isolated Page;
linked-domain pages still surface ahead of bare siblings and beyond the sibling cap;
`test_page_to_page_siblings_link_by_title` (test_ontology_pages.py:936) stays green;
`test_related_augmentation_is_membership_neutral_eval_flat_or_up` (:990) and
`test_related_precision_only_real_graph_nodes_no_invented_fqn` (:1009) stay green;
`genie_ont_eval` precision/recall/F1 flat-or-up (membership untouched).

ACCEPTANCE (offline): `./scripts/test.sh` green (report the new floor + update the playbook copy in
the SAME commit; the two parity-mirrored floors in `.cursor/rules/mv-advisor.mdc` +
`mv-advisor-playbook.md` must move together — test_rules_parity enforces it);
`git status -- uv.lock` clean. (No frontend change — `related_fqns` shape is unchanged, so
`PageDraftCard` renders the improved list as-is.) Then STOP for deploy-verify.

---

## Deploy-verify gate (human, after offline-green — the STOP checkpoint)
`./scripts/deploy.sh --update` — **backend-only is sufficient (`SKIP_FRONTEND_BUILD=1`); no frontend
change.** `deploy.sh` reads `GENIE_DEPLOY_PROFILE` from `.env.deploy` (today `fevm-serverless` →
workspace 6t92c3) and IGNORES a `DATABRICKS_CONFIG_PROFILE=…` prefix; to target another workspace
edit `.env.deploy`, do not prefix the command. A Related-ranking change flows through the materialize
batch, so trigger a fresh materialize (with the app's `run_now` recipe — pass `workspace_id` +
`metastore_id`, MV-D49; the job's empty default is a no-op on the app dataset), or wait for the
mirror to refresh, before verifying. Then in the LIVE app:
1. Open a bulk `Routing` Page (e.g. an "Average … Delay" page): the **Related** section no longer
   shows a wall of same-area sibling Pages — bare siblings are capped (≤2) and the ones shown share
   Sources with this Page; asset relations and cross-domain (linked-area) pages take the other slots.
2. Confirm in `genie_ont_pages`: the count of `_SIBLING_PAGE_WHY` ("Related page in the same area.")
   whys drops materially from the 4.1j baseline (641 across the estate), while `related_fqns` still
   carries the asset relations and any linked-area pages; no `related_fqns` entry is empty/invented.
3. Confirm the harness (`genie_ont_eval`) precision/recall/F1 is **flat-or-up** vs the prior run
   (page↔page ranking must not perturb domain/page membership).
Review with a human — the ranked siblings are actually more relevant (shared Sources), the cap holds,
and isolated Pages still link their single closest sibling — before marking Stage 4.1k BUILT and
registering **MV-D104**.
