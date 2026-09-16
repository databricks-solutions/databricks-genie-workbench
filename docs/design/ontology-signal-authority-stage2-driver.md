# Ontology Signal Authority — Stage 2 (Authority: certification) · Goal-Mode driver (MV-D95)

> **One stage per run, on the `ontology` branch.** Build spec:
> `ontology-signal-authority-build.md` §4 (authoritative). This driver turns certification into
> the **`curated` authority rung** and makes **`deprecated` a rank-time firewall** — WITHOUT ever
> letting certification name a domain. Additive, read-only, no new table/column/dependency. STOP
> before deploy — a human runs the deploy-verify gate.

## Why now
Stage 1 (popularity) is deploy-verified and the §10 eval/trust harness (MV-D59) is live to catch
regressions. Stage 2 activates the `curated` (0.6) rung the ladder already reserves but never
populates (`_governance_map` only emits `governed`), and adds the `deprecated` firewall the
OntoRank stale-source down-rank calls for. Independent of Stage 1; small (one reader + one firewall).

## Grounded facts (probed 2026-09-16, tbzqg7 `serverless_stable_tbzqg7_catalog`)
- On this estate certification is the governed tag **`certified`** with values **`true`/`false`**
  (17 true · 50 false) — NOT the native `system.certification_status`, and there are **no
  `deprecated` values here**. So the reader must support BOTH surfaces: native
  `system.certification_status ∈ {certified, deprecated}` AND the `certified` boolean convention
  (`certified=true` → certified). `certified=false` is **not** deprecated (uncertified → absent).
- **Naming invariant already holds and must not move:** `transforms._FACET_EXACT` contains
  `certified`/`certification`, and `is_domain_entity_tag` explicitly drops `certified` /
  `system.certification_status` — certification never names a domain. Stage 2 redirects that same
  tag into *authority*; it must NOT touch `_FACET_EXACT` / `is_domain_entity_tag` / the denylist.
- Seams: reader `certification_status(allowlist)` in `run_ontology_materialize.py` (mirror
  `usage_signals`, read via `_rows_safe` from `system.information_schema.table_tags`) →
  `materialize._governance_map` (~L486, extend to emit `curated`) + a deprecated set →
  `rank.RankSignals` → a new firewall in `rank._score_row` (~L652, beside PII/policy/provenance).
- `rank._GOVERNANCE_VALUE = {governed:1.0, curated:0.6, ungoverned:0.2}`; `_governance_factor`
  (~L401) takes the **max** rung over a proposal's assets. `RankSignals` (frozen) currently holds
  `usage`/`centrality`/`governance` — add a 4th field for deprecation. Promotion of certified to
  1.0 is deferred (a scoreboard call on the harness); Stage 2 lands it at the `curated` rung.

## Testability seam
Keep the reader thin (fetch rows only); put the pure mapping in the wheel — a pure
`certification_map(rows) -> dict[str, str]` (FQN → "certified"|"deprecated") — so it is
unit-testable and I/O-free. `rank._score_row` arithmetic stays untouched (MV-D35).

---

## GOAL PROMPT (paste verbatim into Goal Mode)

Implement **Stage 2 (Authority: certification)** of `docs/design/ontology-signal-authority-build.md`
§4 on the `ontology` branch. Additive, read-only, offline-green, STOP before deploy.

BUILD A — pure mapping (wheel, unit-tested):
- Add a pure `certification_map(rows) -> dict[str, str]` (FQN → "certified"|"deprecated"). Accept
  rows from two surfaces: native `system.certification_status` (value `certified`/`deprecated`) and
  the boolean `certified` tag (`true` → certified). `certified=false` / any absent tag ⇒ OMIT
  (honest-gap, MV-D43). Deterministic (sorted, no wall-clock).

BUILD B — job reader (`run_ontology_materialize.certification_status(allowlist)`):
- Mirror `usage_signals`. Empty allowlist ⇒ `{}`. Else `_rows_safe` over
  `system.information_schema.table_tags` for the in-scope catalogs, selecting the certification
  rows (`tag_name='system.certification_status'` OR `tag_name='certified'`), build the FQN, and
  pass to `certification_map`. Any read failure ⇒ `{}` (never raises, never a faked map).

BUILD C — wire authority + firewall:
- `materialize._governance_map`: thread the certification map in (new optional param, byte-identical
  when empty). After emitting `governed` for aboutness-tagged FQNs, emit `curated` for a certified
  FQN that is not already `governed` (leave `governed`=1.0 — `_governance_factor` takes the max).
- Add `RankSignals.deprecated: frozenset[str]` (default empty); populate it with the map's
  deprecated FQNs where `RankSignals` is built in `materialize`.
- `rank._score_row`: add a firewall beside PII/policy/provenance — if any of the row's `assets` is
  in `signals.deprecated`, set `blocked=true`, `block_reason="deprecated_asset"` so `surfaced=false`
  even at a high raw score. Mirror the PII block exactly.

BUILD D — tests:
- Mapping: native certified/deprecated + `certified=true`→certified, `certified=false`/absent→omitted;
  deterministic.
- Governance: a certified-only FQN lifts its proposal's governance factor to the `curated` rung; an
  already-`governed` FQN stays `governed` (max); an untagged FQN is absent.
- Firewall: a proposal with a deprecated asset is `blocked`, `block_reason=="deprecated_asset"`,
  `surfaced==false` even with a high blend (mirror `test_pii_tag_name_is_blocked_and_dropped_from_surfaced`).
- Invariant: assert `certified` / `system.certification_status` are still dropped by
  `is_domain_entity_tag` (certification never names a domain).

GUARDRAILS (hard): do NOT change `FACTOR_WEIGHTS`/`blend`/`coverage_cap`/`tier_of`/`confidence_band`
or the existing firewalls' logic (MV-D35); do NOT touch `_FACET_EXACT`/`is_domain_entity_tag`/
`domain_facet_denylist` (MV-D51 naming invariant). No new Delta table/column (MV-D49); no new
dependency (MV-D45) — `uv.lock`/`package-lock.json` byte-identical; read-only, no governed-tag write
(MV-D26); deterministic (MV-D82). Live path default-correct: no certification grant ⇒ `{}` = today's
bytes.

ACCEPTANCE (offline): `./scripts/test.sh` green; targeted rank/materialize tests green; `uv.lock` +
`frontend/package-lock.json` untouched; only wheel files changed. Then STOP.

---

## Deploy-verify gate (human, after offline-green)
`SKIP_FRONTEND_BUILD=1 DATABRICKS_CONFIG_PROFILE=fevm-serverless-tbzqg7 ./scripts/deploy.sh --update`,
then `run-now` the materialize job (`512526067383740`) with
`catalog_allowlist=["serverless_stable_tbzqg7_catalog"]`, `domain_facet_denylist=["modeled"]`
(~20 min). Confirm:
- Certified airline tables lift their domain's `evidence.rank.factors.governance` to the `curated`
  rung; `certified` still never appears as a domain name.
- (If any deprecated asset exists) it carries `block_reason=deprecated_asset` and drops from surfaced.
- The MV-D59 harness P/R/F is **flat-or-up** vs the last baseline — capture the `genie_ont_eval`
  row before re-running so `compare_reports` has a before/after.
