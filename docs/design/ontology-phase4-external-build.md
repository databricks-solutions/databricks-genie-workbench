# Ontology — Phase 4 (17h): external Context Pack + Context Sources tier — build spec

> **Status:** DRAFTED (this doc). The genuinely undrafted piece of P3/17h is the **external
> Context Pack tier** (MV-D38/D44/D46/D47): the Context Sources registry, the batch Context
> Pack resolver + firewall, and the tier-5 UI/grants. The **§9 industry-alignment** half of
> 17h is already drafted — `docs/design/ontology-industry-alignment-driver.md` (MV-D58) — and
> it *consumes* the pack this phase authors; it is scheduled **after Stage B** below and is not
> re-drafted here.
>
> **Source of truth:** `ontology-engine-architecture.md` §6 (the `Provenanced<T>` envelope +
> `ContextPack` data model + provenance ladder + plug-points + user surface) and
> `mv-advisor-playbook.md` Prompt 17h (registry, resolver, firewall, tier-5 panel, grants,
> tests). Honor them exactly; invent nothing beyond them.

---

## 1. Scope

Phase 4 makes the ontology **domain-aware** — business-language names, synonyms, gap
hypotheses, and Page "Recent-context" overlay — by resolving an **opt-in, DEFAULT-OFF**
external-context tier into a cached, versioned, self-validated **Context Pack**, and wiring it
into exactly **two** plug-points. It never touches structure. It ships in **three stages** with
a **human STOP between each**; §9 alignment folds in after Stage B.

### In (Phase 4 · 17h)

**Stage A — Context Sources registry + firewall + capability probe + config (the safe backbone; NO egress).**
- A curated **Context Sources registry** (`backend/ontology/services/context_sources.py`): each
  entry `{id, label, mcp_fqn, klass: "internal"|"external", provenance_tier: "T0".."T3",
  influence, default_enabled}`. Lead registry per MV-D47: `system.ai.web_search` + You.com
  (`myyoumcp`) [T3 web], Confluence / Google Drive / Microsoft 365 [T1 company docs], internal
  Genie One (`/api/2.0/mcp/genie`) + Databricks SQL (`/api/2.0/mcp/sql`) [T0 verified].
  Gmail / Slack / Calendar **excluded / default-off**.
- **Firewall-by-class** helper (`influence_allows(entry, target) -> bool`): external/overlay
  MCPs may reach ONLY `naming | description | synonym | gap_hypothesis | recent_context`;
  internal UC-backed MCPs may additionally reach `structural_signal | validation`. NEVER
  `membership | measure | certification`.
- **Capability probe** (`probe_source(entry, client) -> SourceStatus`): resolves `EXECUTE` on
  the MCP Service securable (or a configured fallback); degrades — a source with no `EXECUTE`
  is `unavailable` with a plain reason, never an error (MV-D43/D45).
- **Config** (`ont_settings.py`): `external_context = {enabled: false, sources: {<id>: bool}}`
  via idempotent `ADD COLUMN IF NOT EXISTS`; mirrored in `types.ts`.
- **Make tier-5 real** in `backend/ontology/routers/preflight.py`: the existing
  `enrichment_tier` (`id="external_enrichment"`, `identity="batch"`) reports **per-source**
  `{class, provenance_tier, influence, execute_status}` + a copy-ready `GRANT EXECUTE …` line
  when missing; when nothing is enabled/available the tier is a plain **disabled** reason and
  the engine stays **estate-only**.

**Stage B — Context Pack resolver (BATCH identity) + web search + the two plug-points.**
- **Resolver** (`packages/genie-space-optimizer/.../ontology/context_pack.py`): resolve enabled
  sources **once per run** into the versioned, cached, self-validated `genie_ont_context_pack`
  (+ `genie_ont_context_sources`). The pack shape is architecture §6's `ContextPack`
  (industry resolution, `canonical_domains` template, `lexicon`, `financial_context`,
  `regulatory_notes`, `competitors`, `egress_log`). Every external leaf is a `Provenanced<T>`
  (tier / source_url / source_kind / as_of / confidence / decay_weight).
- **Web search (MV-D46)** via the Unity **AI Gateway** MCP: primary `system.ai.web_search`
  (`POST https://<host>/ai-gateway/mcp-services/system.ai.web_search`, JSON-RPC `tools/call`);
  fallback ladder → You.com MCP (`myyoumcp`) → a Model-Serving-native web tool → **estate-only**.
  The Gateway proxies managed credentials — **no tokens in app code**. HIPAA/BAA hard-off.
- **Self-validation BEFORE it steers naming** (MV-D38): firewall-by-class + **no-unsourced-
  numbers** (extend `leakage.LeakageOracle`) + **PII scan** + **confidence-gate** (a
  low-confidence company→industry map suppresses the canonical-domain gap-check).
- **Two plug-points, nowhere else:** (1) L4/L5 cluster + Page **naming / gap hypotheses**
  (`rank.py` — provenanced priors, never outranking a T0/curated fact); (2) Page
  **Recent-context** (the MV-D28 informational overlay — labeled, sourced, dated, `certify-no`;
  MV-D28 is subsumed here).
- **Additive persistence** (MV-D49): new `genie_ont_context_pack` + `genie_ont_context_sources`
  (metastore-keyed, CDF on, no key/grain change on existing tables).

**Stage C — Tier-5 Context Sources panel (frontend) + grants/auth.**
- Banner tier-5 becomes a **Context Sources panel**: each source row shows class, provenance
  tier, influence, `EXECUTE`/policy status, and a copy-ready `GRANT EXECUTE` when missing;
  nothing enabled / no source available ⇒ the toggle is **disabled** with a plain reason.
- **User surface** (architecture §6): exactly **one opt-in toggle** ("Use industry context to
  improve naming") + **one optional low-confidence industry confirm**; a per-suggestion labeled
  **Sources** chip. Packs / tiers / validation are **never** surfaced.
- **Grants/auth:** `scripts/grant_permissions.py` adds `GRANT EXECUTE` on the enabled MCP
  Services; the app OAuth integration adds scopes `genie, sql, unity-catalog, ai-search` for the
  OBO managed-MCP path.

### Out (deferred — do NOT pull forward)

- **§9 industry alignment** (MV-D58) — already drafted (`ontology-industry-alignment-driver.md`);
  runs **after Stage B**. This phase only guarantees the pack seam it consumes.
- **The eval harness** (§10 / MV-D59) — separate driver.
- **Any structural change** — graph / cluster / Leiden / edge-weights / re-grain are untouched.
- **The apply path** (17i) — external context is read-only; no `SET/UNSET/CREATE GOVERNED TAG`.

---

## 2. Decisions honored

MV-D38 (external = naming/description/hypothesis PRIOR + overlay, never structural; resolved once
into an internal, self-validated Context Pack; zero user burden), MV-D44 (DEFAULT OFF; estate-only
when off), MV-D46 (web search via AI Gateway `system.ai.web_search` MCP + fallback ladder +
degrade), MV-D47 (registry of MCP Services classified by `{class, tier, influence}`, firewalled by
class), MV-D45 (no net-new managed service; probe-and-degrade), MV-D43 (degrade-not-hang),
MV-D49 (metastore grain; additive DDL only), MV-D50 (OBO-first foundations; the resolver runs at
BATCH identity), MV-D35 (provenance ladder T0>T1>T2>T3). Inherit MV-D26/D37/D48/D58/D59 unchanged.

---

## 3. Subsystem layout (files touched)

| Stage | New / changed | Why |
|---|---|---|
| A | `backend/ontology/services/context_sources.py` (new) | registry + firewall-by-class + probe |
| A | `backend/ontology/services/ont_settings.py` | `external_context` config (idempotent ADD COLUMN) |
| A | `backend/ontology/routers/preflight.py` | make `enrichment_tier` real (per-source status + GRANT) |
| A | `backend/ontology/models.py` | `SourceStatus` / tier-5 fields (additive) |
| A | `frontend/src/ontology/types.ts` | mirror `external_context` + source status |
| B | `packages/genie-space-optimizer/.../ontology/context_pack.py` (new) | resolver + `Provenanced<T>` + self-validation |
| B | `packages/genie-space-optimizer/.../ontology/web_search.py` (new) | AI Gateway MCP JSON-RPC + fallback ladder |
| B | `.../ontology/leakage.py` | extend LeakageOracle: no-unsourced-numbers on pack leaves |
| B | `.../ontology/rank.py` | plug-point 1 — provenanced naming / gap hypotheses |
| B | `.../ontology/ddl.py` | `genie_ont_context_pack` + `genie_ont_context_sources` (additive) |
| B | `jobs/run_ontology_materialize.py` | resolve pack once (batch), thread as read-only prior |
| C | `frontend/src/ontology/**` (banner + SettingsForm) | Context Sources panel + the two touch-points |
| C | `scripts/grant_permissions.py` | `GRANT EXECUTE` on enabled MCP Services + OAuth scopes |

---

## 4. Contracts

- **`Provenanced<T>`** and **`ContextPack`** — verbatim from architecture §6 (do not redesign the
  shape). Every T1–T3 number carries a `source_url` + `as_of` or is **dropped**.
- **`SourceStatus`** (new, backend model): `{id, label, klass, provenance_tier, influence,
  execute_status: "ok"|"missing"|"blocked"|"unavailable", grant_line: str|null, reason: str}`.
- **Firewall target vocabulary** (the only allowed influence targets): `naming | description |
  synonym | gap_hypothesis | recent_context` (all classes) + `structural_signal | validation`
  (internal UC-backed only). `membership | measure | certification` are **never** reachable.

---

## 5. The resolver flow (Stage B)

```
enabled sources ─▶ probe (EXECUTE?) ─▶ resolve once (batch identity)
   ├─ web_search (AI Gateway MCP; ladder → estate-only on degrade)
   └─ T1 docs MCPs / T0 internal MCPs
        ▼
   assemble ContextPack (Provenanced<T> leaves)
        ▼
   SELF-VALIDATE  ── firewall-by-class ─┐
                   ── no-unsourced-numbers (LeakageOracle) ─┤ FAIL ⇒ drop leaf / skip pack
                   ── PII scan on tag-name-bound text ──────┤
                   ── confidence-gate (industry τ) ─────────┘
        ▼
   cache + version → genie_ont_context_pack (status active|stale|superseded)
        ▼
   read-only prior → rank.py naming + gap-check ; Page Recent-context
```

`enabled=false` (default) ⇒ resolver is **never invoked**; the run is byte-identical to today.

---

## 6. Persistence & grain (MV-D49)

- `genie_ont_context_pack` (metastore-keyed: `company_key, version`; `content_hash`,
  `generated_at`, `status`, `pack_json`) — CDF on, blob-preferred (MV-D49: the pack rides a JSON
  column, no per-field schema churn).
- `genie_ont_context_sources` (`company_key, source_id`, `enabled`, `execute_status`, `as_of`).
- No key/grain change on any existing `genie_ont_*` table; alignment relations (§9) ride the
  existing `evidence` JSON (its own driver's obligation).

---

## 7. Firewall & the class carve

- External/overlay MCPs are structurally prevented from reaching a membership/measure/
  certification writer — enforced by `influence_allows` at every call site AND a **positive guard
  test** that no external source id can be threaded into a structural writer.
- `system.ai.web_search` ships a platform write-block policy; we add no app-built egress. Every
  call is usage-tracked (`system.ai_gateway.usage`, `service_type='MCP_SERVICE'`) + audited
  (`system.access.audit` `mcpCall`), read via the GenieWatch SP pattern.

---

## 8. Zero-burden copy (MV-D23)

The curator sees only ranked Domain/Sub-Domain/Page drafts + one opt-in toggle + one optional
low-confidence confirm + a labeled/dated **Sources** chip. No pack, version, tier, or validation
state is ever surfaced. When no source is available: the toggle is disabled with
*"External context needs a web-search source; none is available in this workspace"* — never MCP /
provider jargon.

---

## 9. Industry-reference alignment (§9 / MV-D58) — folds in after Stage B

Not re-drafted here. Build order: after the Stage B pack seam exists, schedule
`docs/design/ontology-industry-alignment-driver.md` (it loads the Vibe industry model, emits typed
`exact/narrower/broader/derived/not-equivalent` correspondences + gap hypotheses, provenance-gated
T2/T3, off by default, additive). This build only guarantees the `ContextPack` + `Provenanced<T>`
seam that alignment consumes.

---

## 10. Stages & STOP gates

| Stage | Deliverable | STOP |
|---|---|---|
| A | registry + firewall + probe + config + real tier-5 (NO egress) | offline-green → human deploy-verify (tier-5 renders per-source status; engine estate-only) |
| B | pack resolver + web search + LeakageOracle + two plug-points + DDL | offline-green → human deploy-verify (enable a source, confirm names/synonyms/gap hypotheses appear, off ⇒ no regression) |
| C | tier-5 panel + touch-points + grants/auth | offline-green → human deploy-verify (panel + toggle + grants applied) |
| §9 | industry alignment (existing driver) | its own STOP |

Each stage: **offline code + tests first**, then a human runs deploy-verify. No stage deploys
itself. No stage runs the materialize job.

---

## 11. Tests (offline — `./scripts/test.sh` + `cd frontend && npm run test`)

- **Off-by-default parity:** `external_context.enabled=false` ⇒ a materialize run is
  byte-identical; the resolver is never invoked; no pack rows.
- **Registry classification:** every entry carries a valid `{class, tier, influence}`; the lead
  registry matches MV-D47; Gmail/Slack/Calendar are excluded/off.
- **Firewall-by-class (positive guard):** an external source can reach naming/description/
  synonym/gap/recent-context but NEVER a membership/measure/certification writer; internal
  UC-backed sources may add structural_signal/validation.
- **Capability probe:** no `EXECUTE` ⇒ tier-5 source `unavailable` + copy-ready GRANT; nothing
  available ⇒ toggle disabled with plain reason; engine degrades to estate-only.
- **Resolver self-validation (Stage B):** an unsourced number is dropped (LeakageOracle); a PII
  hit on tag-name-bound text is quarantined; a low-confidence industry suppresses the gap-check.
- **Provenance ladder:** a T2/T3 name conflicting with a T0/curated fact ⇒ T0/curated wins;
  external becomes corroborating evidence.
- **Degrade-not-hang:** web-search primary + fallbacks all absent ⇒ estate-only, run completes.
- **Determinism:** same estate + same enabled sources ⇒ same pack `content_hash` + same naming.
- **No new dependency:** `uv.lock` + `package-lock.json` byte-identical.

---

## 12. Definition of done

- Stages A/B/C offline-green (`./scripts/test.sh` + frontend `npm run test`/`lint`/`tsc`);
  additive-only diff; no new dependency; DEFAULT OFF; estate-only when off is byte-identical.
- Tier-5 renders per-source `{class, tier, influence, EXECUTE status, GRANT line}`; the toggle
  disables with a plain reason when no source is available.
- The `ContextPack` + `Provenanced<T>` seam exists and is consumed only at the two plug-points;
  the firewall guard test passes; every external number is sourced-or-dropped.
- §9 alignment can be scheduled against the seam without further pack work.
- Each stage STOPs before deploy; a human runs deploy-verify.
