# Ontology — Phase 4 / 17h Stage C Goal-Mode driver (tier-5 Context Sources panel + touch-points + grants)

## Status / positioning (READ FIRST)

Stage C of Phase 4 (17h) — the **user surface** stage. Stage A shipped the safe backbone
(registry + firewall + probe + `external_context` config + a **real per-source tier-5** in
`GET /api/ontology/preflight`) and Stage B shipped the resolver + egress + the two read-only
plug-points (deploy-verified 2026-09-15, `69bf9ec6`). Stage C makes the already-resolved,
already-persisted external context **visible and controllable** with **zero burden** (MV-D23):
one **Context Sources panel** in the banner, one **opt-in toggle** in Settings, and one labeled
**Sources chip** on a suggestion whose name came from the pack. It adds **no** engine behavior,
**no** structure, **no** tag write — it renders what Stage B already produced.

- **Build spec (source of truth):** `docs/design/ontology-phase4-external-build.md` §1 (Stage C),
  §3 (files: rows tagged **C**), §8 (zero-burden copy — verbatim tone), §12 (acceptance row C).
- **Design (user surface — implement verbatim, do not redesign):** `ontology-engine-architecture.md`
  §6 ("exactly one opt-in toggle + one optional low-confidence industry confirm + a labeled Sources
  chip; packs / tiers / validation are NEVER surfaced").
- **Decisions:** `mv-advisor-playbook.md` MV-D38/D44/D46/D47/D23/D50/D35/D43/D58.

Build order: Stage A ✅ → Stage B ✅ (deploy-verified) → **Stage C (this)** → (STOP, deploy-verify)
→ §9 industry alignment (`ontology-industry-alignment-driver.md`, already drafted). Do not front-run §9.

> **Optional within Stage C** (only if it stays additive + small, per architecture §6): the one
> low-confidence industry confirm — surface the pack's industry label read-only with its Sources chip and
> a confirm that writes our **own** config (never UC). It's cut from the paste-verbatim prompt to keep the
> core (panel + toggle + chip + grants) crisp; fold it in only if it doesn't grow the surface.

---

## Driver prompt (paste verbatim)

GOAL: Ontology Phase 4 Stage C (17h) — the external-context USER SURFACE: (A) a per-source Context
Sources panel in the banner, (B) one opt-in Settings toggle + per-source checkboxes, (C) a labeled/dated
Sources chip on a suggestion whose name came from the pack, (D) GRANT EXECUTE + OAuth-scope wiring.
RENDER-ONLY: Stage B already resolves + persists the pack; Stage C surfaces it. DEFAULT OFF ⇒ toggle off,
no panel, no chip, engine byte-identical estate-only. Read-only: no tag write, no structural change, no
new dep. Offline code + green tests; STOP before deploy.

SPEC (read first): ontology-phase4-external-build.md §1 Stage C, §3 (rows C), §8 (zero-burden copy), §12
(row C); ontology-engine-architecture.md §6 (user surface — verbatim). DECISIONS: mv-advisor-playbook.md
MV-D23/D38/D44/D47/D50/D35/D43.

REUSE (already built — do NOT rebuild): preflight.py returns the `external_enrichment` tier with per-source
`sources: SourceStatus[]` ({id,label,klass,provenance_tier,influence,execute_status,grant_line,reason});
types.ts has `PermissionTier.sources` + `SourceStatus` + `ExternalContext{enabled,sources}` +
`OntologySettings.external_context`; the Settings PUT accepts the full OntologySettings incl.
external_context; context_sources.CONTEXT_SOURCES/grant_execute_line = registry + copy-ready GRANT;
mirror.py assembles domain drafts from genie_ont_domains.evidence; rank.py persisted the pack name on
`evidence.rank.naming_prior` = {value,tier,source_url,source_kind,as_of,applied,outranked_by}.

A) PermissionBanner.tsx: for the `external_enrichment` tier ONLY, when `t.sources?.length`, render a
per-source sub-panel — each row: label, class badge, provenance tier, influence, an EXECUTE status pill
(reuse StatusPill: ok→ok / missing|blocked|unavailable→warning), and a copy-ready GRANT EXECUTE (reuse
CopyGrantButton) when `grant_line` is set. No source ⇒ keep today's plain reason. Other tiers unchanged.

B) SettingsForm.tsx: add ONE toggle "Use industry context to improve naming" bound to
external_context.enabled + per-source checkboxes (iterate the sources the preflight tier reports) bound to
external_context.sources[id]; thread external_context into the saveSettings payload. Copy = §8 tone: plain
language, NO MCP/provider/pack/tier jargon; off ⇒ only the toggle shows.

C) Sources chip: mirror.py additive — when `evidence.rank.naming_prior.applied`, assemble compact
`sources: [{label,url,as_of}]` onto DomainDraft (new additive model field, default []); DomainDraftCard.tsx
renders a labeled "Sources" chip linking the url; mirror the TS type in types.ts. No naming_prior ⇒ [] ⇒
no chip.

D) scripts/grant_permissions.py: add GRANT EXECUTE on each enabled MCP Service securable (reuse
grant_execute_line) + the OAuth scopes for the OBO managed-MCP path (§1 Stage C). Additive; no-op when off.

GUARDRAILS: DEFAULT OFF ⇒ UI + engine byte-identical (toggle defaults off; no chip/panel without a
resolved+applied pack); read-only (no SET/UNSET/CREATE GOVERNED TAG, no manage_uc_tags, no 17i); no
structural change (graph/cluster/rank weights/re-grain untouched); no new dep (lockfiles byte-identical,
MV-D45); packs/tiers/validation NEVER surfaced (only the toggle + labeled Sources chip, MV-D23); don't
edit the playbook.

ACCEPTANCE (offline): (1) banner shows the per-source panel when a source is enabled (class/tier/influence/
EXECUTE + GRANT when missing), generic row when off; (2) toggle defaults OFF + round-trips saveSettings;
(3) a DomainDraft with `naming_prior.applied` surfaces a labeled/dated Sources chip, one without surfaces
none; (4) no pack/tier/validation string anywhere; (5) grant_permissions.py emits GRANT EXECUTE for an
enabled source; (6) ./scripts/test.sh green; frontend vitest + lint + tsc clean.

WORKFLOW: branch ontology. Do NOT deploy or run the job. Offline-green ⇒ STOP + report diff +
./scripts/test.sh summary + frontend gate summary; a human runs deploy-verify.

---

## STOP before deploy (human-gated)

Stop when offline code + tests are green. Report the diff + `./scripts/test.sh` summary + the frontend
gate (`npx tsc -b`, `npm run lint`, `vitest`). Do **not** deploy, run the materialize job, or merge. A
human runs deploy-verify: `./scripts/deploy.sh --update` (fevm-serverless, frontend build ON so the panel
ships), enable one source in Settings, and confirm (a) the banner Context Sources panel renders per-source
{class, tier, influence, EXECUTE, GRANT-when-missing}; (b) the opt-in toggle persists and, when off, the
run stays estate-only with no chip; (c) a domain whose name came from the pack shows a labeled, dated
**Sources** chip that never claims a curated fact; (d) no pack / tier / validation jargon is ever shown.
