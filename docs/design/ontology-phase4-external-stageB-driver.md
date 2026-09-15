# Ontology — Phase 4 / 17h Stage B Goal-Mode driver (Context Pack resolver + web search + plug-points)

## Status / positioning (READ FIRST)

Stage B of Phase 4 (17h) — the **egress + influence** stage. It turns Stage A's safe backbone
(registry + firewall + probe + `external_context` config + real tier-5) into a working, cached,
self-validated **Context Pack** and wires it into exactly **two** plug-points (cluster/Page
**naming + gap hypotheses**, and Page **Recent-context**). It resolves the pack **once per run at
BATCH identity** and only when the tier is enabled with an available source; with the toggle off
(default) the resolver is **never invoked** and the run stays **byte-identical estate-only**
(MV-D44). External context is a **read-only PRIOR/overlay** — it never touches structure, never
writes a tag, and never outranks a T0/curated fact (MV-D38/D35).

- **Build spec (source of truth):** `docs/design/ontology-phase4-external-build.md` §1 Stage B,
  §4 (contracts), §5 (resolver flow), §6 (persistence/grain), §7 (firewall), §11 (tests).
- **Design (pack shape — implement verbatim, do not redesign):** `ontology-engine-architecture.md`
  §6 (`Provenanced<T>` + `ContextPack`) + §6.1 (AI-Gateway MCP registry, MV-D46/D47).
- **Decisions:** `mv-advisor-playbook.md` MV-D38/D44/D46/D47/D45/D43/D49/D50/D35/D28/D58/D59.

Build order: Stage A ✅ (landed) → **Stage B (this)** → (STOP, deploy-verify) → Stage C (tier-5
panel + touch-points + grants) → §9 alignment (existing driver). Do not front-run C or §9.

---

## Driver prompt (paste verbatim)

GOAL: Ontology Phase 4 Stage B (17h) — Context Pack RESOLVER (batch identity) + web search +
self-validation + the TWO read-only plug-points + additive DDL. DEFAULT OFF: external_context.
enabled=false (or no source) ⇒ resolver NEVER invoked and a materialize run is BYTE-IDENTICAL
estate-only. It's a read-only naming/overlay prior — never structure, never a tag. Offline code +
green tests; STOP before deploy.

SPEC (read first): ontology-phase4-external-build.md §1 Stage B, §4/§5/§6/§7/§11;
ontology-engine-architecture.md §6 (Provenanced<T> + ContextPack shape — verbatim) + §6.1.
DECISIONS: mv-advisor-playbook.md MV-D38/D44/D46/D47/D45/D43/D49/D50/D35/D28.

REUSE (Stage A): backend/ontology/services/context_sources.py = registry (CONTEXT_SOURCES/
ContextSource/get_source) + firewall (influence_allows + target vocab) + probe_source→SourceStatus;
ont_settings.py = external_context{enabled,sources:{id:bool}}; models.py = ContextClass/
ProvenanceTier/ExecuteStatus/SourceStatus. The RESOLVER + PLUG-POINTS live in the WHEEL, which
CANNOT import backend.*.

A) Single-source in the wheel (additive, no behavior change): move the pure firewall (vocabulary +
influence_allows) and pure registry (ContextSource/CONTEXT_SOURCES/get_source) into wheel modules
(ontology/context_firewall.py + context_registry.py); backend/context_sources.py imports them
(probe_source/SourceStatus stay in backend). Keep "FORBIDDEN checked first"; Stage-A tests stay green.

B) web_search.py (new, wheel): AI-Gateway MCP JSON-RPC — primary POST
https://<host>/ai-gateway/mcp-services/system.ai.web_search (tools/call); fallback → You.com
(myyoumcp) → Model-Serving web tool → estate-only. Gateway proxies managed creds — NO tokens in
app. HIPAA/BAA hard-off. Degrade-not-hang: all absent ⇒ empty, never raise (MV-D43/D46).

C) context_pack.py (new, wheel): resolve enabled sources ONCE per run (batch identity, MV-D50) into
§6's ContextPack (fields + Provenanced<T> leaves verbatim); a T1–T3 number with no source_url is
DROPPED; versioned + content_hash + status. SELF-VALIDATE before naming (MV-D38): firewall-by-class
(reuse A) + no-unsourced-numbers (extend leakage.LeakageOracle) + PII scan on tag-name-bound text +
confidence-gate (low industry τ ⇒ suppress the canonical-domain gap-check); FAIL ⇒ drop leaf / skip
pack.

D) DDL (ddl.py, additive, MV-D49, shapes per §6): new genie_ont_context_pack (pack_json blob, CDF
on) + genie_ont_context_sources (per-leaf citations); metastore-keyed; NO grain change to existing
genie_ont_* tables.

E) TWO plug-points ONLY: (1) rank.py L4/L5 naming + gap hypotheses as PROVENANCED priors that never
outrank a T0/curated fact (ladder T0>T1>T2>T3; a gap ranks below every graph-backed proposal);
(2) Page Recent-context overlay (MV-D28 subsumed — labeled, sourced, dated, certify-no).

F) Wire jobs/run_ontology_materialize.py: resolve the pack ONCE when enabled AND ≥1 source
available; thread as READ-ONLY prior into rank/pages; off ⇒ never called, no pack rows.

GUARDRAILS: DEFAULT OFF byte-identical (MV-D44); read-only (no SET/UNSET/CREATE GOVERNED TAG, no
manage_uc_tags, no 17i); NO structural change (graph/cluster/Leiden/weights/re-grain untouched);
external NEVER reaches membership/measure/certification (FORBIDDEN carve + guard test); NO new dep
(lockfiles byte-identical, MV-D45); additive DDL only (MV-D49); T0/curated beats T2/T3 (MV-D35); no
Stage-C panel; don't edit the playbook.

ACCEPTANCE (offline, full list in §11): off-by-default parity (enabled=false ⇒ byte-identical,
resolver never invoked, zero pack rows); provenance ladder (T2/T3 vs T0/curated ⇒ T0 wins);
degrade-not-hang (all providers absent ⇒ estate-only, completes); firewall positive guard passes;
lockfiles byte-identical; ./scripts/test.sh green; frontend lint/tsc clean.

WORKFLOW: branch ontology. Do NOT deploy or run the job. Offline-green ⇒ STOP + report diff +
./scripts/test.sh summary; a human runs deploy-verify.

---

## STOP before deploy (human-gated)

Stop when offline code + tests are green. Report the diff + `./scripts/test.sh` summary. Do **not**
deploy, run the materialize job, or merge. A human runs deploy-verify: `SKIP_FRONTEND_BUILD=1
./scripts/deploy.sh --update` (fevm-serverless), enable one web-search source, trigger a scoped
materialize, and confirm (a) business-language names / synonyms / gap hypotheses appear as **labeled,
sourced** priors that never outrank a curated fact, (b) a Page shows a dated **Recent-context**
overlay (certify-no), (c) with the toggle **off** the run is unchanged (estate-only, no pack rows),
and (d) `system.ai_gateway.usage` / `system.access.audit` show the egress was governed + logged.
