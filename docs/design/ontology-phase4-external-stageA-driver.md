# Ontology — Phase 4 / 17h Stage A Goal-Mode driver (external-context safe backbone)

## Status / positioning (READ FIRST)

Stage A of Phase 4 (17h) — the **safe backbone** of the external-context tier: a Context Sources
**registry**, a **firewall-by-class** helper, a **capability probe**, the `external_context`
**config**, and a **real tier-5** in the preflight banner. It builds **none** of the Context Pack,
web search, or naming influence — those are **Stage B**. With the toggle off (default) the engine
is **byte-identical estate-only**, so Stage A is inert and safe to land first.

- **Build spec (source of truth):** `docs/design/ontology-phase4-external-build.md` §1 Stage A,
  §3/§4/§7/§11.
- **Design:** `ontology-engine-architecture.md` §6 (the `Provenanced<T>` / `ContextPack` shape +
  provenance ladder + user surface — context only; Stage A builds none of the pack).
- **Decisions:** `mv-advisor-playbook.md` MV-D38/D44/D46/D47/D45/D43/D49/D50/D35.

Build order: Stage A → (STOP, deploy-verify) → Stage B (resolver + web search + plug-points) →
(STOP) → Stage C (tier-5 panel + grants) → §9 alignment (existing driver). Do not front-run B/C.

---

## Driver prompt (paste verbatim)

GOAL: Ontology Phase 4 Stage A (17h) — the external-context SAFE BACKBONE, DEFAULT OFF, NO egress.
Ship a Context Sources registry + firewall-by-class helper + capability probe + config, and make
the preflight banner's tier-5 real. Build NO Context Pack, NO web search, NO naming influence
(those are Stage B). Estate-only behavior stays byte-identical. Offline code + green tests; STOP
before deploy.

SPEC: docs/design/ontology-phase4-external-build.md §1 Stage A, §3/§4/§7/§11 (primary);
ontology-engine-architecture.md §6 (Provenanced<T>, ContextPack, provenance ladder, user surface —
context only; Stage A builds none of the pack). DECISIONS: mv-advisor-playbook.md
MV-D38/D44/D46/D47/D45/D43/D49/D50/D35. Read first. Do NOT edit the playbook.

CONTEXT: backend/ontology/routers/preflight.py already has a stub enrichment_tier
(id="external_enrichment", identity="batch", status="not_exercised", _ENRICHMENT_GRANTS) — Stage A
makes it real. ont_settings.py holds ontology config; add config via idempotent ADD COLUMN IF NOT
EXISTS. Registry lead (MV-D47): system.ai.web_search + You.com (myyoumcp) [T3 web];
Confluence/Google Drive/Microsoft 365 [T1 docs]; Genie One (/api/2.0/mcp/genie) + Databricks SQL
(/api/2.0/mcp/sql) [T0 verified]; Gmail/Slack/Calendar excluded or default-off.

BUILD A — registry (new backend/ontology/services/context_sources.py): the curated list, each
entry {id,label,mcp_fqn,klass:internal|external,provenance_tier:T0..T3,influence,default_enabled};
a firewall helper influence_allows(entry,target)->bool where external reaches ONLY
naming|description|synonym|gap_hypothesis|recent_context, internal UC-backed adds
structural_signal|validation, and NONE reach membership|measure|certification; a
probe_source(entry,client)->SourceStatus that resolves EXECUTE on the MCP securable and degrades
(status missing|blocked|unavailable + a copy-ready GRANT EXECUTE line), never raising (MV-D43).

BUILD B — config (ont_settings.py + backend/ontology/models.py + frontend/src/ontology/types.ts):
external_context={enabled:false, sources:{<id>:bool}} via idempotent ADD COLUMN IF NOT EXISTS; a
new SourceStatus model {id,label,klass,provenance_tier,influence,execute_status,grant_line,reason};
mirror the type in types.ts. No UI panel yet (Stage C).

BUILD C — real tier-5 (preflight.py): the enrichment_tier reports per-source
{class,provenance_tier,influence,execute_status} + a copy-ready GRANT EXECUTE when missing; nothing
enabled/available ⇒ a plain disabled reason; the tier NEVER raises and the engine stays estate-only.

HARD GUARDRAILS: NO egress — no web_search, no AI Gateway call, no Context Pack, no
genie_ont_context_* table, no resolver, no naming/gap/rank change (all Stage B). Read-only — no
SET/UNSET/CREATE GOVERNED TAG, no manage_uc_tags, no apply-path (17i) edit. DEFAULT OFF (MV-D44):
enabled=false ⇒ byte-identical estate-only run. NO new dependency (uv.lock + package-lock.json
untouched, MV-D45). Additive config only (idempotent ADD COLUMN, MV-D49). Probe-and-degrade, never
hang (MV-D43/D45). Do NOT edit mv-advisor-playbook.md.

ACCEPTANCE (offline): registry entries all carry a valid {class,tier,influence} and match the
MV-D47 lead (Gmail/Slack/Calendar excluded); influence_allows lets an external source reach
naming/synonym/gap/recent-context but NEVER a membership/measure/certification target, proven by a
positive guard test; a probe with no EXECUTE ⇒ SourceStatus unavailable + a GRANT line; nothing
available ⇒ tier-5 disabled reason; enabled=false ⇒ byte-identical run (no pack, no egress);
uv.lock + package-lock.json byte-identical; ./scripts/test.sh green; cd frontend && npm run lint +
tsc clean.

WORKFLOW: branch ontology. Do NOT deploy or run the job. When offline-green, STOP and report the
diff + ./scripts/test.sh summary; a human runs deploy-verify (tier-5 renders per-source status;
engine estate-only).

---

## STOP before deploy (human-gated)

Stop when offline code + tests are green. Report the diff + `./scripts/test.sh` summary. Do **not**
deploy, run the materialize job, or merge. A human runs deploy-verify: `SKIP_FRONTEND_BUILD=1
./scripts/deploy.sh --update` (fevm-serverless), confirm the tier-5 banner renders each source's
`{class, tier, influence, EXECUTE status, GRANT line}`, the toggle is disabled with a plain reason
when no source is available, and the ontology run is unchanged (estate-only).
