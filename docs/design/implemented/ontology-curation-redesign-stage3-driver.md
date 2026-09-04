# Ontology — Curation Redesign · Stage 3 Goal-Mode driver

Copy-paste launcher for **Stage 3 of the curation redesign** (gates + explainable
why/confidence + config) with a long-running agent (Claude Code / Cursor Goal Mode).
Run it on the **`ontology`** branch **after Stage 1 + Stage 2 have landed and been
deploy-verified**. Unlike Stages 1–2 this slice **does** touch backend + frontend, but
strictly **additively** — new settings columns (the MV-D50 `ADD COLUMN` pattern), new
draft fields, no new route or frame. The offline code + tests are the agent's job; it
stops before deploy.

- **Spec (source of truth):** `docs/design/ontology-curation-redesign-build.md` (§7;
  honor the §5.2 facet gate it builds on, §11–§14)
- **Live evidence (this estate, Stage-2 verify run):**
  `docs/design/ontology-signal-inventory-findings.md` §5.1 — the concrete noise Stage 3
  gates: duplicate labels (`Cost Attribution` ×9, `Skyloyalty Dev` ×6, `Northpeak` ×4
  under `split by structure`), the curated `Alaska Airlines Maintenance and Engineering`
  split from the FK `airline_demo_mvm_maintenance`, and ~24 shared-schema junk domains
  (`e2e_real_*`, `bakehouse`, `wanderbricks_*`) that a legitimacy bar prunes.
- **Decisions register:** `docs/design/mv-advisor-playbook.md` (MV-D56 / D57; honor
  MV-D35 / D43 / D45 / D49 / D50 / D53)
- **Design context:** `docs/design/ontology-engine-architecture.md` §5–§6
- **Baseline (do NOT regress):** Stage 1 + Stage 2 + 17g + 17f + re-grain + 3b + 3a
- **Visual contract:** additive only — enrich `DomainDraftCard` (band + gap in place of
  the bare tier) and `SettingsForm` (the config surface); reuse existing card/settings
  layout; no new frame/route
- **Project rules:** `AGENTS.md`

**Post-Stage-2 reality (verified against the code — reuse, don't re-add):** the tiering
home already exists (`transforms.tier_of` / `coverage_cap` / `proposal_kind_of`); the
rank blend already emits per-factor `present`/`coverage` into `evidence.rank`
(`rank.blend`); the draft assembler already builds **evidence chips** and a **why**
string (`mirror._domain_chips` / `_domain_why` / `_assemble_domain_draft`);
`OntologySettings` already carries the additive `read_identity` field via the MV-D50
`ADD COLUMN IF NOT EXISTS` pattern (`models.py` / `ont_settings.py` /
`lakebase.ont_upsert_settings`); and `refresh._launch` already threads
`job_parameters` (`catalog_allowlist`). Stage 3 extends each — it invents nothing new.

Acceptance is **offline for the code, deploy-gated for verification**: the agent builds
+ tests green, then STOPS; a human runs `./scripts/deploy.sh --update`, refreshes, and
confirms the junk domains are gated, duplicate labels are gone, the curated tag absorbs
its FK twin, and each surfaced draft shows an honest band + gap.

---

## Driver prompt (paste verbatim)

```text
GOAL: Build Stage 3 of the ontology curation redesign — GATES + explainable
why/confidence + config. Turn noisy Stage-1/2 output into a legitimate, deduped,
self-explaining queue. Branch: ontology, atop LANDED Stage 1+2. ADDITIVE only.

SPEC (§7; honor §5.2 facet gate, §11-§14): docs/design/ontology-curation-redesign-build.md
DECISIONS: mv-advisor-playbook.md (MV-D56/57; honor MV-D35/43/45/49/50/53)
LIVE EVIDENCE: findings §5.1 (dup labels; curated-vs-FK split; shared-schema junk).
BASELINE (no regress): Stage 1+2 + 17g/17f/re-grain/3b/3a. RULES: AGENTS.md.

BUILD A — WHEEL gates (pure, deterministic, offline):
  - transforms.py: legitimacy_ok(n_tables, n_schemas, connected, *, min_tables,
    min_schemas, require_connection) -> (ok, reason); confidence_band(rank_block) ->
    {band, signals_present, gap} from evidence.rank factors/coverage — NEVER a %
    (MV-D35); reuse tier_of/coverage_cap (the ONE tiering home).
  - cluster.py: (1) NAME DEDUP/QUALIFICATION — after domains_with_reason and for
    sub-domains, no two proposals share a rendered name: qualify by distinguishing
    scope (schema/parent) or collapse if the member set is identical. domain_id stays
    the member fingerprint. (2) CURATED-TAG ABSORBS FK COMPONENT — when a curated
    domain-tag group and an FK-component group overlap >= a deterministic threshold
    (or share a governed home), emit ONE: curated wins name+reuse, the FK signal
    becomes corroborating evidence.
  - rank.py/materialize.py: apply legitimacy_ok as a gate — below-bar rows KEPT but
    surfaced=false with evidence 'add to existing domain: <X>'; write confidence_band
    into evidence. Facet filter (§5.2) stays the front gate.

BUILD B — CONFIG (additive/defaulted; MV-D50 ADD COLUMN IF NOT EXISTS):
  - models.OntologySettings + ont_settings.py + lakebase.ont_upsert_settings: add
    domain_facet_denylist (lift Stage-1 constants), domain_min_tables=3,
    domain_min_schemas=2, domain_require_connection=true, industry_alignment=
    {enabled:false,reference_model:null} (STORED+DORMANT — §9 is Phase 4). Old rows
    read defaults.
  - refresh._launch: thread the bar + denylist as job_parameters (like
    catalog_allowlist); run_ontology_materialize reads them with in-code defaults so a
    param-less run still works (MV-D43).

BUILD C — PRESENTATION (contracts frozen; additive fields only):
  - mirror.py: reuse _domain_chips + _domain_why; add the confidence band to the
    assembled draft (additive field).
  - types.ts + DomainDraftCard.tsx: render band + gap in place of the bare tier;
    SettingsForm.tsx exposes the config surface. Reuse existing layout; NO new
    route/frame.

HARD GUARDRAILS:
  - NO new route; existing response keys byte-identical (additive only).
  - NO SET/UNSET/CREATE TAG, manage_uc_tags, web_search; wheel writes no ledger.
    Metastore grain (MV-D49); OBO reads (MV-D50); degrade-not-hang (MV-D43); NO new
    dependency (MV-D45) — uv.lock UNTOUCHED (git status).
  - Deterministic + offline wheel; fixed Leiden seed. Do NOT change Stage 1/2 grouping
    or sub-domain boundaries; do NOT pull forward Stage 4, §9 alignment, or §10. NO DEPLOY.

ACCEPTANCE (./scripts/test.sh green): a 1-table/1-schema group -> 'add to existing
domain' (kept, surfaced=false); two same-named domains qualified or collapsed; a
curated tag absorbs an overlapping FK component (one row; FK -> evidence);
confidence_band = band+present+gap, never a %; settings round-trip (old row ->
defaults); job runs BOTH param-less (defaults) and param-driven; contract-frozen
(routes + OntologyRefreshStatus/taxonomy/tag-lens/drafts existing keys); firewall
unchanged. npm run test + lint + tsc clean; uv.lock untouched.

WORKFLOW: transforms -> cluster -> rank/materialize -> models/ont_settings/lakebase ->
  refresh/_launch + job reader -> mirror -> types/SettingsForm/DomainDraftCard.
  ./scripts/test.sh per slice. STOP and ask if ambiguous or a guardrail would break.
```

---

## After the run (human-gated — the agent must not do these)

```bash
git diff --stat            # expect: packages/.../ontology/{transforms,cluster,rank,materialize}.py,
                           #   packages/.../jobs/run_ontology_materialize.py,
                           #   backend/ontology/{models.py,services/{ont_settings,lakebase,refresh,mirror}.py},
                           #   frontend/src/ontology/{types.ts,components/{DomainDraftCard,SettingsForm}.tsx},
                           #   packages/.../tests/unit/**, frontend/**/*.test.tsx
./scripts/test.sh          # re-confirm green
cd frontend && npm run test && npm run lint && npm run build && cd ..
git status --porcelain uv.lock   # UNCHANGED (MV-D45; uv lock --check fails structurally here)

git add packages/genie-space-optimizer backend frontend
git commit -m "feat(ontology): Stage 3 gates + honest confidence + config — legitimacy bar, name dedup/qualification, curated-tag absorbs FK component (MV-D56/57)"
git push -u origin ontology
```

Then **you** run the deploy-and-verify gate:

```bash
./scripts/deploy.sh --update   # rebuilds frontend + wheel + redeploys the job (no dep change)
# In the live app: Settings → confirm the config surface (facet denylist, min
#   tables/schemas, require-connection) and keep the airline catalog allowlist set.
#   Ontology → Refresh ontology. When the materialize job finishes, confirm:
#   1. The ~24 shared-schema junk domains (e2e_real_*, bakehouse, wanderbricks_*) are
#      gated below the legitimacy bar — surfaced=false with an "add to existing
#      domain" hint, not standalone Domains.
#   2. No two Domains / sub-domains share a rendered name (Cost Attribution etc.
#      qualified or collapsed).
#   3. The curated "Alaska Airlines Maintenance and Engineering" absorbs the FK
#      "airline_demo_mvm_maintenance" — ONE domain, the FK signal now in its evidence.
#   4. Each surfaced draft shows an honest band + which signals are present + the gap
#      (never an "NN%").
# Queries to eyeball:
#   -- gated (below-bar) vs surfaced
#   SELECT get_json_object(evidence,'$.surfaced') surfaced, count(*)
#   FROM <gso_catalog>.genie_space_optimizer.genie_ont_domains
#   WHERE parent_id IS NULL GROUP BY 1;
#   -- any duplicate rendered names left?
#   SELECT name, count(*) n FROM <gso_catalog>.genie_space_optimizer.genie_ont_domains
#   GROUP BY name HAVING count(*) > 1 ORDER BY n DESC;
```

Stage 3 is offline and deterministic, but the settings round-trip, the config-driven
job parameters, the Delta write, and the synced mirror can only be validated in a
deployed app — which is why the offline slice stops here. Stage 4 (Pages) builds next.
