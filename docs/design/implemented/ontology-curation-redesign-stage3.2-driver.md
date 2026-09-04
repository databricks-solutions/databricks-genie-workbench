# Ontology — Curation Redesign · Stage 3.2 Goal-Mode driver

Copy-paste launcher for **Stage 3.2** (diffuse-FK root cause + Gate-B) with a
long-running agent (Claude Code / Cursor Goal Mode). Run on the **`ontology`** branch
**after Stage 3.1 has landed and been deploy-verified**. This slice is **wheel + job +
config only**; frontend is optional/additive. The agent builds + tests offline, then
**STOPS** before deploy.

- **Spec (source of truth):** `docs/design/implemented/ontology-curation-redesign-stage3.2-build.md`
  (honor the consolidated build §5.1 / §7 / §A.3 it extends)
- **Live evidence:** the Step-0 probe in the spec's Appendix + consolidated §A.3
  (the diffuse `Airline Demo Mvm Maintenance` hairball, 9 schemas / 0.32 home).
- **Decisions register:** `docs/design/mv-advisor-playbook.md` — **MV-D61 / MV-D62**;
  honor MV-D35 / D43 / D45 / D49 / D50 / D52 / D53 / D56 / D57.
- **Baseline (do NOT regress):** Stage 1 + 2 + 3 + 3.1 + 17g + 17f + re-grain + 3b + 3a.
- **Visual contract:** none required; `SettingsForm` MAY expose the new knobs
  (additive) — no new route/frame.
- **Project rules:** `AGENTS.md`.

**Post-Stage-3.1 reality (verified against code — reuse, don't re-add):**
`schema_signals.shared_join_column_edges` already stars-by-column with a per-column
`max_tables` cap and `JOIN_COLUMN_SUFFIXES=("_id","_key","_code")`; `fk_edges` is
separate and decisive. `rank._apply_legitimacy_gate` + `_is_curated_domain` already gate
top-level non-curated Domains and exempt curated ones — Gate-B mirrors it. `transforms`
is the ONE tiering/gate home (`legitimacy_ok`, `coverage_cap`). `OntologySettings`
carries additive policy fields via the MV-D50 `ADD COLUMN IF NOT EXISTS` pattern and
`refresh._launch` threads them as `job_parameters` with in-code job defaults. Stage 3.2
extends each — it invents nothing new.

---

## Driver prompt (paste verbatim)

```text
GOAL: Build Stage 3.2 — dissolve the diffuse cross-schema FK hairball at its ROOT + add
Gate-B net. Branch: ontology, atop LANDED Stage 3.1. ADDITIVE, wheel/job/config. NO DEPLOY.

SPEC (source of truth): docs/design/implemented/ontology-curation-redesign-stage3.2-build.md
DECISIONS: mv-advisor-playbook.md MV-D61/62 (honor MV-D35/43/45/49/50/52/53/56/57).
Evidence: spec Appendix (Step-0 probe) + build §A.3.
BASELINE (no regress): Stage 1/2/3/3.1 + 17g/17f/re-grain/3b/3a. RULES: AGENTS.md.

BUILD A — schema_signals.py root cause (pure/deterministic/offline):
  - filter_denylisted_schemas(rows,*,denylist): drop any FQN whose catalog.schema hits a
    denylist entry (exact, bare schema, or fnmatch glob e2e_*/ *_dev). Job reader applies
    it to ALL row inputs BEFORE building edges.
  - JOIN_COLUMN_SUFFIXES default -> ("_id","_key") (drop "_code": reference/enum, not a
    key); keep configurable (join_suffixes).
  - shared_join_column_edges: add max_schemas:int=2 (skip a column spanning > that many
    distinct catalog.schema) + name_denylist:frozenset (skip denylisted cols; seed
    {"id","user_id","workspace_id","category_id","tenant_id","account_id"}). fk_edges
    EXEMPT (declared FK is decisive). Thread join_suffixes/max_tables/max_schemas/
    name_denylist through join_key_edges.
  - jobs/run_ontology_materialize.py: read 6 new params (spec §3 defaults), apply
    filter_denylisted_schemas, pass knobs into join_key_edges. Degrade-to-empty stays.

BUILD B — Gate-B (mirror _apply_legitimacy_gate exactly):
  - transforms.is_diffuse(n_schemas, home_concentration, *, max_schemas=6,
    min_home_concentration=0.5)->(diffuse,reason): diffuse when n_schemas>=max_schemas
    AND home_concentration<min (strict <). home_concentration = members in the most
    common catalog.schema / total members.
  - rank._apply_diffuseness_gate: top-level, non-curated, structural (create/FK) Domains
    ONLY (reuse _is_curated_domain to exempt curated; skip sub/reassign/page). Below net
    -> KEPT, evidence.surfaced=false, rank.diffuse=true, rank.diffuse_reason="too broad
    — spans N schemas, home concentration X; split or attach to a specific area". Wire in
    _score_row right AFTER _apply_legitimacy_gate; thresholds from config w/ in-code
    defaults. Curated Domains NEVER diffuse-gated.

BUILD C — config (additive/defaulted; MV-D50 ADD COLUMN IF NOT EXISTS):
  - OntologySettings + ont_settings.py + lakebase.ont_upsert_settings add the 6 fields
    from spec §3 (schema_denylist, join_col_suffixes, join_col_max_schemas,
    join_col_denylist, max_diffuse_schemas, min_home_concentration). Old rows read
    defaults. refresh._launch threads all six as job_parameters. SettingsForm.tsx MAY
    expose them (OPTIONAL, no new route/frame).

DO NOT: touch absorb_curated_into_structural (declined a); add finer-γ split (declined
c); change fk_edges; change Stage-1/2 grouping beyond the edge-hygiene effect; pull
forward Stage 4/§9/§10.

HARD GUARDRAILS: no new route/table/DDL; response keys byte-identical. No SET/UNSET/
CREATE TAG, manage_uc_tags, web_search; wheel writes no ledger. Metastore grain (D49);
OBO reads (D50); degrade-not-hang (D43); no new dep (D45) — uv.lock UNTOUCHED (git).

ACCEPTANCE (./scripts/test.sh green): _code -> NO proxy edge by default (yes when
configured back); a 3-schema column skipped at max_schemas=2 but a 2-schema one stars;
a denylisted schema dropped from EVERY edge kind; a cross-schema declared FK KEPT;
is_diffuse(9,0.32)=diffuse,(6,0.5)=not,(3,0.8)=not; non-curated 7-schema/0.3 Domain ->
kept+surfaced=false+rank.diffuse, curated -> surfaced; settings round-trip (old row->6
defaults); job runs param-less AND param-driven; contract-frozen; firewall unchanged;
npm test/lint/build/tsc clean; uv.lock untouched.

WORKFLOW: schema_signals -> job reader -> is_diffuse -> rank gate -> models/ont_settings/
  lakebase -> refresh/_launch + job params -> (optional SettingsForm). test.sh per slice.
  STOP if ambiguous or a guardrail breaks.
```

---

## After the run (human-gated — the agent must not do these)

```bash
git diff --stat   # expect: packages/.../ontology/{schema_signals,transforms,rank}.py,
                  #   packages/.../jobs/run_ontology_materialize.py,
                  #   backend/ontology/{models.py,services/{ont_settings,lakebase,refresh}.py},
                  #   frontend/src/ontology/{types.ts,components/SettingsForm.tsx} (optional),
                  #   packages/.../tests/unit/**
./scripts/test.sh
cd frontend && npm run test && npm run lint && npm run build && cd ..
git status --porcelain uv.lock   # UNCHANGED (MV-D45; uv lock --check fails structurally here)

git add packages/genie-space-optimizer backend frontend
git commit -m "feat(ontology): Stage 3.2 — dissolve diffuse FK hairball (schema/edge denylist, _code drop, schema-span cap) + Gate-B diffuseness net (MV-D61/62)"
git push -u origin ontology
```

Then **you** run the deploy-verify gate (spec §7): `./scripts/deploy.sh --update` on
`fevm-serverless`, set `domain_schema_denylist` to the estate's infra schemas in
Settings, Refresh scoped to `serverless_stable_6t92c3_catalog`, and confirm
`Airline Demo Mvm Maintenance` no longer surfaces standalone while the three curated
Domains still do (Stage-3.1 regression guard) — using the §7 histogram query.
