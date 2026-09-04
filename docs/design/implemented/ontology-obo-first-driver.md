# Ontology — OBO-first foundations; job `run_as`; SP optional (MV-D50) Goal-Mode driver

Copy-paste launcher for the **OBO-first identity refactor** (MV-D50) with a long-running
agent (Claude Code / Cursor Goal Mode). Run it on the **`ontology`** branch, on top of the
shipped Phase-1/2/3a/3b spine **and the metastore re-grain (MV-D49)**. It is an **identity
refactor** — the two foundation reads default to OBO (the admin viewer), the SP becomes an
opt-in, the batch reads as a `run_as` identity, and the banner stops implying an SP grant
is mandatory. **No grain change, no new table, no dependency, no response-shape change.**

- **Spec (source of truth):** `docs/design/implemented/ontology-obo-first-build.md`
- **Design context:** `ontology-engine-architecture.md` §2 + read tiers (MV-D37/D43/D44)
- **Decision:** `docs/design/mv-advisor-playbook.md` (MV-D50; also MV-D49/D45/D44/D43/D42)
- **Baseline (no regress):** the re-grain + Phase-3b/3a/2/1 spine
- **Project rules:** `AGENTS.md`

Acceptance is **offline for the code, deploy-gated for verification** (set
`ontology_job_run_as` to a real metastore admin, confirm the mirror populates with no
app-SP system grant). No dependency changes, so `uv.lock` is untouched.

---

## Driver prompt (paste verbatim)

```text
GOAL: OBO-first ontology foundations (MV-D50). The two foundation reads (governed-tag graph +
usage/lineage signals) default to OBO (admin viewer's identity), NOT the app SP; SP is an
OPT-IN upgrade; batch reads as a configurable run_as identity; the banner stops implying an
SP grant is mandatory. Identity refactor ONLY — NO grain change
(MV-D49 holds), NO new table/dependency/response-shape change. Branch: ontology, atop the
metastore re-grain + Phase-1/2/3a/3b.

SPEC (§1-§12): ontology-obo-first-build.md. DESIGN: ontology-engine-architecture.md §2 + read
tiers. DECISION: mv-advisor-playbook.md MV-D50 (+MV-D49/D45/D44/D43/D42). BASELINE: re-grain +
phase3b/3a/2/1. RULES: AGENTS.md.

REUSE, DON'T FORK: backend/services/auth.py already has require_obo_workspace_client() (OBO,
no SP fallback), get_service_principal_client(), get_workspace_client() — the resolver is a
switch over read_identity, NOT a new auth path. Reuse the tag_graph.py /
watch/services/system_tables.py TTL cache + permission-error detection AS-IS; only the cache
KEY gains the resolved principal.

HARD GUARDRAILS:
  - DEFAULT OBO: tag_graph._run (probe, sp_assignment_count, build_graph) + the ontology
    signals read resolve their client from read_identity. "obo" (default) ->
    require_obo_workspace_client(), NO silent SP fallback (no OBO context degrades the tier,
    never widens to SP); "sp" -> get_service_principal_client(); "auto" -> SP when the SP
    probe last succeeded, else OBO.
  - PER-IDENTITY CACHE: key includes the principal id so an OBO (privilege-filtered) result
    is NEVER served to another user and an SP result never leaks into OBO.
  - SETTINGS: OntologySettings gains read_identity: Literal["obo","sp","auto"]="obo"
    (ADDITIVE, defaulted); mirror the optional field in frontend types.
  - SIGNALS OPTIONAL (MV-D44): its identity can fail to read system.access/billing/query ->
    tier degrades, never blocks.
  - BANNER (PermissionBanner.tsx): signals + tag_graph tiers render identity "OBO (admin) or
    SP"; MOVE the copy button OUT of needsGrant (show whenever grants.length>0, incl. status
    ok); reword reason -> SP grants are an OPTIONAL upgrade (shared cache / consumer serving),
    not required to view.
  - RUN_AS (databricks.yml ontology-materialize-runner): add bundle var ontology_job_run_as;
    set -> emit run_as { user_name | service_principal_name }; unset -> no run_as (backward
    compatible). Reword job description + header off "as the SP" -> "as the run_as identity".
    refresh._launch (jobs.run_now via SP) UNCHANGED — run_now only STARTS the job;
    execution identity is run_as.
  - NO grain/DDL/MERGE/key change (MV-D49 intact); NO new table; NO new API route/model shape
    (PermissionTier identity enum unchanged — "OBO (admin) or SP" is a frontend label);
    taxonomy/tag-lens/refresh byte-identical; firewall unchanged (no governed-tag DDL /
    manage_uc_tags / web_search). NO NEW DEPENDENCY (MV-D45): uv.lock untouched.
  - Page stays ADMIN-GATED (OBO is privilege-filtered). NO DEPLOY (no deploy.sh/bundle
    deploy/uvicorn/npm dev/live job run).

ACCEPTANCE (before done): ./scripts/test.sh green covering EVERY §11 case — default OBO (SP
client NOT used); no silent SP fallback; sp/auto resolution; per-identity cache isolation;
settings defaulted; preflight framing ("optional upgrade" not "required");
banner vitest ("OBO (admin) or SP" + copy button on a green tier); run_as wiring (set ->
block, unset -> none); grain/contract-frozen; firewall unchanged. uv lock --check green; npm
lint + tsc clean; §12 done.

WORKFLOW: auth resolver -> tag_graph.py (resolver + per-identity cache key) ->
watch/system_tables.py -> ont_settings + models.py (read_identity) -> preflight.py
(active identity + optional-upgrade reason) -> PermissionBanner.tsx (label + always-on copy)
-> databricks.yml (ontology_job_run_as -> run_as) + job header reword -> tests. Run
./scripts/test.sh per slice; stop if ambiguous or a guardrail breaks.
```

---

## After the run (human-gated — the agent must not do these)

```bash
git diff --stat   # expect: backend/services/auth.py (resolver), backend/ontology/{services/
                  #   tag_graph.py,services/ont_settings.py,routers/preflight.py,models.py},
                  #   backend/watch/services/system_tables.py, frontend/src/ontology/**,
                  #   frontend/src/types/**, databricks.yml, packages/.../run_ontology_materialize.py,
                  #   backend/tests/**
./scripts/test.sh
cd frontend && npm run lint && npm run build && cd ..
uv lock --check   # UNCHANGED — no dependency touched (MV-D45)

git add backend frontend databricks.yml packages/genie-space-optimizer
git commit -m "feat(ontology): OBO-first foundations; job run_as; SP optional (MV-D50)"
git push -u origin ontology
```

Then **you** run the deploy-and-verify gate:

```bash
# Set the job run_as to a metastore admin (or a granted SP) in the bundle var, then:
./scripts/deploy.sh --update
# In the live app (as an admin): Ontology renders the taxonomy with read_identity="obo"
#   and NO app-SP system grant. Click "Refresh ontology" to run the materialize job under
#   run_as; confirm genie_ont_* populate. Optionally flip read_identity to sp/auto after
#   granting the SP to compare the shared-cache path.
```

The identity refactor is offline and deterministic, but OBO auth, the `run_as` execution
identity, and the live system-table reads can only be validated in a deployed app — which
is why this offline slice stops here.
