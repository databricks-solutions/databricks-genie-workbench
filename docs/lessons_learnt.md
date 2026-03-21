# Deploy Lessons Learnt

Issues found and resolved during the 2026-03-18 deploy debugging session.

## Issues Found

### 1. OAuth scopes not persisting after `apps deploy`

**Symptom:** App showed 0 Genie spaces. OBO token lacked `dashboards.genie` scope, falling back to SP which had no access.

**Root cause:** `databricks apps deploy --source-code-path` reads `app.yaml` as the complete app config. The `user_api_scopes` were defined in `databricks.yml` (bundle resource) but NOT in `app.yaml`. When `apps deploy` ran after `bundle deploy`, it overwrote the scopes to defaults.

**Fix:** Added `user_api_scopes` to `app.yaml`. Also PATCH scopes via API before `apps deploy` as a belt-and-suspenders.

**TODO:** Investigate whether `apps deploy` actually strips scopes from the app config, or if it just doesn't read them from `app.yaml`. If the former, the pre-deploy PATCH may be the only reliable way.

### 2. App resources (SQL warehouse, Lakebase) wiped on every `apps deploy`

**Symptom:** App resource page showed "No resources specified" after deploy.

**Root cause:** Same as #1 — `apps deploy` overwrites the app config with whatever `app.yaml` declares. Resources defined in `databricks.yml` (bundle) were lost. `app.yaml` uses `valueFrom: sql-warehouse` but this requires the resource to exist on the app object.

**Fix:** Pre-deploy PATCH sets the SQL warehouse resource via API before `apps deploy`, so `valueFrom` references resolve. Lakebase resource wiring was removed entirely (manual UI step now).

**TODO:** Test whether `databricks bundle deploy` + `apps deploy` ordering matters. Consider whether the bundle should be the sole deployer (no separate `apps deploy`).

### 3. Lakebase postgres resource impossible to wire reliably via deploy script

**Symptom:** Lakebase resource kept getting reset to empty stub `{"name": "lakebase-db"}` after every `apps deploy`, even with pre-deploy and post-deploy PATCH attempts.

**Root cause:** `apps deploy` is asynchronous and overwrites the app's resource config at an unpredictable time. PATCHing before or after the deploy both race with the deploy's own config write.

**Fix:** Removed automated Lakebase wiring from `deploy.sh`. Lakebase is now connected manually via the Databricks Apps UI. The app auto-creates all required tables on first connect.

**TODO:**
- Investigate if `databricks apps update` (not `apps deploy`) can set resources without triggering a code deployment
- Consider using `databricks bundle deploy` exclusively (no separate `apps deploy`) to avoid the config overwrite issue
- File a feature request: `app.yaml` should support inline resource definitions (not just `valueFrom`)

### 4. Lakebase credential generation API not available

**Symptom:** `No API found for 'POST /postgres/generate-database-credential'`

**Root cause:** The autoscaling Lakebase credential generation API doesn't exist on the target workspace. The old provisioned instance API (`/api/2.0/database/credentials`) also fails because it doesn't recognize autoscaling project names.

**Fix:** Removed all credential generation code from `lakebase.py`. When Lakebase is connected via UI, the platform injects `LAKEBASE_HOST` and `LAKEBASE_PASSWORD` as environment variables automatically.

**TODO:** Confirm that UI-connected Lakebase resources actually inject `LAKEBASE_PASSWORD`. If not, investigate the correct credential mechanism for autoscaling postgres projects.

### 5. SP permission check for Genie spaces defaulted to `True`

**Symptom:** Optimization could be started on spaces where the SP had no CAN_EDIT permission.

**Root cause:** `sp_can_manage_space()` in `genie_client.py` couldn't fetch the ACL (OBO token lacked `access-management` scope) and fell back to `return True`.

**Fix:** Replaced with a probe — SP attempts to fetch the serialized space config. However, the probe was unreliable: CAN_VIEW is sufficient to fetch the space config, producing false positives. Final fix: try the REST ACL endpoint (`GET /api/2.0/permissions/genie/{id}`) with both the OBO client and the SP client (matching the standalone GSO's `_cached_perms_rest` pattern). If neither client can retrieve the ACL, return `False` instead of probing.

### 6. First deploy after destroy takes >3 minutes (verification timeout)

**Symptom:** Deploy script reported "App deployment still IN_PROGRESS after 3 minutes" but the deploy eventually succeeded.

**Root cause:** Fresh deploys install all Python/Node dependencies from scratch (no cache), taking ~4-5 minutes total. The verification step only waits 3 minutes (18 x 10s).

**TODO:** Increase verification timeout for fresh deploys, or detect fresh deploy and skip the wait (just report the status).

### 7. PATCH API replaces all resources (not a merge)

**Symptom:** PATCHing the app with just `sql-warehouse` wiped the manually-added `postgres` resource.

**Root cause:** The `PATCH /api/2.0/apps/{name}` API treats all fields as full replacements, not merges. Omitting `resources` or `user_api_scopes` resets them to empty.

**Fix:** Deploy script now reads existing resources via GET, filters out empty stubs (API rejects them), and includes all valid resources in the PATCH. Also auto-discovers Lakebase postgres config from the project API when the resource shows as an empty stub.

### 8. Lakebase password not injected by platform

**Symptom:** `LAKEBASE_HOST` resolves via `valueFrom: postgres` but `LAKEBASE_PASSWORD` is not set. Credential generation APIs also fail.

**Root cause:** The autoscaling Lakebase credential generation API (`/api/2.0/postgres/generate-database-credential`) doesn't exist on this workspace. The platform doesn't inject `LAKEBASE_PASSWORD` for postgres resources the way it does for other resource types.

**Status:** Unresolved platform limitation. App uses in-memory fallback. When Databricks adds credential injection for autoscaling postgres app resources, the app will auto-connect.

**Progress (2026-03-19):**
- OAuth token generation works (client_credentials grant with IAM scopes)
- Host resolution works (valueFrom gives resource path, resolved via API to actual hostname)
- SASL auth fails — token with `iam.current-user:read iam.groups:read iam.service-principals:read iam.users:read` scopes is rejected
- SP role exists in Lakebase (`1d77ab45-...`, OAuth auth) but may lack privileges

**TODO:**
- Grant the SP role `databricks_superuser` or `LOGIN` in the Lakebase Roles UI
- If SASL still fails, investigate if the `sub` claim in the SP's token matches the Lakebase role name
- Consider using the **Lakebase Data API** (REST) instead of direct Postgres protocol: https://docs.databricks.com/aws/en/oltp/projects/data-api
- Apps UI support for Lakebase Autoscaling is "coming soon" (per Daniel Price in #apa-apps, Mar 2026) — may auto-inject credentials properly
- Reference tutorial: https://docs.databricks.com/aws/en/oltp/projects/tutorial-databricks-apps-autoscaling

## Architecture Notes for Future Robustness

1. **`databricks bundle deploy` vs `databricks apps deploy`:** These two commands have overlapping but different behaviors. Bundle deploy manages the Terraform state (creates/updates resources). Apps deploy deploys code from a workspace path. When both are used, the apps deploy can overwrite bundle-managed config. Consider using bundle deploy exclusively.

2. **Pre-deploy PATCH pattern:** The current pattern of PATCHing the app config before `apps deploy` works for scopes and SQL warehouse. This is because `apps deploy` reads the app's current resource config to resolve `valueFrom` references, and our PATCH runs synchronously before the deploy.

3. **Lakebase as manual step:** The cleanest current approach is to let the user wire Lakebase via UI. The app's `_ensure_schema()` auto-creates all tables, so no manual SQL is needed.
