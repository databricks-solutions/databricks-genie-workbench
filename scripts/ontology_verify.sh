#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# ontology_verify.sh — one-command deploy-verify gate for ontology lanes.
#
# Automates the manual gate we run after every ontology stage:
#   1. deploy code-only (frontend build skipped — reuses frontend/dist)
#   2. resolve the ontology materialize job from bundle state
#   3. trigger it scoped to a catalog allowlist, poll to completion
#   4. run acceptance SQL and print PASS / FAIL (nonzero exit on FAIL)
#
# It writes NOTHING to Unity Catalog governed tags and runs only SELECT SQL
# against the genie_ont_* snapshot tables. Config comes from .env.deploy via
# deploy-config.sh (PROFILE / CATALOG / GSO_SCHEMA / WAREHOUSE_ID).
#
# Usage:
#   scripts/ontology_verify.sh [--no-deploy] [--scope <catalog>]
#                              [--sql <acceptance.sql>] [--profile <p>]
#
#   --no-deploy     Skip step 1 (job trigger + verify only).
#   --scope CAT     Catalog the job SCANS (catalog_allowlist). Default: $CATALOG.
#   --sql FILE      Lane acceptance SQL. Its final SELECT must return a column
#                   named `ok` (boolean) in row 0; PASS iff ok is true. Extra
#                   columns are printed for context. Omit for the built-in smoke
#                   gate (>=1 completed run AND >=1 domain).
#   --profile P     Override the CLI profile (default from .env.deploy).
# ---------------------------------------------------------------------------
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
source "$PROJECT_DIR/scripts/deploy-config.sh"   # -> CATALOG GSO_SCHEMA WAREHOUSE_ID PROFILE LLM_MODEL

DEPLOY=1
SCOPE="$CATALOG"
SQL_FILE=""
while [ $# -gt 0 ]; do
    case "$1" in
        --no-deploy) DEPLOY=0; shift ;;
        --scope)     SCOPE="$2"; shift 2 ;;
        --sql)       SQL_FILE="$2"; shift 2 ;;
        --profile)   PROFILE="$2"; shift 2 ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

OUT="${CATALOG}.${GSO_SCHEMA}"   # genie_ont_* output tables live here (deploy catalog)
ALLOWLIST="[\"${SCOPE}\"]"        # JSON-array string the job expects

say() { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }

# ── SQL over the statements API (SELECT-only), polled to completion ────────
# $1 = statement text. Prints the raw JSON response on success.
sql_exec() {
    local stmt resp sid state
    stmt=$(python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))' <<<"$1")
    resp=$(databricks api post /api/2.0/sql/statements --profile "$PROFILE" --json \
        "{\"warehouse_id\":\"$WAREHOUSE_ID\",\"statement\":$stmt,\"wait_timeout\":\"50s\",\"format\":\"JSON_ARRAY\",\"on_wait_timeout\":\"CONTINUE\"}")
    sid=$(python3 -c 'import json,sys;print(json.load(sys.stdin).get("statement_id",""))' <<<"$resp")
    state=$(python3 -c 'import json,sys;print(json.load(sys.stdin).get("status",{}).get("state",""))' <<<"$resp")
    while [ "$state" = "PENDING" ] || [ "$state" = "RUNNING" ]; do
        sleep 3
        resp=$(databricks api get "/api/2.0/sql/statements/$sid" --profile "$PROFILE")
        state=$(python3 -c 'import json,sys;print(json.load(sys.stdin).get("status",{}).get("state",""))' <<<"$resp")
    done
    if [ "$state" != "SUCCEEDED" ]; then
        echo "SQL did not succeed (state=$state):" >&2
        echo "$resp" | python3 -c 'import json,sys;print(json.load(sys.stdin).get("status",{}))' >&2 || echo "$resp" >&2
        return 1
    fi
    echo "$resp"
}

# ── Step 1: deploy (code-only) ─────────────────────────────────────────────
if [ "$DEPLOY" = "1" ]; then
    say "Deploy (SKIP_FRONTEND_BUILD=1 ./scripts/deploy.sh --update)"
    SKIP_FRONTEND_BUILD=1 "$PROJECT_DIR/scripts/deploy.sh" --update
else
    say "Deploy skipped (--no-deploy)"
fi

# ── Step 2: resolve the ontology materialize job id ────────────────────────
say "Resolve ontology materialize job"
JOB_ID=$(cd "$PROJECT_DIR" && databricks bundle summary -t app \
    --var="catalog=$CATALOG" --var="warehouse_id=$WAREHOUSE_ID" --var="llm_model=$LLM_MODEL" \
    --profile "$PROFILE" -o json 2>/dev/null \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["resources"]["jobs"]["ontology-materialize-runner"]["id"])')
if [ -z "${JOB_ID:-}" ]; then echo "Could not resolve ontology-materialize-runner job id from bundle state." >&2; exit 1; fi
echo "  job_id=$JOB_ID  scan_scope=$ALLOWLIST  out=$OUT"

# ── Step 3: trigger scoped, poll to terminal ───────────────────────────────
say "Trigger materialize (catalog_allowlist=$ALLOWLIST)"
# NB: current Databricks CLI rejects a positional job_id alongside --json; job_id must
# ride INSIDE the JSON payload.
RUN_ID=$(databricks jobs run-now --no-wait --profile "$PROFILE" \
    --json "{\"job_id\":$JOB_ID,\"job_parameters\":{\"catalog_allowlist\":\"$(printf '%s' "$ALLOWLIST" | sed 's/"/\\"/g')\"}}" \
    | python3 -c 'import json,sys;print(json.load(sys.stdin)["run_id"])')
echo "  run_id=$RUN_ID"
LIFE=""; RESULT=""
while [ "$LIFE" != "TERMINATED" ] && [ "$LIFE" != "SKIPPED" ] && [ "$LIFE" != "INTERNAL_ERROR" ]; do
    sleep 15
    read -r LIFE RESULT < <(databricks jobs get-run "$RUN_ID" --profile "$PROFILE" -o json \
        | python3 -c 'import json,sys;d=json.load(sys.stdin);s=d.get("state",{});print(s.get("life_cycle_state",""),s.get("result_state",""))')
    echo "  ...$LIFE${RESULT:+/$RESULT}"
done
if [ "$RESULT" != "SUCCESS" ]; then
    echo "Materialize run did not succeed (life=$LIFE result=$RESULT). See run $RUN_ID." >&2
    exit 1
fi

# ── Step 4: acceptance ─────────────────────────────────────────────────────
say "Acceptance"
if [ -n "$SQL_FILE" ]; then
    [ -f "$SQL_FILE" ] || { echo "acceptance SQL file not found: $SQL_FILE" >&2; exit 1; }
    STMT=$(sed "s/\${OUT}/$OUT/g; s/\${SCOPE}/$SCOPE/g" "$SQL_FILE")
else
    STMT="SELECT
      (SELECT COUNT(*) FROM ${OUT}.genie_ont_runs WHERE state='succeeded') AS completed_runs,
      (SELECT COUNT(*) FROM ${OUT}.genie_ont_domains) AS domains,
      (SELECT COUNT(*) FROM ${OUT}.genie_ont_pages)   AS pages,
      ((SELECT COUNT(*) FROM ${OUT}.genie_ont_runs WHERE state='succeeded') > 0
        AND (SELECT COUNT(*) FROM ${OUT}.genie_ont_domains) > 0) AS ok"
fi
RESP=$(sql_exec "$STMT")
echo "$RESP" | python3 - "$RUN_ID" <<'PY'
import json, sys
resp = json.load(sys.stdin)
cols = [c["name"] for c in resp["manifest"]["schema"]["columns"]]
rows = resp.get("result", {}).get("data_array") or []
if not rows:
    print("FAIL — acceptance query returned no rows"); sys.exit(1)
row = dict(zip(cols, rows[0]))
for k, v in row.items():
    print(f"  {k} = {v}")
ok = str(row.get("ok", "")).lower() in ("true", "1", "t")
print(("\n\033[1;32mPASS\033[0m" if ok else "\n\033[1;31mFAIL\033[0m") + f" — run {sys.argv[1]}")
sys.exit(0 if ok else 1)
PY
