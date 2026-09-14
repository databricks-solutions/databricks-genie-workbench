#!/usr/bin/env bash
# =============================================================================
# AI Gateway (Unity Gateway) migration de-risk probe for Genie Workbench
# -----------------------------------------------------------------------------
# Read-only validation harness. Makes tiny (<=32 token) FMAPI calls on the
# gateway path AND the current classic serving-endpoints path, proves every
# request shape the workbench uses, and writes a PASS/FAIL report with a
# GO / NO-GO decision matrix. It NEVER modifies workbench code or workspace
# state (the only writes are cheap model invocations that cost a few tokens).
#
# What it de-risks (each maps to a real workbench call site):
#   - llm_utils.call_serving_endpoint          -> non-streaming chat (S3)
#   - create_agent._stream_llm                 -> streaming + tools loop (S4)
#   - GSO llm_client.call_llm / optimizer_utils-> OpenAI-shape + response_format (S3)
#   - model_catalog / /api/models              -> name mapping + enumeration (S2)
#   - cost attribution (Kyle/Vijayalakshmi ask)-> request-tags -> usage table (S5)
#   - OBO robustness                           -> entitlement/error shape (S6)
#   - "regression is not an option"            -> classic path still green (S7)
#
# Usage (full suite):
#   scripts/ai-gateway-validation/probe.sh --profile <PROFILE> \
#       [--host <https://...>] [--warehouse-id <id>] \
#       [--tok-sp <service-principal-bearer-token>] \
#       [--models databricks-claude-sonnet-4-6,databricks-gpt-5-4,databricks-gpt-5-2] \
#       [--embed-model databricks-gte-large-en] \
#       [--out <report.md>]
#   --models      : comma list, streaming+tools family coverage (S4) — covers BOTH
#                   Claude and GPT (item 1). GPT ends the stream with
#                   finish_reason=tool_calls and NO [DONE]; Claude emits [DONE].
#   --embed-model : classic embeddings endpoint; S3b probes it on the gateway
#                   /embeddings path (item 2).
#
# Lightweight post-deploy modes (skip the suite; confirm SP/403 legs):
#   # SP leg — after the deployed app has made tagged calls, confirm they landed
#   scripts/ai-gateway-validation/probe.sh --profile <P> --warehouse-id <id> \
#       --requery [--tag-app genie-workbench] [--since '6 HOURS'] [--marker <substr>]
#     -> PASS when a requester_type=SERVICE_PRINCIPAL row appears for the app tag.
#
#   # 403 leg — fire one tagged call as a de-entitled identity, inspect the status
#   scripts/ai-gateway-validation/probe.sh --profile <P> \
#       --leg403 <bearer-token> [--model system.ai.claude-sonnet-4-6]
#     -> PASS when it returns a clean 401/403 with a parseable body (downgrade signal).
#     (Pass the app SP token here to seed an SP-tagged row, then --requery it.)
#
#   # S9 · BYOK leg — probe a CUSTOM Unity Gateway Model Service (customer-owned,
#   # full catalog.schema.name, not system.ai.*). Runs BYOK-1..5.
#   scripts/ai-gateway-validation/probe.sh --profile <P> \
#       --byok <catalog.schema.model_service>
#     -> BYOK-1 chat/completions drop-in, BYOK-2/2b tools (reasoning_effort:none),
#        BYOK-3 /responses, BYOK-4 tagging, BYOK-5 enumeration gap.
#
# Notes on identity:
#   The workbench runs under OBO (the *user's* token) with an SP fallback.
#   A personal CLI profile authenticates as the USER, which is the most
#   important (and most uncertain) leg. Pass --tok-sp to additionally exercise
#   the service-principal identity; without it, the SP leg is reported SKIPPED.
# =============================================================================

set -uo pipefail

PROFILE=""; HOST=""; WAREHOUSE_ID=""; TOK_SP=""; OUT=""
# lightweight post-deploy modes (skip the full suite):
MODE_REQUERY=""      # --requery : read system.ai_gateway.usage for tagged rows (SP-leg confirm)
LEG403_TOK=""        # --leg403 <token> : fire one tagged call as a (de-entitled) identity
TAG_APP="genie-workbench"  # --tag-app
SINCE="3 HOURS"      # --since (SQL INTERVAL literal, e.g. '30 MINUTES','6 HOURS')
MARKER=""            # --marker <substr> : filter request_tags['component'] LIKE %substr%
MODEL_OVERRIDE=""    # --model <classic-or-gateway-name> for --leg403
BYOK_MODEL=""        # --byok <catalog.schema.name> : S9 BYOK / custom Model Service probe
CURATED=(
  databricks-claude-opus-5
  databricks-claude-sonnet-5
  databricks-claude-opus-4-8
  databricks-claude-opus-4-7
  databricks-claude-sonnet-4-6   # workbench default (LLM_MODEL)
  databricks-gpt-5-4
  databricks-gpt-5-2
)
DEFAULT_MODEL="databricks-claude-sonnet-4-6"
# S4 streaming+tools family coverage (both Claude and GPT), and S3 embeddings model:
STREAM_MODELS="databricks-claude-sonnet-4-6,databricks-gpt-5-4,databricks-gpt-5-2"
EMBED_MODEL="databricks-gte-large-en"   # classic name; gateway name derived via strip rule

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile) PROFILE="$2"; shift 2;;
    --host) HOST="$2"; shift 2;;
    --warehouse-id) WAREHOUSE_ID="$2"; shift 2;;
    --tok-sp) TOK_SP="$2"; shift 2;;
    --out) OUT="$2"; shift 2;;
    --requery) MODE_REQUERY="yes"; shift;;
    --leg403) LEG403_TOK="$2"; shift 2;;
    --tag-app) TAG_APP="$2"; shift 2;;
    --since) SINCE="$2"; shift 2;;
    --marker) MARKER="$2"; shift 2;;
    --model) MODEL_OVERRIDE="$2"; shift 2;;
    --byok) BYOK_MODEL="$2"; shift 2;;
    --models) STREAM_MODELS="$2"; shift 2;;
    --embed-model) EMBED_MODEL="$2"; shift 2;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0;;
    *) echo "Unknown arg: $1" >&2; exit 2;;
  esac
done

[[ -z "$PROFILE" ]] && { echo "ERROR: --profile is required" >&2; exit 2; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TS="$(date +%Y%m%d-%H%M%S)"
[[ -z "$OUT" ]] && OUT="$SCRIPT_DIR/report-$TS.md"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

# ---- counters -------------------------------------------------------------
PASS=0; FAIL=0; WARN=0; SKIP=0
declare -a GATE_FAILS=()

c_reset=$'\033[0m'; c_g=$'\033[32m'; c_r=$'\033[31m'; c_y=$'\033[33m'; c_b=$'\033[36m'
log(){ echo -e "$*"; }
section(){ log "\n${c_b}=== $* ===${c_reset}"; echo -e "\n## $*\n" >> "$OUT"; }
record(){ # record <PASS|FAIL|WARN|SKIP> <msg> [gate]
  local s="$1"; shift; local gate=""; local msg="$1"; shift || true
  [[ "${1:-}" == "gate" ]] && gate="yes"
  case "$s" in
    PASS) PASS=$((PASS+1)); log "  ${c_g}PASS${c_reset} $msg"; echo "- ✅ **PASS** — $msg" >> "$OUT";;
    FAIL) FAIL=$((FAIL+1)); log "  ${c_r}FAIL${c_reset} $msg"; echo "- ❌ **FAIL** — $msg" >> "$OUT";
          [[ -n "$gate" ]] && GATE_FAILS+=("$msg");;
    WARN) WARN=$((WARN+1)); log "  ${c_y}WARN${c_reset} $msg"; echo "- ⚠️ **WARN** — $msg" >> "$OUT";;
    SKIP) SKIP=$((SKIP+1)); log "  ${c_y}SKIP${c_reset} $msg"; echo "- ⏭️ **SKIP** — $msg" >> "$OUT";;
  esac
}
note(){ echo "$*" >> "$OUT"; log "  ${c_b}·${c_reset} $*"; }

# ---- resolve host + token -------------------------------------------------
[[ -z "$HOST" ]] && HOST="$(databricks auth describe -p "$PROFILE" 2>/dev/null | awk '/^Host:/{print $2; exit}')"
[[ -z "$HOST" ]] && { echo "ERROR: could not resolve host; pass --host" >&2; exit 2; }
HOST="${HOST%/}"

get_user_token(){
  local t
  t="$(databricks auth token -p "$PROFILE" 2>/dev/null | python3 -c 'import sys,json;print(json.load(sys.stdin).get("access_token",""))' 2>/dev/null)"
  if [[ -z "$t" ]]; then
    # fallback: PAT stored in ~/.databrickscfg
    t="$(python3 - "$PROFILE" <<'PY'
import configparser,os,sys
p=configparser.ConfigParser(); p.read(os.path.expanduser("~/.databrickscfg"))
print(p[sys.argv[1]].get("token","")) if sys.argv[1] in p else print("")
PY
)"
  fi
  echo "$t"
}
TOK_USER="$(get_user_token)"
[[ -z "$TOK_USER" ]] && { echo "ERROR: could not obtain a user bearer token for profile $PROFILE" >&2; exit 2; }

# ---- HTTP helpers ---------------------------------------------------------
GW="$HOST/ai-gateway/mlflow/v1/chat/completions"     # gateway unified chat
GW_EMBED="$HOST/ai-gateway/mlflow/v1/embeddings"     # gateway unified embeddings
gw_name(){ echo "system.ai.${1#databricks-}"; }       # classic -> gateway model name

# Provider-agnostic terminal check: Anthropic emits [DONE]; OpenAI/GPT ends by
# closing the stream after a finish_reason=tool_calls chunk (NO [DONE]). Accept either.
stream_tools_ok(){ # <sse_file>
  grep -q '"tool_calls"' "$1" && { grep -q '"finish_reason":"tool_calls"' "$1" || grep -q '\[DONE\]' "$1"; }
}

# shared single-tool definition (S4 + S9/BYOK)
TOOLS_DEF='[{"type":"function","function":{"name":"get_weather","description":"get weather","parameters":{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}}}]'

# POST json, echo "<http_code>\n<body>"
post(){ # post <url> <token> <json> [extra_header]
  local url="$1" tok="$2" body="$3" hdr="${4:-}"
  if [[ -n "$hdr" ]]; then
    curl -sS -m 60 -X POST "$url" -H "Authorization: Bearer $tok" \
      -H "Content-Type: application/json" -H "$hdr" -d "$body" -w $'\n%{http_code}'
  else
    curl -sS -m 60 -X POST "$url" -H "Authorization: Bearer $tok" \
      -H "Content-Type: application/json" -d "$body" -w $'\n%{http_code}'
  fi
}
http_code(){ tail -n1 <<<"$1"; }
http_body(){ sed '$d' <<<"$1"; }
json_field(){ python3 -c 'import sys,json;
try:
 d=json.load(sys.stdin); print(eval("d"+sys.argv[1]))
except Exception as e: print("")' "$1" 2>/dev/null; }

# ---- SQL (usage table) via Statement Execution API ------------------------
run_sql(){ # run_sql <query> -> prints result rows as TSV
  [[ -z "$WAREHOUSE_ID" ]] && { echo "__NO_WAREHOUSE__"; return; }
  local q="$1" payload resp sid state
  payload="$(python3 -c 'import json,sys;print(json.dumps({"warehouse_id":sys.argv[1],"statement":sys.argv[2],"wait_timeout":"50s"}))' "$WAREHOUSE_ID" "$q")"
  resp="$(curl -sS -m 90 -X POST "$HOST/api/2.0/sql/statements" -H "Authorization: Bearer $TOK_USER" -H "Content-Type: application/json" -d "$payload")"
  python3 - <<PY
import json
r=json.loads('''$resp''')
st=r.get("status",{}).get("state")
if st!="SUCCEEDED":
    print("__SQL_STATE__:"+str(st)); print(json.dumps(r.get("status",{})))
else:
    data=r.get("result",{}).get("data_array") or []
    for row in data: print("\t".join("" if c is None else str(c) for c in row))
PY
}

# =============================================================================
# LIGHTWEIGHT MODES (post-deploy) — run one, then exit before the full suite
# =============================================================================

# --byok <catalog.schema.name>: S9 — probe a CUSTOM Unity Gateway Model Service
# (customer-owned, full 3-level UC name, NOT system.ai.*). Runs BYOK-1..5.
if [[ -n "$BYOK_MODEL" ]]; then
  log "${c_b}[S9 · BYOK]${c_reset} custom Model Service = $BYOK_MODEL  host=$HOST"
  # BYOK-1: chat/completions (the workbench's own path)
  R1="$(post "$GW" "$TOK_USER" '{"model":"'"$BYOK_MODEL"'","max_tokens":16,"messages":[{"role":"user","content":"Reply with the single word: pong"}]}')"
  if [[ "$(http_code "$R1")" == "200" ]]; then
    back="$(http_body "$R1" | json_field "['model']")"
    log "  ${c_g}BYOK-1 PASS${c_reset} chat/completions 200 (drop-in; backing model=$back). No Responses adapter needed."
  else
    log "  ${c_r}BYOK-1 FAIL${c_reset} chat/completions $(http_code "$R1") — custom service may be Responses-only: $(http_body "$R1" | head -c 160)"
  fi
  # BYOK-2: streaming+tools default (reasoning models may 400)
  curl -sS -N -m 90 -X POST "$GW" -H "Authorization: Bearer $TOK_USER" -H "Content-Type: application/json" \
    -d '{"model":"'"$BYOK_MODEL"'","max_tokens":256,"stream":true,"tools":'"$TOOLS_DEF"',"messages":[{"role":"user","content":"Weather in Paris? Use get_weather."}]}' > "$TMP/byok2.sse" 2>/dev/null
  if stream_tools_ok "$TMP/byok2.sse"; then
    log "  ${c_g}BYOK-2 PASS${c_reset} streaming+tools OK with no extra flags"
  elif grep -q 'reasoning_effort' "$TMP/byok2.sse"; then
    log "  ${c_y}BYOK-2 WARN${c_reset} model-specific: tools need reasoning_effort:none (reasoning model). Retrying (BYOK-2b)…"
    curl -sS -N -m 90 -X POST "$GW" -H "Authorization: Bearer $TOK_USER" -H "Content-Type: application/json" \
      -d '{"model":"'"$BYOK_MODEL"'","max_tokens":256,"stream":true,"reasoning_effort":"none","tools":'"$TOOLS_DEF"',"messages":[{"role":"user","content":"Weather in Paris? Use get_weather."}]}' > "$TMP/byok2b.sse" 2>/dev/null
    if stream_tools_ok "$TMP/byok2b.sse"; then
      log "  ${c_g}BYOK-2b PASS${c_reset} streaming+tools OK with reasoning_effort:none — client tool path must set this."
    else
      log "  ${c_r}BYOK-2b FAIL${c_reset} tools still failing with reasoning_effort:none (see $TMP/byok2b.sse)"
    fi
  else
    log "  ${c_r}BYOK-2 FAIL${c_reset} streaming+tools failed (see $TMP/byok2.sse)"
  fi
  # BYOK-3: Responses API surface
  R3="$(post "$HOST/ai-gateway/mlflow/v1/responses" "$TOK_USER" '{"model":"'"$BYOK_MODEL"'","max_output_tokens":64,"input":[{"role":"user","content":[{"type":"input_text","text":"Reply pong"}]}]}')"
  [[ "$(http_code "$R3")" == "200" ]] && log "  ${c_g}BYOK-3 PASS${c_reset} Responses API (/responses) 200" || log "  ${c_y}BYOK-3 NOTE${c_reset} /responses $(http_code "$R3")"
  # BYOK-4: tagging accepted at the custom service
  MK="byok-$(date +%s)"
  R4="$(post "$GW" "$TOK_USER" '{"model":"'"$BYOK_MODEL"'","max_tokens":8,"messages":[{"role":"user","content":"ping"}]}' \
        "Databricks-Ai-Gateway-Request-Tags: {\"application\":\"$TAG_APP\",\"component\":\"BYOK-$MK\"}")"
  [[ "$(http_code "$R4")" == "200" ]] && log "  ${c_g}BYOK-4 PASS${c_reset} tagged call accepted (marker=BYOK-$MK; --requery --marker BYOK-$MK after lag)" || log "  ${c_r}BYOK-4 FAIL${c_reset} tagged call $(http_code "$R4")"
  # BYOK-5: enumeration — does any standard list API surface this object?
  SE="$(databricks --profile "$PROFILE" serving-endpoints list 2>/dev/null | grep -ci "$(echo "$BYOK_MODEL" | awk -F. '{print $NF}')")"
  if [[ "$SE" =~ ^[1-9] ]]; then
    log "  ${c_g}BYOK-5 PASS${c_reset} custom service is listed by serving-endpoints (auto-discoverable)"
  else
    log "  ${c_y}BYOK-5 GAP${c_reset} NOT in serving-endpoints list — BYOK ids must be user-configured (auto-discovery pending correct API)"
  fi
  exit 0
fi

# --leg403 <token>: fire ONE tagged gateway call as the supplied identity and
# report the HTTP status/body. Use a de-entitled user's token to confirm the
# 403 downgrade shape, or the app SP token to seed an SP-tagged row for --requery.
if [[ -n "$LEG403_TOK" ]]; then
  MODEL="${MODEL_OVERRIDE:-$(gw_name "$DEFAULT_MODEL")}"
  MK="leg403-$(date +%s)"
  log "${c_b}[leg403]${c_reset} POST $GW model=$MODEL as supplied token (marker=$MK)"
  R="$(post "$GW" "$LEG403_TOK" '{"model":"'"$MODEL"'","max_tokens":8,"messages":[{"role":"user","content":"entitlement probe"}]}' \
        "Databricks-Ai-Gateway-Request-Tags: {\"application\":\"$TAG_APP\",\"component\":\"$MK\"}")"
  code="$(http_code "$R")"; body="$(http_body "$R" | head -c 300)"
  log "  HTTP $code"
  log "  body: $body"
  case "$code" in
    200) log "  ${c_g}RESULT${c_reset} identity IS entitled (200). If this was meant to be de-entitled, the grant is too broad. Row tagged '$MK' — confirm via --requery --marker $MK.";;
    401|403) log "  ${c_g}RESULT${c_reset} clean $code — explicit forbidden; the app maps this to suggest-only. Body is parseable above.";;
    404) log "  ${c_g}RESULT${c_reset} 404 NOT_FOUND — this is how de-entitlement APPEARS: the gateway HIDES a Model Service the caller can't access (same shape as an unknown model, S6). ⚠️ The app must treat a 404 on a model it believes is configured as 'not entitled / unavailable' -> downgrade, NOT as a hard error. Entitlement cannot be keyed on 403.";;
    *) log "  ${c_y}RESULT${c_reset} unexpected $code — inspect body; confirm error handling.";;
  esac
  exit 0
fi

# --requery: read-only rollup of tagged rows from system.ai_gateway.usage.
# Confirms the SP leg (look for requester_type=SERVICE_PRINCIPAL) and any marker,
# WITHOUT re-running the suite. Requires --warehouse-id.
if [[ -n "$MODE_REQUERY" ]]; then
  [[ -z "$WAREHOUSE_ID" ]] && { echo "ERROR: --requery needs --warehouse-id" >&2; exit 2; }
  MFILT=""; [[ -n "$MARKER" ]] && MFILT=" AND request_tags['component'] LIKE '%$MARKER%'"
  log "${c_b}[requery]${c_reset} app='$TAG_APP' since=$SINCE marker='${MARKER:-<any>}' host=$HOST"
  log "  (usage table is asynchronous — allow for ingestion lag, minutes-scale)"
  log "\n${c_b}-- ingestion lag --${c_reset}"
  run_sql "SELECT timestampdiff(MINUTE, MAX(event_time), current_timestamp()) AS lag_min, MAX(event_time) AS head FROM system.ai_gateway.usage"
  log "\n${c_b}-- rollup by requester_type + component (SP leg shows as SERVICE_PRINCIPAL) --${c_reset}"
  run_sql "SELECT requester_type, request_tags['component'] AS component, count(*) AS n, sum(total_tokens) AS tokens
           FROM system.ai_gateway.usage
           WHERE request_tags['application']='$TAG_APP'$MFILT
             AND event_time>current_timestamp()-INTERVAL $SINCE
           GROUP BY requester_type, request_tags['component'] ORDER BY n DESC"
  log "\n${c_b}-- most recent tagged rows --${c_reset}"
  run_sql "SELECT date_format(event_time,'HH:mm:ss') AS t, requester, requester_type, request_tags['component'] AS component, api_type, total_tokens
           FROM system.ai_gateway.usage
           WHERE request_tags['application']='$TAG_APP'$MFILT
             AND event_time>current_timestamp()-INTERVAL $SINCE
           ORDER BY event_time DESC LIMIT 20"
  log "\n${c_b}Tip:${c_reset} SP-leg PASS = a SERVICE_PRINCIPAL row for your app tag appears above."
  exit 0
fi

# =============================================================================
# REPORT HEADER  (full suite)
# =============================================================================
{
  echo "# AI Gateway migration de-risk report"
  echo ""
  echo "- Generated: $(date)"
  echo "- Profile: \`$PROFILE\`  |  Host: \`$HOST\`"
  echo "- Warehouse (usage-table SQL): \`${WAREHOUSE_ID:-<none: S5 SQL skipped>}\`"
  echo "- SP identity leg: $([[ -n "$TOK_SP" ]] && echo 'provided' || echo 'SKIPPED (no --tok-sp)')"
  echo "- Streaming+tools family coverage (S4): \`$STREAM_MODELS\`"
  echo "- Embeddings model (S3b): \`$EMBED_MODEL\` -> \`$(gw_name "$EMBED_MODEL")\`"
} > "$OUT"

log "${c_b}AI Gateway de-risk probe${c_reset}  host=$HOST  profile=$PROFILE  report=$OUT"

# =============================================================================
section "S0 · Prerequisites & identity"
ME="$(databricks --profile "$PROFILE" current-user me 2>/dev/null | json_field "['userName']")"
[[ -z "$ME" ]] && ME="$(databricks --profile "$PROFILE" current-user me 2>/dev/null | json_field "['emails'][0]['value']")"
if [[ -n "$ME" ]]; then record PASS "CLI auth OK as: $ME"; else record FAIL "CLI cannot authenticate profile $PROFILE" gate; fi
note "Region/entitlement: confirm this workspace is in a Unity Gateway-supported region (manual)."

# =============================================================================
section "S1 · Auth & reachability parity (gateway path)"
R="$(post "$GW" "$TOK_USER" '{"model":"'"$(gw_name "$DEFAULT_MODEL")"'","max_tokens":8,"messages":[{"role":"user","content":"ping"}]}')"
if [[ "$(http_code "$R")" == "200" ]]; then record PASS "USER token accepted on gateway ($(gw_name "$DEFAULT_MODEL"))"; else record FAIL "USER token rejected on gateway: $(http_code "$R") $(http_body "$R" | head -c 200)" gate; fi
if [[ -n "$TOK_SP" ]]; then
  RS="$(post "$GW" "$TOK_SP" '{"model":"'"$(gw_name "$DEFAULT_MODEL")"'","max_tokens":8,"messages":[{"role":"user","content":"ping"}]}')"
  if [[ "$(http_code "$RS")" == "200" ]]; then record PASS "SP token accepted on gateway"; else record FAIL "SP token rejected on gateway: $(http_code "$RS")"; fi
else
  record SKIP "SP identity leg (pass --tok-sp to exercise the workbench SP fallback)"
fi

# =============================================================================
section "S2 · Model-name mapping + enumeration (picker / model_catalog)"
note "Rule under test: gateway name = 'system.ai.' + classic_endpoint_name without the 'databricks-' prefix."
echo "" >> "$OUT"
echo "| Classic endpoint | Gateway name | Classic /invocations | Gateway /mlflow/v1 |" >> "$OUT"
echo "|---|---|---|---|" >> "$OUT"
MAP_OK=1
for m in "${CURATED[@]}"; do
  gn="$(gw_name "$m")"
  CC="$(post "$HOST/serving-endpoints/$m/invocations" "$TOK_USER" '{"max_tokens":6,"messages":[{"role":"user","content":"hi"}]}')"; cc="$(http_code "$CC")"
  GG="$(post "$GW" "$TOK_USER" '{"model":"'"$gn"'","max_tokens":6,"messages":[{"role":"user","content":"hi"}]}')"; gg="$(http_code "$GG")"
  echo "| \`$m\` | \`$gn\` | $cc | $gg |" >> "$OUT"
  if [[ "$gg" == "200" ]]; then :; else MAP_OK=0; record FAIL "gateway name mismatch for $m -> $gn returned $gg (needs explicit mapping)"; fi
  [[ "$cc" != "200" ]] && record WARN "classic endpoint $m returned $cc in this workspace (model availability differs by workspace)"
done
[[ "$MAP_OK" == "1" ]] && record PASS "all ${#CURATED[@]} curated models resolve on the gateway via the strip-prefix rule"
UC_CNT="$(databricks --profile "$PROFILE" api get "/api/2.1/unity-catalog/models?catalog_name=system&schema_name=ai&max_results=50" 2>/dev/null | python3 -c 'import sys,json;
try: print(len(json.load(sys.stdin).get("registered_models",[])))
except Exception: print("?")')"
note "Enumeration surface: UC registered-models list under system.ai returned ~$UC_CNT entries. NOTE: those full_names carry the 'databricks-' prefix that the GATEWAY name drops — so the picker must map, not echo, UC names. A per-model probe (this table) is the authoritative source."

# =============================================================================
section "S3 · Non-streaming chat parity (llm_utils + GSO call_llm)"
# GSO shape: response_format json_object, no temperature.
GJSON='{"model":"'"$(gw_name "$DEFAULT_MODEL")"'","max_tokens":64,"response_format":{"type":"json_object"},"messages":[{"role":"user","content":"Return JSON object {\"ok\":true}"}]}'
R3="$(post "$GW" "$TOK_USER" "$GJSON")"
if [[ "$(http_code "$R3")" == "200" ]]; then
  C3="$(http_body "$R3" | json_field "['choices'][0]['message']['content']")"
  if [[ -n "$C3" ]]; then record PASS "gateway non-streaming + response_format(json_object) returns content"; else record FAIL "gateway non-streaming returned empty content"; fi
else
  record WARN "gateway rejected response_format (code $(http_code "$R3")); workbench already has a response_format retry-fallback, so this is tolerable"
fi
# classic baseline (same GSO shape sans model in URL)
CJSON='{"max_tokens":64,"response_format":{"type":"json_object"},"messages":[{"role":"user","content":"Return JSON object {\"ok\":true}"}]}'
RC3="$(post "$HOST/serving-endpoints/$DEFAULT_MODEL/invocations" "$TOK_USER" "$CJSON")"
[[ "$(http_code "$RC3")" == "200" ]] && record PASS "classic non-streaming baseline OK (regression anchor)" || record WARN "classic non-streaming baseline code $(http_code "$RC3")"

# item 3: prompt-based JSON is GSO's REAL path (it strips response_format on 400 and re-asks).
# Confirm the gateway returns parseable JSON from a plain prompt.
PJ="$(post "$GW" "$TOK_USER" '{"model":"'"$(gw_name "$DEFAULT_MODEL")"'","max_tokens":64,"messages":[{"role":"user","content":"Reply with ONLY a JSON object {\"ok\":true}"}]}')"
if [[ "$(http_code "$PJ")" == "200" ]]; then
  VALID="$(http_body "$PJ" | python3 -c 'import sys,json
try:
 c=json.load(sys.stdin)["choices"][0]["message"]["content"]
 s=c.find("{"); e=c.rfind("}")
 json.loads(c[s:e+1]); print("yes")
except Exception: print("no")')"
  [[ "$VALID" == "yes" ]] && record PASS "gateway plain-prompt JSON parses (GSO fallback path works without response_format)" || record WARN "gateway plain-prompt JSON did not parse cleanly"
else
  record FAIL "gateway plain-prompt JSON call failed: $(http_code "$PJ")"
fi

# =============================================================================
section "S3b · Embeddings on gateway (leakage firewall + mv_suggest) ⭐"
# Workbench embeddings go through w.serving_endpoints.query() today (classic path).
# On the gateway they use /ai-gateway/mlflow/v1/embeddings. NOTE: the gateway name
# follows the SAME strip rule (databricks-gte-large-en -> system.ai.gte-large-en);
# the UC entity_name (gte_large_en_v1_5) is a DIFFERENT string and 404s here.
EGN="$(gw_name "$EMBED_MODEL")"
EMB="$(post "$GW_EMBED" "$TOK_USER" '{"model":"'"$EGN"'","input":"firewall preflight ping"}')"
if [[ "$(http_code "$EMB")" == "200" ]]; then
  DIM="$(http_body "$EMB" | python3 -c 'import sys,json
try: print(len(json.load(sys.stdin)["data"][0]["embedding"]))
except Exception: print(0)')"
  [[ "$DIM" -gt 0 ]] && record PASS "gateway embeddings OK on $EGN (dim=$DIM)" || record FAIL "gateway embeddings returned no vector"
else
  record FAIL "gateway embeddings failed on $EGN: $(http_code "$EMB") $(http_body "$EMB" | head -c 140)"
fi
# classic embeddings baseline (today's path)
ECB="$(post "$HOST/serving-endpoints/$EMBED_MODEL/invocations" "$TOK_USER" '{"input":"firewall preflight ping"}')"
[[ "$(http_code "$ECB")" == "200" ]] && record PASS "classic embeddings baseline OK ($EMBED_MODEL)" || record WARN "classic embeddings baseline code $(http_code "$ECB")"
note "Migration note: the embeddings site (leakage.get_embedding) uses the SDK serving_endpoints.query(); routing it via the gateway means switching to /ai-gateway/mlflow/v1/embeddings — a distinct task from the 4 chat sites."

# =============================================================================
section "S3c · Rate-limit / 429 posture (item 4)"
# Not a load test: 5 tiny back-to-back calls to observe the gateway's rate-limit
# surface. GSO/llm_utils already retry on 429; the point is to confirm a 429 (if
# any) carries a parseable body and a standard code the retry logic can key off.
codes=""; saw429=0; body429=""
for i in 1 2 3 4 5; do
  RR="$(post "$GW" "$TOK_USER" '{"model":"'"$(gw_name "$DEFAULT_MODEL")"'","max_tokens":4,"messages":[{"role":"user","content":"hi"}]}')"
  cc="$(http_code "$RR")"; codes="$codes $cc"
  if [[ "$cc" == "429" ]]; then saw429=1; body429="$(http_body "$RR" | head -c 160)"; fi
done
if [[ "$saw429" == "1" ]]; then
  record WARN "gateway returned 429 under a 5-call burst (codes:$codes) — retry/backoff exercised. body: $body429"
else
  record PASS "no 429 under a light 5-call burst (codes:$codes) — headroom OK; existing 429 retry remains the safety net"
fi

# =============================================================================
section "S4 · Streaming + tools (create_agent._stream_llm) ⭐ gating"
TOOLS='[{"type":"function","function":{"name":"get_weather","description":"get weather","parameters":{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}}}]'
S4BODY='{"model":"'"$(gw_name "$DEFAULT_MODEL")"'","max_tokens":256,"stream":true,"tools":'"$TOOLS"',"messages":[{"role":"user","content":"Weather in Paris? Use the tool."}]}'
curl -sS -N -m 60 -X POST "$GW" -H "Authorization: Bearer $TOK_USER" -H "Content-Type: application/json" -d "$S4BODY" > "$TMP/s4.sse" 2>/dev/null
if stream_tools_ok "$TMP/s4.sse"; then
  record PASS "gateway streaming emits tool_calls deltas + terminal finish_reason/[DONE] ($(gw_name "$DEFAULT_MODEL"))" gate
else
  record FAIL "gateway streaming+tools did NOT produce expected tool_calls terminal (see $TMP/s4.sse)" gate
fi
# reconstruct tool args + run turn-2 (assistant tool_calls + tool result) to prove the full loop
ARGS="$(grep '^data: ' "$TMP/s4.sse" | sed 's/^data: //' | python3 -c '
import sys,json
buf=""
tid=""; name=""
for ln in sys.stdin:
    ln=ln.strip()
    if not ln or ln=="[DONE]": continue
    try: d=json.loads(ln)
    except: continue
    for ch in d.get("choices",[]):
        for tc in (ch.get("delta",{}) or {}).get("tool_calls",[]) or []:
            tid = tc.get("id") or tid
            fn=tc.get("function",{}) or {}
            name = fn.get("name") or name
            buf += fn.get("arguments","") or ""
print(json.dumps({"id":tid or "call_1","name":name or "get_weather","args":buf or "{}"}))' )"
TID="$(echo "$ARGS" | json_field "['id']")"; TNAME="$(echo "$ARGS" | json_field "['name']")"; TARGS="$(echo "$ARGS" | json_field "['args']")"
T2="$(python3 -c '
import json,sys
tid,tname,targs=sys.argv[1],sys.argv[2],sys.argv[3]
msgs=[{"role":"user","content":"Weather in Paris? Use the tool."},
 {"role":"assistant","content":None,"tool_calls":[{"id":tid,"type":"function","function":{"name":tname,"arguments":targs or "{}"}}]},
 {"role":"tool","tool_call_id":tid,"content":"18C and sunny"}]
print(json.dumps({"model":sys.argv[4],"max_tokens":64,"messages":msgs}))' "$TID" "$TNAME" "$TARGS" "$(gw_name "$DEFAULT_MODEL")")"
R4B="$(post "$GW" "$TOK_USER" "$T2")"
if [[ "$(http_code "$R4B")" == "200" ]] && [[ -n "$(http_body "$R4B" | json_field "['choices'][0]['message']['content']")" ]]; then
  record PASS "gateway multi-turn tool-result round-trip completes (full Create Agent loop)"
else
  record FAIL "gateway tool-result round-trip failed: $(http_code "$R4B") $(http_body "$R4B" | head -c 160)"
fi
# classic streaming baseline (workbench today)
curl -sS -N -m 60 -X POST "$HOST/serving-endpoints/$DEFAULT_MODEL/invocations" -H "Authorization: Bearer $TOK_USER" -H "Content-Type: application/json" \
  -d '{"max_tokens":256,"stream":true,"tools":'"$TOOLS"',"messages":[{"role":"user","content":"Weather in Paris? Use the tool."}]}' > "$TMP/s4c.sse" 2>/dev/null
if stream_tools_ok "$TMP/s4c.sse"; then record PASS "classic streaming+tools baseline OK (regression anchor)"; else record WARN "classic streaming+tools baseline unexpected (see $TMP/s4c.sse)"; fi

# --- S4 family coverage (item 1): streaming+tools across BOTH Claude and GPT families ---
note "Streaming+tools family coverage across --models (GPT ends by closing the stream with finish_reason=tool_calls and NO [DONE]; Claude emits [DONE] — both are accepted):"
IFS=',' read -ra _SM <<< "$STREAM_MODELS"
for m in "${_SM[@]}"; do
  m="$(echo "$m" | xargs)"; [[ -z "$m" ]] && continue
  gn="$(gw_name "$m")"
  curl -sS -N -m 90 -X POST "$GW" -H "Authorization: Bearer $TOK_USER" -H "Content-Type: application/json" \
    -d '{"model":"'"$gn"'","max_tokens":256,"stream":true,"tools":'"$TOOLS"',"messages":[{"role":"user","content":"Weather in Paris? Use get_weather."}]}' > "$TMP/s4-$m.sse" 2>/dev/null
  if stream_tools_ok "$TMP/s4-$m.sse"; then
    term="$(grep -q '\[DONE\]' "$TMP/s4-$m.sse" && echo '[DONE]' || echo 'finish_reason=tool_calls (stream close)')"
    record PASS "streaming+tools OK on $gn — terminal: $term"
  else
    record FAIL "streaming+tools FAILED on $gn (see $TMP/s4-$m.sse)"
  fi
done

# =============================================================================
section "S5 · Request tagging -> system.ai_gateway.usage (cost attribution)"
MARK="derisk-$(date +%s)"
post "$GW" "$TOK_USER" '{"model":"'"$(gw_name "$DEFAULT_MODEL")"'","max_tokens":8,"messages":[{"role":"user","content":"g"}]}' \
  "Databricks-Ai-Gateway-Request-Tags: {\"application\":\"genie-workbench\",\"component\":\"GATEWAY-$MARK\"}" >/dev/null
post "$HOST/serving-endpoints/$DEFAULT_MODEL/invocations" "$TOK_USER" '{"max_tokens":8,"messages":[{"role":"user","content":"c"}]}' \
  "Databricks-Ai-Gateway-Request-Tags: {\"application\":\"genie-workbench\",\"component\":\"CLASSIC-$MARK\"}" >/dev/null
CALL_EPOCH="$(date +%s)"
record PASS "fired tagged calls on gateway + classic (marker=$MARK)"
if [[ -n "$WAREHOUSE_ID" ]]; then
  # (a) prove the mechanism is live in this workspace, independent of our own (lagging) rows
  LIVE="$(run_sql "SELECT count(*) FROM system.ai_gateway.usage WHERE size(map_keys(request_tags))>0 AND event_time>current_timestamp()-INTERVAL 2 DAYS")"
  if [[ "$LIVE" =~ ^[1-9] ]]; then record PASS "request_tags mechanism is LIVE here ($LIVE tagged rows in last 2 days)"; else record WARN "no tagged rows seen in last 2 days — usage tracking may be off/region-limited"; fi
  # (b) ingestion is asynchronous — do not FAIL on lag; compare table head to our call time
  note "Waiting 150s, then measuring ingestion lag (usage table is asynchronous, minutes-scale)…"; sleep 150
  LAG="$(run_sql "SELECT timestampdiff(MINUTE, MAX(event_time), current_timestamp()) FROM system.ai_gateway.usage")"
  MAXE="$(run_sql "SELECT unix_timestamp(MAX(event_time)) FROM system.ai_gateway.usage")"
  ROWS="$(run_sql "SELECT request_tags['component'] AS c, api_type, destination_type, total_tokens FROM system.ai_gateway.usage WHERE request_tags['application']='genie-workbench' AND request_tags['component'] LIKE '%$MARK' ORDER BY event_time DESC LIMIT 10")"
  echo -e "\n\`\`\`\n$ROWS\n\`\`\`" >> "$OUT"
  note "usage-table ingestion lag ≈ ${LAG} min"
  if grep -q "GATEWAY-$MARK" <<<"$ROWS"; then
    record PASS "gateway request_tags landed in system.ai_gateway.usage"
    if grep -q "CLASSIC-$MARK" <<<"$ROWS"; then
      record PASS "CLASSIC path request_tags ALSO landed -> phase-0 tag-first is viable without migrating"
    else
      record WARN "gateway tags landed but classic-path tags did NOT -> tagging requires the gateway path (informs phasing)"
    fi
  elif [[ -n "$MAXE" && "$MAXE" -lt "$CALL_EPOCH" ]]; then
    record SKIP "our tagged rows not yet ingested (table head is behind our call time by ≈${LAG} min) — re-query marker '$MARK' after the lag clears; NOT a failure"
  else
    record FAIL "table head has advanced past our call time but marker '$MARK' is absent -> tagging did not record"
  fi
else
  record SKIP "usage-table SQL (pass --warehouse-id to auto-verify request_tags landing; else query system.ai_gateway.usage manually for marker $MARK)"
fi

# =============================================================================
section "S6 · Entitlement / failure-mode shape (OBO robustness)"
RNF="$(post "$GW" "$TOK_USER" '{"model":"system.ai.this-model-does-not-exist","max_tokens":8,"messages":[{"role":"user","content":"x"}]}')"
if [[ "$(http_code "$RNF")" == "404" ]]; then
  record PASS "unknown model service returns a clean 404 with parseable body ($(http_body "$RNF" | head -c 80))"
else
  record WARN "unknown model service returned $(http_code "$RNF") (expected 404) — confirm error handling"
fi
note "Manual leg: repeat S1 as a user WITHOUT a UC grant to a chosen model service; confirm a deterministic 403 the app can downgrade on (do NOT reuse GET /permissions — that probes the SP, not the user)."

# =============================================================================
section "S7 · Regression / fallback proof (classic path unchanged)"
REG_OK=1
for m in "$DEFAULT_MODEL" databricks-gpt-5-2; do
  CC="$(post "$HOST/serving-endpoints/$m/invocations" "$TOK_USER" '{"max_tokens":6,"messages":[{"role":"user","content":"hi"}]}')"
  [[ "$(http_code "$CC")" == "200" ]] || { REG_OK=0; record WARN "classic path $m returned $(http_code "$CC")"; }
done
[[ "$REG_OK" == "1" ]] && record PASS "classic serving-endpoints path still fully green -> safe as fallback behind a flag"

# =============================================================================
section "S8 · GO / NO-GO decision"
{
  echo ""
  echo "| Metric | Count |"
  echo "|---|---|"
  echo "| PASS | $PASS |"
  echo "| FAIL | $FAIL |"
  echo "| WARN | $WARN |"
  echo "| SKIP | $SKIP |"
  echo ""
} >> "$OUT"
if [[ ${#GATE_FAILS[@]} -eq 0 && $FAIL -eq 0 ]]; then
  DECISION="GO — gateway is a drop-in for all workbench request shapes; proceed to single-seam refactor (gateway-preferred + flagged classic fallback)."
elif [[ ${#GATE_FAILS[@]} -eq 0 ]]; then
  DECISION="CONDITIONAL GO — no gating failures, but review WARN/FAIL items before coding."
else
  DECISION="NO-GO on the simple path — gating failure(s): ${GATE_FAILS[*]}"
fi
echo "**DECISION:** $DECISION" >> "$OUT"

log "\n${c_b}===================== SUMMARY =====================${c_reset}"
log "PASS=$PASS FAIL=$FAIL WARN=$WARN SKIP=$SKIP"
log "DECISION: $DECISION"
log "Report written to: ${c_g}$OUT${c_reset}"
