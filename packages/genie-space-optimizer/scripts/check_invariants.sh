#!/usr/bin/env bash
# Deterministic architectural invariant checks for the GSO codebase.
#
# Invoked as a PostToolUse Edit|Write hook by Claude Code. See
# packages/genie-space-optimizer/docs/llmdrivenarchitecture/goalMode/05-hook-and-gate-config.md
# for the exact settings.json wiring.
#
# Each check encodes one rule that closes a class of bug previously
# rediscovered by lever-loop trials. A non-zero exit blocks the edit
# that would reintroduce the bug.
#
# Exit codes:
#   0 — no violation
#   2 — violation present

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
SRC_DIR="$REPO_ROOT/packages/genie-space-optimizer/src/genie_space_optimizer"

if [ ! -d "$SRC_DIR" ]; then
  exit 0
fi

if ! command -v rg >/dev/null 2>&1; then
  echo "[check_invariants] warn: ripgrep (rg) not found; skipping check" >&2
  exit 0
fi

FAILED=0
FAIL_REPORT=""

# Helper: report a violation. Args: rule_id, rule_description, hits.
report() {
  local id="$1" desc="$2" hits="$3"
  FAILED=1
  FAIL_REPORT="$FAIL_REPORT
================================================================
BLOCK: invariant violation: $id
$desc
----------------------------------------------------------------
$hits"
}

# Quarantined modules whose re-import is forbidden EVERYWHERE (these
# were deleted in the SM-cutover deletion-first plan).
QUARANTINED_MODULES=(
  "archetype_catalog"
  "lever_rotation"
  "llm_direct_slice_resolver"
  "rca_repair_coverage"
  "forced_synthesis"
)

for mod in "${QUARANTINED_MODULES[@]}"; do
  hits=$(rg --type py "from\s+\S*${mod}\s+import|import\s+\S*${mod}([\.\s]|$)" \
            "$SRC_DIR" 2>/dev/null || true)
  if [ -n "$hits" ]; then
    report "QUARANTINED_MODULE_RE_IMPORT:${mod}" \
      "Module '${mod}' was deleted in plan 'sm-cutover-deletion-first_bbfafbf1' and must not be re-imported anywhere." \
      "$hits"
  fi
done

# Quarantined modules whose re-import is forbidden OUTSIDE _legacy/
# (they may live inside _legacy/ but must not leak out).
LEGACY_QUARANTINED=(
  "routing_gate"
  "escalation_ladder"
)

for mod in "${LEGACY_QUARANTINED[@]}"; do
  hits=$(rg --type py --glob '!**/_legacy/**' \
            "from\s+\S*${mod}\s+import|import\s+\S*${mod}([\.\s]|$)" \
            "$SRC_DIR" 2>/dev/null || true)
  if [ -n "$hits" ]; then
    report "LEGACY_QUARANTINED_RE_IMPORT:${mod}" \
      "Module '${mod}' is quarantined to _legacy/ and must not be re-imported outside _legacy/." \
      "$hits"
  fi
done

# Hand-rolled QID extraction in dispatch / transformer code. Use
# `_qid_extraction.extract_question_id` instead.
hits=$(rg --type py \
        --glob '!**/_legacy/**' \
        --glob '!**/_qid_extraction.py' \
        --glob '!**/test_*.py' \
        --glob '!**/*_test.py' \
        "row\.get\(\"question_id\"\)|row\[\"question_id\"\]" \
        "$SRC_DIR/optimization/state_machine/" \
        "$SRC_DIR/optimization/optimizer.py" \
        2>/dev/null || true)
if [ -n "$hits" ]; then
  report "HAND_ROLLED_QID_EXTRACTION" \
    "Do not extract question_id with row.get / row[]; use _qid_extraction.extract_question_id instead. See packages/genie-space-optimizer/docs/llmdrivenarchitecture/v5/canonical-row-shape-adapter_f0206be1.plan.md" \
    "$hits"
fi

# Per-QID overfit branches (`if qid == "gs_001": ...`).
hits=$(rg --type py --glob '!**/test_*.py' --glob '!**/_legacy/**' \
        '\bif\s+(state\.)?qid\s*==\s*"gs_[0-9]+"' \
        "$SRC_DIR" 2>/dev/null || true)
if [ -n "$hits" ]; then
  report "PER_QID_OVERFIT_BRANCH" \
    "Do not branch on a literal QID. Per-QID overfits are explicitly forbidden by /goal Harness Contract Invariant 6 in packages/genie-space-optimizer/docs/llmdrivenarchitecture/goalMode/01-harness-contract.md" \
    "$hits"
fi

# Closed-vocab archetype literals in transformer code. Plan 11 uses
# RepairShape.OTHER + free-text repair_hypothesis.
hits=$(rg --type py --glob '!**/test_*.py' --glob '!**/_legacy/**' \
        'RepairShape\.(TOP_N_BY_METRIC|JOIN_DISCOVERY|FILTER_REMOVAL|GROUP_BY_FIX|HAVING_FILTER|WHERE_FILTER|ORDER_BY_FIX)' \
        "$SRC_DIR/optimization/state_machine/" 2>/dev/null || true)
if [ -n "$hits" ]; then
  report "CLOSED_VOCAB_ARCHETYPE_IN_TRANSFORMER" \
    "Plan 11 transformers must use RepairShape.OTHER + free-text repair_hypothesis (no closed-vocab archetype literals). See packages/genie-space-optimizer/docs/skills/gso-postmortem/SKILL.md §'Plan 11: free-text repair_hypothesis carrier'" \
    "$hits"
fi

if [ "$FAILED" -ne 0 ]; then
  echo "[check_invariants] FAIL — one or more invariants violated:" >&2
  printf '%s\n' "$FAIL_REPORT" >&2
  exit 2
fi

exit 0
