"""Phase 1 Action 1.3 — mandatory propagation/scoping diagnostic.

Investigation tool. Not production logic. The optimizer's plural
top-N instruction was applied to the ccf1d60d candidate space but
gs_026 still produced single-row output. This script does NOT design
a fix; it only classifies which of four root causes is responsible:

  * propagation_lag             — instruction present but not yet
                                  active for the rerun
  * instruction_not_scoped_to_qid — instruction present but matches
                                  a different question shape
  * instruction_insufficient_force — instruction present and scoped
                                  but the LLM still ignored it
  * eval_cache_stale            — the rerun read a cached failure,
                                  not a fresh Genie response

The engineer runs this script interactively while consulting the
checklist printed at start. The script's pure helpers are
unit-tested; the I/O sections are intentionally thin.

Usage:
    python -m genie_space_optimizer.tools.propagation_diagnostic \\
        --space-id 01f128aea2c210559cffb663d9c58282 \\
        --qid 7now_delivery_analytics_space_gs_026 \\
        --databricks-profile fevm-prashanth
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Optional


PROPAGATION_OUTCOMES = (
    "propagation_lag",
    "instruction_not_scoped_to_qid",
    "instruction_insufficient_force",
    "eval_cache_stale",
)


def classify_outcome(value: str) -> str:
    """Validate that a user-supplied classification string is one of
    the four allowed values. Raises ValueError otherwise.
    """
    if value not in PROPAGATION_OUTCOMES:
        raise ValueError(
            f"propagation_root_cause must be one of "
            f"{PROPAGATION_OUTCOMES}, got {value!r}"
        )
    return value


def locate_query_rules_instruction(serialized_space: dict) -> Optional[str]:
    """Return the QUERY RULES instruction text from the serialized
    Genie Space, or None when absent.

    The instruction is identified by either a title containing
    'QUERY RULES' (case-insensitive) OR a body that starts with
    '## QUERY RULES'. Searches every entry in
    ``instructions.text_instructions``.
    """
    if not isinstance(serialized_space, dict):
        return None
    instr = serialized_space.get("instructions") or {}
    text_instructions = instr.get("text_instructions") or []
    for entry in text_instructions:
        if not isinstance(entry, dict):
            continue
        title = str(entry.get("title") or "")
        content = str(entry.get("content") or "")
        if "QUERY RULES" in title.upper() or content.lstrip().startswith("## QUERY RULES"):
            return content
    return None


def _print_checklist(*, space_id: str, qid: str) -> None:
    """Print the per-engineer checklist; the script then waits for
    the engineer to act between prompts."""
    print()
    print("=" * 72)
    print("Phase 1 Action 1.3 — Propagation/Scoping Diagnostic")
    print("=" * 72)
    print(f"Target space: {space_id}")
    print(f"Target QID:   {qid}")
    print()
    print("Step 1: Fetch the post-apply serialized_space.")
    print(
        "  Run (in another shell):  databricks --profile <prof> api get "
        f"/api/2.0/data-rooms/{space_id} > /tmp/space.json"
    )
    print("  This script will read /tmp/space.json next.")
    print()
    input("  Press ENTER when /tmp/space.json is in place ...")


def _read_serialized_space(path: str = "/tmp/space.json") -> dict:
    try:
        return json.loads(open(path).read())
    except FileNotFoundError:
        print(f"ERROR: {path} not found. Aborting.", file=sys.stderr)
        sys.exit(2)
    except json.JSONDecodeError as exc:
        print(f"ERROR: {path} is not valid JSON: {exc}", file=sys.stderr)
        sys.exit(2)


def _prompt_classification() -> str:
    print()
    print("Step 4: Classify the root cause.")
    print(f"  Allowed values: {', '.join(PROPAGATION_OUTCOMES)}")
    print()
    while True:
        raw = input("  propagation_root_cause = ").strip()
        try:
            return classify_outcome(raw)
        except ValueError as exc:
            print(f"  Invalid: {exc}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--space-id", required=True)
    parser.add_argument("--qid", required=True)
    parser.add_argument("--databricks-profile", required=True)
    args = parser.parse_args()

    _print_checklist(space_id=args.space_id, qid=args.qid)

    serialized_space = _read_serialized_space()
    rules_text = locate_query_rules_instruction(serialized_space)
    if rules_text is None:
        print("WARNING: No QUERY RULES instruction found in the candidate space.")
        print("This itself is a finding — the iter-1 patch did not persist.")
    else:
        print()
        print("Step 2: QUERY RULES instruction located. First 400 chars:")
        print("-" * 72)
        print(rules_text[:400])
        print("-" * 72)

    print()
    print("Step 3: Run the target QID against the candidate space manually")
    print("        via the Genie Conversation API. Record output cardinality.")
    print(
        "  See docs/2026-05-12-phase-1-action-1-3-propagation-diagnostic-results.md "
        "Section 3 for the curl recipe."
    )
    input("  Press ENTER when the cardinality is recorded in the results doc ...")

    classification = _prompt_classification()
    print()
    print("=" * 72)
    print(f"propagation_root_cause_identified = {classification!r}")
    print("=" * 72)
    print(
        "Now: copy this value into Section 5 of the results doc and "
        "commit + freeze the doc."
    )


if __name__ == "__main__":
    main()
