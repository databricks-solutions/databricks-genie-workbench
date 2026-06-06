#!/usr/bin/env python3
"""Deterministic ``EVIDENCE FOR EVALUATOR`` block emitter for the /goal harness.

This is the only emitter for the block. It is invoked by the
``gso-emit-evidence-for-evaluator`` skill at the end of every assistant
turn under a ``/goal`` session.

Contract (full spec):
``packages/genie-space-optimizer/docs/llmdrivenarchitecture/goalMode/04-evidence-for-evaluator-protocol.md``

Tracker convention (canonical):
``packages/genie-space-optimizer/docs/architecture/lever-loop-iteration-tracker.md``
uses ``## Trial N — Title`` (H2, em-dash, integer N) and is APPENDED to
(newest-at-bottom). The latest trial is the LAST canonical match in
document order.

Anchors (canonical):
``e94376a3-d8a6-4570-a605-9fe231e5f99c`` (airline) and
``d13938e7-405d-4444-833a-03f5ac9f7523`` (7now). The legacy ``dc89d1a9``
and ``98ec8950`` anchors are deprecated; use the canonical IDs above.

Inputs (all CLI args):
  ``--trial INT`` (required)
      Latest ``## Trial N`` row number in the canonical tracker.
      The skill resolves this with ``grep -E '^## Trial [0-9]+ — '``
      before invoking.
  ``--phase {offline,trigger,postmortem,plan,land,idle}`` (required)
      The dominant work this turn.
  ``--opt-run-id-airline STR`` and ``--opt-run-id-7now STR``
      Most recent GSO optimization_run_id per anchor, or ``none`` if
      no trial has run yet. ``airline`` = ``e94376a3``; ``7now`` =
      ``d13938e7``.
  ``--offline-iterations INT``
      Number of forward-pipeline test invocations this turn.
  ``--offline-target-stage STR``
      Funnel-stage vocabulary; see file ``02-funnel-and-goal-conditions.md``.
  ``--offline-target-test PATH``
      Pytest path matching the target stage (forward-pipeline file or
      tape replay file).
  ``--offline-target-test-status {pass,fail,skipped,unknown}``
  ``--real-trial-required-reason STR``
      ``none``, ``genie_api``, ``mlflow_reeval``, or ``other:<reason>``.
  ``--next STR``
      action_id the harness plans to take on turn N+1.
  ``--architecture-invariants-held {true,false,unknown}``
      Only honored when ``--phase postmortem`` (fresh postmortem ran
      this turn). Other phases auto-mask to ``unknown`` in the
      ``GOAL_HARNESS_STATUS_V1`` line so a stale value cannot leak
      through and trip the goal-stop on a non-postmortem turn.
  ``--deepest-stage-airline STR`` and ``--deepest-stage-7now STR``
      One of: hard_qid_seen, diagnosed, clustered, proposed, normalized,
      applyable, applied, evaluated, accepted, unknown.
  ``--funnel-advanced-this-trial {true,false,unknown}``
  ``--funnel-advanced-vs-prior-trial {true,false,unknown}``
  ``--tracker PATH`` (optional override; default resolves to canonical path)
  ``--postmortem-root PATH`` (optional override; default ``runid_analysis/``)

Architectural Self-Assessment inputs (the success bar — non-negotiable
for goal achievement; see AGENTS.md ``## /goal Harness Contract`` §
"Architectural Principles"):
  ``--deterministic-shortcuts-added {true,false,unknown}`` (default ``unknown``)
      ``true`` iff this turn's edits introduced any per-QID/per-anchor
      hardcoded branch (e.g. ``if qid == "<literal>"``, ``if space_id ==
      "<UUID>"``, fixture-pinned matchers) in ``src/``. ``false`` is the
      safe value. ``unknown`` means no edits this turn — that vacuously
      passes the principle but does NOT count toward goal achievement.
  ``--generalizable-solution {true,false,unknown}`` (default ``unknown``)
      ``true`` iff this turn's edits work across the RCA family the
      tracker row targets (not just one anchor / one QID). Evidence:
      bright-line replay suite includes at least one non-anchor fixture.
  ``--rca-citation STR`` (default ``unknown``)
      Free-text RCA kind + mechanism + watch-marker citation that this
      turn's edits address. E.g.
      ``extra_defensive_filter -> add_instruction kit (GSO_TRIAL24_KIT_FORCED_V1)``.
      ``unknown`` blocks goal achievement.
  ``--typed-schemas-at-boundaries {true,false,unknown}`` (default ``unknown``)
      ``true`` iff cross-module data this turn flows through Pydantic /
      dataclass types, not ``dict[str, Any]``. ``false`` if any new
      module-boundary edge uses untyped dicts.
  ``--sm-resident-fix {true,false,unknown}`` (default ``unknown``)
      ``true`` iff edits live inside the state machine (stages /
      transitions / repair hooks), not out-of-band branching. Evidence
      should cite ``file:line``.
  ``--llm-reasoning-used {true,false,unknown}`` (default ``unknown``)
      ``true`` iff judgmental fixes (RCA categorisation, mechanism
      selection, justification grounding) go through LLM calls with
      typed prompt/output schemas; deterministic code validates LLM
      output but does not replace LLM judgment. ``false`` if any new
      deterministic branch replaces what should be LLM reasoning.

The aggregate ``architectural_principles_held`` line is auto-derived:
  - ``true``  iff ALL six fields are the safe value
                (deterministic_shortcuts_added=false; the other five = true)
  - ``false`` iff ANY field is explicitly the unsafe value
  - ``unknown`` if any field is ``unknown`` and none are unsafe

Goal achievement requires the literal ``architectural_principles_held = true``
to appear in the EVIDENCE block — ``unknown`` is NOT sufficient.

The script reads the tracker file to extract the latest ``## Trial N``
section (verbatim), and the per-anchor postmortem files for the three
literal ``## Verdict`` lines. If a file or section is missing it emits
the documented ``unknown`` fallback rather than failing — the block
must always be well-formed.

**Phase-gated postmortem masking.** The per-anchor verdict lines are
read from disk ONLY when ``--phase postmortem`` — i.e. a fresh
postmortem ran THIS turn. On any other phase (``trigger``, ``offline``,
``plan``, ``land``, ``idle``) the on-disk postmortem reflects a PRIOR
trial's outcome and would be matched by the Haiku evaluator against
goal/stop conditions designed for fresh evidence. Other phases emit
the masked placeholders ``final_accuracy_pct = unknown``,
``architecture_invariants_held = unknown``, and
``verdict = NO_FRESH_POSTMORTEM_THIS_TURN`` (see
``_stale_verdict_lines()``) so no goal or stop substring can
accidentally match a stale value. The ``architecture_invariants_held``
field in the ``GOAL_HARNESS_STATUS_V1`` line is masked the same way.

Output: writes the block to stdout, then exits 0. No other output is
produced on stdout; stderr carries warnings.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Optional


VALID_PHASES = {"offline", "trigger", "postmortem", "plan", "land", "idle"}
VALID_STAGES = {
    "hard_qid_seen",
    "diagnosed",
    "clustered",
    "proposed",
    "normalized",
    "applyable",
    "applied",
    "evaluated",
    "accepted",
    "unknown",
}
VALID_TRIBOOL = {"true", "false", "unknown"}


CANONICAL_ANCHOR_AIRLINE = "e94376a3-d8a6-4570-a605-9fe231e5f99c"
CANONICAL_ANCHOR_7NOW = "d13938e7-405d-4444-833a-03f5ac9f7523"


def _default_tracker() -> Path:
    """Default tracker path: the CANONICAL architecture tracker.

    The legacy v5/ tracker
    (``docs/llmdrivenarchitecture/v5/lever-loop-architecture-and-iteration-tracker.md``)
    is deprecated; it stopped getting authoritative updates around
    Trial 21. All Trial 20+ rows live in the canonical tracker below.
    """
    here = Path(__file__).resolve()
    repo_root = here.parents[3]
    return (
        repo_root
        / "packages"
        / "genie-space-optimizer"
        / "docs"
        / "architecture"
        / "lever-loop-iteration-tracker.md"
    )


def _default_postmortem_root() -> Path:
    here = Path(__file__).resolve()
    repo_root = here.parents[3]
    return (
        repo_root
        / "packages"
        / "genie-space-optimizer"
        / "docs"
        / "runid_analysis"
    )


# Canonical trial-row heading: ``## Trial <N> — <title>`` with the
# em-dash + literal-space separator. ``<N>`` is captured as an integer.
#
# H3 ``### Trial ...`` headings (e.g. ``### Trial 21 status snapshot``)
# intentionally do NOT match because the canonical tracker is organized
# as one ``## Trial N`` per trial, with ``###`` reserved for sub-sections
# (Hypothesis, Workstreams, Status, etc.) inside a trial row.
CANONICAL_TRIAL_HEADING = re.compile(r"^## Trial (\d+) — ", re.MULTILINE)


def _extract_latest_trial_row(tracker_path: Path) -> str:
    """Return the latest canonical ``## Trial <N> — ...`` section verbatim.

    "Latest" is the LAST canonical-format heading in document order, on
    the convention (documented in the tracker preamble) that new trial
    rows are APPENDED to the bottom of the tracker. In the canonical
    tracker as of Trial 24, line offsets are: Trial 20 = line 15,
    Trial 21 = line 110, Trial 22 = line 198, Trial 23 = line 303,
    Trial 24 = line 454 — newest at bottom.

    The section runs from the matching heading until the next ``## ``
    heading (top-level only — ``### Trial`` sub-sections inside the row
    are part of the row, not a delimiter) or EOF.
    """
    if not tracker_path.exists():
        return f"(tracker not found at {tracker_path})"

    text = tracker_path.read_text(encoding="utf-8")
    matches = list(CANONICAL_TRIAL_HEADING.finditer(text))
    if not matches:
        return "(no canonical '## Trial N — ' rows found in tracker)"

    latest = matches[-1]
    section_start = latest.start()

    after = text[latest.end():]
    next_section_pattern = re.compile(r"^## ", re.MULTILINE)
    next_match = next_section_pattern.search(after)
    if next_match:
        section_end = latest.end() + next_match.start()
    else:
        section_end = len(text)

    return text[section_start:section_end].rstrip()


def _self_test(tracker_override: Optional[Path] = None) -> int:
    """Sanity-check tracker shape against the canonical trial-row format.

    Called by ``goal_bootstrap.sh`` so future tracker-format drift fails
    bootstrap loudly instead of silently emitting the wrong excerpt.

    Returns 0 on success. Returns non-zero with a ``SELF_TEST_FAIL:`` line
    on stderr describing what's wrong. Non-fatal observations are emitted
    as ``SELF_TEST_WARN:`` lines and do not affect the exit code.
    """
    tracker = tracker_override or _default_tracker()
    if not tracker.exists():
        sys.stderr.write(f"SELF_TEST_FAIL: tracker not found at {tracker}\n")
        return 1

    text = tracker.read_text(encoding="utf-8")
    canonical_matches = list(CANONICAL_TRIAL_HEADING.finditer(text))
    if not canonical_matches:
        sys.stderr.write(
            "SELF_TEST_FAIL: no canonical '## Trial N — ' rows in tracker. "
            "Either the file is empty, the tracker has moved, or every "
            "trial heading uses a non-canonical separator (e.g. ASCII '-' "
            "instead of em-dash '—'). The evidence emitter cannot identify "
            "a latest trial.\n"
        )
        return 2

    seen: dict[str, list[int]] = {}
    for m in canonical_matches:
        seen.setdefault(m.group(1), []).append(m.start())

    duplicates = {tid: starts for tid, starts in seen.items() if len(starts) > 1}
    if duplicates:
        details = ", ".join(
            f"Trial {tid} (x{len(starts)})" for tid, starts in duplicates.items()
        )
        sys.stderr.write(
            f"SELF_TEST_FAIL: canonical '## Trial N — ' format has duplicates: "
            f"{details}. Rename all but one so the evidence emitter can uniquely "
            "identify the active trial.\n"
        )
        return 3

    excerpt = _extract_latest_trial_row(tracker)
    if not excerpt.strip() or excerpt.startswith("("):
        sys.stderr.write(
            f"SELF_TEST_FAIL: _extract_latest_trial_row returned empty / error "
            f"string: {excerpt!r}\n"
        )
        return 4

    # Verify the latest trial section has at least one ``### Status``
    # sub-section AND parse its checklist. The /goal driver branches on
    # whether the latest trial has open ``- [ ]`` items.
    if "### Status" not in excerpt:
        sys.stderr.write(
            "SELF_TEST_WARN: latest trial row is missing a '### Status' "
            "sub-section. The /goal driver cannot determine open items "
            "without it. Add a 'Status' checklist to the latest trial.\n"
        )

    # Warn (don't fail) if the tracker still references the legacy anchor
    # IDs (dc89d1a9 / 98ec8950). These are deprecated; canonical anchors
    # are e94376a3 (airline) + d13938e7 (7now).
    legacy_anchors_in_tracker = []
    for legacy in ("dc89d1a9", "98ec8950"):
        if legacy in text:
            legacy_anchors_in_tracker.append(legacy)
    if legacy_anchors_in_tracker:
        sys.stderr.write(
            f"SELF_TEST_WARN: tracker still references legacy anchors "
            f"{legacy_anchors_in_tracker}. Canonical anchors are "
            f"{CANONICAL_ANCHOR_AIRLINE!r} (airline) and "
            f"{CANONICAL_ANCHOR_7NOW!r} (7now). Update tracker prose to "
            "match the canonical anchors before launching /goal mode.\n"
        )

    latest_id = canonical_matches[-1].group(1)
    first_line = excerpt.splitlines()[0] if excerpt else "(empty)"
    sys.stderr.write(
        f"SELF_TEST_OK: latest canonical trial is Trial {latest_id}; "
        f"section starts with: {first_line!r}\n"
    )
    return 0


def _stale_verdict_lines() -> str:
    """Return masked verdict lines when no fresh postmortem ran this turn.

    When ``phase != "postmortem"`` (i.e. ``trigger`` / ``offline`` /
    ``plan`` / ``land`` / ``idle``), the on-disk postmortem reflects a
    PRIOR trial's outcome — it is NOT evidence of this turn's work.
    Emitting the prior ``verdict = X`` or
    ``architecture_invariants_held = false`` lines would let the
    Haiku evaluator substring-match them against goal/stop conditions
    that ONLY make sense against fresh, this-turn evidence (and did
    exactly that, costing one full ~10-min /goal session).

    Mask all three verdict lines to known-safe placeholders that no
    goal-achievement or goal-stop substring matches:
    - ``final_accuracy_pct = unknown``   — never matches ``= 100.0``
    - ``architecture_invariants_held = unknown`` — never matches ``= false``
    - ``verdict = NO_FRESH_POSTMORTEM_THIS_TURN`` — never matches
      ``WHACK_A_MOLE_DETECTED``, ``LOCAL_VERIFICATION_RED``, or
      ``PRETRIAL_GATE_FAILED``.

    The fresh-postmortem turn (phase=postmortem) emits the real
    verdict lines via ``_extract_verdict_lines()``.
    """
    return (
        "  final_accuracy_pct = unknown\n"
        "  architecture_invariants_held = unknown\n"
        "  verdict = NO_FRESH_POSTMORTEM_THIS_TURN"
    )


def _extract_verdict_lines(postmortem_path: Optional[Path]) -> str:
    """Return the three literal lines from `## Verdict` section.

    Format:
        final_accuracy_pct = <X>
        architecture_invariants_held = <bool>
        verdict = <CODE>

    If file missing or section absent, returns the documented `unknown`
    fallback. Only called when ``phase == "postmortem"`` (fresh postmortem
    ran this turn); other phases use ``_stale_verdict_lines()`` to mask.
    """
    fallback = (
        "  final_accuracy_pct = unknown\n"
        "  architecture_invariants_held = unknown\n"
        "  verdict = NO_TRIAL_YET"
    )

    if postmortem_path is None or not postmortem_path.exists():
        return fallback

    text = postmortem_path.read_text(encoding="utf-8")
    verdict_match = re.search(
        r"^##\s+Verdict\s*$([\s\S]*?)(?=^##\s|\Z)",
        text,
        re.MULTILINE,
    )
    if not verdict_match:
        return fallback

    verdict_block = verdict_match.group(1)

    acc = _grep_line(verdict_block, r"final_accuracy_pct\s*=\s*(\S+)") or "unknown"
    inv = _grep_line(verdict_block, r"architecture_invariants_held\s*=\s*(\S+)") or "unknown"
    code = _grep_line(verdict_block, r"verdict\s*=\s*([A-Z0-9_]+)") or "UNKNOWN"

    return (
        f"  final_accuracy_pct = {acc}\n"
        f"  architecture_invariants_held = {inv}\n"
        f"  verdict = {code}"
    )


def _grep_line(block: str, pattern: str) -> Optional[str]:
    m = re.search(pattern, block)
    if m is None:
        return None
    return m.group(1).strip()


def _resolve_postmortem(
    root: Path,
    opt_run_id: str,
) -> Optional[Path]:
    if opt_run_id in ("none", "unknown", ""):
        return None
    candidate = root / opt_run_id / "postmortem.md"
    if candidate.exists():
        return candidate
    return None


def _derive_architectural_principles_held(
    deterministic_shortcuts_added: str,
    generalizable_solution: str,
    typed_schemas_at_boundaries: str,
    sm_resident_fix: str,
    llm_reasoning_used: str,
    rca_citation: str,
) -> str:
    """Auto-derive the ``architectural_principles_held`` aggregate.

    Returns one of ``true`` / ``false`` / ``unknown``:
      - ``true``  iff ALL six fields are at the safe value
                  (deterministic_shortcuts_added=false; the others=true;
                  rca_citation is a non-empty non-"unknown" string).
      - ``false`` iff ANY field is explicitly the unsafe value
                  (deterministic_shortcuts_added=true; any other=false).
      - ``unknown`` otherwise (any of the boolean fields = ``unknown``
                    and none are ``unsafe``; OR rca_citation is unknown
                    but boolean fields are otherwise safe).

    The goal-achievement criterion requires the literal
    ``architectural_principles_held = true`` to appear, so an
    ``unknown`` aggregate keeps the goal open (not failed) but does
    NOT count toward achievement.
    """
    unsafe_bools = (
        deterministic_shortcuts_added == "true"
        or generalizable_solution == "false"
        or typed_schemas_at_boundaries == "false"
        or sm_resident_fix == "false"
        or llm_reasoning_used == "false"
    )
    if unsafe_bools:
        return "false"

    rca_safe = bool(rca_citation) and rca_citation.lower().strip() != "unknown"
    all_safe = (
        deterministic_shortcuts_added == "false"
        and generalizable_solution == "true"
        and typed_schemas_at_boundaries == "true"
        and sm_resident_fix == "true"
        and llm_reasoning_used == "true"
        and rca_safe
    )
    if all_safe:
        return "true"

    return "unknown"


def main(argv: list[str]) -> int:
    # Short-circuit: ``--self-test`` runs sanity checks against the tracker
    # and exits, bypassing the normal required-args path. Honors an
    # optional ``--tracker <path>`` override even in self-test mode.
    if "--self-test" in argv:
        tracker_override: Optional[Path] = None
        for i, arg in enumerate(argv):
            if arg == "--tracker" and i + 1 < len(argv):
                tracker_override = Path(argv[i + 1])
                break
        return _self_test(tracker_override=tracker_override)

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trial", type=int, required=True)
    parser.add_argument("--phase", required=True, choices=sorted(VALID_PHASES))
    parser.add_argument(
        "--opt-run-id-airline",
        required=True,
        help=f"GSO optimization_run_id on the airline anchor ({CANONICAL_ANCHOR_AIRLINE}), or 'none'.",
    )
    parser.add_argument(
        "--opt-run-id-7now",
        required=True,
        dest="opt_run_id_7now",
        help=f"GSO optimization_run_id on the 7now anchor ({CANONICAL_ANCHOR_7NOW}), or 'none'.",
    )
    parser.add_argument("--offline-iterations", type=int, default=0)
    parser.add_argument("--offline-target-stage", default="unknown")
    parser.add_argument("--offline-target-test", default="unknown")
    parser.add_argument(
        "--offline-target-test-status",
        default="unknown",
        choices=["pass", "fail", "skipped", "unknown"],
    )
    parser.add_argument("--real-trial-required-reason", default="none")
    parser.add_argument("--next", dest="next_action", default="unknown")
    parser.add_argument(
        "--architecture-invariants-held",
        default="unknown",
        choices=sorted(VALID_TRIBOOL),
    )
    parser.add_argument(
        "--deepest-stage-airline",
        default="unknown",
        choices=sorted(VALID_STAGES),
    )
    parser.add_argument(
        "--deepest-stage-7now",
        dest="deepest_stage_7now",
        default="unknown",
        choices=sorted(VALID_STAGES),
    )
    parser.add_argument(
        "--funnel-advanced-this-trial",
        default="unknown",
        choices=sorted(VALID_TRIBOOL),
    )
    parser.add_argument(
        "--funnel-advanced-vs-prior-trial",
        default="unknown",
        choices=sorted(VALID_TRIBOOL),
    )
    parser.add_argument("--tracker", type=Path, default=None)
    parser.add_argument("--postmortem-root", type=Path, default=None)

    # Architectural Self-Assessment — the success bar from AGENTS.md
    # ``## /goal Harness Contract`` § "Architectural Principles".
    # All default to "unknown" so a turn that forgets to assert them
    # cannot accidentally pass the goal-achievement gate.
    parser.add_argument(
        "--deterministic-shortcuts-added",
        default="unknown",
        choices=sorted(VALID_TRIBOOL),
        help="true iff this turn added per-QID/per-anchor hardcoded branches in src/.",
    )
    parser.add_argument(
        "--generalizable-solution",
        default="unknown",
        choices=sorted(VALID_TRIBOOL),
        help="true iff this turn's edits generalise across the RCA family (not anchor/QID-specific).",
    )
    parser.add_argument(
        "--rca-citation",
        default="unknown",
        help="Free-text RCA kind + mechanism + marker citation this turn's edits address.",
    )
    parser.add_argument(
        "--typed-schemas-at-boundaries",
        default="unknown",
        choices=sorted(VALID_TRIBOOL),
        help="true iff cross-module data flows through typed schemas (Pydantic/dataclass), not dict[str, Any].",
    )
    parser.add_argument(
        "--sm-resident-fix",
        default="unknown",
        choices=sorted(VALID_TRIBOOL),
        help="true iff edits live inside the state machine (stages/transitions/repairs), not out-of-band.",
    )
    parser.add_argument(
        "--llm-reasoning-used",
        default="unknown",
        choices=sorted(VALID_TRIBOOL),
        help="true iff judgmental fixes go through LLM calls with typed I/O schemas (not deterministic shortcuts).",
    )

    args = parser.parse_args(argv)

    tracker_path = args.tracker or _default_tracker()
    postmortem_root = args.postmortem_root or _default_postmortem_root()

    tracker_excerpt = _extract_latest_trial_row(tracker_path)
    pm_airline = _resolve_postmortem(postmortem_root, args.opt_run_id_airline)
    pm_7now = _resolve_postmortem(postmortem_root, args.opt_run_id_7now)

    # Phase-gated postmortem emission: only ``phase=postmortem`` turns may
    # surface real verdict lines (because only those turns produced fresh
    # postmortems). Other phases would otherwise let the Haiku evaluator
    # match stale on-disk verdicts against goal/stop substrings — see
    # ``_stale_verdict_lines()`` docstring for the bug history.
    if args.phase == "postmortem":
        verdict_airline = _extract_verdict_lines(pm_airline)
        verdict_7now = _extract_verdict_lines(pm_7now)
        status_invariants_held = args.architecture_invariants_held
    else:
        verdict_airline = _stale_verdict_lines()
        verdict_7now = _stale_verdict_lines()
        # Mirror the masking into the status-line aggregate so the
        # GOAL_HARNESS_STATUS_V1 spot-check cannot leak a stale value
        # even if the caller passed --architecture-invariants-held false.
        status_invariants_held = "unknown"

    architectural_principles_held = _derive_architectural_principles_held(
        deterministic_shortcuts_added=args.deterministic_shortcuts_added,
        generalizable_solution=args.generalizable_solution,
        typed_schemas_at_boundaries=args.typed_schemas_at_boundaries,
        sm_resident_fix=args.sm_resident_fix,
        llm_reasoning_used=args.llm_reasoning_used,
        rca_citation=args.rca_citation,
    )

    block = "\n".join(
        [
            "EVIDENCE FOR EVALUATOR",
            "---tracker excerpt---",
            tracker_excerpt,
            "---postmortem excerpts---",
            f"airline ({CANONICAL_ANCHOR_AIRLINE}) ## Verdict:",
            verdict_airline,
            f"7now ({CANONICAL_ANCHOR_7NOW}) ## Verdict:",
            verdict_7now,
            "---funnel status---",
            f"deepest_stage_reached_airline = {args.deepest_stage_airline}",
            f"deepest_stage_reached_7now = {args.deepest_stage_7now}",
            f"funnel_advanced_this_trial = {args.funnel_advanced_this_trial}",
            f"funnel_advanced_vs_prior_trial = {args.funnel_advanced_vs_prior_trial}",
            "---offline iteration---",
            f"offline_iterations_this_turn = {args.offline_iterations}",
            f"offline_target_stage = {args.offline_target_stage}",
            f"offline_target_test = {args.offline_target_test}",
            f"offline_target_test_status = {args.offline_target_test_status}",
            f"real_trial_required_reason = {args.real_trial_required_reason}",
            "---architectural self-assessment---",
            f"deterministic_shortcuts_added = {args.deterministic_shortcuts_added}",
            f"generalizable_solution = {args.generalizable_solution}",
            f"rca_cited = {args.rca_citation}",
            f"typed_schemas_at_boundaries = {args.typed_schemas_at_boundaries}",
            f"sm_resident_fix = {args.sm_resident_fix}",
            f"llm_reasoning_used = {args.llm_reasoning_used}",
            f"architectural_principles_held = {architectural_principles_held}",
            "---status---",
            (
                f"GOAL_HARNESS_STATUS_V1 "
                f"trial={args.trial} "
                f"phase={args.phase} "
                f"opt_run_id_airline={args.opt_run_id_airline} "
                f"opt_run_id_7now={args.opt_run_id_7now} "
                f"architecture_invariants_held={status_invariants_held} "
                f"architectural_principles_held={architectural_principles_held} "
                f"next={args.next_action}"
            ),
            "END EVIDENCE FOR EVALUATOR",
        ]
    )

    sys.stdout.write(block + "\n")
    sys.stdout.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
