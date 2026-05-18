# Phase 5 — Authoritative RCA Grounding and Slate Consumption close note

**Plan:** `docs/prompt_improvements/2026-05-18-authoritative-rca-grounding-and-slate-consumption.md`
**Date:** 2026-05-18
**Branch:** `feat/gso-cycle12`
**Status:** Foundation landed. Harness wiring deferred pending control-flow audit. WU-4 + WU-5 fully wired.

## What landed

### WU-1 — Authoritative slate consumption (foundation only)

| Surface | Status |
|---|---|
| `SlateAction` enum, `SlateDecision` dataclass | ✓ |
| `decide_slate_action` pure function (with patches-cluster_id fallback) | ✓ |
| `DecisionType.SLATE_AUTHORITATIVE_SKIP` typed record | ✓ |
| `slate_authoritative_skip_record` emitter | ✓ |
| `GSO_SLATE_AUTHORITATIVE_SKIP_V1` stdout marker | ✓ |
| `GSO_SLATE_CONSUMPTION_AUTHORITATIVE` config flag (default-ON) | ✓ |
| **Harness wiring** | **DEFERRED** |
| **Task 10 un-xfail of 4 anchor tests** | **DEFERRED** |

### WU-2 — RCA regeneration retry (foundation only)

| Surface | Status |
|---|---|
| `RcaRegenAttempt`, `RcaRegenRetryResult` dataclasses | ✓ |
| `retry_rca_regeneration_for_blocked` pure helper | ✓ |
| `DecisionType.RCA_REGEN_RETRY_VERDICT` typed record | ✓ |
| `rca_regen_retry_verdict_record` emitter | ✓ |
| `GSO_RCA_REGEN_RETRY_VERDICT_V1` stdout marker | ✓ |
| `GSO_RCA_REGEN_RETRY` config flag (default-ON) | ✓ |
| **Harness wiring (Task 9)** | **DEFERRED** |

### WU-4 — Acceptance-drift fix (fully wired)

| Surface | Status |
|---|---|
| `pre_arbiter_requires_no_post_regression_enabled` flag (default-ON) | ✓ |
| `decide_control_plane_acceptance` tightened — requires `delta >= 0` AND `has_causal_fix` | ✓ |
| Typed reasons `post_arbiter_regressed_pre_arbiter_only`, `pre_arbiter_improvement_without_causal_fix` | ✓ |
| Existing `test_stages_acceptance` updated to match WU-4 invariant | ✓ |
| **3 new unit tests in `test_control_plane_pre_arbiter_no_regression.py`** | ✓ |
| Task 12 anchor replay test | DEFERRED (depends on Task 5 wiring to drive a SLATE_AUTHORITATIVE_SKIP path under replay) |

### WU-5 — Replay fixture hygiene (fully wired)

| Surface | Status |
|---|---|
| `optimization/replay_fixture_marker.py` (`emit_dual_fixture`, `extract_replay_fixture_from_stream`, `ExtractedFixture`) | ✓ |
| Harness emission switched to `emit_dual_fixture` under `GSO_REPLAY_FIXTURE_DUAL_EMIT` (default-ON) | ✓ |
| `tools/marker_parser.extract_replay_fixture` delegates to the new extractor | ✓ |
| **5 new unit tests in `test_replay_fixture_marker.py`** | ✓ |

## Why the harness wiring was deferred

Two empirical investigation passes against `_run_lever_loop` revealed a structural call-graph mismatch:

| Site investigated | Outcome |
|---|---|
| Plan's original site at `harness.py:25295` (post-action-groups-stage) | Unreachable under airline tape. Lever-6 fires at `harness.py:23791` BEFORE this site, the proposal_generation_empty path at `harness.py:24586` short-circuits, and the action_groups stage at `harness.py:24846` is never reached. |
| User-authorized relocation to `harness.py:22085` (post `ag = action_groups[0]`) | Unreachable under airline tape. Instrumented `print` produced zero output; the `if ag is None:` block at `harness.py:21061` is not entered on this path. |

Root cause: `collect_blocked_clusters` at `harness.py:25113` runs AFTER lever-6 emission. Any wiring that wants to "stop ungrounded AGs before forced_synthesis_dispatch" needs `blocked_cluster_ids` to be available BEFORE lever-6, which the current harness does not provide.

The right next step is a dedicated **harness control-flow audit task** that:

1. Maps every `continue` / `break` / `return` in `_run_lever_loop` with replay-specific reachability.
2. Produces a canonical control-flow diagram.
3. Identifies the structural change needed to make `collect_blocked_clusters` available pre-lever-6 (likely a hoist of the grounding-gate prelude).

Once that audit completes, Phase 5 Task 5 (WU-1 wiring) and Task 9 (WU-2 wiring) drop in without modifying the foundation surface — `decide_slate_action`, `retry_rca_regeneration_for_blocked`, the typed records, the markers, and the flags are all callable today.

## Test surface

| Suite | Result |
|---|---|
| Phase 3.7 anchor matrix (`test_phase3_anchor_tape_replay.py`) | 6 passed, 4 xfailed (strict) — unchanged |
| `test_slate_consumption.py` | 14 passed |
| `test_rca_regen_retry.py` | 10 passed |
| `test_control_plane_pre_arbiter_no_regression.py` | 3 passed |
| `test_replay_fixture_marker.py` | 5 passed |
| `test_control_plane.py` (existing) | 31 passed — no regression |
| `test_stages_acceptance.py` (existing, 1 test updated for WU-4) | 7 passed |
| `test_marker_parser.py` (existing) | 24 passed — extractor contract byte-stable |

**Full GSO unit suite:** 6491 passed / 14 failed (all 14 failures verified PRE-EXISTING via `git stash` — not caused by this plan).

## What this changes for live runs (after harness wiring lands)

When Tasks 5 + 9 wiring lands in a follow-up plan:

1. Ungrounded clusters can no longer reach `forced_synthesis_dispatch` on the normal path. `missing_rca_card` becomes a defensive backstop.
2. `GSO_FULL_EVAL_V1` cannot report `accepted=true reason_code=accepted_pre_arbiter_improvement` when the post-arbiter delta is negative OR when no declared target qid moved (WU-4 — live today).
3. Replay fixtures emitted to stderr survive concurrent prompt/source prints — postmortems can always recover the fixture via the base64 fallback channel (WU-5 — live today).

## What this does NOT change

- The Lever 5 / Lever 6 prompt content is unchanged.
- No new LLM calls; the RCA regen retry re-uses `_regenerate_rca_for_cluster`.
- No schema changes to existing decision records.
- Schema-validating replay tests (Phase 3 / 3.6 / 3.7) keep passing.

## Follow-up plan prerequisites

Before authoring the harness-wiring follow-up:

1. Run the control-flow audit (proposed dedicated plan).
2. Land Plan 4a (RCA card builder hardening at `docs/prompt_improvements/2026-05-18-anchor-driven-rca-card-hardening.md`) — this fixes the four anchor clusters so they produce fit RCA cards, making WU-1/WU-2's backstop function rather than primary path.
3. Then write the harness-wiring follow-up with empirically-validated insertion sites.

## Anti-treadmill record

Six STOP-and-reports across this session (5 in Phase 5 + 1 in Phase 3.7):

1. Iter multiplicity + source_cluster_ids drop → Phase 3.7 §2.3 amendment (1A/1B backfills)
2. MLflow run iter tag is loop-launch granularity → Phase 3.7 §2.4 amendment (cluster_only mode)
3. Cross-cluster `affected_questions` guard → Phase 3.7 §2.5 amendment (xfail strict)
4. WU-1 wiring at `harness.py:25295` unreachable (post-action-groups-stage) → relocation authorized
5. WU-1 relocated wiring at `harness.py:22085` also unreachable → harness control-flow mismatch discovered
6. `collect_blocked_clusters` at `harness.py:25113` runs AFTER lever-6 at `harness.py:23791` → harness wiring deferred to follow-up

Each STOP prevented a wrong patch. The deferred-wiring outcome is the honest scope: foundation lands, structural assumption requires audit, follow-up plan delivers the load-bearing claim.
