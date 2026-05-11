"""Cycle 11 — Loop invariants over existing markers / decision records.

Each ``check_iN_*`` function is pure: takes a single ``evidence``
dict (markers + decision records + replay-fixture metadata) and
returns a list of ``invariant_violation`` dicts. Empty list = green.

The aggregator ``run_invariants`` calls every implemented check and
returns the combined violation list. CI/replay treat any non-empty
list as a hard failure; production records each violation as an
``INVARIANT_VIOLATION`` decision record and continues (gated by
``loop_invariants_strict``).

The invariants are intentionally read-only over evidence the
harness already produces. No new emitters are required beyond the
``invariant_violation_record`` helper in ``decision_emitters.py``.

Invariant IDs:
  I1 — phase_b.total_records >= replay_fixture.records
  I2 — applied_patch.lever ⊆ ag.Levers ⊇ cluster.recommended_levers
  I3 — acceptance buckets partition target_qids; rollback reason names
       a bucket
  I4 — no two consecutive iterations select the same AG with the same
       applied-patch body-fingerprint set or with Proposals(0 total)
  I5 — replay validity: zero illegal trunk transitions
  I6 — phase_h declared paths == materialized paths
  I7 — every open hard cluster reaching AG-emit has a fit RCA card or
       a cluster_blocked_no_rca typed record
  I8 — plateau decision currently_failing input matches journey-ledger
       hard-cluster set after rollback
  I13 — target_delta_states is total over target_qids; LOOKUP_FAILED
        implies reason_code=target_resolution_failed; FIXED /
        STILL_HARD agree with legacy bucket tuples
"""

from __future__ import annotations

from typing import Any, Mapping


def _violation(
    *,
    invariant_id: str,
    title: str,
    detail: str,
    **extra: Any,
) -> dict:
    out = {
        "invariant_id": str(invariant_id),
        "title": str(title),
        "detail": str(detail),
    }
    out.update({k: v for k, v in extra.items()})
    return out


def check_i1_phase_b_records_present(evidence: Mapping[str, Any]) -> list[dict]:
    """I1 — Phase B's ``total_records`` must be at least the replay
    fixture's record count. Closes the airline / 7NOW silent-mute
    case where producer exceptions caused
    ``phase_b.total_records=0`` while the fixture itself contained
    decision records.
    """
    phase_b = dict(evidence.get("phase_b") or {})
    total = int(phase_b.get("total_records") or 0)
    replay = int(evidence.get("replay_fixture_records") or 0)
    if total < replay:
        return [_violation(
            invariant_id="I1",
            title="phase_b.total_records below replay_fixture.records",
            detail=(
                f"phase_b.total_records={total} < "
                f"replay_fixture.records={replay}"
            ),
            phase_b_total_records=total,
            replay_fixture_records=replay,
            producer_exceptions=dict(phase_b.get("producer_exceptions") or {}),
        )]
    return []


def _ag_levers(ag: Mapping[str, Any]) -> set[int]:
    """Best-effort: read AG levers from the standard fields used by
    the strategist + decomposer. Falls back to keys of
    ``lever_directives``."""
    levers = ag.get("levers") or ag.get("Levers")
    if levers:
        return {int(x) for x in levers if str(x).strip()}
    directives = ag.get("lever_directives") or {}
    return {int(k) for k in directives.keys() if str(k).strip().isdigit()}


def check_i2_lever_coherence(evidence: Mapping[str, Any]) -> list[dict]:
    """I2 — for each iteration, every applied patch's lever must be
    within its AG's declared lever set, and the AG's lever set must
    be a superset of every source cluster's ``recommended_levers``.
    """
    violations: list[dict] = []
    for it in evidence.get("iterations") or []:
        clusters_by_id = {
            str(c.get("cluster_id") or ""): c
            for c in (it.get("clusters") or [])
        }
        for ag in it.get("ags") or []:
            ag_id = str(ag.get("id") or "")
            ag_levers = _ag_levers(ag)
            for cid in ag.get("source_cluster_ids") or []:
                cluster = clusters_by_id.get(str(cid)) or {}
                rec = {
                    int(x) for x in (cluster.get("recommended_levers") or [])
                    if str(x).strip()
                }
                missing = rec - ag_levers
                if missing:
                    violations.append(_violation(
                        invariant_id="I2",
                        title="ag_levers_missing_recommended",
                        detail=(
                            f"AG {ag_id} levers={sorted(ag_levers)} missing "
                            f"recommended {sorted(missing)} from cluster {cid}"
                        ),
                        iteration=int(it.get("iteration") or 0),
                        ag_id=ag_id,
                        cluster_id=str(cid),
                        ag_levers=sorted(ag_levers),
                        missing_levers=sorted(missing),
                    ))
        applied_by_ag: dict[str, set[int]] = {}
        for patch in it.get("applied_patches") or []:
            ag_id = str(patch.get("ag_id") or "")
            try:
                lever = int(patch.get("lever"))
            except (TypeError, ValueError):
                continue
            applied_by_ag.setdefault(ag_id, set()).add(lever)
        ag_index = {str(a.get("id") or ""): a for a in it.get("ags") or []}
        for ag_id, levers_used in applied_by_ag.items():
            ag_levers = _ag_levers(ag_index.get(ag_id) or {})
            outside = levers_used - ag_levers
            if outside:
                violations.append(_violation(
                    invariant_id="I2",
                    title="patch_lever_outside_ag",
                    detail=(
                        f"AG {ag_id} applied lever(s) {sorted(outside)} "
                        f"not in declared {sorted(ag_levers)}"
                    ),
                    iteration=int(it.get("iteration") or 0),
                    ag_id=ag_id,
                    outside_levers=sorted(outside),
                    ag_levers=sorted(ag_levers),
                ))
    return violations


_TARGET_BUCKET_KEYS = (
    "target_fixed_qids",
    "target_still_hard_qids",
    "target_hard_to_soft_qids",
    "target_hard_to_pass_with_judge_debt_qids",
    "target_all_judge_fixed_qids",
    "target_unchanged_qids",
)


def check_i3_acceptance_buckets(evidence: Mapping[str, Any]) -> list[dict]:
    """I3 — target-state buckets partition target_qids; rollback
    reason names a bucket. Closes 7NOW (target_fixed=(), still_hard=(),
    reason=target_qids_not_improved) inconsistency."""
    violations: list[dict] = []
    for it in evidence.get("iterations") or []:
        ad = dict(it.get("acceptance_decision") or {})
        if not ad:
            continue
        target_qids = {str(q) for q in (ad.get("target_qids") or []) if str(q)}
        if not target_qids:
            continue
        bucket_qids: dict[str, set[str]] = {}
        union: set[str] = set()
        seen_twice: set[str] = set()
        for key in _TARGET_BUCKET_KEYS:
            qids = {str(q) for q in (ad.get(key) or []) if str(q)}
            seen_twice.update(qids & union)
            union |= qids
            bucket_qids[key] = qids
        missing = target_qids - union
        if missing:
            violations.append(_violation(
                invariant_id="I3",
                title="target_qids_missing_from_all_buckets",
                detail=(
                    f"target_qids={sorted(target_qids)} not covered by any "
                    f"bucket; missing={sorted(missing)}"
                ),
                iteration=int(it.get("iteration") or 0),
                missing_qids=sorted(missing),
            ))
        if seen_twice:
            violations.append(_violation(
                invariant_id="I3",
                title="target_qids_double_counted_in_buckets",
                detail=f"qids in two buckets: {sorted(seen_twice)}",
                iteration=int(it.get("iteration") or 0),
                double_counted=sorted(seen_twice),
            ))
        reason = str(ad.get("reason_code") or "")
        if reason and reason not in _TARGET_BUCKET_KEYS:
            violations.append(_violation(
                invariant_id="I3",
                title="rollback_reason_does_not_name_a_bucket",
                detail=f"reason_code={reason!r} is not one of {_TARGET_BUCKET_KEYS}",
                iteration=int(it.get("iteration") or 0),
                reason_code=reason,
            ))
    return violations


def check_i4_no_silent_retry(evidence: Mapping[str, Any]) -> list[dict]:
    """I4 — no two consecutive iterations may select the same AG with
    the same applied-patch body-fingerprint set OR with empty proposals.
    Closes airline iter-1/iter-2 H004 retread and 7NOW iter-2..5 spin."""
    violations: list[dict] = []
    iters = list(evidence.get("iterations") or [])
    for i in range(1, len(iters)):
        prev = iters[i - 1]
        curr = iters[i]
        prev_ag = str(prev.get("selected_ag_id") or "")
        curr_ag = str(curr.get("selected_ag_id") or "")
        if not prev_ag or prev_ag != curr_ag:
            continue
        prev_count = int(prev.get("proposal_count") or 0)
        curr_count = int(curr.get("proposal_count") or 0)
        if prev_count == 0 and curr_count == 0:
            violations.append(_violation(
                invariant_id="I4",
                title="consecutive_empty_proposals_same_ag",
                detail=(
                    f"AG {curr_ag} produced 0 proposals in iterations "
                    f"{prev.get('iteration')} and {curr.get('iteration')}"
                ),
                iteration=int(curr.get("iteration") or 0),
                ag_id=curr_ag,
            ))
            continue
        prev_acc = dict(prev.get("acceptance_decision") or {})
        prev_was_rollback = (
            str(prev_acc.get("reason_code") or "")
            != "target_fixed_qids"  # any non-fixed reason ⇒ rollback
            and prev_acc != {}
        )
        if not prev_was_rollback:
            continue
        prev_fp = sorted(
            str(f) for f in (prev.get("applied_patch_body_fingerprints") or [])
        )
        curr_fp = sorted(
            str(f) for f in (curr.get("applied_patch_body_fingerprints") or [])
        )
        if prev_fp and prev_fp == curr_fp:
            violations.append(_violation(
                invariant_id="I4",
                title="same_body_fingerprints_after_rollback",
                detail=(
                    f"AG {curr_ag} re-applied identical patch bodies "
                    f"{prev_fp} after a rollback"
                ),
                iteration=int(curr.get("iteration") or 0),
                ag_id=curr_ag,
                fingerprints=prev_fp,
            ))
    return violations


def check_i5_replay_validity(evidence: Mapping[str, Any]) -> list[dict]:
    """I5 — committed replay fixture for this run validates with zero
    illegal trunk transitions. Closes airline (4 illegal) and 7NOW
    (25 illegal)."""
    rv = dict(evidence.get("replay_validation") or {})
    if not rv:
        return []
    if bool(rv.get("is_valid")):
        return []
    return [_violation(
        invariant_id="I5",
        title="replay_fixture_invalid",
        detail=(
            f"replay reports {int(rv.get('violation_count') or 0)} illegal "
            f"trunk transitions: {dict(rv.get('violation_details') or {})}"
        ),
        violation_count=int(rv.get("violation_count") or 0),
        violation_details=dict(rv.get("violation_details") or {}),
    )]


def check_i6_manifest_paths(evidence: Mapping[str, Any]) -> list[dict]:
    """I6 — manifest declared paths == materialized paths. Closes
    7NOW 130/163 missing while ``missing_pieces=[]``."""
    manifest = dict(evidence.get("manifest") or {})
    declared = {str(p) for p in (manifest.get("declared_paths") or [])}
    materialized = {str(p) for p in (manifest.get("materialized_paths") or [])}
    missing = declared - materialized
    if not missing:
        return []
    return [_violation(
        invariant_id="I6",
        title="manifest_declared_paths_not_materialized",
        detail=(
            f"{len(missing)} of {len(declared)} declared Phase H paths "
            f"are absent from MLflow"
        ),
        declared_count=len(declared),
        materialized_count=len(materialized),
        missing_paths=sorted(missing),
    )]


def check_i7_rca_grounding(evidence: Mapping[str, Any]) -> list[dict]:
    """I7 — every open hard cluster reaching AG-emit has either a fit
    RCA card or a typed cluster_blocked_no_rca decision record. Closes
    7NOW iter-1 where 4/5 hard clusters had no RCA card but the
    strategist proceeded to AG-emit anyway."""
    violations: list[dict] = []
    for it in evidence.get("iterations") or []:
        open_clusters = [str(c) for c in (it.get("open_hard_cluster_ids") or [])]
        rca_present = {
            str(k): bool(v) for k, v in (it.get("rca_cards_present") or {}).items()
        }
        blocked_clusters = {
            str(r.get("cluster_id") or "")
            for r in (it.get("decision_records") or [])
            if str(r.get("decision_type") or "") == "cluster_blocked_no_rca"
        }
        for cid in open_clusters:
            if rca_present.get(cid):
                continue
            if cid in blocked_clusters:
                continue
            violations.append(_violation(
                invariant_id="I7",
                title="open_cluster_ungrounded_at_ag_emit",
                detail=(
                    f"cluster {cid} reached AG-emit with no fit RCA card "
                    f"and no cluster_blocked_no_rca record"
                ),
                iteration=int(it.get("iteration") or 0),
                cluster_id=cid,
            ))
    return violations


def check_i8_plateau_input(evidence: Mapping[str, Any]) -> list[dict]:
    """I8 — when the run terminated via plateau, the plateau decision's
    currently_failing input must equal the union of journey-ledger
    hard-cluster qids in the final iteration. Closes airline
    `plateau_no_open_failures` with 4 open hard clusters."""
    reason = str((evidence.get("convergence") or {}).get("reason") or "")
    if not reason.startswith("plateau_"):
        return []
    plateau_input = dict(evidence.get("plateau_input") or {})
    currently_failing = {
        str(q) for q in (plateau_input.get("currently_failing_qids") or [])
        if str(q)
    }
    journey_hard = {
        str(q) for q in (evidence.get("final_iteration_journey_hard_qids") or [])
        if str(q)
    }
    if currently_failing == journey_hard:
        return []
    return [_violation(
        invariant_id="I8",
        title="plateau_input_diverges_from_journey_ledger",
        detail=(
            f"plateau currently_failing={sorted(currently_failing)} "
            f"!= journey hard={sorted(journey_hard)}; "
            f"source={plateau_input.get('source')!r}"
        ),
        plateau_currently_failing=sorted(currently_failing),
        journey_hard=sorted(journey_hard),
        plateau_source=str(plateau_input.get("source") or ""),
    )]


# I9 compares this exact field set across the DecisionRecord and the
# typed stdout marker. Sorted-tuple normalization on each value makes
# the comparison list-or-tuple agnostic.
_I9_CANONICAL_KEYS: tuple[str, ...] = (
    "reason_code",
    "accepted",
    "target_qids",
    "target_fixed_qids",
    "target_still_hard_qids",
    "target_soft_passing_qids",
    "out_of_target_regressed_qids",
    "soft_to_hard_regressed_qids",
    "passing_to_hard_regressed_qids",
    "unknown_to_hard_regressed_qids",
    "accidentally_improved_qids",
    "unresolved_target_debt_qids",
)


def _i9_normalize(value: Any) -> Any:
    """Canonicalise one field for byte-equality comparison.

    Lists/tuples become sorted tuples of strings; scalars become their
    bool/str/float counterpart. ``None`` becomes the empty tuple so a
    missing list and an explicit ``[]`` compare equal.
    """
    if value is None:
        return ()
    if isinstance(value, bool):
        return bool(value)
    if isinstance(value, (list, tuple)):
        return tuple(sorted(str(x) for x in value))
    return str(value)


def check_i9_acceptance_render_byte_equality(
    evidence: Mapping[str, Any],
) -> list[dict]:
    """I9 — every consumer of ``format_full_eval_marker_payload``
    must render the same canonical field set byte-for-byte.

    Consumed surfaces:
      - ``DecisionRecord.metrics`` of the ``acceptance_decided`` record
        (evidence key: ``iterations[i].acceptance_decision``).
      - ``GSO_FULL_EVAL_V1`` typed stdout marker payload
        (evidence key: ``iterations[i].full_eval_marker``).

    Silent when ``full_eval_marker`` is missing on every iteration so
    legacy replay fixtures (no marker capture) stay green.
    """
    violations: list[dict] = []
    iters = list(evidence.get("iterations") or [])
    saw_marker = False
    for it in iters:
        record = dict(it.get("acceptance_decision") or {})
        marker = dict(it.get("full_eval_marker") or {})
        if not marker:
            continue
        saw_marker = True
        iter_idx = int(it.get("iteration") or 0)
        for key in _I9_CANONICAL_KEYS:
            lhs = _i9_normalize(record.get(key))
            rhs = _i9_normalize(marker.get(key))
            if lhs != rhs:
                violations.append(_violation(
                    invariant_id="I9",
                    title="acceptance_render_byte_inequality",
                    detail=(
                        f"field {key!r} disagrees: "
                        f"acceptance_decision={lhs!r} vs "
                        f"full_eval_marker={rhs!r}"
                    ),
                    iteration=iter_idx,
                    field=str(key),
                    acceptance_decision_value=record.get(key),
                    full_eval_marker_value=marker.get(key),
                ))
    if not saw_marker:
        return []
    return violations


def check_i13_target_delta_totality(evidence: Mapping[str, Any]) -> list[dict]:
    """I13 — every declared target QID has a delta_state entry;
    LOOKUP_FAILED implies reason_code=target_resolution_failed;
    FIXED / STILL_HARD entries agree with the legacy bucket
    tuples. Closes new-anchor 76457773587391 F2 (target neither
    target_fixed nor target_still_hard simultaneously).
    """
    violations: list[dict] = []
    for it in evidence.get("iterations") or []:
        ad = dict(it.get("acceptance_decision") or {})
        if not ad:
            continue
        target_qids = [str(q) for q in (ad.get("target_qids") or []) if str(q)]
        if not target_qids:
            continue
        delta_pairs = ad.get("target_delta_states") or []
        delta_map: dict[str, str] = {}
        for pair in delta_pairs:
            if not isinstance(pair, (list, tuple)) or len(pair) != 2:
                continue
            qid = str(pair[0])
            state = str(pair[1])
            if qid:
                delta_map[qid] = state

        # (a) Totality: every target qid has a delta state.
        missing = [q for q in target_qids if q not in delta_map]
        if missing:
            violations.append(_violation(
                invariant_id="I13",
                title="target_delta_states_not_total_over_target_qids",
                detail=(
                    f"{len(missing)} target qid(s) lack a delta_state entry: "
                    f"{sorted(missing)}"
                ),
                iteration=int(it.get("iteration") or 0),
                missing_target_qids=sorted(missing),
            ))
            continue

        # (b) lookup_failed implies target_resolution_failed reason.
        reason_code = str(ad.get("reason_code") or "")
        has_lookup_failed = any(s == "lookup_failed" for s in delta_map.values())
        if has_lookup_failed and reason_code != "target_resolution_failed":
            violations.append(_violation(
                invariant_id="I13",
                title="lookup_failed_with_legacy_reason_code",
                detail=(
                    f"target_delta_states contains lookup_failed but "
                    f"reason_code={reason_code!r}; expected "
                    f"'target_resolution_failed'"
                ),
                iteration=int(it.get("iteration") or 0),
                reason_code=reason_code,
                lookup_failed_qids=sorted(
                    q for q, s in delta_map.items() if s == "lookup_failed"
                ),
            ))

        # (c) Drift catch: FIXED state must appear in target_fixed_qids;
        # STILL_HARD must appear in target_still_hard_qids. Catches
        # the C14-T2 pre-unification window where the two
        # computations could disagree.
        target_fixed_legacy = {
            str(q) for q in (ad.get("target_fixed_qids") or []) if str(q)
        }
        target_still_legacy = {
            str(q) for q in (ad.get("target_still_hard_qids") or []) if str(q)
        }
        delta_fixed = {q for q, s in delta_map.items() if s == "fixed"}
        delta_still = {q for q, s in delta_map.items() if s == "still_hard"}
        if delta_fixed != target_fixed_legacy or delta_still != target_still_legacy:
            violations.append(_violation(
                invariant_id="I13",
                title="target_delta_states_disagrees_with_legacy_buckets",
                detail=(
                    f"delta_state(FIXED)={sorted(delta_fixed)} vs "
                    f"target_fixed_qids={sorted(target_fixed_legacy)}; "
                    f"delta_state(STILL_HARD)={sorted(delta_still)} vs "
                    f"target_still_hard_qids={sorted(target_still_legacy)}"
                ),
                iteration=int(it.get("iteration") or 0),
                delta_fixed=sorted(delta_fixed),
                target_fixed_legacy=sorted(target_fixed_legacy),
                delta_still=sorted(delta_still),
                target_still_legacy=sorted(target_still_legacy),
            ))

    return violations


# All 9 invariants (I1–I8 + I13) are now implemented and wired in run_invariants.

def run_invariants(evidence: Mapping[str, Any]) -> list[dict]:
    """Aggregate every implemented invariant check; return all
    violations. Empty list = green pilot."""
    violations: list[dict] = []
    for check in (
        check_i1_phase_b_records_present,
        check_i2_lever_coherence,
        check_i3_acceptance_buckets,
        check_i4_no_silent_retry,
        check_i5_replay_validity,
        check_i6_manifest_paths,
        check_i7_rca_grounding,
        check_i8_plateau_input,
        check_i9_acceptance_render_byte_equality,  # Cycle 15.1-T1
        check_i13_target_delta_totality,  # Cycle 14-T0
    ):
        try:
            violations.extend(check(evidence))
        except Exception as exc:  # invariant bugs must not crash runs
            violations.append(_violation(
                invariant_id="I_CHECK_FAILED",
                title=f"invariant check {check.__name__} raised",
                detail=repr(exc)[:512],
            ))
    return violations
