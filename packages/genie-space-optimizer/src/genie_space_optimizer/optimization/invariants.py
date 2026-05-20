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
  I12 — replay validity (canonical HIGH-tier replay-validity invariant,
        Cycle 17 T3). Same predicate as I5; co-exists for tier-
        separation in C16-T4's contract-health summary.
  I14 — P-E1 dedup: at most one live ``lever6_force_llm_declined``
        per ``(iter, cluster_signature, root_cause)``; cached
        records are unbounded.
  I15 — Plan 10 Phase B1. Activation-marker pair completeness:
        every ``GSO_PLAN5_ANCHOR_ACTIVATION_V1`` marker with status
        ``anchor_entered_plan5_dispatch`` must be paired with a
        matching in-dispatcher status marker
        (``plan5_intent_declined`` /
        ``plan5_intent_validator_rejected`` /
        ``plan5_intent_routed`` / ``plan5_intent_materialized``)
        in the same ``(run_id, ag_id, iteration)`` window.
        Violation = the LLM dispatch was entered but exited
        silently (Plan 10 Leak 1's exact signature).
  I16 — Plan 10 Phase B2. Illegal-decline after Plan 9 activation:
        within a single ``(ag_id, iteration)`` window, if a
        marker with status ``anchor_entered_plan5_dispatch`` fires
        then ``GSO_NO_STRUCTURAL_CANDIDATE_V1`` records with
        skipped_reason ``no_top_n_archetype`` or
        ``no_archetype_or_slice`` are illegal UNLESS the same
        window also carries a ``plan5_intent_validator_rejected``
        marker with a concrete typed reason. Catches the silent
        fall-through to the legacy archetype path even when
        individual fixes regress.
"""

from __future__ import annotations

from dataclasses import dataclass
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
    strategist proceeded to AG-emit anyway.

    Detection-side guarantee landed by Cycle 17 (the invariant body
    below). Production-side guarantee landed by Defect Plan 1
    (2026-05-12) — ``harness.collect_blocked_clusters`` now emits one
    ``DecisionType.CLUSTER_BLOCKED_NO_RCA`` record per ungrounded open
    hard cluster at AG-emit time, and
    ``stages.action_groups.select`` drops AGs whose
    ``source_cluster_ids`` intersect the blocked set. With both halves
    landed, a run that has any open hard cluster with ``rca_card=False``
    at AG-emit time SHOULD have zero I7 violations because the green
    branch (cluster present in ``blocked_clusters`` set) is now
    reached in production. A surviving violation indicates either the
    grounding-gate flag is off or the harness wiring failed before
    record emission."""
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
    # Plan P-C addition — close the reason-detail render drift between
    # the operator transcript's Acceptance / Rollback line and the
    # GSO_FULL_EVAL_V1 marker payload (anchor: ccf1d60d iter 1).
    "reason_detail",
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


def check_i10_applied_patch_id_injective(
    evidence: Mapping[str, Any],
) -> list[dict]:
    """I10 — every applied patch's ``expanded_patch_id`` is non-empty
    and globally unique across the run; the triple
    ``(parent_proposal_id, lever, iteration)`` does not collide within
    a single iteration.

    Consumed surface:
      - ``iterations[i].applied_patch_identifiers`` — list of dicts
        ``{expanded_patch_id, proposal_id|parent_proposal_id, lever}``
        captured alongside each iteration's applied-patch list.

    Silent when no iteration carries ``applied_patch_identifiers``.

    Closes C14-T4. Catches: stamper bypass, parent-id reuse with the
    same lever, empty/missing expanded id, split-child suffix
    collision.
    """
    violations: list[dict] = []
    iters = list(evidence.get("iterations") or [])
    saw_any = False
    seen_global: dict[str, int] = {}
    for it in iters:
        rows = list(it.get("applied_patch_identifiers") or [])
        if not rows:
            continue
        saw_any = True
        iter_idx = int(it.get("iteration") or 0)
        seen_in_iter: set[tuple[str, str]] = set()
        for row in rows:
            if not isinstance(row, dict):
                continue
            expanded = str(row.get("expanded_patch_id") or "").strip()
            if not expanded:
                violations.append(_violation(
                    invariant_id="I10",
                    title="empty_expanded_patch_id",
                    detail=(
                        f"applied patch in iter {iter_idx} is missing "
                        f"expanded_patch_id: {row!r}"
                    ),
                    iteration=iter_idx,
                    row=dict(row),
                ))
                continue
            prior = seen_global.get(expanded)
            if prior is not None:
                violations.append(_violation(
                    invariant_id="I10",
                    title="duplicate_expanded_patch_id",
                    detail=(
                        f"expanded_patch_id={expanded!r} appears in both "
                        f"iter {prior} and iter {iter_idx}"
                    ),
                    iteration=iter_idx,
                    expanded_patch_id=expanded,
                    first_iteration=prior,
                ))
            else:
                seen_global[expanded] = iter_idx
            parent = str(
                row.get("parent_proposal_id")
                or row.get("source_proposal_id")
                or row.get("proposal_id")
                or ""
            ).strip()
            lever_raw = row.get("lever")
            lever = "" if lever_raw is None else str(lever_raw)
            if parent:
                key = (parent, lever)
                if key in seen_in_iter:
                    violations.append(_violation(
                        invariant_id="I10",
                        title="duplicate_parent_lever_within_iteration",
                        detail=(
                            f"parent_proposal_id={parent!r} lever={lever!r} "
                            f"appears twice in iter {iter_idx}"
                        ),
                        iteration=iter_idx,
                        parent_proposal_id=parent,
                        lever=lever,
                    ))
                else:
                    seen_in_iter.add(key)
    if not saw_any:
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


def check_i11_causal_continuity(evidence: Mapping[str, Any]) -> list[dict]:
    """I11 — Cycle 16 T5. Causal continuity through safety gates.

    For every iteration where ``structural_causal_dropped_count > 0``
    (the blast-radius gate dropped at least one structural-causal
    patch), require either:

      (a) ``narrow_branch_c_synthesized_count > 0`` — Branch C L5
          synthesis produced at least one survivor for the dropped
          parent(s), OR

      (b) ``no_structural_alternative_ag_ids`` is non-empty — at least
          one AG was honestly halted with the Cycle 16 T4 terminal
          record.

    Silent when no iteration carries ``structural_causal_dropped_count``
    (legacy fixtures pre-Cycle-16 stay green).

    Closes anchor #4 (run 294, 100% accepted airline) F8 instance plus
    the F2/F3 evidence threads cited in roadmap line 663-668: when the
    structural causal patch is dropped, the optimizer either
    synthesizes a narrower causal alternative (Branch C) or halts
    honestly. Never silently degrades to non-structural-only.
    """
    violations: list[dict] = []
    iters = list(evidence.get("iterations") or [])
    saw_signal = False
    for it in iters:
        dropped = int(it.get("structural_causal_dropped_count") or 0)
        synthesized = int(it.get("narrow_branch_c_synthesized_count") or 0)
        halted_ags = tuple(
            it.get("no_structural_alternative_ag_ids") or ()
        )
        # Detect whether the iteration carries any C16 evidence at all.
        if (
            "structural_causal_dropped_count" in it
            or "narrow_branch_c_synthesized_count" in it
            or "no_structural_alternative_ag_ids" in it
        ):
            saw_signal = True
        if dropped <= 0:
            continue
        if synthesized > 0 or halted_ags:
            continue
        violations.append(_violation(
            invariant_id="I11",
            title="causal_continuity_violated",
            detail=(
                f"iter {int(it.get('iteration') or 0)} dropped "
                f"{dropped} structural-causal patch(es) but neither "
                f"Branch C synthesized a survivor nor an AG was halted "
                f"with no_structural_alternative"
            ),
            iteration=int(it.get("iteration") or 0),
            structural_causal_dropped_count=dropped,
            narrow_branch_c_synthesized_count=synthesized,
            no_structural_alternative_ag_ids=list(halted_ags),
        ))
    if not saw_signal:
        return []
    return violations


def check_i12_replay_validity(evidence: Mapping[str, Any]) -> list[dict]:
    """I12 — replay validity: zero illegal trunk transitions.

    Cycle 17 T3 canonical-ID registration. C16-T4's contract-health
    summary reads I12 from the HIGH severity tier; the merge gate
    (C16-T5) blocks run exit when I12 is non-green.

    Reads ``evidence["replay_validation"]`` produced by the harness
    end-of-iteration validator (`_validate_journeys_at_iteration_end`)
    or the run-end replay driver (`lever_loop_replay.run_replay`).
    Silent when no replay validation is present so legacy fixtures
    (pre-Cycle-12 runs without `replay_validation` capture) stay
    green.

    Co-exists with the MEDIUM-severity I5 (same predicate, lower
    tier). Both fire on the same evidence; downstream C16-T4 reads
    them at different severities. The duplication is intentional:
    I5 retains its position for back-compat; I12 is the canonical
    HIGH-tier surface registered under the canonical roadmap ID.

    Anchor: ``runid_analysis/3b050ec5-4032-457f-a785-2d1a3942a097``
    postmortem F9 — 25 illegal trunk transitions clear under
    Cycle 17 T1+T2 + flag-on.
    """
    rv = dict(evidence.get("replay_validation") or {})
    if not rv:
        return []
    if bool(rv.get("is_valid")):
        return []
    return [_violation(
        invariant_id="I12",
        title="replay_validity_violated",
        detail=(
            f"replay reports {int(rv.get('violation_count') or 0)} illegal "
            f"trunk transitions: {dict(rv.get('violation_details') or {})}"
        ),
        violation_count=int(rv.get("violation_count") or 0),
        violation_details=dict(rv.get("violation_details") or {}),
    )]


def check_i14_l6_decline_dedup(evidence: Mapping[str, Any]) -> list[dict]:
    """I14 — P-E1 observable-outcome dedup: at most one *live*
    (``metrics.cached == False``) ``lever6_force_llm_declined``
    decision record per ``(iteration, cluster_signature, root_cause)``
    tuple.

    This is the run-level guard for the iteration-scoped Lever-6
    decline cache landed by P-E1. The cache itself (in
    ``harness._maybe_force_lever6_with_cache``) is the production
    dedup mechanism; this invariant enforces the observable property
    independent of how the cache is wired, so a future regression
    that bypasses the wrapper (e.g. a sibling proposal generator
    emitting its own declined record, or the paranoia-guard taking
    the fail-open path repeatedly) is caught at run end rather than
    leaking redundant LLM-decline noise into dashboards.

    The ``cluster_signature`` is extracted from the record's
    ``evidence_refs`` (the ``signature:<sig>`` token written by
    ``decision_emitters.lever6_force_llm_declined_record`` when the
    P-E1 ``cluster_signature`` argument is supplied). Records without
    a signature evidence ref are legacy / pre-P-E1 fixtures and are
    silently skipped so this invariant stays back-compat.

    Cached records (``metrics.cached == True``) are *unbounded* per
    group — they are the intended dedup mechanism in action. Only
    live declines are counted toward the violation threshold.
    """
    violations: list[dict] = []
    for it in evidence.get("iterations") or []:
        iteration = int(it.get("iteration") or 0)
        groups: dict[tuple[str, str], int] = {}
        for r in it.get("decision_records") or []:
            if str(r.get("reason_code") or "") != "lever6_force_llm_declined":
                continue
            metrics = dict(r.get("metrics") or {})
            if bool(metrics.get("cached")):
                continue
            sig = ""
            for ref in (r.get("evidence_refs") or ()):
                s = str(ref or "")
                if s.startswith("signature:"):
                    sig = s[len("signature:"):]
                    break
            if not sig:
                # Legacy fixture without P-E1 evidence-ref extension —
                # cannot be grouped, skip silently.
                continue
            root_cause = str(r.get("root_cause") or "")
            key = (sig, root_cause)
            groups[key] = groups.get(key, 0) + 1
        for (sig, rc), count in sorted(groups.items()):
            if count <= 1:
                continue
            violations.append(_violation(
                invariant_id="I14",
                title="lever6_force_llm_declined_dedup_violation",
                detail=(
                    f"iteration={iteration} cluster_signature={sig!r} "
                    f"root_cause={rc!r} live_decline_count={count} "
                    "(expected <= 1; cache should have short-circuited "
                    "siblings)"
                ),
                iteration=iteration,
                cluster_signature=sig,
                root_cause=rc,
                live_decline_count=count,
            ))
    return violations


# All invariants (I1–I8 + I11 + I12 + I13 + I14 + I15 + I16) are now
# implemented and wired in run_invariants.


# ---------------------------------------------------------------------------
# Plan 10 Phase B — markers-on-evidence invariants (I15, I16)
# ---------------------------------------------------------------------------
#
# Both invariants read marker payloads produced by ``plan9_activation_markers``
# (``GSO_PLAN5_ANCHOR_ACTIVATION_V1``) and ``run_analysis_contract.no_structural_candidate_marker``
# (``GSO_NO_STRUCTURAL_CANDIDATE_V1``). The marker stream is shaped into the
# evidence dict by upstream postmortem extractors before ``run_invariants``
# runs; the shape is one flat list per marker family:
#
#     evidence["activation_markers"] = [
#         {
#             "marker_name": "GSO_PLAN5_ANCHOR_ACTIVATION_V1",
#             "optimization_run_id": "run-X",
#             "iteration": 3,
#             "ag_id": "AG_DECOMPOSED_H001",
#             "cluster_id": "C_top_n_collapse",
#             "status": "anchor_entered_plan5_dispatch",
#             "reason": "",
#             "patch_type": "",
#             "intent_id": "",
#         },
#         ...
#     ]
#     evidence["no_structural_candidate_markers"] = [
#         {
#             "marker_name": "GSO_NO_STRUCTURAL_CANDIDATE_V1",
#             "ag_id": "AG_DECOMPOSED_H001",
#             "iteration": 3,
#             "attempted_archetypes": [],
#             "skipped_reason": "no_top_n_archetype",
#         },
#         ...
#     ]
#
# Both invariants stay silent (return ``[]``) when neither marker family is
# present in the evidence dict so legacy fixtures from before Plan 9 stay
# green.


_ACTIVATION_MARKER_NAME: str = "GSO_PLAN5_ANCHOR_ACTIVATION_V1"
_NO_STRUCTURAL_CANDIDATE_MARKER_NAME: str = "GSO_NO_STRUCTURAL_CANDIDATE_V1"

_ANCHOR_ENTERED: str = "anchor_entered_plan5_dispatch"
_INNER_STATUSES_PAIRED: frozenset[str] = frozenset({
    "plan5_intent_declined",
    "plan5_intent_validator_rejected",
    "plan5_intent_routed",
    "plan5_intent_materialized",
})
_VALIDATOR_REJECTED: str = "plan5_intent_validator_rejected"
_ILLEGAL_LEGACY_SKIPPED_REASONS: frozenset[str] = frozenset({
    "no_top_n_archetype",
    "no_archetype_or_slice",
})


def _activation_markers(evidence: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    """Pull activation marker dicts out of evidence. Silent on absence so
    legacy fixtures (pre-Plan-9) stay green for I15 + I16."""
    raw = evidence.get("activation_markers") or ()
    out: list[Mapping[str, Any]] = []
    for entry in raw:
        if not isinstance(entry, Mapping):
            continue
        marker = str(entry.get("marker_name") or "")
        if marker and marker != _ACTIVATION_MARKER_NAME:
            continue
        out.append(entry)
    return out


def _no_structural_candidate_markers(
    evidence: Mapping[str, Any],
) -> list[Mapping[str, Any]]:
    """Pull no-structural-candidate marker dicts out of evidence."""
    raw = evidence.get("no_structural_candidate_markers") or ()
    out: list[Mapping[str, Any]] = []
    for entry in raw:
        if not isinstance(entry, Mapping):
            continue
        marker = str(entry.get("marker_name") or "")
        if marker and marker != _NO_STRUCTURAL_CANDIDATE_MARKER_NAME:
            continue
        out.append(entry)
    return out


def check_i15_activation_pair_completeness(
    evidence: Mapping[str, Any],
) -> list[dict]:
    """I15 — Plan 10 Phase B1. Every harness-level
    ``anchor_entered_plan5_dispatch`` activation marker must be paired
    with a matching in-dispatcher status marker on the same
    ``(run_id, ag_id, iteration)`` key.

    Closes Plan 10 Leak 1's exact signature: the LLM dispatch path was
    entered (T9.1 harness-level marker fired) but exited silently
    because the gate's ``rca_evidence_typed`` precondition was empty.
    The in-dispatcher status (``plan5_intent_invoked`` /
    ``plan5_intent_declined`` / ``plan5_intent_validator_rejected`` /
    ``plan5_intent_routed`` / ``plan5_intent_materialized``) never
    surfaced, so postmortems saw "entered but no outcome" — exactly
    the silent-fall-through pattern this plan exists to prevent.

    Pairing rule: every ``anchor_entered_plan5_dispatch`` marker must
    have at least one accompanying marker on the same
    ``(optimization_run_id, ag_id, iteration)`` triple with status in
    ``_INNER_STATUSES_PAIRED``. ``plan5_intent_invoked`` is the
    pre-outcome marker, so it does NOT satisfy the pairing — an
    INVOKED marker without a following terminal status is itself a
    silent-exit violation.

    Silent (returns ``[]``) when ``evidence["activation_markers"]`` is
    absent so back-compat with pre-Plan-9 fixtures is preserved.
    """
    markers = _activation_markers(evidence)
    if not markers:
        return []
    entered: list[Mapping[str, Any]] = []
    paired_keys: set[tuple[str, str, int]] = set()
    for m in markers:
        status = str(m.get("status") or "")
        key = (
            str(m.get("optimization_run_id") or ""),
            str(m.get("ag_id") or ""),
            int(m.get("iteration") or 0),
        )
        if status == _ANCHOR_ENTERED:
            entered.append(m)
        elif status in _INNER_STATUSES_PAIRED:
            paired_keys.add(key)
    violations: list[dict] = []
    for m in entered:
        key = (
            str(m.get("optimization_run_id") or ""),
            str(m.get("ag_id") or ""),
            int(m.get("iteration") or 0),
        )
        if key in paired_keys:
            continue
        violations.append(_violation(
            invariant_id="I15",
            title="activation_pair_completeness_violated",
            detail=(
                f"anchor_entered_plan5_dispatch fired for "
                f"run={key[0]!r} ag={key[1]!r} iter={key[2]} but no "
                "paired in-dispatcher status marker followed "
                "(expected one of plan5_intent_declined / "
                "plan5_intent_validator_rejected / "
                "plan5_intent_routed / plan5_intent_materialized)"
            ),
            optimization_run_id=key[0],
            ag_id=key[1],
            iteration=key[2],
            cluster_id=str(m.get("cluster_id") or ""),
        ))
    return violations


def check_i16_no_legacy_decline_after_activation(
    evidence: Mapping[str, Any],
) -> list[dict]:
    """I16 — Plan 10 Phase B2. Within a single ``(ag_id, iteration)``
    window, if a marker with status ``anchor_entered_plan5_dispatch``
    fires, then ``GSO_NO_STRUCTURAL_CANDIDATE_V1`` records with
    ``skipped_reason`` in {``no_top_n_archetype``,
    ``no_archetype_or_slice``} are illegal UNLESS the same window
    also carries a ``plan5_intent_validator_rejected`` marker with
    a concrete typed reason.

    Catches the "silent fall-through to legacy archetype path" pattern
    even when an individual Plan 10 fix regresses. The legacy
    archetype skipped_reasons are the deterministic-classifier
    signals that the AG was handed back to the pre-Plan-9 lever-5
    pipeline despite the LLM-direct lane being entered. The carve-out
    for ``plan5_intent_validator_rejected`` honors the path where the
    LLM produced a structurally invalid intent — the validator
    rejection is a typed, observable decline, not a silent fall-back.

    Silent (returns ``[]``) when either marker family is absent so
    pre-Plan-9 fixtures stay green.
    """
    activation = _activation_markers(evidence)
    no_struct = _no_structural_candidate_markers(evidence)
    if not activation or not no_struct:
        return []
    entered_keys: dict[tuple[str, int], Mapping[str, Any]] = {}
    validator_rejected_keys: set[tuple[str, int]] = set()
    for m in activation:
        ag_id = str(m.get("ag_id") or "")
        iteration = int(m.get("iteration") or 0)
        status = str(m.get("status") or "")
        if status == _ANCHOR_ENTERED:
            entered_keys[(ag_id, iteration)] = m
        elif status == _VALIDATOR_REJECTED:
            reason = str(m.get("reason") or "").strip()
            if reason:
                validator_rejected_keys.add((ag_id, iteration))
    violations: list[dict] = []
    for r in no_struct:
        skipped = str(r.get("skipped_reason") or "")
        if skipped not in _ILLEGAL_LEGACY_SKIPPED_REASONS:
            continue
        ag_id = str(r.get("ag_id") or "")
        iteration = int(r.get("iteration") or 0)
        key = (ag_id, iteration)
        entered = entered_keys.get(key)
        if entered is None:
            continue
        if key in validator_rejected_keys:
            continue
        violations.append(_violation(
            invariant_id="I16",
            title="legacy_decline_after_activation_violated",
            detail=(
                f"GSO_NO_STRUCTURAL_CANDIDATE_V1 with "
                f"skipped_reason={skipped!r} fired for ag={ag_id!r} "
                f"iter={iteration} after anchor_entered_plan5_dispatch "
                "without a paired plan5_intent_validator_rejected "
                "marker carrying a concrete typed reason. The "
                "LLM-direct lane silently fell through to the legacy "
                "archetype path."
            ),
            optimization_run_id=str(entered.get("optimization_run_id") or ""),
            ag_id=ag_id,
            iteration=iteration,
            cluster_id=str(entered.get("cluster_id") or ""),
            skipped_reason=skipped,
        ))
    return violations


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
        check_i10_applied_patch_id_injective,  # Cycle 15.1-T2
        check_i11_causal_continuity,  # Cycle 16 T5
        check_i13_target_delta_totality,  # Cycle 14-T0
        check_i14_l6_decline_dedup,  # P-E1
        check_i12_replay_validity,  # Cycle 17 T3
        check_i15_activation_pair_completeness,  # Plan 10 Phase B1
        check_i16_no_legacy_decline_after_activation,  # Plan 10 Phase B2
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


# ---------------------------------------------------------------------------
# Plan P-F (2026-05-12) — iteration-level coverage invariant
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ProposalFailureCoverageResult:
    """Outcome of ``check_proposal_failure_decided_coverage``."""

    violated: bool
    message: str


_NO_APPLIED_EXIT_PATHS: frozenset[str] = frozenset({
    "proposals_empty",
    "skipped_no_applied_patches",
    "no_causal_applyable_patch",
    "dead_on_arrival",
})


def check_proposal_failure_decided_coverage(
    iter_inputs: Mapping[str, Any],
) -> ProposalFailureCoverageResult:
    """Plan P-F (2026-05-12) — iteration-level coverage invariant.

    For every iteration whose ``applied_patches_total == 0`` AND whose
    ``exit_path`` is one of the no-applied exit paths, the iteration's
    ``decision_records`` MUST carry at least one
    ``decision_type == "proposal_failure_decided"`` record. Otherwise
    the loop has stalled silently.

    The invariant runs under the existing ``warn-and-degrade`` policy
    (see ``GSO_LOOP_INVARIANTS_STRICT``); the harness emits
    ``GSO_INVARIANT_VIOLATION_V1`` with
    ``invariant_name="proposal_failure_decided_coverage"`` and
    continues.
    """
    applied_total = int(iter_inputs.get("applied_patches_total") or 0)
    if applied_total > 0:
        return ProposalFailureCoverageResult(violated=False, message="")

    exit_path = str(iter_inputs.get("exit_path") or "").strip()
    if exit_path and exit_path not in _NO_APPLIED_EXIT_PATHS:
        return ProposalFailureCoverageResult(violated=False, message="")

    decision_records = iter_inputs.get("decision_records") or []
    has_failure_decided = any(
        str(rec.get("decision_type") or "") == "proposal_failure_decided"
        for rec in decision_records
    )
    if has_failure_decided:
        return ProposalFailureCoverageResult(violated=False, message="")

    return ProposalFailureCoverageResult(
        violated=True,
        message=(
            f"iteration applied_patches_total=0 exit_path={exit_path!r} "
            f"but no proposal_failure_decided record present "
            f"({len(decision_records)} decision_records seen)"
        ),
    )


@dataclass(frozen=True)
class DirectiveOutcomeCoverageResult:
    """Outcome of ``check_directive_outcome_coverage``."""

    violated: bool
    message: str
    offending_ag_ids: tuple[str, ...] = ()
    offending_lever_keys_by_ag: tuple[tuple[str, tuple[int, ...]], ...] = ()


def check_directive_outcome_coverage(
    iter_inputs: Mapping[str, Any],
) -> DirectiveOutcomeCoverageResult:
    """Phase 3 (2026-05-13) — per-AG-per-lever-directive coverage invariant.

    For every AG with ``ag.lever_directives`` non-empty in this iteration,
    every ``lever_key`` in the directive dict MUST appear as a key in the
    iteration's ``directive_outcomes`` ledger
    (``iter_inputs["directive_outcomes_by_ag"][ag_id].outcomes_by_lever``).
    Otherwise the AG silently lost a directive — the 2314bb2c AG2 budget-burn
    pattern.

    Runs under warn-and-degrade: the harness emits
    ``GSO_INVARIANT_VIOLATION_V1`` with
    ``invariant_name="directive_outcome_coverage"`` and continues.

    Expected ``iter_inputs`` shape::

        {
          "action_groups": [
            {"id": "AG1", "lever_directives": {"5": {...}, "6": {...}}},
            ...
          ],
          "directive_outcomes_by_ag": {
            "AG1": AgDirectiveLedger(
              outcomes_by_lever={5: ..., 6: ...}
            ),
            ...
          },
        }
    """
    action_groups = iter_inputs.get("action_groups") or []
    outcomes_by_ag = iter_inputs.get("directive_outcomes_by_ag") or {}

    offending: list[tuple[str, tuple[int, ...]]] = []
    for ag in action_groups:
        ag_id = str(ag.get("id") or ag.get("ag_id") or "")
        if not ag_id:
            continue
        directives = ag.get("lever_directives") or {}
        if not directives:
            continue
        ledger = outcomes_by_ag.get(ag_id)
        if ledger is None:
            offending.append(
                (ag_id, tuple(sorted(int(k) for k in directives.keys())))
            )
            continue
        # AgDirectiveLedger.outcomes_by_lever is dict[int, DirectiveOutcomeCode].
        present_keys = set(getattr(ledger, "outcomes_by_lever", {}).keys())
        expected_keys = set(int(k) for k in directives.keys())
        missing = sorted(expected_keys - present_keys)
        if missing:
            offending.append((ag_id, tuple(missing)))

    if not offending:
        return DirectiveOutcomeCoverageResult(violated=False, message="")

    offending_ag_ids = tuple(ag_id for ag_id, _ in offending)
    return DirectiveOutcomeCoverageResult(
        violated=True,
        message=(
            f"directive_outcome_coverage: {len(offending)} AG(s) carried "
            f"lever_directives without a matching outcome ledger entry — "
            f"silent budget burn"
        ),
        offending_ag_ids=offending_ag_ids,
        offending_lever_keys_by_ag=tuple(offending),
    )
