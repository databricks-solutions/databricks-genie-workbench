"""Pure causal-control-plane helpers for the lever loop.

The helpers in this module define the shared contract between clustering,
RCA, proposal grounding, and acceptance. They intentionally avoid Spark,
WorkspaceClient, LLM calls, and Genie API calls so they can be unit tested
without a Databricks workspace.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Iterable

from genie_space_optimizer.common.config import (
    IGNORED_OPTIMIZATION_JUDGES as _CONFIG_IGNORED_OPTIMIZATION_JUDGES,
    attribution_drift_reattribution_enabled,
)
from genie_space_optimizer.optimization.eval_row_access import (
    row_qid as _row_qid,
    rows_by_qid,
    rows_for_qids,
)
from genie_space_optimizer.optimization.evaluation import (
    get_failed_judges,
    has_individual_judge_failure,
    row_is_hard_failure,
)

IGNORED_OPTIMIZATION_JUDGES: frozenset[str] = frozenset(
    _CONFIG_IGNORED_OPTIMIZATION_JUDGES
)
"""Judges that may be logged but must not drive optimization work.

Sourced from ``common.config.IGNORED_OPTIMIZATION_JUDGES`` so the
``GSO_IGNORED_OPTIMIZATION_JUDGES`` env var is the single source of
truth across the optimizer engine. Re-exported here as a frozenset for
fast membership checks in the control-plane path.
"""


def actionable_failed_judges(row: dict) -> tuple[str, ...]:
    """Return failed judges that are allowed to drive optimizer action."""
    failed = tuple(get_failed_judges(row or {}))
    return tuple(j for j in failed if j not in IGNORED_OPTIMIZATION_JUDGES)


def is_actionable_soft_signal_row(row: dict) -> bool:
    """Return true when a non-hard row has actionable non-text judge failures."""
    if row_is_hard_failure(row or {}):
        return False
    if not has_individual_judge_failure(row or {}):
        return False
    return bool(actionable_failed_judges(row or {}))


def _qid_from_question_ref(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        return str(
            value.get("question_id")
            or value.get("id")
            or value.get("qid")
            or ""
        ).strip()
    return ""


def target_qids_from_action_group(
    action_group: dict,
    source_clusters: Iterable[dict],
) -> tuple[str, ...]:
    """Resolve the qids an action group claims to fix.

    Explicit ``affected_questions`` is accepted only when its entries
    match known source-cluster qids. LLMs sometimes emit the natural
    language question text in this field; that must fall back to
    ``source_cluster_ids`` rather than scoping grounding to zero rows.
    """
    source_ids = {
        str(cid)
        for cid in action_group.get("source_cluster_ids", []) or []
        if str(cid)
    }
    known_qids: list[str] = []
    for cluster in source_clusters or []:
        if source_ids and str(cluster.get("cluster_id", "")) not in source_ids:
            continue
        for qid in cluster.get("question_ids", []) or []:
            if qid:
                known_qids.append(str(qid))

    known_set = set(known_qids)
    explicit: list[str] = []
    for ref in action_group.get("affected_questions") or []:
        qid = _qid_from_question_ref(ref)
        if qid and (not known_set or qid in known_set):
            explicit.append(qid)

    if explicit:
        return tuple(dict.fromkeys(explicit))
    return tuple(dict.fromkeys(known_qids))


def _cluster_judges(cluster: dict) -> tuple[str, ...]:
    raw = (
        cluster.get("affected_judges")
        or cluster.get("dominant_failed_judges")
        or [cluster.get("affected_judge")]
        or []
    )
    if isinstance(raw, str):
        raw = [raw]
    return tuple(str(j) for j in raw if str(j))


def _is_response_quality_only_cluster(cluster: dict) -> bool:
    judges = tuple(j for j in _cluster_judges(cluster) if j)
    return bool(judges) and all(j in IGNORED_OPTIMIZATION_JUDGES for j in judges)


def clusters_for_strategy(
    hard_clusters: list[dict],
    soft_clusters: list[dict],
    *,
    hard_only_threshold: int = 3,
    soft_min_questions: int = 5,
    max_soft_clusters: int = 1,
) -> tuple[list[dict], list[dict]]:
    """Return clusters that may drive the strategist.

    Hard failures remain first priority. When the hard set is small and a
    soft cluster covers many questions, include a bounded soft lane so the
    optimizer can learn broad corpus guidance without starving hard fixes.
    """
    hard = list(hard_clusters or [])
    soft = [
        c for c in (soft_clusters or [])
        if isinstance(c, dict) and not _is_response_quality_only_cluster(c)
    ]
    if not hard:
        return [], soft
    if len(hard) > int(hard_only_threshold):
        return hard, []

    large_soft = sorted(
        [
            c for c in soft
            if len(c.get("question_ids", []) or []) >= int(soft_min_questions)
        ],
        key=lambda c: len(c.get("question_ids", []) or []),
        reverse=True,
    )
    return hard, large_soft[: int(max_soft_clusters)]


def hard_failure_qids(rows: Iterable[dict]) -> tuple[str, ...]:
    """Return qids whose rows are hard failures under the shared predicate."""
    qids: list[str] = []
    for row in rows or []:
        if isinstance(row, dict) and row_is_hard_failure(row):
            qid = _row_qid(row)
            if qid:
                qids.append(qid)
    return tuple(dict.fromkeys(qids))


def compute_accidentally_improved_qids(
    *,
    pre_rows: Iterable[dict],
    post_rows: Iterable[dict],
    target_qids: Iterable[str],
) -> tuple[str, ...]:
    """Cycle 14-C T1 — return the QIDs that were ``hard`` in baseline,
    ``passing`` in candidate, and NOT in the named target set.

    These are the QIDs that explain a global accuracy gain when the
    strategist's named target did not flip — i.e. the
    ``accepted_with_attribution_drift`` branch fired. Surfacing
    them is the cycle's primary contribution: keeps the win, but
    attributes the gain to the QIDs that actually moved instead of
    silently crediting the still-hard target.

    Pure: no I/O, no globals, no side effects. Suitable for unit
    tests on synthetic inputs.

    Anchor: airline run 1105451933925748 iter 1 — target=gs_024
    remained STILL_HARD; multiple non-target QIDs flipped from
    baseline-hard to candidate-passing.

    Returns a sorted tuple (canonical order so the resulting
    ``ControlPlaneAcceptance`` field is byte-stable across runs).
    """
    pre_rows_list = list(pre_rows or [])
    post_rows_list = list(post_rows or [])
    target_set = {str(q) for q in (target_qids or ()) if str(q)}
    pre_hard = set(hard_failure_qids(pre_rows_list))
    pre_hard |= {
        str(row.get("question_id") or row.get("id") or "")
        for row in pre_rows_list
        if isinstance(row, dict)
        and str(row.get("row_status", "")).lower() == "hard"
    }
    pre_hard.discard("")
    post_passing = {
        str(row.get("question_id") or row.get("id") or "")
        for row in post_rows_list
        if isinstance(row, dict)
        and str(row.get("row_status", "")).lower() == "passing"
    }
    accidentally_improved = (pre_hard & post_passing) - target_set
    return tuple(sorted(q for q in accidentally_improved if q))


# ── Cycle 14-T0: per-QID target-delta classifier ─────────────────────
#
# Total function over target_qids. Every declared target lands in
# exactly one DeltaState value. LOOKUP_FAILED is the explicit
# "I could not resolve this target" answer that replaces the legacy
# set-arithmetic fall-through where target_fixed=() AND
# target_still=() could be simultaneously empty for an evaluated
# target QID (new anchor 76457773587391 F2).


class DeltaState(StrEnum):
    """Per-target outcome of a single iteration's full eval.

    Values are the lowercase names so that JSON serialisation
    yields stable strings consumable by Cycle 14-T2's render and
    Cycle 16-T4's contract-health marker without further mapping.
    """

    FIXED = "fixed"
    STILL_HARD = "still_hard"
    SOFT_TO_HARD = "soft_to_hard"
    SOFT_PASSING = "soft_passing"
    REGRESSED_TO_UNKNOWN = "regressed_to_unknown"
    LOOKUP_FAILED = "lookup_failed"


def compute_target_delta_states(
    *,
    target_qids: Iterable[str],
    pre_rows: Iterable[dict],
    post_rows: Iterable[dict],
    candidate_failed_qids: Iterable[str],
) -> dict[str, "DeltaState"]:
    """Classify every declared target QID into one DeltaState.

    Total over ``target_qids``: every QID appears in the returned
    dict exactly once. Duplicates in ``target_qids`` are de-duped
    via ``dict.fromkeys`` to preserve declaration order.

    Resolution rules (first-match wins):
      1. QID present in pre_rows AND post_rows                   -> use row_status delta
      2. QID present in pre_rows only, absent from candidate     -> SOFT_PASSING / FIXED
      3. QID absent from pre_rows but present in post_rows hard  -> REGRESSED_TO_UNKNOWN
      4. QID absent from BOTH pre_rows and post_rows             -> LOOKUP_FAILED
    """
    targets = tuple(dict.fromkeys(str(q) for q in target_qids or [] if str(q)))
    if not targets:
        return {}

    pre_by_qid: dict[str, dict] = {}
    for row in pre_rows or []:
        if not isinstance(row, dict):
            continue
        qid = _row_qid(row)
        if qid:
            pre_by_qid[qid] = row

    post_by_qid: dict[str, dict] = {}
    for row in post_rows or []:
        if not isinstance(row, dict):
            continue
        qid = _row_qid(row)
        if qid:
            post_by_qid[qid] = row

    candidate_failed = {str(q) for q in candidate_failed_qids or () if str(q)}

    out: dict[str, DeltaState] = {}
    for qid in targets:
        pre_row = pre_by_qid.get(qid)
        post_row = post_by_qid.get(qid)

        if pre_row is None and post_row is None:
            out[qid] = DeltaState.LOOKUP_FAILED
            continue

        if pre_row is not None and post_row is not None:
            pre_status = row_status(pre_row)
            post_status = row_status(post_row)
            if pre_status == "hard" and post_status != "hard":
                out[qid] = DeltaState.FIXED
            elif pre_status == "hard" and post_status == "hard":
                out[qid] = DeltaState.STILL_HARD
            elif pre_status == "soft" and post_status == "hard":
                out[qid] = DeltaState.SOFT_TO_HARD
            elif pre_status == "soft" and post_status != "hard":
                out[qid] = DeltaState.SOFT_PASSING
            else:
                out[qid] = DeltaState.SOFT_PASSING  # passing -> passing/soft
            continue

        if pre_row is not None and post_row is None:
            # Target present in baseline, absent from candidate. The
            # candidate_failed_qids list is the authoritative "did
            # this fail in the candidate" signal; absence from both
            # the failed list AND post_rows means the candidate
            # passed it via omission (most candidates don't emit a
            # row for trivially-passing QIDs).
            if qid in candidate_failed:
                # Listed as failed but no row materialised — predicate
                # disagreement. Route to the residual rather than
                # claiming FIXED.
                out[qid] = DeltaState.LOOKUP_FAILED
                continue
            pre_status = row_status(pre_row)
            if pre_status == "hard":
                out[qid] = DeltaState.FIXED
            elif pre_status == "soft":
                out[qid] = DeltaState.SOFT_PASSING
            else:
                out[qid] = DeltaState.SOFT_PASSING  # was passing, still passing
            continue

        # pre_row is None and post_row is not None -> target wasn't
        # in the baseline rows but appeared in the candidate eval.
        # If hard in candidate, it's an unattributable regression
        # for this target's slot.
        post_status = row_status(post_row) if post_row else "passing"
        if post_status == "hard":
            out[qid] = DeltaState.REGRESSED_TO_UNKNOWN
        else:
            out[qid] = DeltaState.LOOKUP_FAILED

    return out


def row_is_passing(row: dict) -> bool:
    """Return True when a row is neither a hard failure nor an actionable soft signal."""
    if not isinstance(row, dict):
        return False
    return not row_is_hard_failure(row) and not is_actionable_soft_signal_row(row)


def row_is_actionable_soft(row: dict) -> bool:
    """Return True when a row is an actionable soft-signal failure."""
    if not isinstance(row, dict):
        return False
    return is_actionable_soft_signal_row(row)


def row_status(row: dict) -> str:
    """Return ``"hard"``, ``"soft"``, or ``"passing"`` for a row."""
    if not isinstance(row, dict):
        return "passing"
    if row_is_hard_failure(row):
        return "hard"
    if row_is_actionable_soft(row):
        return "soft"
    return "passing"


def _arbiter_value(row: dict) -> str:
    return str(
        row.get("feedback/arbiter/value")
        or row.get("arbiter/value")
        or row.get("arbiter")
        or ""
    ).strip().lower()


def _result_correctness_value(row: dict) -> str:
    return str(
        row.get("feedback/result_correctness/value")
        or row.get("result_correctness/value")
        or row.get("result_correctness")
        or ""
    ).strip().lower()


def _cluster_qids(cluster: dict) -> set[str]:
    return {str(q) for q in cluster.get("question_ids", []) or [] if str(q)}


def uncovered_patchable_clusters(
    source_clusters: list[dict],
    action_groups: list[dict],
) -> list[dict]:
    """Return patchable hard clusters not covered by strategist output."""
    covered_cluster_ids: set[str] = set()
    covered_qids: set[str] = set()
    for ag in action_groups or []:
        covered_cluster_ids.update(
            str(cid) for cid in ag.get("source_cluster_ids", []) or [] if str(cid)
        )
        covered_qids.update(
            str(q) for q in ag.get("affected_questions", []) or [] if str(q)
        )

    uncovered: list[dict] = []
    for cluster in source_clusters or []:
        cid = str(cluster.get("cluster_id") or "")
        qids = _cluster_qids(cluster)
        if cid and cid in covered_cluster_ids:
            continue
        if qids and qids <= covered_qids:
            continue
        uncovered.append(cluster)
    return uncovered


_DIAGNOSTIC_AG_DIRECTIVES: dict[str, dict[str, str]] = {
    "plural_top_n_collapse":     {"lever": "5", "kind": "sql_shape"},
    "missing_temporal_filter":   {"lever": "5", "kind": "sql_shape"},
    "time_window_pivot":         {"lever": "5", "kind": "sql_shape"},
    "missing_filter":            {"lever": "6", "kind": "sql_snippet_filter"},
    "wrong_filter_condition":    {"lever": "6", "kind": "sql_snippet_filter"},
    "missing_scd_filter":        {"lever": "6", "kind": "sql_snippet_filter"},
    "wrong_aggregation":         {"lever": "5", "kind": "sql_shape"},
    "missing_aggregation":       {"lever": "5", "kind": "sql_shape"},
    "missing_dimension":         {"lever": "5", "kind": "sql_shape"},
    "wrong_grouping":            {"lever": "5", "kind": "sql_shape"},
    "missing_join_spec":         {"lever": "4", "kind": "join_specification"},
    "wrong_join_spec":           {"lever": "4", "kind": "join_specification"},
    "wrong_join":                {"lever": "4", "kind": "join_specification"},
    "wrong_join_type":           {"lever": "5", "kind": "sql_shape"},
    "column_disambiguation":     {"lever": "1", "kind": "column_metadata"},
    "format_difference":         {"lever": "5", "kind": "example_sql"},
}


def union_ag_levers_with_recommended(
    *,
    ag: dict,
    cluster: dict,
) -> dict:
    """Cycle 10 W2 — return a new AG dict whose ``lever_directives``
    contain at minimum every lever in ``cluster.recommended_levers``.

    The diagnostic directive's existing payload (kind, root_cause,
    guidance, target_qids) is preserved unchanged. Levers added from
    the cluster's recommendation that are not already present get a
    ``recommended_passthrough`` directive shape so the strategist
    sees them in the AG-emit slate without a hard guidance string.

    Cycle 10 W8 — the returned dict carries ``_levers_before_union``,
    a snapshot of the keys that existed before the union, so the AG-
    emit finalize site can decide whether to emit
    ``AG_LEVERS_UNIONED``.

    Pure: no I/O, no clock, no logger. Always returns a new dict
    (does not mutate input).
    """
    out = dict(ag or {})
    existing = dict((ag or {}).get("lever_directives") or {})
    existing_before_keys = list(existing.keys())
    rec_levers = list((cluster or {}).get("recommended_levers") or [])
    for lv in rec_levers:
        key = str(lv)
        if key not in existing:
            existing[key] = {
                "kind": "recommended_passthrough",
                "root_cause": str((cluster or {}).get("root_cause") or "unknown"),
                "guidance": (
                    "Cluster recommended this lever; consider it during "
                    "AG-emit even when no diagnostic directive applies."
                ),
                "target_qids": [
                    str(q) for q in (cluster or {}).get("question_ids", []) or []
                    if str(q)
                ],
            }
    out["lever_directives"] = existing
    out["_levers_before_union"] = existing_before_keys
    return out


def diagnostic_action_group_for_cluster(cluster: dict) -> dict:
    """Build a deterministic AG when the strategist omits a hard cluster.

    Dispatches the lever directive on the cluster's structured ``root_cause``
    rather than instruction-text substrings, so AG_COVERAGE AGs do not ship
    "do not collapse rank=1" bodies for column disambiguation clusters.
    """
    cid = str(cluster.get("cluster_id") or "H_UNKNOWN")
    qids = [str(q) for q in cluster.get("question_ids", []) or [] if str(q)]
    root = str(cluster.get("root_cause") or cluster.get("asi_failure_type") or "unknown")
    fixes = [
        str(f) for f in cluster.get("asi_counterfactual_fixes", []) or []
        if str(f)
    ]
    fix_text = (
        fixes[0] if fixes
        else "Use the cluster RCA evidence to produce a targeted metadata change."
    )
    lever_directives: dict[str, dict] = {}
    if root in _DIAGNOSTIC_AG_DIRECTIVES:
        spec = _DIAGNOSTIC_AG_DIRECTIVES[root]
        lever_directives[spec["lever"]] = {
            "kind": spec["kind"],
            "root_cause": root,
            "guidance": fix_text,
            "target_qids": qids,
        }
    # Cycle 5 T3 — coverage-gap AG marks itself as
    # ``needs_rca_regeneration`` when the cluster has no parent RCA
    # (the iter-2 H001/H002 case in run 2423b960). The harness routes
    # ``needs_rca_regeneration=True`` AGs to the regen branch when
    # GSO_DIAGNOSTIC_AG_RCA_REGEN is on.
    has_parent_rca = bool(cluster.get("rca_id"))
    base_ag = {
        "id": f"AG_COVERAGE_{cid}",
        "root_cause_summary": f"{root}: {fix_text}",
        "affected_questions": qids,
        "source_cluster_ids": [cid],
        "coverage_reason": "strategist_omitted_patchable_hard_cluster",
        "lever_directives": lever_directives,
        "ag_kind": "diagnostic" if has_parent_rca else "diagnostic_no_parent_rca",
        "needs_rca_regeneration": not has_parent_rca,
        "rca_id": str(cluster.get("rca_id") or ""),
        "primary_cluster_id": cid,
    }
    try:
        from genie_space_optimizer.common.config import (
            ag_levers_union_recommended_enabled,
        )
        if ag_levers_union_recommended_enabled():
            return union_ag_levers_with_recommended(
                ag=base_ag, cluster=cluster,
            )
    except Exception:
        # Cycle 10 W2: union failure is non-fatal — fall through to legacy.
        pass
    return base_ag


def compute_ag_stable_signature(
    ag: dict,
    clusters: Iterable[dict],
) -> tuple[tuple[str, ...], tuple[str, ...], str]:
    """Return a hashable signature stable across iterations.

    Track D (Phase A burn-down): buffered and diagnostic AGs are keyed
    by ``H00N`` cluster ids today, and those ids re-number every
    iteration. The May-01 ESR / 23:04 7Now runs reused buffered AGs
    against the wrong target as a result. The signature here is
    derived from properties that survive re-clustering:

    * ``cluster_signatures`` — stable hashes produced by the clusterer
      (e.g., ``plural_top_n_collapse|cat.sch.fact|cy_sales``).
    * ``qid_set`` — sorted tuple of qids the AG claims to fix; survives
      re-clustering even when ``cluster_id`` changes.
    * ``root_cause_family`` — first lever_directive's root cause if
      present, else empty.

    Returns ``(signatures, qids, root_cause)``. Hashable (tuple-of-tuples)
    so callers can use it as a dict key or store it in a set.
    """
    cluster_lookup = {
        str(c.get("cluster_id") or ""): str(c.get("cluster_signature") or "")
        for c in clusters or []
        if c.get("cluster_id")
    }
    src_ids = [str(cid) for cid in (ag.get("source_cluster_ids") or []) if str(cid)]
    sigs = tuple(
        dict.fromkeys(
            cluster_lookup.get(cid, "")
            for cid in src_ids
            if cluster_lookup.get(cid, "")
        )
    )
    qids = tuple(
        sorted(
            str(q) for q in (ag.get("affected_questions") or []) if str(q)
        )
    )
    root_cause = ""
    for lever_dir in (ag.get("lever_directives") or {}).values():
        if isinstance(lever_dir, dict):
            rc = str(lever_dir.get("root_cause") or "").strip()
            if rc:
                root_cause = rc
                break
    return (sigs, qids, root_cause)


def ag_root_cause_families(
    ag: dict,
    clusters: Iterable[dict],
) -> frozenset[str]:
    """Return the set of distinct root_cause values across the AG's
    source clusters.

    Track 4 (Phase A burn-down): an AG with two or more distinct
    families is heterogeneous and triggers the decomposition
    guardrail unless it carries a shared direct fix.
    """
    cluster_lookup = {
        str(c.get("cluster_id") or ""): str(c.get("root_cause") or "").strip()
        for c in clusters or []
        if c.get("cluster_id")
    }
    src_ids = [str(cid) for cid in (ag.get("source_cluster_ids") or []) if str(cid)]
    families = {
        cluster_lookup.get(cid, "")
        for cid in src_ids
    }
    families.discard("")
    return frozenset(families)


def ag_table_families(
    ag: dict,
    clusters: Iterable[dict],
) -> frozenset[str]:
    """Return the set of distinct blame-asset table names across the
    AG's source clusters.

    Track 4: an AG that spans unrelated tables is heterogeneous even
    when the root_cause family matches (e.g., two missing_filter
    clusters for two different tables compete for one cap slot).
    """
    cluster_lookup = {
        str(c.get("cluster_id") or ""): list(c.get("blame_assets") or [])
        for c in clusters or []
        if c.get("cluster_id")
    }
    src_ids = [str(cid) for cid in (ag.get("source_cluster_ids") or []) if str(cid)]
    tables: set[str] = set()
    for cid in src_ids:
        for asset in cluster_lookup.get(cid, []):
            asset_str = str(asset or "").strip()
            if asset_str:
                tables.add(asset_str)
    return frozenset(tables)


def ag_has_shared_direct_fix(
    ag: dict,
    clusters: Iterable[dict],
) -> bool:
    """Return True when the AG's patch bundle contains at least one
    direct-fix patch whose target_qids cover every source cluster's
    question_ids.

    Track 4: a heterogeneous multi-cluster AG is allowed when this
    predicate is True. Direct-fix here means a behavior-shape patch
    type with a non-empty root_cause — see
    ``patch_selection._is_direct_behavior_patch`` for the canonical
    definition. We duplicate the type set rather than importing
    patch_selection because control_plane already owns the diagnostic-
    AG dispatcher and we keep the dependency direction one-way.
    """
    direct_behavior_types = {
        "add_instruction",
        "update_instruction_section",
        "add_sql_snippet_filter",
        "add_sql_snippet_measure",
        "add_sql_snippet_calculation",
        "add_sql_snippet_expression",
        "add_example_sql",
    }
    behavior_root_causes = {
        "missing_filter",
        "wrong_filter_condition",
        "wrong_aggregation",
        "wrong_measure",
        "plural_top_n_collapse",
        "missing_temporal_filter",
        "time_window_pivot",
        "missing_aggregation",
        "missing_dimension",
        "wrong_grouping",
        "wrong_join_type",
    }

    cluster_lookup = {
        str(c.get("cluster_id") or ""): {
            str(q) for q in (c.get("question_ids") or []) if str(q)
        }
        for c in clusters or []
        if c.get("cluster_id")
    }
    src_ids = [str(cid) for cid in (ag.get("source_cluster_ids") or []) if str(cid)]
    cluster_qid_sets = [cluster_lookup.get(cid, set()) for cid in src_ids]

    for patch in ag.get("patches") or []:
        ptype = str(patch.get("type") or patch.get("patch_type") or "")
        root = str(patch.get("root_cause") or patch.get("rca_kind") or "").strip()
        if ptype not in direct_behavior_types or root not in behavior_root_causes:
            continue
        target_qids = {str(q) for q in (patch.get("target_qids") or []) if str(q)}
        if all(qid_set & target_qids for qid_set in cluster_qid_sets if qid_set):
            return True
    return False


def decompose_overbroad_ag(
    ag: dict,
    clusters: Iterable[dict],
) -> list[dict]:
    """Return either ``[ag]`` (unchanged) or per-cluster diagnostic AGs.

    Track 4 (Phase A burn-down): the decomposition guardrail. An AG
    is considered over-broad when ALL of these hold:

      * It spans two or more source clusters, AND
      * Those clusters span two or more root-cause families OR two or
        more table families, AND
      * The patch bundle has no single direct-fix patch covering every
        cluster (``ag_has_shared_direct_fix`` returns False).

    When over-broad, the AG is split into one diagnostic AG per source
    cluster — each new AG inherits the parent's metadata but scopes
    ``source_cluster_ids`` and ``affected_questions`` to its own
    cluster. Each new AG carries a stable signature stamped at
    construction (Track D) so the buffered-AG reuse path treats them
    as distinct entries.

    When not over-broad, the AG is returned unchanged in a single-
    element list. Callers can splice the result back into
    ``action_groups`` without distinguishing the two cases.
    """
    clusters_list = list(clusters or [])
    src_ids = [str(cid) for cid in (ag.get("source_cluster_ids") or []) if str(cid)]
    if len(src_ids) < 2:
        return [ag]

    families = ag_root_cause_families(ag, clusters_list)
    tables = ag_table_families(ag, clusters_list)
    if len(families) < 2 and len(tables) < 2:
        return [ag]

    if ag_has_shared_direct_fix(ag, clusters_list):
        return [ag]

    cluster_lookup = {
        str(c.get("cluster_id") or ""): c
        for c in clusters_list
        if c.get("cluster_id")
    }

    decomposed: list[dict] = []
    for cid in src_ids:
        cluster = cluster_lookup.get(cid)
        if not cluster:
            continue
        cluster_qids = [
            str(q) for q in (cluster.get("question_ids") or []) if str(q)
        ]
        # Build a per-cluster diagnostic AG using the same dispatcher
        # the strategist coverage-gap path uses, so the lever directive
        # is consistent with diagnostic AGs from any other source.
        new_ag = diagnostic_action_group_for_cluster(cluster)
        # Tag the decomposition so operators can trace the split.
        new_ag["id"] = f"AG_DECOMPOSED_{cid}"
        new_ag["coverage_reason"] = "decomposed_overbroad_parent_ag"
        new_ag["_decomposed_from"] = str(ag.get("id") or "")
        # Stamp the stable signature (Track D) so revalidation in
        # later iterations works consistently.
        new_ag["_stable_signature"] = compute_ag_stable_signature(
            new_ag, [cluster]
        )
        # Scope affected_questions to this cluster's qids only.
        new_ag["affected_questions"] = cluster_qids
        decomposed.append(new_ag)

    # Defensive fallback: if every src cluster missed the lookup,
    # return the original so we don't silently drop the AG.
    if not decomposed:
        return [ag]
    return decomposed


def patchable_hard_failure_qids(rows: Iterable[dict]) -> tuple[str, ...]:
    """Rows where GT is confirmed correct and Genie should be patched."""
    qids: list[str] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        if _result_correctness_value(row) not in {"no", "false", "0", "0.0"}:
            continue
        if _arbiter_value(row) != "ground_truth_correct":
            continue
        qid = _row_qid(row)
        if qid:
            qids.append(qid)
    return tuple(dict.fromkeys(qids))


def decide_quarantine_continuation(
    *,
    quarantined_qids: set[str],
    unresolved_patchable_qids: set[str],
    hard_cluster_count_after_prune: int,
    soft_cluster_count_after_prune: int,
) -> dict:
    """Decide whether quarantine may remove qids and continue the loop.

    Quarantine must not silently remove unresolved patchable hard failures
    while the lever loop pivots to soft signals. When the intersection of
    quarantined and unresolved-patchable is non-empty, the loop either stops
    for human review (no hard clusters remain) or carries those qids in a
    diagnostic lane (hard clusters remain).
    """
    blocking = sorted(
        str(q) for q in (quarantined_qids or set()) & (unresolved_patchable_qids or set())
    )
    if blocking and int(hard_cluster_count_after_prune or 0) == 0:
        return {
            "action": "stop_for_human_review",
            "reason": "quarantined_patchable_hard_failures",
            "blocking_qids": blocking,
        }
    if blocking:
        return {
            "action": "diagnostic_lane",
            "reason": "quarantined_patchable_hard_failures",
            "blocking_qids": blocking,
        }
    if int(hard_cluster_count_after_prune or 0) > 0:
        return {"action": "continue", "reason": "hard_clusters_remain", "blocking_qids": []}
    return {
        "action": "continue",
        "reason": "no_quarantined_patchable_hard_failures",
        "blocking_qids": [],
    }


def ambiguous_failure_qids(rows: Iterable[dict]) -> tuple[str, ...]:
    """Rows where neither answer is endorsed and benchmark review is safer."""
    qids: list[str] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        if _result_correctness_value(row) not in {"no", "false", "0", "0.0"}:
            continue
        if _arbiter_value(row) != "neither_correct":
            continue
        qid = _row_qid(row)
        if qid:
            qids.append(qid)
    return tuple(dict.fromkeys(qids))


@dataclass(frozen=True)
class ControlPlaneAcceptance:
    accepted: bool
    reason_code: str
    baseline_accuracy: float
    candidate_accuracy: float
    delta_pp: float
    target_qids: tuple[str, ...]
    target_fixed_qids: tuple[str, ...]
    target_still_hard_qids: tuple[str, ...]
    out_of_target_regressed_qids: tuple[str, ...]
    regression_debt_qids: tuple[str, ...] = ()
    protected_regressed_qids: tuple[str, ...] = ()
    soft_to_hard_regressed_qids: tuple[str, ...] = ()
    passing_to_hard_regressed_qids: tuple[str, ...] = ()
    # P1 — residual bucket: every out-of-target new-hard qid that
    # is NOT classified as soft_to_hard or passing_to_hard. Catches
    # missing-pre-row and predicate-disagreement cases that today
    # silently slip out of attribution.
    unknown_to_hard_regressed_qids: tuple[str, ...] = ()
    # Cycle 14-T0: per-target delta states, total over target_qids.
    # Sorted tuple of (qid, DeltaState.value) pairs — frozen,
    # JSON-friendly, byte-stable for MLflow replay.
    target_delta_states: tuple[tuple[str, str], ...] = ()
    # Cycle 14-W T1: SOFT_PASSING bucket for targets that improved
    # but didn't fully flip to PASSING. Surfaced as a first-class
    # render bucket in format_full_eval_marker_payload so postmortem
    # tools see soft signals alongside FIXED / STILL_HARD.
    target_soft_passing_qids: tuple[str, ...] = ()
    # Cycle 14-C T2: reattribution accounting for the
    # accepted_with_attribution_drift branch. Populated in T3 when
    # the branch fires; empty tuple in every other branch and on
    # legacy / pre-T2 replay fixtures.
    accidentally_improved_qids: tuple[str, ...] = ()
    unresolved_target_debt_qids: tuple[str, ...] = ()


def _fmt_qids(qids: Iterable[str]) -> str:
    values = tuple(str(q) for q in qids or () if str(q))
    return ", ".join(values) if values else "(none)"


def format_control_plane_acceptance_detail(
    decision: ControlPlaneAcceptance,
) -> str:
    """Return a compact operator-facing reason for control-plane rejection."""
    return (
        f"reason={decision.reason_code}; "
        f"target_qids={_fmt_qids(decision.target_qids)}; "
        f"target_fixed_qids={_fmt_qids(decision.target_fixed_qids)}; "
        f"target_still_hard_qids={_fmt_qids(decision.target_still_hard_qids)}; "
        f"out_of_target_regressed_qids={_fmt_qids(decision.out_of_target_regressed_qids)}; "
        f"regression_debt_qids={_fmt_qids(decision.regression_debt_qids)}; "
        f"protected_regressed_qids={_fmt_qids(decision.protected_regressed_qids)}; "
        f"soft_to_hard_regressed_qids={_fmt_qids(decision.soft_to_hard_regressed_qids)}; "
        f"passing_to_hard_regressed_qids={_fmt_qids(decision.passing_to_hard_regressed_qids)}; "
        f"unknown_to_hard_regressed_qids={_fmt_qids(decision.unknown_to_hard_regressed_qids)}"
    )


def format_full_eval_marker_payload(
    decision: ControlPlaneAcceptance,
    *,
    ag_id: str,
    iteration: int,
    accepted_label: str,
) -> dict:
    """Cycle 14-T2 — canonical render of one AG's full-eval outcome.

    Returns a JSON-serialisable dict consumed by:
      - ``GSO_FULL_EVAL_V1`` typed stdout marker
        (``run_analysis_contract.full_eval_marker``).
      - ``acceptance_decided`` ``DecisionRecord`` (decision_emitters
        ``ag_outcome_decision_record``).
      - ``FULL EVAL [...]`` human-readable print block (harness).
      - Phase B / Phase H rendered transcript (via the
        ``DecisionRecord`` above).

    Behind ``GSO_CANONICAL_ACCEPTANCE_RENDER`` (default on); on
    flag-off each surface continues to use its legacy renderer for
    byte-stable replay of pre-T2 fixtures.

    Cycle 14-V Task 3: when ``decision.target_delta_states`` is
    populated, the rendered ``target_fixed_qids`` /
    ``target_still_hard_qids`` are derived from it as the single
    canonical source. This eliminates the contradiction surfaced by
    7Now anchor 338386531912450 where ``gs_026`` simultaneously
    rendered as ``soft_to_hard`` (in delta_states) AND
    ``target_still_hard_qids``. Target QIDs are also subtracted
    from ``unknown_to_hard_regressed_qids`` because
    ``target_delta_states`` is exhaustive over targets — closes the
    airline anchor 833709971504406 ``gs_016`` mis-classification.

    Pre-T0 fixtures with empty ``target_delta_states`` fall through
    to legacy fields verbatim for back-compat.
    """
    delta_states_pairs = tuple(decision.target_delta_states or ())
    delta_states_list = [[str(qid), str(state)] for qid, state in delta_states_pairs]

    if delta_states_pairs:
        # Cycle 14-V T3: single source of truth.
        derived_fixed = tuple(
            str(q) for q, s in delta_states_pairs
            if str(s) == DeltaState.FIXED.value
        )
        derived_still_hard = tuple(
            str(q) for q, s in delta_states_pairs
            if str(s) == DeltaState.STILL_HARD.value
        )
        # Cycle 14-W T1: derive target_soft_passing_qids from
        # target_delta_states so SOFT_PASSING targets have a
        # first-class bucket field rather than being silently
        # absent from every legacy bucket.
        derived_soft_passing = tuple(
            str(q) for q, s in delta_states_pairs
            if str(s) == DeltaState.SOFT_PASSING.value
        )
        target_fixed_qids = derived_fixed
        target_still_hard_qids = derived_still_hard
        target_soft_passing_qids = derived_soft_passing

        # Subtract target QIDs from the unknown_to_hard bucket
        # because target QIDs are exhaustively classified by
        # target_delta_states.
        target_qid_set = {str(q) for q, _ in delta_states_pairs}
        unknown_to_hard_qids = tuple(
            str(q) for q in (decision.unknown_to_hard_regressed_qids or ())
            if str(q) not in target_qid_set
        )
    else:
        # Pre-T0 back-compat: legacy fields verbatim.
        target_fixed_qids = tuple(
            str(q) for q in (decision.target_fixed_qids or ())
        )
        target_still_hard_qids = tuple(
            str(q) for q in (decision.target_still_hard_qids or ())
        )
        # Cycle 14-W T1: legacy fixtures don't carry a
        # target_soft_passing_qids field; default to empty so the
        # payload key is always present.
        target_soft_passing_qids = tuple(
            str(q)
            for q in (getattr(decision, "target_soft_passing_qids", ()) or ())
        )
        unknown_to_hard_qids = tuple(
            str(q) for q in (decision.unknown_to_hard_regressed_qids or ())
        )

    payload = {
        "iteration": int(iteration),
        "ag_id": str(ag_id),
        "accepted": bool(decision.accepted),
        "reason_code": str(decision.reason_code or ""),
        "accepted_label": str(accepted_label),
        "baseline_accuracy": float(decision.baseline_accuracy),
        "candidate_accuracy": float(decision.candidate_accuracy),
        "delta_pp": float(decision.delta_pp),
        "target_qids": [str(q) for q in (decision.target_qids or ())],
        "target_fixed_qids": list(target_fixed_qids),
        "target_still_hard_qids": list(target_still_hard_qids),
        "target_soft_passing_qids": list(target_soft_passing_qids),
        "target_delta_states": delta_states_list,
        "out_of_target_regressed_qids": [
            str(q) for q in (decision.out_of_target_regressed_qids or ())
        ],
        "regression_debt_qids": [
            str(q) for q in (decision.regression_debt_qids or ())
        ],
        "soft_to_hard_regressed_qids": [
            str(q) for q in (decision.soft_to_hard_regressed_qids or ())
        ],
        "passing_to_hard_regressed_qids": [
            str(q) for q in (decision.passing_to_hard_regressed_qids or ())
        ],
        "unknown_to_hard_regressed_qids": list(unknown_to_hard_qids),
        "accidentally_improved_qids": [
            str(q)
            for q in (
                getattr(decision, "accidentally_improved_qids", ()) or ()
            )
        ],
        "unresolved_target_debt_qids": [
            str(q)
            for q in (
                getattr(decision, "unresolved_target_debt_qids", ()) or ()
            )
        ],
        "reason_detail": format_control_plane_acceptance_detail(decision),
    }

    # Cycle 14-V Task 4: self-check the rendered payload for same-QID
    # contradictions. Silent on clean payloads; emits a typed alarm
    # marker on regression. The check runs behind a default-on flag
    # so flag-off (replay byte-stability) keeps the legacy silence.
    try:
        from genie_space_optimizer.common.config import (
            canonical_render_invariant_enabled,
        )
        if canonical_render_invariant_enabled():
            from genie_space_optimizer.optimization.run_analysis_contract import (
                canonical_render_invariant_marker,
            )
            for violation in _detect_render_contradictions(payload):
                print(canonical_render_invariant_marker(
                    optimization_run_id="",
                    iteration=int(iteration),
                    ag_id=str(ag_id),
                    violation_class=str(violation.get("class") or ""),
                    contradicting_qids=tuple(violation.get("qids") or ()),
                    detail=str(violation.get("detail") or ""),
                ))
    except Exception:
        # Defensive: invariant self-check must never crash the render.
        pass

    return payload


def _detect_render_contradictions(payload: dict) -> list[dict]:
    """Cycle 14-V Task 4 — pure helper that returns one violation
    per same-QID contradiction across rendered fields. Returns []
    on clean payloads.

    Detected violations:
      - ``fixed_and_still_hard_overlap`` — a QID appears in BOTH
        ``target_fixed_qids`` AND ``target_still_hard_qids``.
      - ``target_in_out_of_target_set`` — a QID appears in BOTH
        ``target_delta_states`` AND ``out_of_target_regressed_qids``.
      - ``delta_state_disagrees_with_bucket`` — ``target_delta_states``
        classifies a QID as FIXED but the QID is also in
        ``target_still_hard_qids`` (and vice versa).
    """
    violations: list[dict] = []
    fixed = set(payload.get("target_fixed_qids") or ())
    still_hard = set(payload.get("target_still_hard_qids") or ())
    # Cycle 14-W T1: SOFT_PASSING is now a first-class bucket field;
    # check pairwise overlap across {fixed, still_hard, soft_passing}.
    soft_passing = set(payload.get("target_soft_passing_qids") or ())
    delta_pairs = payload.get("target_delta_states") or ()
    out_of_target = set(payload.get("out_of_target_regressed_qids") or ())

    overlap = fixed & still_hard
    if overlap:
        violations.append({
            "class": "fixed_and_still_hard_overlap",
            "qids": sorted(overlap),
            "detail": (
                f"qids appear in both target_fixed_qids and "
                f"target_still_hard_qids: {sorted(overlap)}"
            ),
        })

    # Cycle 14-W T1: detect overlaps involving the new
    # ``soft_passing`` bucket. The existing ``fixed_and_still_hard_overlap``
    # class above already covers the {fixed, still_hard} pair; this
    # generic class extends coverage to any pair involving
    # ``soft_passing`` without double-counting the legacy case.
    soft_passing_pairs = (
        ("fixed", fixed, "soft_passing", soft_passing),
        ("still_hard", still_hard, "soft_passing", soft_passing),
    )
    for name_a, set_a, name_b, set_b in soft_passing_pairs:
        bucket_overlap = set_a & set_b
        if bucket_overlap:
            violations.append({
                "class": "qid_in_multiple_state_buckets",
                "qids": tuple(sorted(bucket_overlap)),
                "detail": (
                    f"qids appear in both target_{name_a}_qids "
                    f"and target_{name_b}_qids: "
                    f"{sorted(bucket_overlap)}"
                ),
            })

    target_qids_in_delta = {str(p[0]) for p in delta_pairs if p}
    target_in_out = target_qids_in_delta & out_of_target
    if target_in_out:
        violations.append({
            "class": "target_in_out_of_target_set",
            "qids": sorted(target_in_out),
            "detail": (
                f"qids classified by target_delta_states must not also "
                f"appear in out_of_target_regressed_qids: "
                f"{sorted(target_in_out)}"
            ),
        })

    delta_dict = {str(p[0]): str(p[1]) for p in delta_pairs if p}
    disagree = []
    for qid, state in delta_dict.items():
        if state == DeltaState.FIXED.value and qid in still_hard:
            disagree.append((qid, state, "still_hard_qids"))
        elif state == DeltaState.STILL_HARD.value and qid in fixed:
            disagree.append((qid, state, "fixed_qids"))
    if disagree:
        violations.append({
            "class": "delta_state_disagrees_with_bucket",
            "qids": sorted({q for q, _, _ in disagree}),
            "detail": (
                f"target_delta_states disagrees with legacy bucket "
                f"membership: {disagree}"
            ),
        })

    return violations


def assert_regression_debt_partition_complete(
    decision: ControlPlaneAcceptance,
) -> None:
    """P1 invariant — out_of_target_regressed_qids must equal the
    disjoint union of soft_to_hard / passing_to_hard / unknown_to_hard.

    Disabled when ``GSO_REGRESSION_DEBT_INVARIANT=0``. Default ON.
    """
    from genie_space_optimizer.common.config import (
        regression_debt_invariant_enabled,
    )

    if not regression_debt_invariant_enabled():
        return

    out_of_target = {str(q) for q in decision.out_of_target_regressed_qids if str(q)}
    soft = {str(q) for q in decision.soft_to_hard_regressed_qids if str(q)}
    passing = {str(q) for q in decision.passing_to_hard_regressed_qids if str(q)}
    unknown = {str(q) for q in decision.unknown_to_hard_regressed_qids if str(q)}

    union = soft | passing | unknown
    missing = sorted(out_of_target - union)
    if missing:
        raise AssertionError(
            f"regression-debt partition incomplete: out_of_target qids "
            f"{missing} are not in soft_to_hard / passing_to_hard / "
            f"unknown_to_hard. Bucket attribution silently dropped these."
        )

    overlaps = sorted(
        (soft & passing) | (soft & unknown) | (passing & unknown)
    )
    if overlaps:
        raise AssertionError(
            f"regression-debt partition not disjoint: qids {overlaps} "
            f"appear in multiple sub-buckets simultaneously."
        )


def decide_control_plane_acceptance(
    *,
    baseline_accuracy: float,
    candidate_accuracy: float,
    target_qids: Iterable[str],
    pre_rows: Iterable[dict],
    post_rows: Iterable[dict],
    min_gain_pp: float = 0.0,
    max_new_hard_regressions: int = 1,
    max_new_passing_to_hard_regressions: int | None = None,
    protected_qids: Iterable[str] = (),
    baseline_pre_arbiter_accuracy: float | None = None,
    candidate_pre_arbiter_accuracy: float | None = None,
    min_pre_arbiter_gain_pp: float = 2.0,
    thresholds_met: bool = True,
    # Cycle 14B-T2 — partial-harvest with debt
    cumulative_debt: int = 0,
    threshold_pass_rate: float = 1.0,
) -> ControlPlaneAcceptance:
    """Accept only causal post-arbiter improvement with no hard regressions.

    Reason codes:
      missing_target_qids               — strategist did not declare causal targets
      rejected_missing_causal_target    — alias for missing_target_qids
      missing_pre_rows                  — gate was given an empty baseline
      stale_or_candidate_pre_rows       — pre rows are not the accepted baseline
      post_arbiter_not_improved         — global accuracy did not move
      rejected_no_gain                  — gain below min_gain_pp threshold
      target_fixed_offset_by_regression — Cycle 5 T4: target fixed but a
        non-target qid moved from passing/soft to hard, net delta ≤ 0
      target_fixed_with_unresolved_other_hard — Cycle 5 T4: target
        fixed but pre-existing non-target hard qids remain hard, net
        delta ≤ 0
      target_qids_not_improved          — none of the declared causal targets flipped
      accepted_pre_arbiter_improvement  — post saturated at the same value but pre-arbiter improved by >= min_pre_arbiter_gain_pp with no collateral hard regression
      accepted_with_attribution_drift   — net global gain, zero regressions, target unchanged
      accepted_with_regression_debt     — net gain with bounded collateral debt
      accepted_with_partial_harvest_debt — Cycle 14B-T2: candidate fixed
        >= policy.min_target_clusters_fixed targets AND cleared the
        policy's aggregate-gain / threshold / bucket / cumulative
        gates with bounded debt. Behind GSO_PARTIAL_HARVEST_WITH_DEBT.
      out_of_target_hard_regression     — at least one prior-passing qid went hard
      rejected_unbounded_collateral     — collateral exceeds debt budget
      accepted                          — net causal win, no collateral regressions

    The pre-arbiter branch fires only when callers pass both
    ``baseline_pre_arbiter_accuracy`` and ``candidate_pre_arbiter_accuracy``,
    AND ``min_gain_pp == 0.0`` (so the caller is in saturation mode rather
    than enforcing an explicit positive post-arbiter gain). It still
    requires zero out-of-target hard regressions, zero soft-to-hard
    moves, and zero passing-to-hard moves on the broader pre-arbiter
    surface, mirroring the existing collateral-regression protections.
    """
    pre_rows_list = list(pre_rows or [])
    post_rows_list = list(post_rows or [])
    targets = tuple(dict.fromkeys(str(q) for q in target_qids or [] if str(q)))
    pre_hard = set(hard_failure_qids(pre_rows_list))
    post_hard = set(hard_failure_qids(post_rows_list))
    target_set = set(targets)
    target_fixed = tuple(sorted((pre_hard & target_set) - post_hard))
    target_still = tuple(sorted(post_hard & target_set))
    out_of_target_regressed = tuple(sorted((post_hard - pre_hard) - target_set))
    delta = round(float(candidate_accuracy) - float(baseline_accuracy), 1)

    fixed_count = len(target_fixed)
    regression_count = len(out_of_target_regressed)
    protected_set = {str(q) for q in protected_qids or () if str(q)}
    protected_regressed = tuple(
        q for q in out_of_target_regressed if q in protected_set
    )
    pre_by_qid = {
        str(row.get("question_id") or row.get("id") or ""): row
        for row in pre_rows_list
        if isinstance(row, dict)
    }
    # P1 — partition out_of_target_regressed into three disjoint
    # buckets. ``row_status`` returns "passing" for a missing pre_row,
    # so we explicitly distinguish "no pre_row at all" (unknown) from
    # "pre_row exists and was passing".
    soft_to_hard_list: list[str] = []
    passing_to_hard_list: list[str] = []
    unknown_to_hard_list: list[str] = []
    for q in out_of_target_regressed:
        pre_row = pre_by_qid.get(q)
        if pre_row is None:
            unknown_to_hard_list.append(q)
            continue
        status = row_status(pre_row)
        if status == "soft":
            soft_to_hard_list.append(q)
        elif status == "passing":
            passing_to_hard_list.append(q)
        else:
            # row_status returned "hard" - the pre_row says hard but
            # hard_failure_qids said not pre-hard. Predicate
            # disagreement; route to the residual.
            unknown_to_hard_list.append(q)
    soft_to_hard = tuple(soft_to_hard_list)
    passing_to_hard = tuple(passing_to_hard_list)
    unknown_to_hard = tuple(unknown_to_hard_list)
    has_gain = delta >= float(min_gain_pp) and delta > 0
    has_causal_fix = bool(target_fixed)
    # Task 7 — when callers do not specify a tighter passing-to-hard cap,
    # default to the overall ``max_new_hard_regressions`` budget. This
    # prevents a single passing-to-hard regression from rejecting a
    # net-positive AG that fixed its declared causal target.
    if max_new_passing_to_hard_regressions is None:
        effective_passing_to_hard_budget = int(max_new_hard_regressions)
    else:
        effective_passing_to_hard_budget = int(max_new_passing_to_hard_regressions)
    collateral_bounded = (
        regression_count <= int(max_new_hard_regressions)
        and len(passing_to_hard) <= effective_passing_to_hard_budget
        and regression_count <= max(fixed_count, 1)
        and not protected_regressed
    )

    if not targets:
        reason = "missing_target_qids"
        accepted = False
    elif not pre_rows_list:
        reason = "missing_pre_rows"
        accepted = False
    elif (
        post_rows_list
        and pre_hard == post_hard
        and delta != 0.0
    ):
        reason = "stale_or_candidate_pre_rows"
        accepted = False
        target_fixed = ()
        target_still = ()
        out_of_target_regressed = ()
    elif not has_gain:
        # PR-E: pre-arbiter secondary signal. Saturation-mode acceptance
        # (no caller-set min_gain_pp) yields to a positive pre-arbiter
        # delta when collateral regressions are zero on every axis.
        pre_arbiter_supplied = (
            baseline_pre_arbiter_accuracy is not None
            and candidate_pre_arbiter_accuracy is not None
        )
        in_saturation_mode = float(min_gain_pp) <= 0.0
        if pre_arbiter_supplied and in_saturation_mode:
            pre_delta = round(
                float(candidate_pre_arbiter_accuracy)
                - float(baseline_pre_arbiter_accuracy),
                1,
            )
            collateral_clear = (
                not out_of_target_regressed
                and not protected_regressed
                and not soft_to_hard
                and not passing_to_hard
            )
            if (
                pre_delta >= float(min_pre_arbiter_gain_pp)
                and collateral_clear
            ):
                reason = "accepted_pre_arbiter_improvement"
                accepted = True
            else:
                # Cycle 5 T4 — granular reason codes when accuracy
                # didn't move. Distinguish target-fixed-with-regression
                # from target-fixed-with-unresolved from the legacy
                # post_arbiter_not_improved (no target fix at all).
                if has_causal_fix and (
                    soft_to_hard or passing_to_hard or unknown_to_hard
                ):
                    reason = "target_fixed_offset_by_regression"
                elif has_causal_fix and (post_hard - target_set):
                    reason = "target_fixed_with_unresolved_other_hard"
                else:
                    reason = "post_arbiter_not_improved"
                accepted = False
        else:
            # Cycle 5 T4 — same granularity in the non-saturation path.
            if has_causal_fix and (
                soft_to_hard or passing_to_hard or unknown_to_hard
            ):
                reason = "target_fixed_offset_by_regression"
            elif has_causal_fix and (post_hard - target_set):
                reason = "target_fixed_with_unresolved_other_hard"
            else:
                reason = (
                    "rejected_no_gain"
                    if float(min_gain_pp) > 0
                    else "post_arbiter_not_improved"
                )
            accepted = False
    elif (
        not has_causal_fix
        and not out_of_target_regressed
        and not protected_regressed
        and not soft_to_hard
        and not passing_to_hard
    ):
        # Track F (Phase A burn-down MVP): a candidate that improves
        # overall accuracy with zero regressions on every budget axis
        # must accept even when the named target qid did not specifically
        # move. The rationale is that the optimizer's RCA, clustering,
        # cap, applier, and rollback all worked; the only reason the
        # candidate looks "wrong" is attribution drift between the
        # named target qid set and the qids that actually flipped.
        #
        # Optimizer Control-Plane Hardening Plan — Task A: when below
        # thresholds, attribution drift is no longer a free pass. The
        # caller passes ``thresholds_met=False`` to require the named
        # target qid to actually move; legacy default
        # ``thresholds_met=True`` preserves the prior behaviour.
        if thresholds_met:
            reason = "accepted_with_attribution_drift"
            accepted = True
        else:
            reason = "rejected_below_threshold_no_target_progress"
            accepted = False
            target_fixed = ()
    elif not has_causal_fix:
        reason = "target_qids_not_improved"
        accepted = False
    elif out_of_target_regressed and collateral_bounded:
        reason = "accepted_with_regression_debt"
        accepted = True
    elif out_of_target_regressed:
        reason = "rejected_unbounded_collateral"
        accepted = False
    else:
        reason = "accepted"
        accepted = True

    # Cycle 14-T0: total per-target classification (consumed by
    # T2 render, C14B partial-harvest policy, C16-T3 enum
    # extension). Independent of the existing target_fixed /
    # target_still set arithmetic so this is byte-stable: the
    # legacy fields stay populated as before; the new field adds
    # information without removing any.
    delta_state_map = compute_target_delta_states(
        target_qids=targets,
        pre_rows=pre_rows_list,
        post_rows=post_rows_list,
        candidate_failed_qids=hard_failure_qids(post_rows_list),
    )

    # Cycle 14-T0: when any target landed in LOOKUP_FAILED, the
    # legacy reason codes (missing_pre_rows, target_qids_not_improved)
    # under-describe the failure. Route to the typed
    # target_resolution_failed reason behind the GSO_TARGET_DELTA_STRICT
    # flag. Only flips the reason on rejections; never flips an
    # accepted decision. The allowlist preserves more-specific
    # misconfig reasons (stale_or_candidate_pre_rows, collateral
    # codes) so they remain distinct in postmortems.
    from genie_space_optimizer.common.config import target_delta_strict_enabled

    _OVERRIDABLE_REASONS = {"missing_pre_rows", "target_qids_not_improved"}
    has_lookup_failure = any(
        state == DeltaState.LOOKUP_FAILED for state in delta_state_map.values()
    )
    if (
        has_lookup_failure
        and not accepted
        and reason in _OVERRIDABLE_REASONS
        and target_delta_strict_enabled()
    ):
        reason = "target_resolution_failed"

    target_delta_states_tuple = tuple(
        sorted((qid, state.value) for qid, state in delta_state_map.items())
    )

    # Cycle 14B-T2: partial-harvest with bounded debt. The branch
    # fires on rejections the legacy code routed to
    # rejected_unbounded_collateral / target_qids_not_improved /
    # target_fixed_offset_by_regression — when a
    # RegressionDebtPolicy says the candidate is under-policy, override
    # the rejection with accepted_with_partial_harvest_debt. Never
    # flips an already-accepted decision; never fires when there is
    # no out-of-target debt to harvest (the legacy ``accepted`` reason
    # is correct in that case).
    from genie_space_optimizer.common.config import (
        partial_harvest_with_debt_enabled,
    )

    if not accepted and partial_harvest_with_debt_enabled():
        from genie_space_optimizer.optimization.acceptance_policy import (
            regression_debt_policy_from_config,
        )

        synthetic = ControlPlaneAcceptance(
            accepted=False,
            reason_code=reason,
            baseline_accuracy=round(float(baseline_accuracy), 1),
            candidate_accuracy=round(float(candidate_accuracy), 1),
            delta_pp=delta,
            target_qids=targets,
            target_fixed_qids=target_fixed,
            target_still_hard_qids=target_still,
            out_of_target_regressed_qids=out_of_target_regressed,
            regression_debt_qids=(),
            protected_regressed_qids=protected_regressed,
            soft_to_hard_regressed_qids=soft_to_hard,
            passing_to_hard_regressed_qids=passing_to_hard,
            unknown_to_hard_regressed_qids=unknown_to_hard,
            target_delta_states=target_delta_states_tuple,
        )
        verdict = evaluate_regression_debt(
            decision=synthetic,
            policy=regression_debt_policy_from_config(),
            cumulative_debt=int(cumulative_debt),
            threshold_pass_rate=float(threshold_pass_rate),
        )
        if verdict.under_policy and verdict.debt_qids:
            reason = "accepted_with_partial_harvest_debt"
            accepted = True
            out_of_target_regressed = verdict.debt_qids

    regression_debt_qids = (
        out_of_target_regressed if accepted and out_of_target_regressed else ()
    )

    # Cycle 14-C T3: when the accepted_with_attribution_drift branch
    # fires, populate accidentally_improved_qids and
    # unresolved_target_debt_qids so the next iteration's strategist
    # sees the correct attribution. Behind the default-on circuit-
    # breaker GSO_ATTRIBUTION_DRIFT_REATTRIBUTION; flag-off restores
    # byte-identical pre-14-C output on this branch.
    if (
        reason == "accepted_with_attribution_drift"
        and attribution_drift_reattribution_enabled()
    ):
        accidentally_improved_qids = compute_accidentally_improved_qids(
            pre_rows=pre_rows_list,
            post_rows=post_rows_list,
            target_qids=targets,
        )
        unresolved_target_debt_qids = tuple(targets)
    else:
        accidentally_improved_qids = ()
        unresolved_target_debt_qids = ()

    return ControlPlaneAcceptance(
        accepted=accepted,
        reason_code=reason,
        baseline_accuracy=round(float(baseline_accuracy), 1),
        candidate_accuracy=round(float(candidate_accuracy), 1),
        delta_pp=delta,
        target_qids=targets,
        target_fixed_qids=target_fixed,
        target_still_hard_qids=target_still,
        out_of_target_regressed_qids=out_of_target_regressed,
        regression_debt_qids=regression_debt_qids,
        protected_regressed_qids=protected_regressed,
        soft_to_hard_regressed_qids=soft_to_hard,
        passing_to_hard_regressed_qids=passing_to_hard,
        unknown_to_hard_regressed_qids=unknown_to_hard,
        target_delta_states=target_delta_states_tuple,
        accidentally_improved_qids=accidentally_improved_qids,
        unresolved_target_debt_qids=unresolved_target_debt_qids,
    )


# ── Cycle 14B-T1: evaluate_regression_debt — pure policy evaluator ───


@dataclass(frozen=True)
class RegressionDebtVerdict:
    """Result of evaluating a ControlPlaneAcceptance against a
    RegressionDebtPolicy.

    ``under_policy=True`` means the candidate satisfies every gate
    in the policy and the partial-harvest branch in
    ``decide_control_plane_acceptance`` should accept-with-debt.
    ``under_policy=False`` reasons:

      no_target_clusters_fixed         — zero FIXED targets
      aggregate_gain_below_floor       — delta_pp < min_aggregate_improvement_pp
      debt_exceeds_per_iter_max        — len(debt_qids) > max_debt_qids
      debt_bucket_disallowed           — debt qid lands outside allowed_debt_buckets
      cumulative_debt_cap_hit          — cumulative_debt + len(debt_qids) > cumulative_debt_max
      threshold_pass_rate_below_floor  — threshold_pass_rate < min_threshold_pass_rate
      no_debt_present                  — under_policy=True but with empty debt
                                         (the legacy `accepted` reason is correct
                                         here; partial-harvest does not apply)
    """

    under_policy: bool
    reason_code: str
    debt_qids: tuple[str, ...]
    policy_diagnostics: dict


def evaluate_regression_debt(
    *,
    decision: ControlPlaneAcceptance,
    policy: "Any",
    cumulative_debt: int = 0,
    threshold_pass_rate: float = 1.0,
) -> RegressionDebtVerdict:
    """Evaluate a ControlPlaneAcceptance against a debt policy.

    Pure: no I/O, no globals, no side effects. Suitable for unit
    tests on synthetic inputs.

    Order of gates (first-fail wins so reason_code is deterministic):
      1. min_target_clusters_fixed
      2. min_aggregate_improvement_pp
      3. min_threshold_pass_rate
      4. max_debt_qids (per-iteration cap)
      5. allowed_debt_buckets (every debt qid must land in an allowed bucket)
      6. cumulative_debt_max (running total cap)

    ``policy`` is typed as ``Any`` (with the runtime expectation of
    ``RegressionDebtPolicy``) to avoid a circular import: the policy
    module imports ``DeltaState`` from this module.
    """
    delta_states = dict(decision.target_delta_states or ())
    target_fixed_count = (
        sum(1 for s in delta_states.values() if s == DeltaState.FIXED.value)
        if delta_states
        else len(decision.target_fixed_qids)
    )

    debt_qids = tuple(decision.out_of_target_regressed_qids or ())
    debt_count = len(debt_qids)
    aggregate_gain = float(decision.delta_pp)

    diagnostics: dict = {
        "debt_count": debt_count,
        "debt_count_max": int(policy.max_debt_qids),
        "aggregate_gain_pp": aggregate_gain,
        "aggregate_gain_floor_pp": float(policy.min_aggregate_improvement_pp),
        "target_clusters_fixed": target_fixed_count,
        "target_clusters_fixed_min": int(policy.min_target_clusters_fixed),
        "cumulative_debt_used": int(cumulative_debt),
        "cumulative_debt_max": int(policy.cumulative_debt_max),
        "threshold_pass_rate": float(threshold_pass_rate),
        "threshold_pass_rate_min": float(policy.min_threshold_pass_rate),
    }

    if target_fixed_count < int(policy.min_target_clusters_fixed):
        return RegressionDebtVerdict(
            under_policy=False,
            reason_code="no_target_clusters_fixed",
            debt_qids=debt_qids,
            policy_diagnostics=diagnostics,
        )

    if aggregate_gain < float(policy.min_aggregate_improvement_pp):
        return RegressionDebtVerdict(
            under_policy=False,
            reason_code="aggregate_gain_below_floor",
            debt_qids=debt_qids,
            policy_diagnostics=diagnostics,
        )

    if float(threshold_pass_rate) < float(policy.min_threshold_pass_rate):
        return RegressionDebtVerdict(
            under_policy=False,
            reason_code="threshold_pass_rate_below_floor",
            debt_qids=debt_qids,
            policy_diagnostics=diagnostics,
        )

    if debt_count > int(policy.max_debt_qids):
        return RegressionDebtVerdict(
            under_policy=False,
            reason_code="debt_exceeds_per_iter_max",
            debt_qids=debt_qids,
            policy_diagnostics=diagnostics,
        )

    # Bucket admissibility: every debt qid's delta state must be in
    # allowed_debt_buckets. The legacy fields soft_to_hard /
    # passing_to_hard / unknown_to_hard are the source. Map each to
    # the closest DeltaState value; passing_to_hard has no exact
    # match in the current DeltaState (C16-T3 will extend) so it
    # routes to REGRESSED_TO_UNKNOWN — disallowed by default.
    bucket_for_qid: dict[str, DeltaState] = {}
    for q in decision.soft_to_hard_regressed_qids or ():
        bucket_for_qid[q] = DeltaState.SOFT_TO_HARD
    for q in decision.passing_to_hard_regressed_qids or ():
        bucket_for_qid[q] = DeltaState.REGRESSED_TO_UNKNOWN
    for q in decision.unknown_to_hard_regressed_qids or ():
        bucket_for_qid[q] = DeltaState.LOOKUP_FAILED

    disallowed = [
        q
        for q in debt_qids
        if bucket_for_qid.get(q, DeltaState.REGRESSED_TO_UNKNOWN)
        not in policy.allowed_debt_buckets
    ]
    if disallowed:
        diagnostics["disallowed_debt_qids"] = sorted(disallowed)
        return RegressionDebtVerdict(
            under_policy=False,
            reason_code="debt_bucket_disallowed",
            debt_qids=debt_qids,
            policy_diagnostics=diagnostics,
        )

    if int(cumulative_debt) + debt_count > int(policy.cumulative_debt_max):
        return RegressionDebtVerdict(
            under_policy=False,
            reason_code="cumulative_debt_cap_hit",
            debt_qids=debt_qids,
            policy_diagnostics=diagnostics,
        )

    if debt_count == 0:
        # No debt to harvest — partial-harvest branch should not fire.
        # The caller (decide_control_plane_acceptance) keeps the
        # legacy `accepted` reason in this case.
        return RegressionDebtVerdict(
            under_policy=True,
            reason_code="no_debt_present",
            debt_qids=(),
            policy_diagnostics=diagnostics,
        )

    return RegressionDebtVerdict(
        under_policy=True,
        reason_code="accepted_with_partial_harvest_debt",
        debt_qids=debt_qids,
        policy_diagnostics=diagnostics,
    )


@dataclass(frozen=True)
class PreArbiterRegressionDecision:
    accepted: bool
    reason_code: str
    delta_pp: float


def decide_pre_arbiter_regression_guardrail(
    *,
    baseline_pre_arbiter_accuracy: float,
    candidate_pre_arbiter_accuracy: float,
    target_fixed_qids: tuple[str, ...],
    max_pre_arbiter_regression_pp: float = 5.0,
) -> PreArbiterRegressionDecision:
    """Reject candidates that drop broad pre-arbiter accuracy without fixing any target.

    A target fix is sufficient cause to accept the candidate (the regression
    budget is in service of letting hard targets land). Without one, drops
    larger than ``max_pre_arbiter_regression_pp`` are blocked so a wide
    instruction edit cannot trade healthy questions for nothing.
    """
    delta = round(
        float(candidate_pre_arbiter_accuracy) - float(baseline_pre_arbiter_accuracy),
        1,
    )
    if target_fixed_qids:
        return PreArbiterRegressionDecision(True, "target_fixed", delta)
    if delta <= -abs(float(max_pre_arbiter_regression_pp)):
        return PreArbiterRegressionDecision(
            False,
            "pre_arbiter_regression_without_target_fix",
            delta,
        )
    return PreArbiterRegressionDecision(True, "within_pre_arbiter_regression_budget", delta)


def select_control_plane_baseline_rows(
    *,
    latest_state_iteration: dict | None,
    latest_full_iteration: dict | None,
) -> tuple[list[dict], str]:
    """Pick baseline rows for the pre-arbiter regression guardrail.

    Clustering reads ``load_latest_state_iteration`` (eval_scope ∈ {full,
    enrichment}). The control-plane guardrail must use the same source
    so a candidate is not flagged a regression against a stale
    pre-enrichment baseline.

    Returns ``(rows, eval_scope)`` where ``eval_scope`` is one of
    ``"full"``, ``"enrichment"``, or ``"unknown"``.
    """
    state = latest_state_iteration or {}
    state_rows = list(state.get("rows") or [])
    if state_rows:
        return state_rows, str(state.get("eval_scope") or "full")
    full = latest_full_iteration or {}
    full_rows = list(full.get("rows") or [])
    if full_rows:
        return full_rows, str(full.get("eval_scope") or "full")
    return [], "unknown"


def assert_quarantine_attribution_sound(
    *,
    quarantined_qids: Iterable[str],
    currently_passing_qids: Iterable[str],
    currently_hard_qids: Iterable[str],
) -> None:
    """Track H — fail loud on quarantine attribution drift.

    Two invariants:
      1. No currently-passing qid may appear in the quarantine list.
      2. When the live hard set has size 1, that qid cannot be quarantined
         (singleton-hard floor — quarantine is for *recurring* failure,
         not for the only remaining target).
    """
    quarantined = {str(q) for q in quarantined_qids if str(q)}
    passing = {str(q) for q in currently_passing_qids if str(q)}
    hard = {str(q) for q in currently_hard_qids if str(q)}

    bad_passing = sorted(quarantined & passing)
    if bad_passing:
        raise AssertionError(
            f"quarantine attribution drift: passing qids appear in quarantine: "
            f"{bad_passing}; quarantine source must be currently-failing rows only"
        )
    if len(hard) == 1 and (hard & quarantined):
        raise AssertionError(
            f"singleton-hard qid cannot be quarantined: hard={sorted(hard)}, "
            f"quarantined={sorted(quarantined)}; the only remaining hard "
            f"target must be available to the strategist"
        )


def _base_qid(qid: str) -> str:
    """Return the base qid by stripping any trailing ``:vN`` benchmark variant.

    The benchmark-suffix scheme produces qids like ``retail_..._002:v2`` and
    ``retail_..._002:v3`` whose canonical key is ``retail_..._002``. Mirrors
    the normalization used by ``_is_quarantined_qid`` in the harness so the
    soft-cluster currency check matches the row-routing behavior at the
    source.
    """
    qid = str(qid or "")
    if ":" in qid:
        return qid.split(":", 1)[0]
    return qid


def assert_soft_cluster_currency(
    *,
    soft_cluster_qids: Iterable[str],
    current_eval_rows: Iterable[dict],
) -> None:
    """Track H — soft-clustering must read the current eval row state.

    Invariant: every qid emitted in any soft cluster must, in the latest
    eval rows, exhibit at least one row where
    :func:`has_individual_judge_failure` returns ``True``. If a soft-cluster
    qid has no such row, the soft-clusterer is reading stale ASI / cached
    rows that no longer reflect the latest evaluation.

    The May-01 23:04 7Now run originated this helper: ``gs_001`` (a
    just-fixed target with all judges passing post-enrichment) appeared
    in soft cluster ``S003 wrong_table`` because the clusterer read a
    stale ASI snapshot. Under this invariant that case raises, while the
    legitimate "arbiter rescued the row but a non-info judge still flagged
    `no`" pattern (the design intent of the soft pile in
    ``_analyze_and_distribute``) is silently allowed.

    qids are compared on their *base* form (``:vN`` benchmark-suffix
    variants are stripped on both sides) so a soft cluster listing
    ``q_002`` matches a current row carrying ``q_002:v2``.
    """
    soft_bases = {_base_qid(q) for q in soft_cluster_qids if str(q)}
    if not soft_bases:
        return

    judge_failing_bases: set[str] = set()
    for row in current_eval_rows or ():
        if not isinstance(row, dict):
            continue
        qid = _base_qid(_row_qid(row))
        if not qid:
            continue
        if has_individual_judge_failure(row):
            judge_failing_bases.add(qid)

    bad = sorted(soft_bases - judge_failing_bases)
    if bad:
        raise AssertionError(
            f"soft-cluster currency drift: qids appear in soft clusters but "
            f"no row in the current eval shows an actionable judge failure: "
            f"{bad}; the soft-clusterer read stale ASI / cached rows that no "
            f"longer reflect the latest eval"
        )
