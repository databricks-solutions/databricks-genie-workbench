"""Patch selection helpers for RCA-driven action-group bundles."""

from __future__ import annotations

from typing import Any

_RISK_ORDER = {"low": 0, "medium": 1, "high": 2}


def _proposal_id(patch: dict[str, Any], index: int) -> str:
    return str(
        patch.get("proposal_id")
        or patch.get("expanded_patch_id")
        or patch.get("source_proposal_id")
        or patch.get("parent_proposal_id")
        or patch.get("id")
        or f"idx_{index}"
    )


def _lever(patch: dict[str, Any]) -> int:
    try:
        return int(patch.get("lever", 5))
    except (TypeError, ValueError):
        return 5


def _score(patch: dict[str, Any], name: str, default: float = 0.0) -> float:
    try:
        return float(patch.get(name, default) or default)
    except (TypeError, ValueError):
        return default


def _risk_rank(patch: dict[str, Any]) -> int:
    return _RISK_ORDER.get(str(patch.get("risk_level", "medium")).lower(), 1)


def _has_target_qids(patch: dict[str, Any]) -> bool:
    return bool(_target_qids_from_any(patch))


def _target_qids_from_any(patch: dict[str, Any]) -> tuple[str, ...]:
    raw: list = []
    raw.extend(patch.get("_grounding_target_qids") or [])
    raw.extend(patch.get("target_qids") or [])
    return tuple(dict.fromkeys(str(q) for q in raw if str(q)))


def causal_attribution_tier(patch: dict[str, Any]) -> int:
    """Return how specifically this patch is tied to a causal failure.

    3 = explicit RCA/theme plus target QIDs
    2 = target QIDs or grounding target QIDs
    1 = source cluster/action group only
    0 = no causal attribution
    """
    has_rca = bool(str(patch.get("rca_id") or "").strip())
    has_qids = _has_target_qids(patch)
    has_cluster = bool(patch.get("source_cluster_ids") or patch.get("primary_cluster_id"))
    has_ag = bool(patch.get("action_group_id") or patch.get("ag_id"))
    if has_rca and has_qids:
        return 3
    if has_qids:
        return 2
    if has_cluster or has_ag:
        return 1
    return 0


def _target_fingerprint(patch: dict[str, Any]) -> str:
    """Return a deterministic fingerprint of where this patch writes.

    Used as a tiebreaker in ``_stable_identity`` so two patches that share
    a ``proposal_id`` (e.g., a section split-child and a standalone snippet
    that inherited the same parent index) cannot collide. Order matches the
    fields the applier inspects when routing: section, table, column,
    snippet name, and target_object.
    """
    parts: list[str] = []
    for key in (
        "section_name",
        "instruction_section",
        "table",
        "column",
        "snippet_name",
        "snippet_type",
        "target_object",
        "target_table",
    ):
        value = patch.get(key)
        if value is None or value == "":
            continue
        parts.append(f"{key}={value}")
    return "|".join(parts)


def _stable_identity(patch: dict[str, Any]) -> str:
    """Return a stable identity that survives id-stamping bugs upstream.

    History: the optimizer used to key on ``expanded_patch_id or
    proposal_id`` alone. A rewrite_instruction split-child and a
    standalone snippet that inherited the parent's sequential index
    could land on the same id (``P001#2``) but differ in lever and
    type. Identity now includes lever, type, and target_fingerprint so
    those distinct patches do not collapse silently in dedup.
    """
    base_id = str(
        patch.get("expanded_patch_id")
        or patch.get("proposal_id")
        or id(patch)
    )
    parent = str(patch.get("parent_proposal_id") or "")
    lever = str(patch.get("lever", ""))
    ptype = str(patch.get("type") or patch.get("patch_type") or "")
    fingerprint = _target_fingerprint(patch)
    return f"{parent}|{base_id}|L{lever}|{ptype}|{fingerprint}"


def _deduplicate_decisions(decisions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return decisions with at most one entry per stable identity.

    Identity matches ``_stable_identity``: parent + id + lever + type +
    target_fingerprint. A decision row that lacks identity-relevant
    fields is preserved as-is so callers can still see it.
    """
    seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for decision in decisions:
        identity = _stable_identity(decision)
        if not identity or identity == "||L||":
            deduped.append(decision)
            continue
        if identity in seen:
            continue
        seen.add(identity)
        deduped.append(decision)
    return deduped


def _assert_cap_conservation(
    *,
    func_name: str,
    input_count: int,
    decisions: list[dict[str, Any]],
) -> None:
    """Hard-fail when a cap selector loses or duplicates a decision row.

    Surfaces the May-01 ESR ``Original 4, Kept 3, Dropped 0`` defect class
    immediately rather than letting it propagate through survival ledgers
    and journey events.
    """
    if len(decisions) != input_count:
        identities = [_stable_identity(d) for d in decisions]
        raise AssertionError(
            f"{func_name}: cap conservation violated: input={input_count} "
            f"decisions={len(decisions)} identities={identities!r}"
        )
    kept = sum(1 for d in decisions if d.get("decision") == "selected")
    dropped = sum(1 for d in decisions if d.get("decision") == "dropped")
    if kept + dropped != input_count:
        raise AssertionError(
            f"{func_name}: kept ({kept}) + dropped ({dropped}) != input "
            f"({input_count})"
        )


def _deduplicate_patches(patches: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return ``patches`` with at most one entry per ``_stable_identity``."""
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for patch in patches:
        identity = _stable_identity(patch)
        if identity in seen:
            continue
        seen.add(identity)
        out.append(patch)
    return out


def _identity_fields(patch: dict[str, Any], pid: str) -> dict[str, Any]:
    return {
        "parent_proposal_id": patch.get("parent_proposal_id") or pid,
        "expanded_patch_id": patch.get("expanded_patch_id") or pid,
        "rca_id": patch.get("rca_id"),
        "target_qids": list(patch.get("target_qids") or []),
        "_grounding_target_qids": list(patch.get("_grounding_target_qids") or []),
        "causal_attribution_tier": causal_attribution_tier(patch),
        "patch_type": patch.get("patch_type") or patch.get("type"),
    }


def select_causal_patch_cap(
    patches: list[dict[str, Any]],
    *,
    max_patches: int,
    active_cluster_ids: tuple[str, ...] = (),
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Select a capped patch bundle with causal relevance as the primary key.

    Diversity is intentionally a tiebreaker. A lower-relevance patch should
    never displace a higher-relevance RCA patch merely because it belongs to
    a different lever or instruction section.
    """
    patches = _deduplicate_patches(patches)
    _input_count = len(patches)
    if max_patches <= 0:
        _deduped = _deduplicate_decisions([
            {
                "proposal_id": _proposal_id(p, idx),
                "decision": "dropped",
                "selection_reason": "max_patches_zero",
                "rank": None,
                **_identity_fields(p, _proposal_id(p, idx)),
                "lever": _lever(p),
                "type": p.get("type") or p.get("patch_type"),
                "section_name": p.get("section_name"),
                "instruction_section": p.get("instruction_section"),
                "table": p.get("table"),
                "column": p.get("column"),
                "snippet_name": p.get("snippet_name"),
                "snippet_type": p.get("snippet_type"),
                "target_object": p.get("target_object"),
                "target_table": p.get("target_table"),
            }
            for idx, p in enumerate(patches)
        ])
        _assert_cap_conservation(
            func_name="select_causal_patch_cap",
            input_count=_input_count,
            decisions=_deduped,
        )
        return [], _deduped
    if len(patches) <= max_patches:
        _deduped = _deduplicate_decisions([
            {
                "proposal_id": _proposal_id(p, idx),
                "decision": "selected",
                "selection_reason": "under_cap",
                "rank": idx + 1,
                "relevance_score": _score(p, "relevance_score"),
                "lever": _lever(p),
                "type": p.get("type") or p.get("patch_type"),
                "section_name": p.get("section_name"),
                "instruction_section": p.get("instruction_section"),
                "table": p.get("table"),
                "column": p.get("column"),
                "snippet_name": p.get("snippet_name"),
                "snippet_type": p.get("snippet_type"),
                "target_object": p.get("target_object"),
                "target_table": p.get("target_table"),
                **_identity_fields(p, _proposal_id(p, idx)),
            }
            for idx, p in enumerate(patches)
        ])
        _assert_cap_conservation(
            func_name="select_causal_patch_cap",
            input_count=_input_count,
            decisions=_deduped,
        )
        return list(patches), _deduped

    remaining: list[tuple[int, dict[str, Any]]] = list(enumerate(patches))
    selected: list[tuple[int, dict[str, Any], str]] = []
    seen_levers: set[int] = set()

    while remaining and len(selected) < max_patches:
        def sort_key(item: tuple[int, dict[str, Any]]) -> tuple:
            idx, patch = item
            lever = _lever(patch)
            relevance = _score(patch, "relevance_score")
            diversity_bonus = 1 if lever not in seen_levers else 0
            return (
                -_active_cluster_match_tier(patch, active_cluster_ids),
                -relevance,
                -causal_attribution_tier(patch),
                -diversity_bonus,
                _risk_rank(patch),
                -_score(patch, "confidence"),
                -_score(patch, "net_impact"),
                idx,
            )

        best = min(remaining, key=sort_key)
        remaining.remove(best)
        _, patch = best
        reason = (
            "highest_causal_relevance"
            if not selected or _score(patch, "relevance_score") > 0
            else "stable_fallback"
        )
        selected.append((best[0], patch, reason))
        seen_levers.add(_lever(patch))

    selected_identities = {_stable_identity(p) for _idx, p, _reason in selected}
    rank_by_identity = {
        _stable_identity(p): rank
        for rank, (_idx, p, _reason) in enumerate(selected, start=1)
    }
    reason_by_identity = {
        _stable_identity(p): reason
        for _idx, p, reason in selected
    }
    decisions: list[dict[str, Any]] = []
    for idx, patch in enumerate(patches):
        pid = _proposal_id(patch, idx)
        identity = _stable_identity(patch)
        selected_flag = identity in selected_identities
        decisions.append({
            "proposal_id": pid,
            "decision": "selected" if selected_flag else "dropped",
            "selection_reason": (
                reason_by_identity[identity] if selected_flag else "lower_causal_rank"
            ),
            "rank": rank_by_identity.get(identity),
            "relevance_score": _score(patch, "relevance_score"),
            "lever": _lever(patch),
            "type": patch.get("type") or patch.get("patch_type"),
            "section_name": patch.get("section_name"),
            "instruction_section": patch.get("instruction_section"),
            "table": patch.get("table"),
            "column": patch.get("column"),
            "snippet_name": patch.get("snippet_name"),
            "snippet_type": patch.get("snippet_type"),
            "target_object": patch.get("target_object"),
            "target_table": patch.get("target_table"),
            **_identity_fields(patch, pid),
        })

    _deduped = _deduplicate_decisions(decisions)
    _assert_cap_conservation(
        func_name="select_causal_patch_cap",
        input_count=_input_count,
        decisions=_deduped,
    )
    return [p for _idx, p, _reason in selected], _deduped


def _target_qids(patch: dict[str, Any]) -> tuple[str, ...]:
    raw: list = []
    raw.extend(patch.get("_grounding_target_qids") or [])
    raw.extend(patch.get("target_qids") or [])
    return tuple(dict.fromkeys(str(q) for q in raw if str(q)))


_BEHAVIOR_ROOT_CAUSES = frozenset(
    {
        # Existing: filter/aggregation/measure shape errors.
        "missing_filter",
        "wrong_filter_condition",
        "wrong_aggregation",
        "wrong_measure",
        # Track 2 (Phase A burn-down): SQL-shape failures observed in
        # the May-01 7Now and 23:04 7Now runs. The strategist's direct
        # fix for these is an instruction or snippet that pins the
        # canonical SQL shape (LIMIT N, ROW_NUMBER, ts_filter), so the
        # patch must qualify for global direct-behavior reservation
        # alongside filter/aggregation fixes. The set is aligned with
        # ``control_plane._DIAGNOSTIC_AG_DIRECTIVES`` entries whose
        # ``kind == "sql_shape"`` so any cluster the diagnostic-AG
        # dispatcher recognizes as SQL-shape also earns cap reservation.
        "plural_top_n_collapse",
        "missing_temporal_filter",
        "time_window_pivot",
        "missing_aggregation",
        "missing_dimension",
        "wrong_grouping",
        "wrong_join_type",
    }
)


def _root_cause(patch: dict[str, Any]) -> str:
    raw = patch.get("root_cause") or patch.get("rca_kind") or ""
    return str(raw).strip().split(":", 1)[0]


def _cluster_ids(patch: dict[str, Any]) -> tuple[str, ...]:
    raw: list[Any] = []
    raw.extend(patch.get("source_cluster_ids") or [])
    raw.append(patch.get("primary_cluster_id"))
    raw.append(patch.get("cluster_id"))
    return tuple(dict.fromkeys(str(v) for v in raw if str(v)))


def _active_cluster_match_tier(
    patch: dict[str, Any],
    active_cluster_ids: tuple[str, ...],
) -> int:
    """Return 2 for primary-cluster match, 1 for any source cluster match, 0 otherwise."""
    active = {str(cid) for cid in active_cluster_ids or () if str(cid)}
    if not active:
        return 0
    patch_clusters = set(_cluster_ids(patch))
    if patch.get("primary_cluster_id") and str(patch.get("primary_cluster_id")) in active:
        return 2
    if patch_clusters & active:
        return 1
    return 0


def _is_direct_behavior_patch(patch: dict[str, Any]) -> bool:
    if _root_cause(patch) not in _BEHAVIOR_ROOT_CAUSES:
        return False
    if _lever(patch) in {5, 6}:
        return True
    patch_type = str(patch.get("type") or patch.get("patch_type") or "")
    return patch_type in {
        "add_instruction",
        "update_instruction_section",
        "add_sql_snippet_filter",
        "add_sql_snippet_measure",
        "add_sql_snippet_calculation",
    }


def _patch_cluster(p: dict[str, Any]) -> str:
    """Return the canonical cluster id for decision-row attribution.

    Track 2 (Phase A burn-down): reads the same four fields as
    ``_cluster_ids`` so a patch with lineage in ``source_cluster_ids``
    or ``primary_cluster_id`` is not invisible to the per-cluster slot
    floor or the decision-row ``cluster_id`` field. Priority order
    matches ``_cluster_ids`` so the canonical id is the first entry of
    the tuple ``_cluster_ids`` would produce.
    """
    ids = _cluster_ids(p)
    return ids[0] if ids else ""


def _patch_belongs_to_cluster(p: dict[str, Any], cluster_id: str) -> bool:
    """Return True when ``cluster_id`` appears in any of the patch's
    cluster-identity fields. Used by the per-cluster slot floor so a
    patch with lineage only in ``source_cluster_ids`` still counts.
    """
    cid = str(cluster_id or "").strip()
    if not cid:
        return False
    return cid in _cluster_ids(p)


def _patch_family(p: dict[str, Any]) -> str:
    """Return a family identifier shared by all split-children of one
    rewrite_instruction parent.

    Track C (Phase A burn-down): when a ``rewrite_instruction`` proposal
    is expanded into K section-split children by
    ``_split_rewrite_instruction_patch``, every child carries
    ``_split_from == "rewrite_instruction"`` and the same
    ``parent_proposal_id``. The cap collapses these K children into one
    family slot for reservation purposes so they cannot crowd out a
    direct-fix patch in the same AG.

    Non-split patches return their own ``proposal_id`` as the family
    (each is its own family of size one).
    """
    if p.get("_split_from") == "rewrite_instruction":
        parent = str(p.get("parent_proposal_id") or "").strip()
        if parent:
            return f"split:{parent}"
    pid = str(
        p.get("expanded_patch_id")
        or p.get("proposal_id")
        or ""
    ).strip()
    return pid or f"_anon_{id(p)}"


def _lever_diversity_tier(patch: dict[str, Any]) -> int:
    """Return an impact tier for the patch's lever.

    2 = behavior levers (5, 6) — instructions and SQL snippets
    1 = direct asset levers (3, 4) — function/snippet edits
    0 = description-only levers (1, 2) and unknown
    """
    lever = _lever(patch)
    if lever in (5, 6):
        return 2
    if lever in (3, 4):
        return 1
    return 0


def select_target_aware_causal_patch_cap(
    patches: list[dict[str, Any]],
    *,
    target_qids: tuple[str, ...],
    max_patches: int,
    active_cluster_ids: tuple[str, ...] = (),
    per_cluster_slot_floor: int = 0,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Cap patches while preserving at least one patch per target QID.

    Selection passes (in order):

    1. **Per-cluster slot floor.** For each cluster in ``active_cluster_ids``,
       reserve up to ``per_cluster_slot_floor`` patches assigned to that
       cluster. Highest-tier direct-behavior patches win for that cluster
       first; otherwise highest-relevance for that cluster wins.
    2. **Direct-behavior reservation.** Globally reserve the highest-tier
       direct-behavior patch (legacy behavior).
    3. **Per-target QID coverage.** For each target QID in order, pick the
       highest-relevance patch targeting that QID.
    4. **Filler.** Fill remaining capacity from the global causal-relevance
       ranking via ``select_causal_patch_cap``.

    Decision rows for ALL input patches include score provenance —
    ``relevance_score``, ``lever_diversity_tier``,
    ``active_cluster_match_tier``, and ``is_direct_behavior`` — so the
    next dropped patch is debuggable.
    """
    patches = _deduplicate_patches(patches)
    # Conservation invariant: this function's two early returns delegate to
    # ``select_causal_patch_cap``, which enforces conservation. The third
    # return path below builds its own decisions and asserts conservation
    # explicitly.
    _input_count = len(patches)
    if max_patches <= 0:
        return select_causal_patch_cap(patches, max_patches=max_patches)
    if len(patches) <= max_patches:
        return select_causal_patch_cap(patches, max_patches=max_patches)

    target_set = tuple(dict.fromkeys(str(q) for q in target_qids if str(q)))
    active_set = tuple(
        dict.fromkeys(str(c).strip() for c in active_cluster_ids or () if str(c).strip())
    )
    selected: list[dict[str, Any]] = []
    selected_ids: set[str] = set()

    per_cluster_reserved_pids: set[str] = set()
    reserved_direct_fix_pids: set[str] = set()
    active_cluster_reserved_pids: set[str] = set()

    # Pass 1: per-cluster slot floor.
    if per_cluster_slot_floor > 0 and active_set:
        for cluster_id in active_set:
            if len(selected) >= max_patches:
                break
            # Already-reserved patches for this cluster count toward the floor.
            already_reserved_for_cluster = sum(
                1 for p in selected if _patch_belongs_to_cluster(p, cluster_id)
            )
            slots_needed = per_cluster_slot_floor - already_reserved_for_cluster
            if slots_needed <= 0:
                continue
            # Candidates: patches assigned to this cluster, not yet selected.
            cluster_candidates = [
                (idx, patch)
                for idx, patch in enumerate(patches)
                if _patch_belongs_to_cluster(patch, cluster_id)
                and _proposal_id(patch, idx) not in selected_ids
            ]
            for _ in range(slots_needed):
                if not cluster_candidates or len(selected) >= max_patches:
                    break
                idx, patch = min(
                    cluster_candidates,
                    key=lambda item: (
                        # Prefer direct-behavior fixes first.
                        0 if _is_direct_behavior_patch(item[1]) else 1,
                        -_lever_diversity_tier(item[1]),
                        -_score(item[1], "relevance_score"),
                        -causal_attribution_tier(item[1]),
                        _risk_rank(item[1]),
                        -_score(item[1], "confidence"),
                        item[0],
                    ),
                )
                cluster_candidates.remove((idx, patch))
                pid = _proposal_id(patch, idx)
                selected.append(patch)
                selected_ids.add(pid)
                per_cluster_reserved_pids.add(pid)

    # Pass 2: direct-behavior reservation (legacy).
    if max_patches > 0 and len(selected) < max_patches:
        direct_candidates = [
            (idx, patch)
            for idx, patch in enumerate(patches)
            if _is_direct_behavior_patch(patch)
            and _proposal_id(patch, idx) not in selected_ids
        ]
        if direct_candidates:
            idx, patch = min(
                direct_candidates,
                key=lambda item: (
                    -_active_cluster_match_tier(item[1], active_set),
                    -_score(item[1], "relevance_score"),
                    -causal_attribution_tier(item[1]),
                    _risk_rank(item[1]),
                    -_score(item[1], "confidence"),
                    item[0],
                ),
            )
            selected.append(patch)
            pid = _proposal_id(patch, idx)
            selected_ids.add(pid)
            reserved_direct_fix_pids.add(pid)
            if _active_cluster_match_tier(patch, active_set) > 0:
                active_cluster_reserved_pids.add(pid)

    # Pass 3: per-target QID coverage (legacy).
    for target in target_set:
        if len(selected) >= max_patches:
            break
        # Skip targets already covered by an already-selected patch.
        if any(target in _target_qids(p) for p in selected):
            continue
        candidates = [
            (idx, patch)
            for idx, patch in enumerate(patches)
            if target in _target_qids(patch)
            and _proposal_id(patch, idx) not in selected_ids
        ]
        if not candidates:
            continue
        idx, patch = min(
            candidates,
            key=lambda item: (
                -_active_cluster_match_tier(item[1], active_set),
                -_score(item[1], "relevance_score"),
                -causal_attribution_tier(item[1]),
                _risk_rank(item[1]),
                -_score(item[1], "confidence"),
                item[0],
            ),
        )
        selected.append(patch)
        selected_ids.add(_proposal_id(patch, idx))

    # Pass 4: filler from global causal ranking.
    remaining = [
        patch
        for idx, patch in enumerate(patches)
        if _proposal_id(patch, idx) not in selected_ids
    ]
    if len(selected) < max_patches and remaining:
        filler, _ = select_causal_patch_cap(
            remaining,
            max_patches=max_patches - len(selected),
            active_cluster_ids=active_set,
        )
        selected.extend(filler)
        for fp in filler:
            try:
                fp_idx = patches.index(fp)
            except ValueError:
                continue
            selected_ids.add(_proposal_id(fp, fp_idx))

    rank_by_pid: dict[str, int] = {}
    for rank, patch in enumerate(selected, start=1):
        try:
            idx = patches.index(patch)
        except ValueError:
            idx = rank - 1
        rank_by_pid[_proposal_id(patch, idx)] = rank

    selected_pid_set = set(rank_by_pid)
    decisions: list[dict[str, Any]] = []
    for idx, patch in enumerate(patches):
        pid = _proposal_id(patch, idx)
        selected_flag = pid in selected_pid_set
        if selected_flag and pid in per_cluster_reserved_pids:
            selection_reason = "per_cluster_slot_floor_reserved"
        elif selected_flag and pid in active_cluster_reserved_pids:
            selection_reason = "active_cluster_direct_behavior_reserved"
        elif selected_flag and pid in reserved_direct_fix_pids:
            selection_reason = "behavior_direct_fix_reserved"
        elif selected_flag:
            selection_reason = "target_coverage"
        else:
            selection_reason = "lower_causal_rank"
        decisions.append({
            "proposal_id": pid,
            "cluster_id": _patch_cluster(patch),
            "patch_type": patch.get("patch_type") or patch.get("type"),
            "target": patch.get("target"),
            "decision": "selected" if selected_flag else "dropped",
            "selection_reason": selection_reason,
            "rank": rank_by_pid.get(pid),
            "relevance_score": _score(patch, "relevance_score"),
            "lever": _lever(patch),
            "lever_diversity_tier": _lever_diversity_tier(patch),
            "active_cluster_match_tier": _active_cluster_match_tier(patch, active_set),
            "is_direct_behavior": _is_direct_behavior_patch(patch),
            "type": patch.get("type") or patch.get("patch_type"),
            "section_name": patch.get("section_name"),
            "instruction_section": patch.get("instruction_section"),
            "table": patch.get("table"),
            "column": patch.get("column"),
            "snippet_name": patch.get("snippet_name"),
            "snippet_type": patch.get("snippet_type"),
            "target_object": patch.get("target_object"),
            "target_table": patch.get("target_table"),
            **_identity_fields(patch, pid),
        })

    _deduped = _deduplicate_decisions(decisions)
    _assert_cap_conservation(
        func_name="select_target_aware_causal_patch_cap",
        input_count=_input_count,
        decisions=_deduped,
    )
    return _deduplicate_patches(selected), _deduped
