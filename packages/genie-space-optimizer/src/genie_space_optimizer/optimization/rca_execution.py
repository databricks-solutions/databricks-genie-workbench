"""Executable RCA control-plane helpers.

RCA themes are no longer only strategist context. This module turns typed RCA
themes into deterministic execution plans that the harness can use to choose
mandatory levers, stamp grounding terms, and feed structured reflection.

The helpers are deliberately pure: no Spark, Databricks client, MLflow, Genie
API, or LLM calls.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable, Mapping


_IDENT_RE = re.compile(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\b")


@dataclass(frozen=True)
class ExpectedFix:
    """Typed shape of a patch intent inside an :class:`RcaExecutionPlan`.

    Phase C Task 2: ``RcaExecutionPlan.patch_intents`` was previously
    ``tuple[dict, ...]`` — the only un-typed link in the RCA contract
    chain. Promoting to a frozen dataclass matches the surrounding
    types (``RcaFinding``, ``RcaPatchTheme``, ``RcaExecutionPlan``,
    ``RcaNextActionDecision``) and gives the unified RCA-groundedness
    gate (Phase C Task 4) a stable surface for grounding-term lookup.

    ``extras`` is the open-ended bucket for patch-type-specific keys
    (``snippet_name``, ``expression``, ``description``, ``synonyms``,
    etc.) that not every patch shape carries. The four canonical fields
    (``patch_type``, ``target``, ``intent``, ``lever``) are extracted
    explicitly because every consumer reads them.
    """

    patch_type: str
    target: str = ""
    intent: str = ""
    lever: int = 0
    grounding_terms: tuple[str, ...] = ()
    extras: tuple[tuple[str, Any], ...] = ()

    @property
    def extras_dict(self) -> dict[str, Any]:
        """Convenience view over ``extras`` for callers that prefer dict
        access. The underlying storage is a tuple of pairs so the
        dataclass can stay hashable/frozen."""
        return dict(self.extras)

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any] | None) -> "ExpectedFix":
        """Build an ExpectedFix from one of today's untyped patch dicts.

        Lookup is forgiving: the LLM/strategist paths sometimes write
        ``patch_type`` and sometimes ``type``. Both are accepted.
        """
        d = dict(raw or {})
        canonical = {"patch_type", "type", "target", "intent", "lever"}
        patch_type = str(d.get("patch_type") or d.get("type") or "")
        target = str(d.get("target") or d.get("column") or d.get("table") or "")
        intent = str(d.get("intent") or d.get("new_text") or "")
        try:
            lever = int(d.get("lever") or 0)
        except (TypeError, ValueError):
            lever = 0
        grounding = _patch_grounding_terms(d)
        extras = tuple(
            (str(k), v) for k, v in d.items() if k not in canonical
        )
        return cls(
            patch_type=patch_type,
            target=target,
            intent=intent,
            lever=lever,
            grounding_terms=grounding,
            extras=extras,
        )

    def as_dict(self) -> dict[str, Any]:
        """Inverse of :meth:`from_dict` — emits the dict shape today's
        downstream consumers (applier, fixture serializer) expect."""
        out: dict[str, Any] = {
            "type": self.patch_type,
            "patch_type": self.patch_type,
            "target": self.target,
            "intent": self.intent,
            "lever": self.lever,
        }
        out.update(self.extras_dict)
        return out


@dataclass(frozen=True)
class RcaExecutionPlan:
    rca_id: str
    rca_kind: str
    patch_family: str
    target_qids: tuple[str, ...]
    required_levers: tuple[int, ...]
    grounding_terms: tuple[str, ...]
    defect_key: str
    patch_intents: tuple[ExpectedFix, ...]
    confidence: float = 0.0
    evidence_summary: str = ""


def _field(obj: Any, name: str, default: Any = None) -> Any:
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _normalize_term(value: Any) -> str:
    return str(value or "").strip().lower()


def _iter_strings(value: Any) -> Iterable[str]:
    if isinstance(value, str):
        if value.strip():
            yield value
    elif isinstance(value, dict):
        for child in value.values():
            yield from _iter_strings(child)
    elif isinstance(value, (list, tuple, set)):
        for child in value:
            yield from _iter_strings(child)


def _terms_from_text(value: Any) -> list[str]:
    terms: list[str] = []
    for text in _iter_strings(value):
        raw = _normalize_term(text)
        if raw:
            terms.append(raw)
        for token in _IDENT_RE.findall(text):
            token = _normalize_term(token)
            if token:
                terms.append(token)
        for dotted in re.findall(r"[a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)+", text):
            dotted = _normalize_term(dotted)
            if dotted:
                terms.append(dotted)
                terms.extend(part for part in dotted.split(".") if part)
    return terms


def _dedupe(items: Iterable[Any]) -> tuple[Any, ...]:
    out: list[Any] = []
    seen: set[Any] = set()
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return tuple(out)


def _patch_lever(patch: dict) -> int | None:
    raw = patch.get("lever")
    try:
        lever = int(raw)
    except Exception:
        return None
    return lever if 1 <= lever <= 6 else None


def _patch_grounding_terms(patch: dict) -> tuple[str, ...]:
    fields = (
        "target",
        "target_object",
        "target_table",
        "table",
        "column",
        "metric",
        "join_target",
        "snippet_name",
        "expression",
        "sql",
        "intent",
        "new_text",
        "description",
        "synonyms",
        "expected_objects",
        "actual_objects",
        "blame_set",
    )
    terms: list[str] = []
    for field in fields:
        if field in patch:
            terms.extend(_terms_from_text(patch.get(field)))
    return _dedupe(t for t in terms if t)


def _theme_grounding_terms(theme: Any, patches: tuple[dict, ...]) -> tuple[str, ...]:
    terms: list[str] = []
    terms.extend(_terms_from_text(_field(theme, "touched_objects", ())))
    terms.extend(_terms_from_text(_field(theme, "evidence_summary", "")))
    for patch in patches:
        terms.extend(_patch_grounding_terms(patch))
    return _dedupe(t for t in terms if t)


def defect_key_for_theme(theme: Any) -> str:
    patch_family = str(_field(theme, "patch_family", "") or "unknown")
    touched_terms = [
        _normalize_term(t)
        for t in (_field(theme, "touched_objects", ()) or ())
        if _normalize_term(t)
    ]
    if touched_terms:
        return f"{patch_family}:{'|'.join(sorted(set(touched_terms)))}"
    target_qids = [
        _normalize_term(q)
        for q in (_field(theme, "target_qids", ()) or ())
        if _normalize_term(q)
    ]
    return f"{patch_family}:qids:{'|'.join(sorted(target_qids))}"


def build_rca_execution_plans(themes: Iterable[Any]) -> list[RcaExecutionPlan]:
    plans: list[RcaExecutionPlan] = []
    for theme in themes or []:
        patches = tuple(
            p for p in (_field(theme, "patches", ()) or ())
            if isinstance(p, dict)
        )
        recommended = tuple(
            int(x)
            for x in (_field(theme, "recommended_levers", ()) or ())
            if str(x).isdigit() and 1 <= int(x) <= 6
        )
        patch_levers = tuple(
            lever
            for lever in (_patch_lever(p) for p in patches)
            if lever is not None
        )
        required = _dedupe((*recommended, *patch_levers))
        if not required:
            continue
        target_qids = _dedupe(
            str(q).strip()
            for q in (_field(theme, "target_qids", ()) or ())
            if str(q).strip()
        )
        if not target_qids:
            continue
        rca_kind = _field(theme, "rca_kind", "")
        if hasattr(rca_kind, "value"):
            rca_kind = rca_kind.value
        plans.append(RcaExecutionPlan(
            rca_id=str(_field(theme, "rca_id", "")),
            rca_kind=str(rca_kind or ""),
            patch_family=str(_field(theme, "patch_family", "")),
            target_qids=target_qids,
            required_levers=tuple(int(x) for x in required),
            grounding_terms=_theme_grounding_terms(theme, patches),
            defect_key=defect_key_for_theme(theme),
            patch_intents=tuple(ExpectedFix.from_dict(p) for p in patches),
            confidence=float(_field(theme, "confidence", 0.0) or 0.0),
            evidence_summary=str(_field(theme, "evidence_summary", "") or ""),
        ))
    return plans


def target_qids_for_action_group_execution(
    action_group: dict,
    source_clusters: Iterable[dict] = (),
) -> tuple[str, ...]:
    """Resolve action-group target QIDs using the same contract as grounding.

    Strategist-emitted ``affected_questions`` is sometimes natural-language
    text rather than canonical QIDs; the canonical resolver in
    ``control_plane`` falls back to ``source_cluster_ids`` so RCA execution
    sees the same QID scope that grounding and acceptance use.
    """
    try:
        from genie_space_optimizer.optimization.control_plane import (
            target_qids_from_action_group,
        )

        return target_qids_from_action_group(action_group, source_clusters)
    except Exception:
        return _dedupe(
            str(q).strip()
            for q in (action_group.get("affected_questions") or [])
            if str(q).strip()
        )


def required_levers_for_action_group(
    action_group: dict,
    plans: Iterable[RcaExecutionPlan],
    *,
    source_clusters: Iterable[dict] = (),
) -> tuple[int, ...]:
    ag_qids = set(
        target_qids_for_action_group_execution(action_group, source_clusters)
    )
    if not ag_qids:
        return ()
    levers: list[int] = []
    for plan in plans or []:
        if not ag_qids.intersection(plan.target_qids):
            continue
        levers.extend(plan.required_levers)
    return tuple(int(x) for x in _dedupe(levers))


def union_execution_levers(
    strategist_levers: Iterable[str | int],
    required_levers: Iterable[int],
) -> list[str]:
    out: list[str] = []
    for lever in strategist_levers or []:
        s = str(lever)
        if s.isdigit() and s not in out:
            out.append(s)
    for lever in required_levers or []:
        s = str(int(lever))
        if s not in out:
            out.append(s)
    return out


def forced_levers_from_reflections(
    reflection_buffer: Iterable[dict],
    *,
    target_rca_ids: Iterable[str],
    min_repeats: int = 2,
) -> tuple[int, ...]:
    target_ids = {str(x) for x in target_rca_ids or [] if str(x)}
    if not target_ids:
        return ()
    counts: dict[str, int] = {}
    levers_by_rca: dict[str, list[int]] = {}
    for entry in reflection_buffer or []:
        if entry.get("accepted"):
            continue
        if entry.get("rollback_reason") != "no_grounded_patches":
            continue
        payload = entry.get("rca_execution")
        if not isinstance(payload, dict):
            continue
        ids = [str(x) for x in (payload.get("rca_ids") or []) if str(x)]
        required = [
            int(x) for x in (payload.get("required_levers") or [])
            if str(x).isdigit()
        ]
        for rca_id in ids:
            if rca_id not in target_ids:
                continue
            counts[rca_id] = counts.get(rca_id, 0) + 1
            levers_by_rca.setdefault(rca_id, []).extend(required)
    forced: list[int] = []
    for rca_id, count in counts.items():
        if count >= min_repeats:
            forced.extend(levers_by_rca.get(rca_id, []))
    return tuple(int(x) for x in _dedupe(forced))


def next_grounding_remediation(
    reflection_buffer: Iterable[dict],
    *,
    target_rca_ids: Iterable[str],
    min_repeats: int = 2,
) -> dict:
    """Inspect repeated ungrounded reflections and recommend remediation.

    Returns ``action`` plus ``forced_levers``: when grounding has failed for
    the same RCA target ``min_repeats`` times with the same category, ask the
    harness to repair the grounding contract or rotate to a different patch
    family rather than retrying the same dead end.
    """
    target_ids = {str(x) for x in target_rca_ids or [] if str(x)}
    counts: dict[str, int] = {}
    for entry in reflection_buffer or []:
        if entry.get("accepted"):
            continue
        if entry.get("rollback_reason") != "no_grounded_patches":
            continue
        payload = entry.get("rca_execution")
        if not isinstance(payload, dict):
            continue
        ids = {str(x) for x in (payload.get("rca_ids") or []) if str(x)}
        if target_ids and not (ids & target_ids):
            continue
        category = str(entry.get("grounding_failure_category") or "unknown")
        counts[category] = counts.get(category, 0) + 1

    from genie_space_optimizer.optimization.rca_next_action import (
        next_action_for_rejection,
    )

    for category, count in counts.items():
        if count < min_repeats:
            continue
        decision = next_action_for_rejection(
            rollback_reason="no_grounded_patches",
            grounding_failure_category=category,
            repeated_count=count,
        )
        if decision.action.value != "none":
            return {
                "action": decision.action.value,
                "forced_levers": decision.forced_levers,
                "terminal_status": decision.terminal_status,
                "reason": decision.reason,
            }
    return {"action": "none", "forced_levers": ()}


def plans_for_action_group(
    action_group: dict,
    plans: Iterable[RcaExecutionPlan],
    *,
    source_clusters: Iterable[dict] = (),
) -> tuple[RcaExecutionPlan, ...]:
    ag_qids = set(
        target_qids_for_action_group_execution(action_group, source_clusters)
    )
    return tuple(
        plan for plan in (plans or [])
        if ag_qids.intersection(plan.target_qids)
    )


def _cluster_terms(cluster: dict) -> set[str]:
    terms: set[str] = set()
    for key in (
        "asi_blame_set",
        "blame_set",
        "expected_objects",
        "actual_objects",
        "asi_counterfactual_fixes",
        "counterfactual_fixes",
    ):
        terms.update(_terms_from_text(cluster.get(key)))
    return {
        t for t in terms
        if len(t) > 2 and t not in {"none", "null", "unknown", "other"}
    }


def clusters_share_defect_identity(left: dict, right: dict) -> bool:
    left_terms = _cluster_terms(left)
    right_terms = _cluster_terms(right)
    if not left_terms or not right_terms:
        return False
    shared = left_terms & right_terms
    if shared:
        return True
    left_fn = {t for t in left_terms if "fn" in t or "tvf" in t or "function" in t}
    right_fn = {t for t in right_terms if "fn" in t or "tvf" in t or "function" in t}
    return bool(left_fn and right_fn and left_fn & right_fn)


@dataclass(frozen=True)
class ObservedEffect:
    """Typed post-eval delta for a single applied patch.

    Phase C Task 3: closes the loop between intended fix
    (``RcaExecutionPlan`` / :class:`ExpectedFix`) and what actually
    happened after the patch was applied. Today this signal lives as
    ad-hoc keys on ``apply_log`` plus free-text ``observed_effect``
    strings on :class:`DecisionRecord`. The dataclass gives the next-
    action mapper and replay validators a single typed surface.

    ``arbiter_verdict_change`` is one of ``""`` (unknown / no change),
    ``"hold"``, ``"fail->pass"``, ``"pass->fail"``. Any other string is
    accepted but treated as opaque by downstream consumers.
    """

    iteration: int
    ag_id: str
    proposal_id: str
    pre_passing_qids: tuple[str, ...]
    post_passing_qids: tuple[str, ...]
    iq_delta: float
    arbiter_verdict_change: str
    judge_failure_delta: int


def build_observed_effects(
    *,
    iteration: int,
    ag_id: str,
    apply_log: Mapping[str, Any] | None,
    pre_passing_qids: Iterable[str],
    post_passing_qids: Iterable[str],
    pre_iq: float,
    post_iq: float,
    arbiter_verdict_change: str,
    pre_judge_failures: int,
    post_judge_failures: int,
) -> list[ObservedEffect]:
    """One :class:`ObservedEffect` per applied patch from ``apply_log``.

    All applied patches inside a single AG share the same iteration-
    level pre/post snapshot — the harness applies an AG's patches as a
    bundle and re-evaluates once. So the per-patch ``ObservedEffect``
    rows differ only in ``proposal_id``; downstream consumers
    aggregate by ``ag_id`` when an AG-level view is wanted.

    Defensive: applier rows occasionally omit ``proposal_id``. Skip
    those rather than emit a sentinel, so the list stays a faithful
    index of attributed applications.
    """
    pre_set = tuple(str(q) for q in (pre_passing_qids or ()) if str(q))
    post_set = tuple(str(q) for q in (post_passing_qids or ()) if str(q))
    iq_delta = float(post_iq) - float(pre_iq)
    judge_delta = int(post_judge_failures) - int(pre_judge_failures)

    effects: list[ObservedEffect] = []
    applied = (apply_log or {}).get("applied") or []
    for entry in applied:
        if not isinstance(entry, Mapping):
            continue
        patch = entry.get("patch")
        if not isinstance(patch, Mapping):
            continue
        proposal_id = str(patch.get("proposal_id") or "")
        if not proposal_id:
            continue
        effects.append(ObservedEffect(
            iteration=int(iteration),
            ag_id=str(ag_id),
            proposal_id=proposal_id,
            pre_passing_qids=pre_set,
            post_passing_qids=post_set,
            iq_delta=iq_delta,
            arbiter_verdict_change=str(arbiter_verdict_change or ""),
            judge_failure_delta=judge_delta,
        ))
    return effects
