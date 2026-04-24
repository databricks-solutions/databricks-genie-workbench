"""Pre-flight example_sql synthesis (Bug #4 follow-up).

Proactive, leak-free "knowledge booster" that fills
``instructions.example_question_sqls`` up to :data:`PREFLIGHT_EXAMPLE_SQL_TARGET`
(default 20). Distinct from the reactive AFS-driven path in
:mod:`optimization.synthesis` — this fires in pre-flight from schema
alone, with no failure cluster required.

Design invariants (enforced structurally + tested):

1. **No benchmark content in any generator prompt.** The rendering
   function takes no ``benchmarks`` parameter; the prompt template has
   no ``{{ benchmarks }}`` variable. Both halves are checked by
   ``test_synthesis_prompt_excludes_benchmarks``.
2. **Runtime firewall on every output.** Every candidate is
   fingerprint-checked against ``BenchmarkCorpus`` before persist via
   :func:`_apply_proactive_example_sqls(... benchmarks=...)` which runs
   ``is_benchmark_leak`` inline.
3. **Threshold gate is load-bearing.** ``need = max(0, TARGET - existing)``
   — the stage cannot overflow the target, cannot churn across re-runs,
   and is idempotent by construction.
4. **Feature flag ON by default.** Operators opt *out* via
   ``GENIE_SPACE_OPTIMIZER_ENABLE_PREFLIGHT_EXAMPLE_SQL=false``.

The pipeline:

1. **Coverage planner** — :func:`plan_asset_coverage` emits
   ``(archetype, AssetSlice)`` plans biased toward exercising every
   table / metric view / join spec before piling more on any one asset.
2. **Synthesis** — :func:`synthesize_preflight_candidate` runs an LLM
   call with a *narrowed* identifier allowlist (slice only). Narrow
   allowlists drive higher EXPLAIN pass rates and curb hallucinations.
3. **5-gate validator** — reuses
   :func:`optimization.synthesis.validate_synthesis_proposal` as-is
   (parse / execute / structural / arbiter-no-op-until-P2 / firewall).
4. **Apply** — dedups against the current space config and any other
   already-accepted candidate in this run, then hands the first
   ``need`` survivors to :func:`_apply_proactive_example_sqls`.

P2 adds the Genie-vs-synthesized arbiter gate between steps 3 and 4 via
:func:`_gate_genie_agreement`.
"""

from __future__ import annotations

import copy
import json
import logging
import random
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Iterator, Protocol, runtime_checkable

from genie_space_optimizer.common.config import (
    PREFLIGHT_COLUMN_COVERAGE_K,
    PREFLIGHT_EXAMPLE_SQL_OVERDRAW,
    PREFLIGHT_EXAMPLE_SQL_PER_ARCHETYPE,
    PREFLIGHT_EXAMPLE_SQL_TARGET,
    PREFLIGHT_EXAMPLE_SYNTHESIS_PROMPT,
    PREFLIGHT_PROFILE_VALUES_CAP,
    PREFLIGHT_PROFILE_VALUE_LEN_CAP,
    format_mlflow_template,
)
from genie_space_optimizer.optimization.archetypes import (
    ARCHETYPES,
    Archetype,
    _col_type,
    schema_traits,
)
# Module-level imports so tests can ``patch("preflight_synthesis.X")`` at the
# orchestrator's attribute. Synthesis is already a dependency via the reused
# 5-gate pipeline, so this doesn't add any import-time heaviness.
from genie_space_optimizer.optimization.synthesis import (
    GateResult,
    validate_synthesis_proposal,
)

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════
# 1. SynthesisContext protocol + AssetSlice (schema-driven implementor)
# ═══════════════════════════════════════════════════════════════════════
#
# Protocol so the pre-flight engine (synthesis + gates + apply) can serve
# both the schema-driven ``AssetSlice`` (pre-flight) and the cluster-
# driven ``ClusterContext`` (Bug #4 Phase 3, see
# ``optimization/cluster_driven_synthesis.py``).
#
# Byte-equivalence contract: pre-flight's prompt rendering MUST stay
# byte-for-byte identical before and after this refactor. AFS rendering
# is therefore NOT part of the protocol; it lives in the cluster-driven
# module and is prepended to the pre-flight prompt output by a wrapper.
# This avoids adding an "empty AFS block" placeholder to
# ``PREFLIGHT_EXAMPLE_SYNTHESIS_PROMPT`` that would silently change the
# LLM's prompt surface even when AFS is absent.


@runtime_checkable
class SynthesisContext(Protocol):
    """Minimal interface the synthesis engine needs from a context.

    Both ``AssetSlice`` (schema-driven, pre-flight) and ``ClusterContext``
    (failure-driven, cluster-driven) conform. Any future context types
    (e.g. iterative top-up) can plug into the same engine without
    touching the synthesis prompt or the 5-gate validator.
    """

    def to_identifier_allowlist(self) -> str:
        """Render a narrowed FQ-identifier allowlist for the LLM prompt."""
        ...

    def asset_ids(self) -> list[str]:
        """Lower-cased, deduped identifiers this context touches.

        Used by planners for coverage accounting and by orchestrators
        for per-asset observability.
        """
        ...


@dataclass
class AssetSlice:
    """Narrow schema view passed to the synthesis LLM for one candidate.

    Holds the tables / metric view / columns / optional join spec the
    synthesized query is expected to reference. Produces a tight
    identifier allowlist so the LLM cannot reference unrelated assets
    (which would mostly fail EXPLAIN anyway) and cannot hallucinate
    columns it hasn't been shown.

    Fields
    ------
    tables : list[dict]
        Zero, one, or two table snapshots (mirroring
        ``metadata_snapshot["data_sources"]["tables"][i]``). Two only
        when a ``join_spec`` is present.
    metric_view : dict | None
        Optional metric view snapshot. Mutually exclusive with having
        both ``tables`` entries populated — an MV-centric slice uses
        its dimensions/measures directly.
    columns : list[tuple[str, str]]
        ``[(table_identifier, column_name), ...]`` — the top-K columns
        to prioritise for this slice. Kept case-preserving; the allow-
        list builder lowercases when emitting backtick-wrapped prose.
    join_spec : dict | None
        A join-spec snapshot (shape mirrors
        ``metadata_snapshot["instructions"]["join_specs"][i]``). When
        present, both ``tables[0]`` and ``tables[1]`` are the
        ``left`` / ``right`` assets.
    """

    tables: list[dict] = field(default_factory=list)
    metric_view: dict | None = None
    columns: list[tuple[str, str]] = field(default_factory=list)
    join_spec: dict | None = None

    def asset_ids(self) -> list[str]:
        """Identifiers for all assets in the slice, lower-cased + deduped.

        Used by the coverage planner to update its tally after a slice
        is emitted and by the orchestrator's logging.
        """
        ids: list[str] = []
        for t in self.tables:
            ident = (t.get("identifier") or t.get("name") or "").strip().lower()
            if ident and ident not in ids:
                ids.append(ident)
        if self.metric_view:
            ident = (
                self.metric_view.get("identifier")
                or self.metric_view.get("name")
                or ""
            ).strip().lower()
            if ident and ident not in ids:
                ids.append(ident)
        return ids

    def to_identifier_allowlist(self) -> str:
        """Render a narrowed allowlist for the synthesis prompt.

        Format mirrors :func:`optimization.optimizer._build_identifier_allowlist`'s
        prose output but restricted to the slice's assets + columns.
        A tight allowlist raises EXPLAIN pass rates and anchors the
        LLM to the coverage focus.
        """
        lines: list[str] = []
        assets: list[dict] = list(self.tables)
        if self.metric_view is not None:
            assets.append(self.metric_view)

        # Map from table identifier to its columns in the slice — keeps
        # the printed order stable and lets us surface the asset names
        # even when the per-column list is short.
        cols_by_asset: dict[str, list[str]] = {}
        for tid, cname in self.columns:
            cols_by_asset.setdefault(tid.strip().lower(), []).append(cname)

        for asset in assets:
            ident = (asset.get("identifier") or asset.get("name") or "").strip()
            if not ident:
                continue
            lines.append(f"- {ident}")
            cols = cols_by_asset.get(ident.lower(), [])
            for col in cols:
                # Preserve original case from the column snapshot when
                # possible — Spark is case-insensitive but logs are
                # easier to read when the case matches the schema.
                lines.append(f"    - {ident}.{col}")
        if self.join_spec:
            left_id = (
                self.join_spec.get("left", {}).get("identifier", "") or ""
            ).strip()
            right_id = (
                self.join_spec.get("right", {}).get("identifier", "") or ""
            ).strip()
            sql_field = self.join_spec.get("sql", [])
            cond = (
                sql_field[0]
                if isinstance(sql_field, list) and sql_field
                else str(sql_field or "")
            )
            if left_id and right_id:
                lines.append(
                    f"- Join: {left_id} <-> {right_id} ON {cond[:120]}"
                )
        return "\n".join(lines) if lines else "(no assets in slice)"


# ═══════════════════════════════════════════════════════════════════════
# 2. Coverage planner
# ═══════════════════════════════════════════════════════════════════════


def _top_k_columns(asset: dict, k: int) -> list[tuple[str, str]]:
    """Rank the asset's columns and return the top-K as (identifier, name).

    Ranking rules (highest first):

    1. Columns with non-empty ``description`` in ``column_configs`` —
       the user (or Create Agent) has invested meaning in them.
    2. Columns whose data type is numeric / date / categorical —
       likely query targets.
    3. Columns NOT flagged as PII by heuristic (name contains ``ssn``,
       ``email``, ``phone``, ``token``, ``secret``). PII should be
       explicitly referenced, not sampled by the booster.
    4. Alphabetical as final tie-breaker for determinism.

    Returns at most ``k`` entries. Metric-view ``measures`` and
    ``dimensions`` are merged with ``columns`` / ``column_configs``
    so MV slices get the right references.
    """
    if not isinstance(asset, dict):
        return []
    ident = (asset.get("identifier") or asset.get("name") or "").strip()
    if not ident:
        return []

    pii_markers = ("ssn", "email", "phone", "token", "secret", "password")

    # Column name → (has_description, is_priority_type, name)
    candidates: dict[str, tuple[bool, bool, str]] = {}

    def _bump(name: str, desc: str, dtype: str) -> None:
        if not name:
            return
        key = name.lower()
        # Filter out PII candidates entirely — pre-flight synthesis should
        # not surface them via random-column sampling. If a user wants
        # PII-aware examples, they should curate them by hand.
        if any(m in key for m in pii_markers):
            return
        has_desc = bool(str(desc or "").strip())
        dt_low = str(dtype or "").lower().split("(")[0].strip()
        is_priority = any(
            x in dt_low for x in (
                "int", "double", "decimal", "long", "float", "numeric",
                "date", "timestamp", "string", "varchar",
            )
        )
        existing = candidates.get(key)
        prev_desc = existing[0] if existing else False
        candidates[key] = (has_desc or prev_desc, is_priority, name)

    # Regular columns
    for col in asset.get("columns", []) or []:
        if isinstance(col, dict):
            _bump(
                col.get("name", ""),
                col.get("description", ""),
                _col_type(col),
            )
    # column_configs
    for cc in asset.get("column_configs", []) or []:
        if isinstance(cc, dict):
            _bump(
                cc.get("column_name", ""),
                cc.get("description", ""),
                _col_type(cc),
            )
    # Metric view measures + dimensions
    for m in asset.get("measures", []) or []:
        if isinstance(m, dict):
            _bump(m.get("name", ""), m.get("description", ""), "numeric")
    for d in asset.get("dimensions", []) or []:
        if isinstance(d, dict):
            _bump(d.get("name", ""), d.get("description", ""), "string")

    # Rank: described first, then priority-typed, then alpha.
    def _rank_key(item: tuple[str, tuple[bool, bool, str]]) -> tuple:
        key, (has_desc, is_priority, name) = item
        return (
            not has_desc,       # True sorts after False — described first
            not is_priority,    # priority types first
            name.lower(),
        )

    ranked = sorted(candidates.items(), key=_rank_key)
    return [(ident, value[2]) for _, value in ranked[:max(0, k)]]


def _archetype_by_name(name: str) -> Archetype | None:
    for a in ARCHETYPES:
        if a.name == name:
            return a
    return None


# A small, curated set of archetype preferences per "kind" of coverage.
# We pick from these when we know we want a join / MV / solo plan. The
# planner filters further by ``preflight_eligible`` and trait match.
_JOIN_COVERAGE_ARCHETYPES = (
    "segment_compare", "ratio_by_dimension", "time_window_aggregate",
    "top_n_by_metric", "distinct_count_by_dim",
)
_MV_COVERAGE_ARCHETYPES = (
    "top_n_by_metric", "time_window_aggregate", "distinct_count_by_dim",
    "period_over_period", "ratio_by_dimension",
)
_TABLE_COVERAGE_ARCHETYPES = (
    "filter_compose", "distinct_count_by_dim", "top_n_by_metric",
    "pct_change", "pivot_wide",
)


def _eligible_archetypes(traits: set[str]) -> list[Archetype]:
    """Archetypes that pass the pre-flight gate AND have their trait
    requirements satisfied by ``traits``."""
    out: list[Archetype] = []
    for a in ARCHETYPES:
        if not a.preflight_eligible:
            continue
        if a.required_schema_traits and not a.required_schema_traits.issubset(
            traits
        ):
            continue
        out.append(a)
    return out


def _pick_archetype_from_preferences(
    preferences: tuple[str, ...],
    traits: set[str],
    exclude: set[str] | None = None,
) -> Archetype | None:
    """Walk ``preferences`` and return the first archetype that is
    pre-flight-eligible, trait-compatible, and not in ``exclude``.

    Falls back to any eligible archetype if nothing in preferences
    matches. Returns None for schemas too sparse to support any
    archetype at all (pure small-space fallback triggers in that case).
    """
    excl = exclude or set()
    for name in preferences:
        a = _archetype_by_name(name)
        if a is None:
            continue
        if not a.preflight_eligible:
            continue
        if a.required_schema_traits and not a.required_schema_traits.issubset(
            traits
        ):
            continue
        if a.name in excl:
            continue
        return a
    # Preference miss — fall back to any eligible archetype.
    for a in _eligible_archetypes(traits):
        if a.name not in excl:
            return a
    return None


def _resolve_asset_by_identifier(
    metadata_snapshot: dict, identifier: str,
) -> dict | None:
    """Return the table / metric-view snapshot matching ``identifier``.

    Matches on full FQ identifier first, then on short (last-segment)
    name. Returns ``None`` when no match exists — the caller treats
    that as a "join spec references an absent asset" edge case and
    skips the plan.
    """
    if not identifier:
        return None
    ident_lower = identifier.strip().lower()
    short = ident_lower.split(".")[-1]
    ds = metadata_snapshot.get("data_sources", {}) or {}
    for bucket in ("tables", "metric_views"):
        for t in ds.get(bucket, []) or []:
            if not isinstance(t, dict):
                continue
            tid = (t.get("identifier") or t.get("name") or "").strip().lower()
            if tid == ident_lower or tid.split(".")[-1] == short:
                return t
    return None


def plan_asset_coverage(
    metadata_snapshot: dict,
    need: int,
    *,
    overdraw: float = PREFLIGHT_EXAMPLE_SQL_OVERDRAW,
    column_k: int = PREFLIGHT_COLUMN_COVERAGE_K,
    per_archetype: int = PREFLIGHT_EXAMPLE_SQL_PER_ARCHETYPE,
    rng: random.Random | None = None,
) -> list[tuple[Archetype, AssetSlice]]:
    """Emit ``(archetype, AssetSlice)`` plans biased toward asset coverage.

    The first passes are must-cover: every join spec gets a plan, then
    every metric view, then every solo table not already touched. After
    that the remaining slots are filled greedily by picking the slice
    that minimises the skew in the per-asset tally (keeps diversity
    high even when the schema is large).

    Parameters
    ----------
    metadata_snapshot
        Current space config snapshot.
    need
        How many applied examples the caller wants at minimum. Planner
        emits ``ceil(need * overdraw)`` plans to absorb gate rejects.
    overdraw, column_k, per_archetype
        Overridable for tests; default from :mod:`common.config`.
    rng
        Optional seeded RNG for deterministic tests. Defaults to a
        ``random.Random()`` without a seed for production runs.

    Returns
    -------
    list[tuple[Archetype, AssetSlice]]
        Plans in application order. May be shorter than the overdraw
        target on small or trait-sparse schemas (caller logs the
        fallback, synthesis still proceeds with whatever plans exist).
    """
    if need <= 0:
        return []

    rng = rng or random.Random()
    target = max(1, int(-(-need * overdraw // 1)))  # ceil(need * overdraw)

    ds = metadata_snapshot.get("data_sources", {}) or {}
    tables = [t for t in (ds.get("tables", []) or []) if isinstance(t, dict)]
    metric_views = [
        mv for mv in (ds.get("metric_views", []) or []) if isinstance(mv, dict)
    ]
    join_specs = [
        j for j in (
            (metadata_snapshot.get("instructions", {}) or {}).get("join_specs", []) or []
        )
        if isinstance(j, dict)
    ]

    traits = schema_traits(metadata_snapshot)
    eligible = _eligible_archetypes(traits)
    # Phase 1.R7: when archetype diversity is narrow (≤ 3 eligible), raise
    # the per-archetype cap so the planner can still reach ``target`` plans.
    # With broader eligibility (4+) we keep the cap tight to prevent any
    # single archetype from dominating the example-SQL output.
    if 0 < len(eligible) <= 3:
        effective_per_archetype = max(
            per_archetype, (target // max(len(eligible), 1)) + 1,
        )
        if effective_per_archetype != per_archetype:
            logger.info(
                "preflight.plan.per_archetype_adaptive eligible=%d "
                "per_archetype=%d -> %d (target=%d)",
                len(eligible), per_archetype,
                effective_per_archetype, target,
            )
            per_archetype = effective_per_archetype
    logger.info(
        "preflight.plan.traits traits=%s eligible_archetypes=%s "
        "tables=%d mvs=%d joins=%d per_archetype=%d",
        sorted(traits),
        [a.name for a in eligible],
        len(tables), len(metric_views), len(join_specs),
        per_archetype,
    )
    # Empty-trait fingerprint — the schema_traits detector silently
    # returned no traits, so only the trait-free ``filter_compose``
    # archetype survived. Historically this was caused by
    # ``schema_traits`` reading from the wrong snapshot path; surface
    # a clear warning in case a future regression re-introduces the
    # bug or a caller passes a snapshot shape we don't recognise.
    if len(eligible) == 1 and eligible[0].name == "filter_compose" and not traits:
        logger.warning(
            "preflight.plan.empty_traits_only_filter_compose "
            "tables=%d mvs=%d — schema_traits() returned no traits; "
            "planner will cap at per_archetype=%d candidates. Likely "
            "cause: metadata_snapshot shape unrecognised by schema_traits.",
            len(tables), len(metric_views), per_archetype,
        )
    if not eligible:
        logger.info(
            "preflight.plan.no_eligible_archetypes traits=%s — small-space fallback empty",
            sorted(traits),
        )
        return []

    plans: list[tuple[Archetype, AssetSlice]] = []
    coverage: dict[str, int] = {}
    archetype_usage: dict[str, int] = {}

    def _bump_coverage(slice_: AssetSlice) -> None:
        for aid in slice_.asset_ids():
            coverage[aid] = coverage.get(aid, 0) + 1

    def _record(archetype: Archetype, slice_: AssetSlice) -> None:
        if archetype_usage.get(archetype.name, 0) >= per_archetype:
            # Respect per-archetype cap — the greedy fill will pick a
            # different archetype next time round.
            return
        plans.append((archetype, slice_))
        archetype_usage[archetype.name] = archetype_usage.get(archetype.name, 0) + 1
        _bump_coverage(slice_)

    # ── Pass 1: every join spec gets a plan ────────────────────────
    for js in join_specs:
        if len(plans) >= target:
            break
        left = js.get("left", {}) or {}
        right = js.get("right", {}) or {}
        left_asset = _resolve_asset_by_identifier(
            metadata_snapshot, left.get("identifier", ""),
        )
        right_asset = _resolve_asset_by_identifier(
            metadata_snapshot, right.get("identifier", ""),
        )
        if not left_asset or not right_asset:
            continue
        archetype = _pick_archetype_from_preferences(
            _JOIN_COVERAGE_ARCHETYPES, traits,
            exclude={n for n in archetype_usage if archetype_usage[n] >= per_archetype},
        )
        if archetype is None:
            continue
        slice_ = AssetSlice(
            tables=[left_asset, right_asset],
            metric_view=None,
            columns=(
                _top_k_columns(left_asset, column_k)
                + _top_k_columns(right_asset, column_k)
            ),
            join_spec=js,
        )
        _record(archetype, slice_)

    # ── Pass 2: every MV not already covered gets a plan ──────────
    for mv in metric_views:
        if len(plans) >= target:
            break
        mv_ident = (mv.get("identifier") or mv.get("name") or "").strip().lower()
        if mv_ident and coverage.get(mv_ident, 0) > 0:
            continue
        archetype = _pick_archetype_from_preferences(
            _MV_COVERAGE_ARCHETYPES, traits,
            exclude={n for n in archetype_usage if archetype_usage[n] >= per_archetype},
        )
        if archetype is None:
            continue
        slice_ = AssetSlice(
            tables=[],
            metric_view=mv,
            columns=_top_k_columns(mv, column_k),
        )
        _record(archetype, slice_)

    # ── Pass 3: every solo table not already covered gets a plan ──
    for t in tables:
        if len(plans) >= target:
            break
        tid = (t.get("identifier") or t.get("name") or "").strip().lower()
        if tid and coverage.get(tid, 0) > 0:
            continue
        archetype = _pick_archetype_from_preferences(
            _TABLE_COVERAGE_ARCHETYPES, traits,
            exclude={n for n in archetype_usage if archetype_usage[n] >= per_archetype},
        )
        if archetype is None:
            continue
        slice_ = AssetSlice(
            tables=[t],
            metric_view=None,
            columns=_top_k_columns(t, column_k),
        )
        _record(archetype, slice_)

    # ── Pass 4: greedy diversity fill ──────────────────────────────
    # Greedy: at each step pick the asset with the lowest current
    # coverage count; rotate archetype choice to keep shape variety.
    all_assets: list[tuple[str, dict, str]] = []  # (kind, asset_dict, identifier_lower)
    for t in tables:
        tid = (t.get("identifier") or t.get("name") or "").strip().lower()
        if tid:
            all_assets.append(("table", t, tid))
    for mv in metric_views:
        mid = (mv.get("identifier") or mv.get("name") or "").strip().lower()
        if mid:
            all_assets.append(("metric_view", mv, mid))

    if not all_assets:
        logger.info(
            "preflight.plan.small_space tables=0 mvs=0 — %d plans from joins only",
            len(plans),
        )
        return plans

    recent_archetypes: list[str] = []

    def _eligible_for_fill() -> list[Archetype]:
        names_capped = {
            n for n in archetype_usage if archetype_usage[n] >= per_archetype
        }
        return [
            a for a in _eligible_archetypes(traits)
            if a.name not in names_capped
        ]

    fallback_counter = 0
    FILL_CYCLE_LIMIT = target * 3  # guard against infinite loop when all caps exhausted

    while len(plans) < target and fallback_counter < FILL_CYCLE_LIMIT:
        fallback_counter += 1
        # Pick the asset with the smallest coverage count, break ties randomly.
        min_count = min(coverage.get(a[2], 0) for a in all_assets)
        least_covered = [a for a in all_assets if coverage.get(a[2], 0) == min_count]
        rng.shuffle(least_covered)
        kind, asset, ident = least_covered[0]

        fill_archetypes = _eligible_for_fill()
        if not fill_archetypes:
            # Every eligible archetype hit per-archetype cap — we're done.
            logger.info(
                "preflight.plan.archetype_caps_exhausted plans=%d target=%d",
                len(plans), target,
            )
            break
        # Rotate: prefer archetypes not used in the last 3 picks.
        candidates = [
            a for a in fill_archetypes if a.name not in recent_archetypes[-3:]
        ] or fill_archetypes
        archetype = rng.choice(candidates)
        recent_archetypes.append(archetype.name)

        if kind == "metric_view":
            slice_ = AssetSlice(
                tables=[], metric_view=asset,
                columns=_top_k_columns(asset, column_k),
            )
        else:
            slice_ = AssetSlice(
                tables=[asset], metric_view=None,
                columns=_top_k_columns(asset, column_k),
            )
        _record(archetype, slice_)

    if len(plans) < target:
        logger.info(
            "preflight.plan.small_space_fallback plans=%d target=%d "
            "tables=%d mvs=%d joins=%d",
            len(plans), target, len(tables), len(metric_views), len(join_specs),
        )

    return plans


# ═══════════════════════════════════════════════════════════════════════
# 3. Synthesis (LLM call, leak-free prompt rendering)
# ═══════════════════════════════════════════════════════════════════════


# Re-exported to tests so they can assert the prompt shape without
# importing from private config module structure.
__all__ = [
    "AssetSlice",
    "SynthesisContext",
    "plan_asset_coverage",
    "synthesize_preflight_candidate",
    "render_preflight_prompt",
]


_MAX_EXISTING_QUESTIONS_IN_PROMPT = 15
_MAX_QUESTION_LEN_IN_PROMPT = 160


def _format_slice_tables(slice_: AssetSlice) -> str:
    """Render the ``tables`` bullet block for the prompt."""
    if not slice_.tables:
        return "(none)"
    out: list[str] = []
    for t in slice_.tables:
        ident = (t.get("identifier") or t.get("name") or "").strip()
        desc = str(t.get("description", "") or "").strip()
        # Table descriptions can be list-of-strings in some snapshots.
        if isinstance(t.get("description"), list):
            desc = " ".join(
                str(x) for x in t.get("description", []) if isinstance(x, str)
            ).strip()
        if ident:
            out.append(
                f"- {ident}" + (f" — {desc[:120]}" if desc else "")
            )
    return "\n".join(out) if out else "(none)"


def _first_asset_identifier(slice_: AssetSlice) -> str:
    """Return one concrete, fully-qualified identifier for the slice so
    the prompt's qualification worked-example uses a real name from this
    schema. Falls back to a literal placeholder when the slice has no
    assets (defensive — planner always emits at least one table today).
    """
    for asset in list(slice_.tables) + (
        [slice_.metric_view] if slice_.metric_view is not None else []
    ):
        ident = (asset.get("identifier") or asset.get("name") or "").strip()
        if ident:
            return ident
    return "catalog.schema.table"


def _format_slice_metric_views(slice_: AssetSlice) -> str:
    if slice_.metric_view is None:
        return "(none)"
    mv = slice_.metric_view
    ident = (mv.get("identifier") or mv.get("name") or "").strip()
    desc = str(mv.get("description", "") or "").strip()
    if isinstance(mv.get("description"), list):
        desc = " ".join(
            str(x) for x in mv.get("description", []) if isinstance(x, str)
        ).strip()
    return f"- {ident}" + (f" — {desc[:120]}" if desc else "") if ident else "(none)"


def _format_slice_join_spec(slice_: AssetSlice) -> str:
    if slice_.join_spec is None:
        return "(none)"
    js = slice_.join_spec
    left = (js.get("left", {}) or {}).get("identifier", "")
    right = (js.get("right", {}) or {}).get("identifier", "")
    sql_field = js.get("sql", [])
    cond = (
        sql_field[0]
        if isinstance(sql_field, list) and sql_field
        else str(sql_field or "")
    )
    return f"- {left} <-> {right} ON {cond[:200]}" if left and right else "(none)"


def _column_description_lookup(slice_: AssetSlice) -> dict[tuple[str, str], str]:
    """Build a ``(table_identifier_lower, column_name_lower) -> description``
    map from the slice's assets. Descriptions come from any of
    ``columns[i]["description"]``, ``column_configs[i]["description"]``,
    measure/dimension descriptions on a metric view. First non-empty
    description wins to match existing ``_bump`` / enrichment semantics.
    """
    lookup: dict[tuple[str, str], str] = {}

    def _record(tid: str, cname: str, desc: Any) -> None:
        if not tid or not cname:
            return
        if isinstance(desc, list):
            desc = " ".join(str(x) for x in desc if isinstance(x, str))
        text = str(desc or "").strip()
        if not text:
            return
        key = (tid.strip().lower(), cname.strip().lower())
        lookup.setdefault(key, text)

    assets: list[dict] = list(slice_.tables)
    if slice_.metric_view is not None:
        assets.append(slice_.metric_view)
    for asset in assets:
        tid = (asset.get("identifier") or asset.get("name") or "").strip()
        for col in asset.get("columns", []) or []:
            if isinstance(col, dict):
                _record(tid, col.get("name", ""), col.get("description", ""))
        for cc in asset.get("column_configs", []) or []:
            if isinstance(cc, dict):
                _record(
                    tid,
                    cc.get("column_name", ""),
                    cc.get("description", ""),
                )
        for m in asset.get("measures", []) or []:
            if isinstance(m, dict):
                _record(tid, m.get("name", ""), m.get("description", ""))
        for d in asset.get("dimensions", []) or []:
            if isinstance(d, dict):
                _record(tid, d.get("name", ""), d.get("description", ""))
    return lookup


def _format_slice_columns(slice_: AssetSlice) -> str:
    """Render the ``Columns to prioritize`` bullet block.

    Extends the earlier bare ``- table.column`` lines with the column's
    description when description enrichment has populated one. Without
    this semantic channel the LLM has to guess what ``location_number``
    means (store count? branch code? category id?) which regularly
    produces filters on non-existent values. When descriptions are
    missing the line falls back to bare form (no crash).
    """
    if not slice_.columns:
        return "(none)"
    descs = _column_description_lookup(slice_)
    out: list[str] = []
    for tid, cname in slice_.columns:
        desc = descs.get((tid.strip().lower(), cname.strip().lower()), "")
        if desc:
            if len(desc) > 140:
                desc = desc[:140].rstrip() + "…"
            out.append(f"- {tid}.{cname}: {desc}")
        else:
            out.append(f"- {tid}.{cname}")
    return "\n".join(out)


def _format_slice_data_profile(
    slice_: AssetSlice, data_profile: dict | None,
) -> str:
    """Render the ``## Column value profile`` bullet block for the prompt.

    For each column in ``slice_.columns`` we look up the actual values
    observed on the warehouse (populated by ``preflight.py``) and print
    cardinality + a bounded sample of distinct values (string / low-
    cardinality categoricals) or a ``[min, max]`` range (numeric / date
    columns). High-cardinality columns render only the cardinality so
    the LLM sees they exist but knows not to filter them.

    Formatting mirrors :func:`optimizer._format_data_profile_for_prompt`
    so the LLM sees a consistent shape across description enrichment
    and synthesis. Caps enforced here:

    * at most :data:`PREFLIGHT_PROFILE_VALUES_CAP` distinct values per
      column, with ``+N more`` suffix when truncated;
    * each individual value string truncated to
      :data:`PREFLIGHT_PROFILE_VALUE_LEN_CAP` characters;
    * only columns actually in ``slice_.columns`` are rendered — the
      planner's ``_top_k_columns`` already bounds this to O(K) per
      asset so the total is naturally small.

    Graceful degradation: when ``data_profile`` is empty or the column
    is not present in the profile, return a ``(no profile available)``
    placeholder so the LLM still sees the section exists.
    """
    if not slice_.columns:
        return "(no columns to profile)"
    if not data_profile:
        return "(no profile available)"

    values_cap = max(1, int(PREFLIGHT_PROFILE_VALUES_CAP))
    val_len_cap = max(8, int(PREFLIGHT_PROFILE_VALUE_LEN_CAP))

    # Index the profile by (table_identifier_lower, column_name_lower) so we
    # can tolerate whatever case / nesting the warehouse sampler emitted.
    indexed: dict[tuple[str, str], dict] = {}
    for tbl, tinfo in (data_profile or {}).items():
        if not isinstance(tinfo, dict):
            continue
        tkey = str(tbl or "").strip().lower()
        for col, cinfo in (tinfo.get("columns") or {}).items():
            if isinstance(cinfo, dict):
                indexed[(tkey, str(col or "").strip().lower())] = cinfo

    def _trunc(val: Any) -> str:
        s = str(val)
        if len(s) > val_len_cap:
            return s[: val_len_cap - 1] + "…"
        return s

    out: list[str] = []
    for tid, cname in slice_.columns:
        key = (tid.strip().lower(), cname.strip().lower())
        cinfo = indexed.get(key)
        if not isinstance(cinfo, dict):
            out.append(f"- {tid}.{cname}: (no profile available)")
            continue

        parts: list[str] = []
        card = cinfo.get("cardinality")
        if card is not None:
            parts.append(f"cardinality={card}")

        vals = cinfo.get("distinct_values")
        if isinstance(vals, (list, tuple)) and vals:
            truncated = [_trunc(v) for v in list(vals)[:values_cap]]
            overflow = max(0, len(vals) - values_cap)
            rendered = "[" + ", ".join(repr(v) for v in truncated) + "]"
            if overflow:
                rendered += f" +{overflow} more"
            parts.append(f"values={rendered}")

        minv = cinfo.get("min")
        maxv = cinfo.get("max")
        if minv is not None or maxv is not None:
            parts.append(f"range=[{_trunc(minv)}, {_trunc(maxv)}]")

        suffix = ", ".join(parts) if parts else "(no profile available)"
        out.append(f"- {tid}.{cname} ({suffix})")

    return "\n".join(out)


def _build_empty_result_feedback(
    proposal: dict,
    data_profile: dict | None,
    slice_: AssetSlice,
) -> str:
    """Render the retry-feedback payload used by Phase 3.R6.

    The message tells the LLM that its last SQL returned zero rows and
    offers actual distinct values / ranges for the columns in the slice
    so it can regenerate with values that exist on this warehouse.

    Returns an empty string if there's nothing useful to say (no slice
    columns and no prior SQL) — the caller treats an empty string as
    "no retry feedback" and falls through to the normal prompt.
    """
    prior_sql = str(proposal.get("example_sql") or "").strip()
    profile_block = _format_slice_data_profile(slice_, data_profile or None)
    if not prior_sql and (not profile_block or profile_block.startswith("(")):
        return ""

    return (
        "Your previous query returned 0 rows on this warehouse:\n"
        f"  {prior_sql or '(no SQL captured)'}\n\n"
        "The filters likely picked values that do not exist in the data. "
        "Actual values / ranges for the profiled columns are:\n"
        f"{profile_block}\n\n"
        "Generate a new version of the query using ONLY values that "
        "exist above. If no suitable values exist for a filter, omit "
        "that filter instead of guessing."
    )


def _build_qualification_feedback(
    proposal: dict,
    slice_: AssetSlice,
    failure_reason: str,
) -> str:
    """Render the retry-feedback payload used by Phase 2.R6.

    Engages when the validator reports an unqualified-identifier or
    unresolved-column / unresolved-table failure. The feedback block
    names the exact failure, echoes the prior SQL (truncated), and
    lists the slice's fully-qualified identifiers — the only legal
    values — so the LLM can self-correct. Structurally mirrors
    :func:`_build_empty_result_feedback` so the prompt shape stays
    predictable across retry classes.
    """
    prior_sql = str(proposal.get("example_sql") or "").strip()
    if not prior_sql and not slice_.asset_ids():
        return ""
    allowlist_block = slice_.to_identifier_allowlist()
    truncated = prior_sql if len(prior_sql) <= 300 else (
        prior_sql[:300].rstrip() + "…"
    )
    reason = (failure_reason or "unresolved identifier").strip()
    return (
        "Your previous query failed validation:\n"
        f"  {reason}\n\n"
        "Your SQL was:\n"
        f"  {truncated or '(no SQL captured)'}\n\n"
        "The ONLY legal table identifiers for this example are:\n"
        f"{allowlist_block}\n\n"
        "Regenerate the example_sql using EXACTLY these identifiers — "
        "never short names, never aliases you haven't declared in this "
        "query's FROM clause. Preserve the question's intent."
    )


# Substrings that classify an execute-gate failure as an identifier /
# schema-resolution error rather than a data-value error. Used by R6
# to decide whether to fire the qualification-feedback retry.
_QUALIFICATION_FAILURE_MARKERS = (
    "UNRESOLVED_COLUMN",
    "UNRESOLVED_TABLE",
    "TABLE_OR_VIEW_NOT_FOUND",
    "UNQUALIFIED_TABLE",
)


def _is_qualification_failure(gate_result: Any) -> bool:
    """Return True when a ``GateResult`` indicates an unqualified or
    unresolved identifier. Matches both the new
    ``identifier_qualification`` gate (Phase 2.R5) and the execute
    gate's Spark-side errors (``UNRESOLVED_COLUMN`` etc.) so we can
    retry with the exact same feedback shape for both sources.
    """
    if gate_result is None or gate_result.passed:
        return False
    if gate_result.gate == "identifier_qualification":
        return True
    reason = (gate_result.reason or "").upper()
    return any(marker in reason for marker in _QUALIFICATION_FAILURE_MARKERS)


def _format_existing_questions(existing_questions: list[str]) -> str:
    """Render a short, truncated list of existing questions for anti-dup.

    Intent, not text, is what the LLM must avoid duplicating — the
    prompt says so — so we only need enough signal for the model to
    recognise overlap. Long prompts dilute attention.
    """
    if not existing_questions:
        return "(none)"
    out: list[str] = []
    for q in existing_questions[:_MAX_EXISTING_QUESTIONS_IN_PROMPT]:
        text = str(q or "").strip()
        if not text:
            continue
        if len(text) > _MAX_QUESTION_LEN_IN_PROMPT:
            text = text[:_MAX_QUESTION_LEN_IN_PROMPT].rstrip() + "…"
        out.append(f"- {text}")
    more = len(existing_questions) - _MAX_EXISTING_QUESTIONS_IN_PROMPT
    if more > 0:
        out.append(f"- (+{more} more not shown)")
    return "\n".join(out) if out else "(none)"


def render_preflight_prompt(
    archetype: Archetype,
    context: AssetSlice,
    existing_questions: list[str],
    *,
    data_profile: dict | None = None,
    retry_feedback: str | None = None,
) -> str:
    """Render :data:`PREFLIGHT_EXAMPLE_SYNTHESIS_PROMPT` for one candidate.

    No ``benchmarks`` parameter — leak-free by construction. The context's
    narrowed allowlist is the ONLY identifier universe the LLM sees.

    ``data_profile`` (optional): the metadata-snapshot ``_data_profile``
    map produced during pre-flight warehouse sampling. When present, the
    profile is rendered under ``## Column value profile`` and the LLM
    is instructed to quote values EXACTLY — this is what stops the
    ``SELECT ... WHERE country='XX'`` class of EMPTY_RESULT failures.
    When the profile would bloat the prompt past the token budget it is
    the first section dropped (see ``_truncate_to_budget`` priority list).

    ``retry_feedback`` (optional): rendered into the prompt as a
    ``## Retry feedback`` section. Used by the :ref:`R6 retry` path when
    the first attempt returned 0 rows — carries the previous SQL plus
    the actual column values from the profile so the LLM can self-correct.

    Historical note: ``context`` was previously named ``slice_`` when the
    only supported context was :class:`AssetSlice`. The signature is now
    context-typed; the slice-specific helpers below accept any object
    with the slice's attributes (``AssetSlice`` is the only shipped
    implementor today).
    """
    retry_block = ""
    if retry_feedback:
        retry_block = "## Retry feedback\n" + str(retry_feedback).strip()

    format_kwargs: dict[str, Any] = {
        "slice_tables": _format_slice_tables(context),
        "slice_metric_views": _format_slice_metric_views(context),
        "slice_join_spec": _format_slice_join_spec(context),
        "slice_columns": _format_slice_columns(context),
        "slice_data_profile": _format_slice_data_profile(context, data_profile),
        "schema_example_identifier": _first_asset_identifier(context),
        "archetype_name": archetype.name,
        "archetype_prompt_template": archetype.prompt_template,
        "archetype_output_shape": json.dumps(archetype.output_shape),
        "identifier_allowlist": context.to_identifier_allowlist(),
        "existing_questions_list": _format_existing_questions(existing_questions),
        "retry_feedback": retry_block,
    }
    # Budget safeguard: if the rendered prompt exceeds the configured
    # token budget we drop the data profile first (it's the newest /
    # largest addition and the LLM can still produce a shape-correct
    # query without it). ``slice_columns`` and ``identifier_allowlist``
    # are structurally load-bearing so they stay.
    from genie_space_optimizer.optimization.optimizer import _truncate_to_budget
    format_kwargs = _truncate_to_budget(
        format_kwargs,
        PREFLIGHT_EXAMPLE_SYNTHESIS_PROMPT,
        priority_keys=[
            "slice_data_profile",
            "existing_questions_list",
            "retry_feedback",
        ],
    )
    return format_mlflow_template(
        PREFLIGHT_EXAMPLE_SYNTHESIS_PROMPT,
        **format_kwargs,
    )


def synthesize_preflight_candidate(
    archetype: Archetype,
    context: AssetSlice,
    existing_questions: list[str],
    *,
    w: Any = None,
    llm_caller: Callable[[str], str] | None = None,
    data_profile: dict | None = None,
    retry_feedback: str | None = None,
) -> dict | None:
    """One LLM call, one candidate. Returns a proposal dict or ``None``.

    The returned dict is shaped like a ``synthesis.py`` proposal so it
    can feed directly into :func:`validate_synthesis_proposal`:

    ``{patch_type: "add_example_sql", example_question, example_sql,
       rationale, usage_guidance, ...}``.

    ``llm_caller`` is injected for tests (takes prompt → raw text).
    Production path uses :func:`_traced_llm_call` with span
    ``"preflight_example_synthesis"``.

    ``data_profile`` and ``retry_feedback`` are passed through to
    :func:`render_preflight_prompt`; the retry path (R6) uses
    ``retry_feedback`` to ask the LLM to regenerate after an
    EMPTY_RESULT on the first attempt.
    """
    prompt = render_preflight_prompt(
        archetype,
        context,
        existing_questions,
        data_profile=data_profile,
        retry_feedback=retry_feedback,
    )
    logger.debug(
        "preflight.synth.prompt archetype=%s slice_assets=%s prompt_len=%d retry=%s\n"
        "---PROMPT---\n%s\n---END---",
        archetype.name,
        context.asset_ids(),
        len(prompt),
        "yes" if retry_feedback else "no",
        prompt,
    )

    def _call() -> str:
        if llm_caller is not None:
            return llm_caller(prompt)
        from genie_space_optimizer.optimization.optimizer import _traced_llm_call
        try:
            raw, _ = _traced_llm_call(
                w, "You are a SQL example author.", prompt,
                span_name="preflight_example_synthesis",
            )
            return raw
        except Exception:
            logger.warning(
                "preflight.synth.llm_call_failed archetype=%s", archetype.name,
                exc_info=True,
            )
            return ""

    raw = _call()
    # Reuse synthesis.py's JSON extractor — handles fenced blocks + inline.
    from genie_space_optimizer.optimization.synthesis import _extract_json_proposal
    proposal = _extract_json_proposal(raw)
    if not proposal:
        return None

    # Shape defaults so ``validate_synthesis_proposal`` sees the fields it
    # expects. ``patch_type`` must match the archetype; ``usage_guidance``
    # is surfaced by the harness applier as the example's instruction text.
    proposal.setdefault("patch_type", archetype.patch_type)
    if "usage_guidance" not in proposal:
        # Fall back to rationale when the LLM omitted usage_guidance —
        # the applier surfaces this to Genie as query-selection guidance.
        proposal["usage_guidance"] = str(proposal.get("rationale") or "").strip()
    return proposal


# ═══════════════════════════════════════════════════════════════════════
# 4. Orchestrator — threshold gate, planning, validation, apply
# ═══════════════════════════════════════════════════════════════════════


def _canonicalize_sql_fingerprint(sql: str) -> str:
    """Very cheap fingerprint for pairwise dedup within a single run.

    Lowercases, collapses whitespace, strips trailing semicolons. This
    is NOT the benchmark firewall fingerprint — that's intentionally
    stricter and lives in :mod:`leakage`. Here we just want
    "essentially the same SQL" within our own overdraw pool.
    """
    if not isinstance(sql, str):
        return ""
    s = sql.strip().rstrip(";").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def _existing_example_sqls(metadata_snapshot: dict) -> list[dict]:
    instr = metadata_snapshot.get("instructions", {}) or {}
    return [
        ex for ex in (instr.get("example_question_sqls", []) or [])
        if isinstance(ex, dict)
    ]


def _existing_example_question_list(metadata_snapshot: dict) -> list[str]:
    out: list[str] = []
    for ex in _existing_example_sqls(metadata_snapshot):
        q = ex.get("question", "")
        if isinstance(q, list):
            q = " ".join(str(x) for x in q)
        text = str(q).strip()
        if text:
            out.append(text)
    return out


def _existing_example_fingerprints(
    metadata_snapshot: dict,
) -> set[str]:
    """Fingerprints of already-applied example SQLs; used for dedup.

    Returns lower-cased, whitespace-collapsed SQL bodies.
    """
    out: set[str] = set()
    for ex in _existing_example_sqls(metadata_snapshot):
        sql = ex.get("sql", "")
        if isinstance(sql, list):
            sql = " ".join(str(x) for x in sql)
        fp = _canonicalize_sql_fingerprint(str(sql))
        if fp:
            out.add(fp)
    return out


def _apply_preflight_proposals(
    proposals: list[dict],
    *,
    w: Any,
    spark: Any,
    run_id: str,
    space_id: str,
    metadata_snapshot: dict,
    config: dict,
    benchmarks: list[dict] | None,
    catalog: str,
    schema: str,
) -> int:
    """Hand proposals to the shared applier; firewall runs inline.

    Separated into its own function so unit tests can stub out the
    harness-level applier without rebuilding the patch pipeline.
    """
    if not proposals:
        return 0
    from genie_space_optimizer.optimization.harness import (
        _apply_proactive_example_sqls,
    )
    _apply_proactive_example_sqls(
        w, spark, run_id, space_id, proposals,
        metadata_snapshot, config, catalog, schema,
        benchmarks=benchmarks,
    )
    return len(proposals)


def run_preflight_example_synthesis(
    w: Any,
    spark: Any,
    run_id: str,
    space_id: str,
    config: dict,
    metadata_snapshot: dict,
    *,
    benchmarks: list[dict] | None,
    catalog: str,
    schema: str,
    warehouse_id: str = "",
    llm_caller: Callable[[str], str] | None = None,
    rng: random.Random | None = None,
    target: int | None = None,
    enforce_genie_agreement: bool = False,
    genie_ask: Callable[[Any, str, str], dict] | None = None,
    warehouse_executor: Callable[[str], list[dict]] | None = None,
    arbiter: Callable[..., dict] | None = None,
) -> dict:
    """Run pre-flight synthesis to fill example_question_sqls up to target.

    Idempotent + threshold-gated: ``need = max(0, target - existing)``.
    When ``need == 0`` the stage returns immediately with no LLM / no
    warehouse activity.

    Parameters
    ----------
    w, spark, run_id, space_id, config, metadata_snapshot, catalog, schema
        Standard harness context.
    benchmarks
        Current run's benchmark corpus — plumbed through to the applier
        so the leakage firewall runs inline. Required (pass an empty
        list to explicitly disable the firewall; tests use ``[]``).
    warehouse_id
        Warehouse for EXPLAIN / execute gates.
    llm_caller
        Injected LLM callable for tests. Defaults to the real
        ``_traced_llm_call`` with span ``"preflight_example_synthesis"``.
    rng
        Optional deterministic RNG for tests.
    target
        Override for :data:`PREFLIGHT_EXAMPLE_SQL_TARGET`. Tests pass a
        small value to exercise the threshold gate without generating
        20 LLM calls.

    Returns
    -------
    dict
        ``{applied, need, existing, generated, passed_parse,
        passed_execute, passed_firewall, passed_structural, passed_arbiter,
        dedup_rejected, rejected_by_gate, asset_coverage,
        archetype_distribution, skipped_reason}``.
    """
    from genie_space_optimizer.common.config import (
        PREFLIGHT_EXAMPLE_SQL_TARGET as CFG_TARGET,
    )
    effective_target = CFG_TARGET if target is None else target

    existing_sqls = _existing_example_sqls(metadata_snapshot)
    existing_count = len(existing_sqls)
    need = max(0, effective_target - existing_count)

    result: dict = {
        "applied": 0,
        "need": need,
        "existing": existing_count,
        "target": effective_target,
        "generated": 0,
        "passed_parse": 0,
        "passed_identifier_qualification": 0,
        "passed_execute": 0,
        "passed_firewall": 0,
        "passed_structural": 0,
        "passed_arbiter": 0,  # synthesis.py's in-pipeline arbiter gate (no-op today)
        "passed_genie_agreement": 0,  # P2 Genie-vs-synthesized gate (opt-in)
        "dedup_rejected": 0,
        "rejected_by_gate": {},
        "asset_coverage": {},
        "archetype_distribution": {},
        "skipped_reason": None,
        # Operator diagnostics — populated after we run the planner so
        # ``_print_summary`` can explain WHY the generated count is low
        # without the operator having to grep debug logs.
        "traits": [],
        "eligible_archetypes": [],
        # Per-candidate gate-rejection reasons — bounded list so the
        # summary can surface WHY candidates died (same observability
        # pattern as SQL Expression Seeding's ``rejected_examples``).
        "gate_rejected_examples": [],
    }

    # Cap the rejection list so a pathological run doesn't balloon the
    # result dict or the write_stage detail payload.
    _MAX_GATE_REJECTED_EXAMPLES = 10

    def _record_gate_rejection(gate: str, reason: str, proposal: dict | None) -> None:
        if len(result["gate_rejected_examples"]) >= _MAX_GATE_REJECTED_EXAMPLES:
            return
        _question = ""
        _sql = ""
        if isinstance(proposal, dict):
            _question = str(proposal.get("example_question") or "")
            _sql = str(proposal.get("example_sql") or "")
        result["gate_rejected_examples"].append({
            "gate": gate,
            "reason": (reason or "")[:200],
            "question_prefix": _question[:120],
            "sql_prefix": _sql[:120],
        })

    # Pre-compute traits so both the empty-traits fingerprint warning
    # and the summary block have them. Also useful when the planner
    # returns zero plans (see no_eligible_plans branch below).
    result["traits"] = sorted(schema_traits(metadata_snapshot))
    result["eligible_archetypes"] = [
        a.name for a in _eligible_archetypes(set(result["traits"]))
    ]

    if need == 0:
        result["skipped_reason"] = "at_target"
        _print_summary(result)
        logger.info(
            "preflight.synthesis.summary existing=%d target=%d need=0 skipped=at_target",
            existing_count, effective_target,
        )
        return result

    # ── Plan ──────────────────────────────────────────────────────
    plans = plan_asset_coverage(metadata_snapshot, need=need, rng=rng)
    if not plans:
        result["skipped_reason"] = "no_eligible_plans"
        _print_summary(result)
        logger.info(
            "preflight.synthesis.summary existing=%d target=%d need=%d "
            "skipped=no_eligible_plans",
            existing_count, effective_target, need,
        )
        return result

    # ── Build benchmark corpus for the firewall gate ──────────────
    benchmark_corpus = None
    try:
        from genie_space_optimizer.optimization.leakage import BenchmarkCorpus
        benchmark_corpus = BenchmarkCorpus.from_benchmarks(benchmarks or [])
    except Exception:
        logger.warning(
            "preflight.synthesis.benchmark_corpus_unavailable — firewall gate "
            "will treat empty corpus as safe",
            exc_info=True,
        )

    # ``validate_synthesis_proposal`` is imported at module scope so tests
    # can patch it on ``preflight_synthesis``.

    # ── State for per-candidate dedup within the run ─────────────
    existing_fps = _existing_example_fingerprints(metadata_snapshot)
    run_fps: set[str] = set()
    existing_questions = _existing_example_question_list(metadata_snapshot)

    # Phase 2.R2b: the warehouse sampler populates ``_data_profile`` on
    # the metadata snapshot during pre-flight. Thread it through to the
    # synthesis prompt so the LLM filters on values that actually exist.
    data_profile = metadata_snapshot.get("_data_profile") or {}

    accepted: list[dict] = []
    reject_by_gate: dict[str, int] = {}
    archetype_counts: dict[str, int] = {}
    asset_counts: dict[str, int] = {}
    # Phase 3.R6 retry counters — exposed in the summary so operators
    # can see how often retries fired and how often they succeeded.
    retries_fired = 0
    retries_succeeded = 0
    retries_still_empty = 0
    # Phase 2.R6: separate counters for the qualification retry path so
    # the summary block can attribute retry volume to the right class.
    retries_on_qualification_fired = 0
    retries_on_qualification_succeeded = 0

    for archetype, slice_ in plans:
        if len(accepted) >= need:
            break
        result["generated"] += 1
        archetype_counts[archetype.name] = archetype_counts.get(archetype.name, 0) + 1

        # ── Synthesize ────────────────────────────────────────────
        # Seed existing_questions with run-accepted ones so the LLM
        # diversifies within the current pool as well.
        anti_dup_questions = existing_questions + [
            p.get("example_question", "") for p in accepted
        ]
        proposal = synthesize_preflight_candidate(
            archetype, slice_, anti_dup_questions,
            w=w, llm_caller=llm_caller,
            data_profile=data_profile,
        )
        if proposal is None:
            reject_by_gate["synthesize_none"] = reject_by_gate.get("synthesize_none", 0) + 1
            _record_gate_rejection(
                "synthesize_none",
                f"archetype={archetype.name}: LLM returned no usable proposal",
                None,
            )
            continue

        # ── Validate via the shared 5-gate ────────────────────────
        slice_allowlist = set(slice_.asset_ids())
        passed, gate_results = validate_synthesis_proposal(
            proposal,
            archetype=archetype,
            benchmark_corpus=benchmark_corpus,
            metadata_snapshot=metadata_snapshot,
            blame_set=None,  # pre-flight has no failure cluster
            spark=spark, catalog=catalog, gold_schema=schema,
            w=w, warehouse_id=warehouse_id,
            identifier_allowlist=slice_allowlist,
        )

        # ── Phase 3.R6 + Phase 2.R6: one retry round-trip ─────────
        # Two retry classes share the same one-retry budget:
        #   - EMPTY_RESULT → feedback carries profile values.
        #   - Unqualified / unresolved identifier → feedback carries
        #     the slice's identifier allowlist so the LLM can self-
        #     correct the FROM/JOIN target.
        # R5's soft-accept classifier applies to the retry's SQL, so
        # the retry may land on an empty-result SQL that still
        # soft-accepts when it carries a WHERE/JOIN.
        if not passed:
            first_fail = next(
                (g for g in gate_results if not g.passed), None,
            )
            feedback: str | None = None
            retry_class: str | None = None
            if (
                first_fail is not None
                and first_fail.gate == "execute"
                and "EMPTY_RESULT" in (first_fail.reason or "")
            ):
                retry_class = "empty_result"
                retries_fired += 1
                feedback = _build_empty_result_feedback(
                    proposal, data_profile, slice_,
                ) or None
            elif _is_qualification_failure(first_fail):
                retry_class = "qualification"
                retries_on_qualification_fired += 1
                feedback = _build_qualification_feedback(
                    proposal, slice_, first_fail.reason or "",
                ) or None

            if retry_class is not None:
                retry_proposal = synthesize_preflight_candidate(
                    archetype, slice_, anti_dup_questions,
                    w=w, llm_caller=llm_caller,
                    data_profile=data_profile,
                    retry_feedback=feedback,
                )
                if retry_proposal is not None:
                    proposal = retry_proposal
                    passed, gate_results = validate_synthesis_proposal(
                        retry_proposal,
                        archetype=archetype,
                        benchmark_corpus=benchmark_corpus,
                        metadata_snapshot=metadata_snapshot,
                        blame_set=None,
                        spark=spark, catalog=catalog, gold_schema=schema,
                        w=w, warehouse_id=warehouse_id,
                        identifier_allowlist=slice_allowlist,
                    )
                    if passed:
                        if retry_class == "empty_result":
                            retries_succeeded += 1
                        else:
                            retries_on_qualification_succeeded += 1
                    else:
                        retry_fail = next(
                            (g for g in gate_results if not g.passed), None,
                        )
                        if (
                            retry_class == "empty_result"
                            and retry_fail is not None
                            and retry_fail.gate == "execute"
                            and "EMPTY_RESULT" in (retry_fail.reason or "")
                        ):
                            retries_still_empty += 1

        # Per-gate counters — passed_*  and rejected_by_gate reflect
        # the same ordering as the pipeline so operators can see where
        # the bottleneck is.
        for gr in gate_results:
            if gr.passed:
                key_map = {
                    "parse": "passed_parse",
                    "identifier_qualification": "passed_identifier_qualification",
                    "execute": "passed_execute",
                    "structural": "passed_structural",
                    "arbiter": "passed_arbiter",
                    "firewall": "passed_firewall",
                }
                bucket = key_map.get(gr.gate)
                if bucket:
                    result[bucket] = result[bucket] + 1
            else:
                reject_by_gate[gr.gate] = reject_by_gate.get(gr.gate, 0) + 1
                _record_gate_rejection(gr.gate, gr.reason, proposal)
                break  # first fail short-circuits the rest
        if not passed:
            continue

        # ── P2 Genie-vs-synthesized agreement (opt-in) ────────────
        if enforce_genie_agreement:
            agreement = _gate_genie_agreement(
                proposal,
                space_id=space_id,
                w=w, warehouse_id=warehouse_id,
                catalog=catalog, gold_schema=schema,
                metadata_snapshot=metadata_snapshot,
                genie_ask=genie_ask,
                warehouse_executor=warehouse_executor,
                arbiter=arbiter,
            )
            if not agreement.passed:
                reject_by_gate["genie_agreement"] = (
                    reject_by_gate.get("genie_agreement", 0) + 1
                )
                logger.info(
                    "preflight.arbiter.rejected reason=%s question=%r",
                    agreement.reason, (proposal.get("example_question") or "")[:80],
                )
                _record_gate_rejection("genie_agreement", agreement.reason, proposal)
                continue
            result["passed_genie_agreement"] += 1

        # ── Dedup: vs existing config + pairwise within this run ──
        fp = _canonicalize_sql_fingerprint(proposal.get("example_sql", ""))
        if not fp:
            reject_by_gate["empty_sql_post_validate"] = (
                reject_by_gate.get("empty_sql_post_validate", 0) + 1
            )
            _record_gate_rejection(
                "empty_sql_post_validate",
                "proposal passed gates but canonical fingerprint was empty",
                proposal,
            )
            continue
        if fp in existing_fps or fp in run_fps:
            result["dedup_rejected"] += 1
            continue

        accepted.append(proposal)
        run_fps.add(fp)
        for aid in slice_.asset_ids():
            asset_counts[aid] = asset_counts.get(aid, 0) + 1

    # ── Apply (firewall runs inline) ──────────────────────────────
    applied = _apply_preflight_proposals(
        accepted[:need],
        w=w, spark=spark, run_id=run_id, space_id=space_id,
        metadata_snapshot=metadata_snapshot, config=config,
        benchmarks=benchmarks, catalog=catalog, schema=schema,
    )

    result["applied"] = applied
    result["rejected_by_gate"] = reject_by_gate
    result["archetype_distribution"] = archetype_counts
    result["asset_coverage"] = asset_counts
    result["retries_fired"] = retries_fired
    result["retries_succeeded"] = retries_succeeded
    result["retries_still_empty"] = retries_still_empty
    result["retries_on_qualification_fired"] = retries_on_qualification_fired
    result["retries_on_qualification_succeeded"] = (
        retries_on_qualification_succeeded
    )

    # ── Observability ─────────────────────────────────────────────
    logger.info(
        "preflight.synthesis.summary existing=%d target=%d need=%d generated=%d "
        "passed_parse=%d passed_identifier_qualification=%d passed_execute=%d "
        "passed_firewall=%d passed_structural=%d "
        "passed_arbiter=%d passed_genie_agreement=%d dedup_rejected=%d applied=%d "
        "retries_fired=%d retries_succeeded=%d retries_still_empty=%d "
        "retries_on_qualification_fired=%d retries_on_qualification_succeeded=%d "
        "asset_coverage=%s rejected_by_gate=%s archetype=%s",
        existing_count, effective_target, need, result["generated"],
        result["passed_parse"],
        result.get("passed_identifier_qualification", 0),
        result["passed_execute"],
        result["passed_firewall"], result["passed_structural"],
        result["passed_arbiter"], result["passed_genie_agreement"],
        result["dedup_rejected"],
        applied,
        retries_fired, retries_succeeded, retries_still_empty,
        retries_on_qualification_fired, retries_on_qualification_succeeded,
        asset_counts, reject_by_gate, archetype_counts,
    )

    # Phase 4.R8: raise the severity on under-target runs so operators
    # get a grep-able signal when applied < need and candidates were
    # rejected at the gates. The per-candidate rejection list (same
    # content that ``_print_summary`` shows) goes into the warning so
    # we don't need two passes through the log.
    gate_rejected_examples = result.get("gate_rejected_examples") or []
    if applied < need and gate_rejected_examples:
        rejection_brief = "; ".join(
            f"[{ex.get('gate', '?')}] "
            f"{(ex.get('question_prefix') or ex.get('sql_prefix') or '')[:60]}"
            f" — {(ex.get('reason') or '')[:120]}"
            for ex in gate_rejected_examples[:3]
        )
        logger.warning(
            "preflight.synthesis.under_target applied=%d need=%d "
            "rejected_by_gate=%s retries_fired=%d retries_still_empty=%d "
            "top_rejections=%s",
            applied, need, reject_by_gate,
            retries_fired, retries_still_empty,
            rejection_brief,
        )

    _print_summary(result)
    return result


def _print_summary(result: dict) -> None:
    """Pretty-print the enrichment run block for the pre-flight stage.

    Imports from ``harness`` helpers lazily so this module doesn't need
    the harness at import time (matters for tests that exercise the
    planner alone).
    """
    try:
        from genie_space_optimizer.optimization.harness import (
            _bar, _kv, _section,
        )
    except Exception:
        # Fallback when harness import fails (e.g. in a narrow unit
        # test). Just use simple formatting.
        def _section(title: str, char: str = "-") -> str:
            return f"-- {title} " + (char * 10)

        def _kv(k: str, v: object, indent: int = 2) -> str:
            return f"{' ' * indent}|  {k:30s}{v}"

        def _bar(char: str = "-") -> str:
            return char * 78

    lines: list[str] = [_section("PRE-FLIGHT EXAMPLE SQL SYNTHESIS")]
    lines.append(_kv("Target", result.get("target", "")))
    lines.append(_kv("Existing examples", result.get("existing", 0)))
    lines.append(_kv("Need", result.get("need", 0)))
    traits = result.get("traits") or []
    eligible = result.get("eligible_archetypes") or []
    if traits or eligible:
        lines.append(_kv(
            "Traits detected",
            ", ".join(traits) if traits else "(none)",
        ))
        lines.append(_kv(
            "Eligible archetypes",
            f"{len(eligible)} — {', '.join(eligible) if eligible else '(none)'}",
        ))
    if result.get("skipped_reason"):
        lines.append(_kv("Status", f"skipped — {result['skipped_reason']}"))
        lines.append(_bar())
        print("\n".join(lines))
        return
    lines.append(_kv("Generated candidates", result.get("generated", 0)))
    lines.append(_kv("Passed parse", result.get("passed_parse", 0)))
    # Phase 2.R5: identifier-qualification gate, inserted between parse
    # and execute. Surfaced only when the gate has actually seen
    # candidates (so legacy runs that pre-date the gate don't print a
    # zero-line).
    qual_passed = result.get("passed_identifier_qualification", 0)
    if qual_passed or "identifier_qualification" in (
        result.get("rejected_by_gate") or {}
    ):
        lines.append(_kv(
            "Passed identifier_qualification", qual_passed,
        ))
    lines.append(_kv("Passed EXPLAIN+execute", result.get("passed_execute", 0)))
    # Phase 3.R6 retry visibility: surfaced right under the execute line
    # so operators can see how often empty-result retries fired and
    # whether they recovered.
    retries_fired = result.get("retries_fired", 0)
    if retries_fired:
        lines.append(_kv(
            "  retries on EMPTY_RESULT",
            f"fired={retries_fired} "
            f"succeeded={result.get('retries_succeeded', 0)} "
            f"still_empty={result.get('retries_still_empty', 0)}",
            indent=4,
        ))
    # Phase 2.R6 retry visibility for the qualification path.
    qual_retries = result.get("retries_on_qualification_fired", 0)
    if qual_retries:
        lines.append(_kv(
            "  retries on qualification",
            f"fired={qual_retries} "
            f"succeeded={result.get('retries_on_qualification_succeeded', 0)}",
            indent=4,
        ))
    lines.append(_kv("Passed firewall", result.get("passed_firewall", 0)))
    lines.append(_kv("Passed structural", result.get("passed_structural", 0)))
    lines.append(_kv("Passed arbiter gate", result.get("passed_arbiter", 0)))
    lines.append(_kv("Passed genie agreement", result.get("passed_genie_agreement", 0)))
    lines.append(_kv("Dedup rejected", result.get("dedup_rejected", 0)))
    lines.append(_kv("Applied", result.get("applied", 0)))

    coverage = result.get("asset_coverage", {}) or {}
    if coverage:
        lines.append(_kv("Assets touched", len(coverage)))
        for aid, count in sorted(coverage.items(), key=lambda x: -x[1])[:8]:
            lines.append(_kv(f"  {aid}", count, indent=4))

    archetypes = result.get("archetype_distribution", {}) or {}
    if archetypes:
        lines.append(_kv("Archetype distribution", ""))
        for name, count in sorted(archetypes.items(), key=lambda x: -x[1]):
            lines.append(_kv(f"  {name}", count, indent=4))

    rejections = result.get("rejected_by_gate", {}) or {}
    if rejections:
        lines.append(_kv(
            "Rejected by gate",
            ", ".join(f"{k}={v}" for k, v in rejections.items()),
        ))

    # Per-candidate rejection reasons — surfaced so operators can see
    # WHY candidates died without having to grep the job log. Bounded
    # list lives on ``result["gate_rejected_examples"]``; we print the
    # first 3.
    rejection_examples = result.get("gate_rejected_examples") or []
    if rejection_examples:
        lines.append(_kv("Rejection examples (up to 3)", ""))
        for _ex in rejection_examples[:3]:
            _gate = _ex.get("gate") or ""
            _question = (_ex.get("question_prefix") or "").strip()
            _reason = (_ex.get("reason") or "").strip()
            _label = _question[:60] if _question else (_ex.get("sql_prefix") or "")[:60]
            lines.append(_kv(
                f"  [{_gate}] {_label}",
                _reason[:140],
                indent=4,
            ))
    lines.append(_bar())
    print("\n".join(lines))


# ═══════════════════════════════════════════════════════════════════════
# 5. Genie-vs-synthesized arbiter gate (P2)
# ═══════════════════════════════════════════════════════════════════════
#
# Extra confidence gate: ask Genie the synthesized question, execute both
# Genie's SQL and the synthesized SQL against the warehouse, and call the
# arbiter on each result-set. Keep iff BOTH verdicts are "yes" — i.e.
# the example is a reinforcement that matches what Genie would produce
# for the current config plus what the LLM proposed. Failures on either
# side reject the candidate.
#
# Wired into the orchestrator through ``enforce_genie_agreement=True``
# (off by default so P1 ships independently; flip the flag to enable P2).


# Genie query result cap for the arbiter comparison. Larger than the
# execution-gate LIMIT 1 because we want enough rows for the arbiter
# to sanity-check the result set shape.
_PREFLIGHT_ARBITER_ROW_CAP = 20


def _rows_from_warehouse_df(df: Any, max_rows: int) -> list[dict]:
    """Best-effort conversion of a warehouse DataFrame into a plain list.

    Handles pandas DataFrame + any other object with ``to_dict``.
    Empty / None → ``[]``. Used for arbiter result-sample input.
    """
    if df is None:
        return []
    try:
        head = df.head(max_rows) if hasattr(df, "head") else df[:max_rows]
    except Exception:
        head = df
    try:
        records = head.to_dict(orient="records")
    except Exception:
        try:
            records = list(head)
        except Exception:
            return []
    return records[:max_rows] if records else []


def _ask_genie_for_question(
    w: Any,
    space_id: str,
    question: str,
    *,
    max_wait: int = 120,
) -> dict:
    """Thin wrapper over :func:`common.genie_client.run_genie_query`.

    Keeps the import local so tests that never call this gate don't
    have to mock the Genie SDK surface.
    """
    try:
        from genie_space_optimizer.common.genie_client import run_genie_query
    except Exception as exc:
        logger.warning("preflight.arbiter: cannot import run_genie_query: %s", exc)
        return {"status": "ERROR", "sql": None, "error": str(exc)}
    try:
        return run_genie_query(w, space_id, question, max_wait=max_wait)
    except Exception as exc:
        logger.warning(
            "preflight.arbiter: run_genie_query raised: %s", exc,
        )
        return {"status": "ERROR", "sql": None, "error": str(exc)}


def _execute_warehouse_limit(
    sql: str, w: Any, warehouse_id: str, catalog: str, schema: str,
    max_rows: int = _PREFLIGHT_ARBITER_ROW_CAP,
) -> list[dict]:
    """Execute ``sql`` via the warehouse wrapped in ``SELECT * FROM (...) LIMIT N``.

    Returns plain row dicts. Defensive: any failure → ``[]`` with a
    WARNING log so the arbiter can still run (the arbiter is then asked
    whether an empty result is correct for the question).
    """
    if not sql or not warehouse_id or not w:
        return []
    wrapped = f"SELECT * FROM ({sql.rstrip(';').strip()}) _preflight LIMIT {int(max_rows)}"
    try:
        from genie_space_optimizer.optimization.evaluation import (
            _execute_sql_via_warehouse,
        )
        df = _execute_sql_via_warehouse(
            w, warehouse_id, wrapped,
            catalog=catalog, schema=schema,
        )
    except Exception as exc:
        logger.info(
            "preflight.arbiter.execute_failed sql=%r err=%s",
            sql[:80], str(exc)[:200],
        )
        return []
    return _rows_from_warehouse_df(df, max_rows)


def _gate_genie_agreement(
    candidate: dict,
    *,
    space_id: str,
    w: Any,
    warehouse_id: str,
    catalog: str,
    gold_schema: str,
    metadata_snapshot: dict,
    genie_ask: Callable[[Any, str, str], dict] | None = None,
    warehouse_executor: Callable[[str], list[dict]] | None = None,
    arbiter: Callable[..., dict] | None = None,
) -> GateResult:
    """Genie-vs-synthesized arbiter gate for pre-flight synthesis (P2).

    Pipeline:

    1. Ask Genie the candidate's question (reuses
       :func:`common.genie_client.run_genie_query`).
    2. Execute Genie's returned SQL AND the synthesized SQL against the
       warehouse with ``LIMIT _PREFLIGHT_ARBITER_ROW_CAP``.
    3. Run :func:`score_example_sql_correctness` on BOTH result-sets.
    4. Pass iff both arbiter verdicts are ``"yes"`` (``both_correct``
       mode — see the planning discussion for why this is the only
       supported mode).

    Injection points (``genie_ask``, ``warehouse_executor``, ``arbiter``)
    let tests exercise every branch of the gate without touching the
    Databricks SDK. Production callers leave them ``None``.

    Returns a :class:`synthesis.GateResult`. Caller decides whether to
    enforce; the orchestrator wires this gate via
    ``enforce_genie_agreement=True``.
    """
    question = str(candidate.get("example_question") or "").strip()
    synth_sql = str(candidate.get("example_sql") or "").strip()
    if not question or not synth_sql:
        return GateResult(False, "genie_agreement", "missing_question_or_sql")

    # ── Step 1: ask Genie ──────────────────────────────────────────
    if genie_ask is None:
        genie_response = _ask_genie_for_question(w, space_id, question)
    else:
        genie_response = genie_ask(w, space_id, question)
    genie_sql = str(genie_response.get("sql") or "").strip() if isinstance(
        genie_response, dict,
    ) else ""
    if not genie_sql:
        return GateResult(False, "genie_agreement", "genie_no_sql")

    # ── Step 2: execute both SQLs ──────────────────────────────────
    if warehouse_executor is None:
        def _exec(sql: str) -> list[dict]:
            return _execute_warehouse_limit(
                sql, w, warehouse_id, catalog, gold_schema,
            )
    else:
        _exec = warehouse_executor
    genie_rows = _exec(genie_sql)
    synth_rows = _exec(synth_sql)

    # ── Step 3: arbiter on both ────────────────────────────────────
    if arbiter is None:
        from genie_space_optimizer.optimization.scorers.arbiter import (
            score_example_sql_correctness,
        )
        _arbiter: Callable[..., dict] = score_example_sql_correctness
    else:
        _arbiter = arbiter

    try:
        genie_verdict = _arbiter(
            question=question, sql=genie_sql, result_rows=genie_rows,
            w=w, metadata_snapshot=metadata_snapshot,
        )
        synth_verdict = _arbiter(
            question=question, sql=synth_sql, result_rows=synth_rows,
            w=w, metadata_snapshot=metadata_snapshot,
        )
    except Exception as exc:
        logger.warning("preflight.arbiter.exception: %s", exc)
        return GateResult(False, "genie_agreement", f"arbiter_error:{exc}")

    gv = str((genie_verdict or {}).get("value", "")).lower()
    sv = str((synth_verdict or {}).get("value", "")).lower()

    # ── Step 4: both_correct mode ──────────────────────────────────
    if gv == "yes" and sv == "yes":
        return GateResult(True, "genie_agreement", "both_correct")
    return GateResult(
        False, "genie_agreement", f"genie={gv} synth={sv}",
    )

