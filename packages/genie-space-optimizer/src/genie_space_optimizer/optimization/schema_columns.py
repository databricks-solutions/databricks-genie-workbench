"""Trial 13i — Stage 1 ``schema_columns`` plumbing + seed FQN normalization.

Background
----------
Trial 13h tightened the Stage 1 prompt so the LLM declines with
``insufficient_blame_set`` when ``schema_columns`` is empty and the
``blame_set_seed`` entries are not 4-part FQNs (``catalog.schema.table.column``).
The post-13h workbench replay on ``98ec_from_capture`` and
``dc89_from_capture`` then surfaced a deeper bottleneck: the SM /
workbench lane never plumbs ``ctx.schema_columns`` at all. The LLM
literally received ``"schema_columns": []`` in every Stage 1 prompt,
which made the contract's "seeds are schema-valid" promise vacuous
and forced every capture-only QID into a decline.

This module is the single source of truth for two coupled helpers:

* :func:`_derive_schema_columns` — fan-in over three sources
  (``metadata_snapshot``, typed RCA evidence union, identifier
  allowlist) into the run-level FQN list every Stage 1 caller passes
  to ``diagnose_failing_qids``. Mirrors the inlined fallback the Plan
  11 batch lane has at ``optimizer.py:_decide_and_run_plan11_dispatch``
  but with explicit source labelling for the
  ``GSO_PLAN11_STAGE1_INPUT_QUALITY_V1`` observability marker.
* :func:`_normalize_seeds_to_fqn` — best-effort resolver from
  free-text ASI judge tokens (``DEST_AIRPORT_CD``, ``zone_vp_name``)
  to 4-part FQNs by case-insensitive column-name suffix match
  against ``schema_columns``. Compound text
  (``LIMIT 10 vs RANK() <= 10``) is dropped rather than guessed at.

Reference plan: ``docs/llmdrivenarchitecture/v5/
trial-13i-stage1-input-quality_*.plan.md``.
"""
from __future__ import annotations

import logging
import re
import time
from typing import Any, Mapping, MutableMapping, Sequence

logger = logging.getLogger(__name__)


# ─── Source labels (closed vocabulary) ────────────────────────────────

SCHEMA_COLUMNS_SOURCE_LABELS: frozenset[str] = frozenset({
    "metadata_snapshot",
    "typed_evidence_union",
    "identifier_allowlist",
    "empty",
})


# ─── Helpers ──────────────────────────────────────────────────────────


def _is_four_part_fqn(value: Any) -> bool:
    """Return True iff ``value`` stringifies to a 4-part dotted FQN.

    A 4-part FQN has the shape ``catalog.schema.table.column`` — every
    segment a non-empty SQL-style identifier (alphanumeric/underscore,
    leading non-digit). Anything else (3-part, 5-part, embedded
    whitespace, parentheses, comparison operators) is rejected.
    """
    if not isinstance(value, str):
        return False
    text = value.strip()
    if not text:
        return False
    parts = text.split(".")
    if len(parts) != 4:
        return False
    for part in parts:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", part):
            return False
    return True


_BARE_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _is_bare_identifier(value: str) -> bool:
    """Return True iff ``value`` is a single SQL-style identifier."""
    return bool(_BARE_IDENT_RE.fullmatch(value.strip()))


def _column_leaf(fqn: str) -> str:
    """Return the trailing dotted segment (column name) of an FQN.

    For ``"main.airline.fact_flights.dest_airport_cd"`` -> ``"dest_airport_cd"``.
    For values without dots returns the value unchanged.
    """
    return fqn.rsplit(".", 1)[-1].lower() if isinstance(fqn, str) else ""


# ─── Public API ───────────────────────────────────────────────────────


def _derive_schema_columns(
    metadata_snapshot: Mapping[str, Any] | None,
    rca_evidence_typed: Mapping[str, Any] | None,
    uc_columns: Sequence[Mapping[str, Any]] | None = None,
) -> tuple[tuple[str, ...], str]:
    """Compute the run-level ``schema_columns`` list + provenance label.

    Priority order (first non-empty wins):

      1. ``metadata_snapshot["schema_columns"]`` -> ``"metadata_snapshot"``.
         This is the authoritative production-populated channel.
         Trial 13l introduced the production writer
         :func:`inject_schema_columns_into_metadata_snapshot`, which
         the harness calls at the top of every lever-loop iteration so
         post-Stage-3-apply schema drift is reflected before the next
         Stage 1 LLM call. The workbench harness
         (``devtools/local_lever_workbench/local_runner.py``) calls the
         same injector for symmetry.
      2. Union of ``rca_evidence_typed[*].blame_set`` ->
         ``"typed_evidence_union"``. Mirrors the Plan 11 batch lane
         fallback at ``optimizer.py:_decide_and_run_plan11_dispatch``.
         ``PerQidRcaEvidence.blame_set`` is documented as
         4-part FQNs (``rca_evidence_typed.py:47``), so this branch
         feeds the Stage 1 LLM grounding values pinned to the schema.
      3. ``_build_identifier_allowlist(metadata_snapshot, uc_columns)``
         re-projected to 4-part FQNs only -> ``"identifier_allowlist"``.
         The allowlist already merges Genie config + UC metadata; we
         just re-assemble its ``tables`` (3-part) + per-table
         ``columns`` (column names) into ``catalog.schema.table.column``
         strings. Only tables whose identifier itself parses as a
         3-part FQN contribute, so any malformed table id is silently
         skipped (the prompt would have rejected the resulting string
         anyway).
      4. Empty -> ``"empty"``. Downstream pre-flight contract should
         abstain with ``missing_schema_columns`` so the LLM is not
         called with an unfulfillable promise.

    Returns ``(schema_columns_tuple, source_label)``. ``source_label``
    is always a member of :data:`SCHEMA_COLUMNS_SOURCE_LABELS`.
    """
    snapshot = dict(metadata_snapshot or {})

    # Priority 1 — explicit metadata channel.
    raw_meta = snapshot.get("schema_columns")
    if isinstance(raw_meta, (list, tuple)):
        cleaned = tuple(
            str(x).strip() for x in raw_meta if str(x or "").strip()
        )
        if cleaned:
            return cleaned, "metadata_snapshot"

    # Priority 2 — union of typed RCA evidence blame_set values.
    if rca_evidence_typed:
        seen: set[str] = set()
        union: list[str] = []
        for ev in rca_evidence_typed.values():
            blame = getattr(ev, "blame_set", None)
            if blame is None and isinstance(ev, Mapping):
                blame = ev.get("blame_set")
            for b in blame or ():
                s = str(b or "").strip()
                if s and s not in seen:
                    seen.add(s)
                    union.append(s)
        if union:
            return tuple(union), "typed_evidence_union"

    # Priority 3 — identifier allowlist re-projected to 4-part FQNs.
    allowlist_fqns = _identifier_allowlist_to_fqns(snapshot, uc_columns)
    if allowlist_fqns:
        return tuple(allowlist_fqns), "identifier_allowlist"

    return (), "empty"


def _identifier_allowlist_to_fqns(
    metadata_snapshot: Mapping[str, Any],
    uc_columns: Sequence[Mapping[str, Any]] | None,
) -> tuple[str, ...]:
    """Project ``_build_identifier_allowlist`` output to 4-part FQNs.

    The allowlist exposes ``tables`` (3-part ``catalog.schema.table``)
    and ``columns`` (mapping ``short_name -> [(col_name, dtype), ...]``).
    We re-join each table-id with each column name to form a
    ``catalog.schema.table.column`` string. Tables that do not parse
    as 3-part identifiers are silently skipped (their resulting FQN
    would not be 4-part either).
    """
    from genie_space_optimizer.optimization.optimizer import (
        _build_identifier_allowlist,
    )

    try:
        allowlist = _build_identifier_allowlist(
            dict(metadata_snapshot or {}),
            list(uc_columns or []) if uc_columns is not None else None,
        )
    except Exception:
        return ()
    tables = allowlist.get("tables") or []
    columns_by_table = allowlist.get("columns") or {}

    seen: set[str] = set()
    out: list[str] = []
    for table_id in tables:
        if not isinstance(table_id, str):
            continue
        parts = table_id.split(".")
        if len(parts) != 3:
            continue
        short = parts[-1].lower()
        cols = columns_by_table.get(short) or []
        for col_entry in cols:
            if isinstance(col_entry, tuple) and col_entry:
                col_name = col_entry[0]
            elif isinstance(col_entry, str):
                col_name = col_entry
            elif isinstance(col_entry, Mapping):
                col_name = (
                    col_entry.get("column_name")
                    or col_entry.get("name")
                    or ""
                )
            else:
                col_name = ""
            col_name = str(col_name or "").strip()
            if not col_name:
                continue
            fqn = f"{table_id}.{col_name}"
            if _is_four_part_fqn(fqn) and fqn not in seen:
                seen.add(fqn)
                out.append(fqn)
    return tuple(out)


def _normalize_seeds_to_fqn(
    seeds: Sequence[str],
    schema_columns: Sequence[str],
) -> tuple[list[str], int, int]:
    """Best-effort resolver from free-text seed tokens to 4-part FQNs.

    Rules, applied per seed in input order:

      1. Already in ``schema_columns`` -> keep as-is.
      2. Bare identifier (e.g. ``"DEST_AIRPORT_CD"``) -> case-insensitive
         match against the trailing column-name segment of every entry
         in ``schema_columns``. Exactly one match -> swap to the FQN.
         Zero or multiple matches -> drop (ambiguous resolution would
         smuggle the wrong column into the LLM prompt; better to omit
         and let other seeds carry the load).
      3. Anything else (compound text, comparison fragments,
         SQL keywords) -> drop.

    Returns ``(normalized_seeds, normalized_count, dropped_count)``:

      * ``normalized_seeds`` is the deduplicated output list (preserves
        first-seen order across rules 1 and 2).
      * ``normalized_count`` counts rule-2 swaps performed (i.e. how
        many free-text tokens were successfully resolved to FQNs).
      * ``dropped_count`` counts inputs rejected by rules 2 or 3.

    When ``schema_columns`` is empty the helper is a no-op: rule 1
    cannot match (nothing to match against), rule 2 cannot resolve
    (no suffix candidates), so every input is dropped. The Trial 13i
    pre-flight contract abstains earlier on this exact case, so the
    no-op path here is only reachable on legacy callers that skip the
    contract.
    """
    schema_lookup: set[str] = {
        s for s in schema_columns if isinstance(s, str) and s.strip()
    }
    # Map: column-leaf (lowercased) -> list of full FQNs sharing that leaf.
    leaf_index: dict[str, list[str]] = {}
    for fqn in schema_lookup:
        leaf = _column_leaf(fqn)
        if leaf:
            leaf_index.setdefault(leaf, []).append(fqn)

    out_seen: set[str] = set()
    out: list[str] = []
    normalized_count = 0
    dropped_count = 0

    for raw in seeds or ():
        token = str(raw or "").strip()
        if not token:
            dropped_count += 1
            continue

        # Rule 1 — passthrough.
        if token in schema_lookup:
            if token not in out_seen:
                out_seen.add(token)
                out.append(token)
            continue

        # Rule 2 — bare-identifier suffix match.
        if _is_bare_identifier(token):
            candidates = leaf_index.get(token.lower(), [])
            if len(candidates) == 1:
                resolved = candidates[0]
                if resolved not in out_seen:
                    out_seen.add(resolved)
                    out.append(resolved)
                    normalized_count += 1
                continue
            # 0 or >1 matches: ambiguous; drop.
            dropped_count += 1
            continue

        # Rule 3 — compound text, drop.
        dropped_count += 1

    return out, normalized_count, dropped_count


# ─── Trial 13l — Genie Space schema_columns fetch (promoted from devtools) ──
#
# Originally introduced by Trial 13j inside the workbench capture path
# (``devtools/local_lever_workbench/mlflow_eval_capture.py``). Trial 13l
# promotes both helpers into this module so production becomes the source
# of truth and the workbench imports through ``mlflow_eval_capture``'s
# thin re-export. Same behaviour, same signatures, same return tuples.


def _extract_fqn_columns(serialized_space: Mapping[str, Any]) -> tuple[str, ...]:
    """Walk ``serialized_space.data_sources.tables[*]`` and emit 4-part FQNs.

    Each table carries ``identifier`` (3-part ``catalog.schema.table``)
    and ``column_configs`` (or ``columns`` as a fallback) — both shapes
    are accepted by the existing batch-lane consumers in
    :mod:`genie_space_optimizer.optimization.optimizer`. Column entries
    are dicts with ``column_name`` (preferred) or ``name``.

    Returns a deduplicated tuple preserving input order. Tables whose
    identifier does not parse as 3-part are silently dropped — the
    resulting concatenation would not be 4-part either.
    """
    ss = (
        serialized_space.get("_parsed_space")
        if isinstance(serialized_space, Mapping)
        else None
    )
    if not isinstance(ss, Mapping):
        ss = serialized_space if isinstance(serialized_space, Mapping) else {}
    data_sources = ss.get("data_sources") if isinstance(ss, Mapping) else None
    if not isinstance(data_sources, Mapping):
        return ()
    tables = data_sources.get("tables") or []
    if not isinstance(tables, list):
        return ()

    seen: set[str] = set()
    out: list[str] = []
    for tbl in tables:
        if not isinstance(tbl, Mapping):
            continue
        ident = str(tbl.get("identifier") or tbl.get("name") or "").strip()
        if not ident or ident.count(".") != 2:
            continue
        cols = tbl.get("column_configs")
        if not isinstance(cols, list) or not cols:
            cols = tbl.get("columns") or []
        if not isinstance(cols, list):
            continue
        for col in cols:
            if isinstance(col, Mapping):
                col_name = str(
                    col.get("column_name") or col.get("name") or ""
                ).strip()
            elif isinstance(col, str):
                col_name = col.strip()
            else:
                col_name = ""
            if not col_name:
                continue
            fqn = f"{ident}.{col_name}"
            if fqn in seen:
                continue
            seen.add(fqn)
            out.append(fqn)
    return tuple(out)


def _fetch_schema_columns_for_space(
    w: Any, space_id: str
) -> tuple[tuple[str, ...], dict, str]:
    """Fetch ``serialized_space`` and project to 4-part column FQNs.

    Returns ``(schema_columns, serialized_space, source_label)`` where
    ``source_label`` is one of:

    - ``"genie_api"`` — fetch succeeded and at least one column FQN
      was extracted. ``serialized_space`` is the raw payload returned
      by :func:`fetch_space_config` so the caller can persist it
      alongside the columns.
    - ``"unavailable"`` — ``space_id`` is empty, the API call raised,
      or the response was malformed. ``schema_columns`` is empty and
      ``serialized_space`` is ``{}``.

    Failures are intentionally swallowed so a fetch error never breaks
    the capture path; the loader's Trial 13i derivation chain still
    runs on the resulting v1-shaped bundle.
    """
    if not space_id:
        return (), {}, "unavailable"
    try:
        from genie_space_optimizer.common.genie_client import fetch_space_config

        config = fetch_space_config(w, space_id)
    except Exception as exc:  # noqa: BLE001 — capture must not raise
        logger.warning(
            "Trial 13j: fetch_space_config(%s) failed: %s; "
            "capture will write a v1 bundle without schema_columns.",
            space_id,
            exc,
        )
        return (), {}, "unavailable"
    if not isinstance(config, Mapping):
        return (), {}, "unavailable"
    cols = _extract_fqn_columns(config)
    if not cols:
        return (), dict(config), "unavailable"
    return cols, dict(config), "genie_api"


# ─── Trial 13l — production injector ──────────────────────────────────
#
# Closed source vocabulary for the per-iteration injection result.
# Trial 13l deliberately omits an ``already_present`` label: every
# iteration re-fetches, so the live Genie Space is always the truth.
# The only mutation path is ``"genie_api"``; every other source is a
# no-op that preserves whatever a prior iteration successfully wrote.

SCHEMA_COLUMNS_INJECTION_SOURCES: frozenset[str] = frozenset({
    "genie_api",     # fetch succeeded + non-empty FQNs extracted (only write path)
    "no_space_id",   # caller passed empty/None space_id; no-op
    "api_error",     # fetch_space_config raised; no-op (prior value retained)
    "empty_extract", # fetch ok but no 4-part FQNs derivable; no-op
})


def inject_schema_columns_into_metadata_snapshot(
    metadata_snapshot: MutableMapping[str, Any],
    *,
    genie_space_id: str | None,
    client: Any | None = None,
) -> tuple[bool, str, int, int]:
    """Refresh ``metadata_snapshot["schema_columns"]`` from the live Genie Space.

    Designed to be called at the top of every lever-loop iteration so
    post-Stage-3-apply schema drift is reflected before the next Stage 1
    LLM call. ``source`` is always a member of
    :data:`SCHEMA_COLUMNS_INJECTION_SOURCES`.

    Write semantics (asymmetric, intentional):

    - ``source == "genie_api"`` — overwrite
      ``metadata_snapshot["schema_columns"]`` with the freshly-fetched
      FQN tuple. The post-apply Genie Space is the ground truth, so a
      refreshed read always replaces any prior value.
    - any other ``source`` — do not mutate ``metadata_snapshot``. If a
      prior iteration successfully wrote schema_columns, that value
      sticks around. This gives the SM graceful degradation through
      transient Genie API failures rather than amnesia.

    The function never raises. Production callers wrap in ``try/except``
    only for defence in depth (e.g. the marker emit step).

    Parameters
    ----------
    metadata_snapshot:
        The lever-loop's per-run snapshot dict. Mutated in place when
        and only when ``source == "genie_api"``.
    genie_space_id:
        4-part hex Genie Space identifier. Empty / ``None`` short-
        circuits to ``"no_space_id"`` and no API round-trip.
    client:
        ``WorkspaceClient`` (or test double) forwarded to
        :func:`_fetch_schema_columns_for_space`.

    Returns
    -------
    ``(injected, source, column_count, latency_ms)`` where ``injected``
    is True iff this call mutated ``metadata_snapshot``.
    """
    start_ns = time.perf_counter_ns()

    if not genie_space_id:
        latency_ms = (time.perf_counter_ns() - start_ns) // 1_000_000
        return False, "no_space_id", 0, int(latency_ms)

    cols, _serialized, fetch_source = _fetch_schema_columns_for_space(
        client, str(genie_space_id),
    )
    latency_ms = (time.perf_counter_ns() - start_ns) // 1_000_000

    if fetch_source == "genie_api" and cols:
        # Overwrite — live Genie Space wins over any stale prior value.
        metadata_snapshot["schema_columns"] = tuple(cols)
        return True, "genie_api", len(cols), int(latency_ms)

    # ``_fetch_schema_columns_for_space`` collapses three sub-cases
    # into ``"unavailable"``: API call raised, non-Mapping response,
    # and parseable response with zero 4-part FQNs. We disambiguate
    # by inspecting whether the fetcher returned the raw config back:
    # api errors return ``_serialized == {}``; empty-extract returns
    # the populated config dict.
    if _serialized:
        return False, "empty_extract", 0, int(latency_ms)
    return False, "api_error", 0, int(latency_ms)


__all__ = [
    "SCHEMA_COLUMNS_INJECTION_SOURCES",
    "SCHEMA_COLUMNS_SOURCE_LABELS",
    "_derive_schema_columns",
    "_extract_fqn_columns",
    "_fetch_schema_columns_for_space",
    "_identifier_allowlist_to_fqns",
    "_normalize_seeds_to_fqn",
    "inject_schema_columns_into_metadata_snapshot",
]
