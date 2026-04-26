"""Catalog-level metric-view detection.

Runs ``DESCRIBE TABLE EXTENDED ... AS JSON`` against a list of UC table
refs and classifies each ref as a metric view (or not) based on whether
its ``view_text`` payload parses as metric-view YAML (a ``source`` plus
``dimensions`` and/or ``measures``).

Lives in ``common`` rather than ``optimization.preflight`` so the same
helper can be invoked by every stage that needs the answer (preflight,
enrichment, follow-up refreshes) without dragging the entire preflight
module into harness's import graph.

Detection is *permissive* — false negatives only, never false positives.
A failed DESCRIBE, a non-JSON envelope, or a YAML that doesn't match the
metric-view shape silently treats the ref as a non-MV so the regular
table-profile path remains correct for real tables.

PR 23 — Detection is also *observable*. Every per-ref outcome is recorded
in a small ``outcomes`` dict (``describe_error``, ``empty_result``,
``no_envelope``, ``no_view_text``, ``yaml_parse_error``, ``not_mv_shape``,
``detected``) so callers can emit a one-line summary even when zero MVs
are detected. Each non-detect outcome is also logged at INFO so a silent
failure cannot hide behind an empty-but-success ``MVs detected: 0``.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


# PR 23 — public outcome codes. Stable strings; banner / log readers
# pivot on these.
OUTCOME_DESCRIBE_ERROR = "describe_error"
OUTCOME_EMPTY_RESULT = "empty_result"
OUTCOME_NO_ENVELOPE = "no_envelope"
OUTCOME_NO_VIEW_TEXT = "no_view_text"
OUTCOME_YAML_PARSE_ERROR = "yaml_parse_error"
OUTCOME_NOT_MV_SHAPE = "not_mv_shape"

# PR 24 — multi-signal classification. ``OUTCOME_DETECTED`` is kept as a
# legacy umbrella alias (= ``detected_via_yaml``) so older test fixtures
# and external callers that pivoted on the canonical "detected" string
# keep working unchanged. New code should pivot on the per-signal codes
# instead so log readers can distinguish a YAML-derived MV (the strict
# old path) from one classified solely by ``type=METRIC_VIEW`` /
# ``language=YAML`` / ``column.is_measure`` flags. ``summarize_outcomes``
# rolls the four ``detected_via_*`` codes back up into the umbrella
# ``detected`` count for the always-on banner / log line.
OUTCOME_DETECTED_VIA_TYPE = "detected_via_type"
OUTCOME_DETECTED_VIA_LANGUAGE = "detected_via_language"
OUTCOME_DETECTED_VIA_YAML = "detected_via_yaml"
OUTCOME_DETECTED_VIA_IS_MEASURE = "detected_via_is_measure"
OUTCOME_DETECTED = OUTCOME_DETECTED_VIA_YAML  # back-compat alias


def detect_metric_views_via_catalog_with_outcomes(
    spark: "SparkSession",
    refs: list[tuple[str, str, str]],
    *,
    w: Any = None,
    warehouse_id: str = "",
    catalog: str = "",
    schema: str = "",
    exec_sql: Any = None,
    diagnostic_samples: dict[str, str] | None = None,
) -> tuple[set[str], dict[str, dict], dict[str, str]]:
    """Catalog-level metric-view detection with per-ref outcomes (PR 23).

    Identical contract to :func:`detect_metric_views_via_catalog` for the
    first two return values; additionally returns ``outcomes``, a dict
    mapping every probed ``fq_lower`` to one of the ``OUTCOME_*``
    constants. Refs with empty/missing components are skipped silently
    and do *not* appear in ``outcomes``.

    Each non-detect outcome is logged at INFO with the failing
    fully-qualified ref so a silent failure cannot hide behind an
    empty-but-success result. Logs include first-120-char snippets of
    the offending payload (``view_text`` for YAML failures, the
    ``repr()`` of the exception for DESCRIBE failures) so the cause is
    actionable from the run log alone.

    PR 28 — when ``diagnostic_samples`` is supplied, the helper populates
    it in-place with one short text sample per non-detect ref
    (``describe_error`` ⇒ exception repr; ``no_envelope`` ⇒ first-cell
    snippet; ``yaml_parse_error`` ⇒ first 120 chars of view_text) so
    upstream callers can surface an actionable example in their banner
    without re-querying the catalog. The dict is keyed by the same
    ``fq_lower`` strings as ``outcomes`` and is *additive* — entries
    overwrite without consulting any prior contents so callers can
    reuse the same dict across multiple invocations.

    PR 28 — also adds a structural-signal retry: when the JSON envelope
    parses but produces no classification signal, and the ref's name
    pattern (or its envelope's ``language`` field) flags it as
    suspicious, the helper falls through to the legacy
    ``DESCRIBE EXTENDED`` path. The fallback may surface ``Type:
    METRIC_VIEW`` from a Spark connection that opted into the
    metricview metadata flag but did not return it in the JSON envelope.
    Name patterns are *only* used to gate the retry — a hit there
    without a structural signal still records ``not_mv_shape`` /
    ``no_view_text`` rather than classifying by name alone.
    """
    import json as _json

    import yaml as _yaml

    if exec_sql is None:
        from genie_space_optimizer.optimization.evaluation import _exec_sql as _exec
    else:
        _exec = exec_sql

    detected: set[str] = set()
    yamls: dict[str, dict] = {}
    outcomes: dict[str, str] = {}

    for cat, sch, name in refs:
        cat = (cat or "").strip()
        sch = (sch or "").strip()
        name = (name or "").strip()
        if not (cat and sch and name):
            continue
        fq_lower = f"{cat}.{sch}.{name}".lower()
        fq_quoted = ".".join(f"`{p}`" for p in (cat, sch, name))

        envelope: dict[str, Any] | None = None
        try:
            describe_df = _exec(
                f"DESCRIBE TABLE EXTENDED {fq_quoted} AS JSON",
                spark,
                w=w,
                warehouse_id=warehouse_id,
                catalog=catalog,
                schema=schema,
            )
        except Exception as exc:
            # PR 25 — when ``AS JSON`` is unsupported (older Spark, some
            # cluster-mode preflight setups, future runtime regressions),
            # attempt a non-AS-JSON fallback before recording the failure.
            # Permission / network errors don't match the unsupported
            # heuristic and continue to record ``describe_error`` immediately.
            fallback_envelope: dict[str, Any] | None = None
            if _is_as_json_unsupported_error(exc):
                fallback_envelope = _describe_metric_view_fallback(
                    fq_quoted, fq_lower,
                    spark=spark, w=w, warehouse_id=warehouse_id,
                    catalog=catalog, schema=schema, exec_sql=_exec,
                )
            if fallback_envelope is not None:
                envelope = fallback_envelope
                logger.info(
                    "MV catalog detection: AS JSON unsupported for %s "
                    "(%s); fallback DESCRIBE EXTENDED succeeded",
                    fq_lower, type(exc).__name__,
                )
            else:
                outcomes[fq_lower] = OUTCOME_DESCRIBE_ERROR
                if diagnostic_samples is not None:
                    diagnostic_samples[fq_lower] = (
                        f"{type(exc).__name__}: {str(exc)[:200]}"
                    )
                logger.info(
                    "MV catalog detection: DESCRIBE failed for %s "
                    "(treating as non-MV) — %s: %s",
                    fq_lower, type(exc).__name__, str(exc)[:200],
                )
                logger.debug(
                    "MV catalog detection: full traceback for %s", fq_lower,
                    exc_info=True,
                )
                continue
        else:
            if describe_df is None or describe_df.empty:
                outcomes[fq_lower] = OUTCOME_EMPTY_RESULT
                if diagnostic_samples is not None:
                    diagnostic_samples[fq_lower] = "empty result from DESCRIBE"
                logger.info(
                    "MV catalog detection: empty DESCRIBE result for %s "
                    "(treating as non-MV)",
                    fq_lower,
                )
                continue

            # The JSON payload may live under different column names depending
            # on whether the warehouse path or the Spark path returned the
            # row; search the row's values for the first parseable JSON
            # envelope.
            first_cell_sample: str = ""
            for value in describe_df.iloc[0].tolist():
                if not isinstance(value, str):
                    continue
                if not first_cell_sample:
                    first_cell_sample = value[:200]
                try:
                    parsed = _json.loads(value)
                except (ValueError, TypeError):
                    continue
                if isinstance(parsed, dict):
                    envelope = parsed
                    break
            if envelope is None:
                outcomes[fq_lower] = OUTCOME_NO_ENVELOPE
                if diagnostic_samples is not None:
                    diagnostic_samples[fq_lower] = (
                        f"row[0]={first_cell_sample!r}"
                        if first_cell_sample
                        else "no parseable JSON envelope in DESCRIBE row"
                    )
                logger.info(
                    "MV catalog detection: no JSON envelope in DESCRIBE row "
                    "for %s (treating as non-MV)",
                    fq_lower,
                )
                continue

        # PR 24 — gather every available signal up-front, then classify.
        # The DBR 16.2+ ``AS JSON`` envelope explicitly carries top-level
        # ``type``/``language`` fields and per-column ``is_measure``
        # flags; relying solely on YAML-parse-of-view_text (the strict
        # pre-PR-24 path) silently misses MVs whose ``view_text`` is
        # NULL (non-owners) or whose YAML cannot round-trip, even when
        # the envelope's structural signals are unambiguous.
        type_str = str(envelope.get("type") or "").strip().upper()
        sig_type = type_str == "METRIC_VIEW"

        language_str = str(envelope.get("language") or "").strip().upper()
        sig_language = language_str == "YAML"

        view_text = (
            envelope.get("view_text")
            or envelope.get("View Text")
            or envelope.get("view_definition")
            or envelope.get("ViewText")
            or ""
        )
        view_text = view_text if isinstance(view_text, str) else ""

        cols = envelope.get("columns") or []
        if not isinstance(cols, list):
            cols = []
        is_measure_cols = [
            c for c in cols
            if isinstance(c, dict) and bool(c.get("is_measure"))
        ]
        sig_is_measure = bool(is_measure_cols)

        # YAML parse — best-effort, only meaningful when ``view_text`` is
        # non-empty; failures are tracked separately so the log surfaces
        # the cause when no other signal classifies the ref.
        yaml_doc: dict | None = None
        yaml_parse_failed = False
        if view_text.strip():
            try:
                parsed_yaml = _yaml.safe_load(view_text)
                if isinstance(parsed_yaml, dict):
                    yaml_doc = parsed_yaml
            except Exception as exc:
                yaml_parse_failed = True
                logger.info(
                    "MV catalog detection: YAML parse failed for %s "
                    "(falling back to structural signals if available) "
                    "— %s: %s; view_text[:120]=%r",
                    fq_lower, type(exc).__name__, str(exc)[:120],
                    view_text[:120],
                )

        yaml_shape_ok = bool(
            isinstance(yaml_doc, dict)
            and yaml_doc.get("source")
            and (yaml_doc.get("dimensions") or yaml_doc.get("measures"))
        )

        # Classify in confidence order: explicit ``type=METRIC_VIEW`` is
        # the strongest signal Databricks emits, then ``language=YAML``
        # paired with non-empty ``view_text``, then a YAML body whose
        # shape matches the metric-view contract, then per-column
        # ``is_measure`` flags as a last-resort signal for envelopes
        # that omit the top-level type field.
        outcome_code: str | None = None
        if sig_type:
            outcome_code = OUTCOME_DETECTED_VIA_TYPE
        elif sig_language and view_text.strip():
            outcome_code = OUTCOME_DETECTED_VIA_LANGUAGE
        elif yaml_shape_ok:
            outcome_code = OUTCOME_DETECTED_VIA_YAML
        elif sig_is_measure:
            outcome_code = OUTCOME_DETECTED_VIA_IS_MEASURE

        if outcome_code is None:
            # PR 28 — structural-signal retry. JSON returned a parseable
            # envelope but no classification signal. If the ref's name
            # pattern or the envelope's ``language`` field flags it as
            # an MV-suspect, re-probe via ``DESCRIBE EXTENDED`` (with
            # ``spark.databricks.metadata.metricview.enabled=true``)
            # because some Spark configurations expose ``Type:
            # METRIC_VIEW`` only on the legacy path. Name patterns
            # gate the *retry only* — a name hit without a structural
            # signal still records a non-detect outcome rather than
            # classifying by name alone (avoiding false positives for
            # ordinary views named ``mv_*``).
            short_lower = name.lower()
            name_suspicious = (
                short_lower.startswith("mv_")
                or short_lower.endswith("_mv")
                or "_mv_" in short_lower
            )
            if name_suspicious or sig_language:
                retry_envelope = _describe_metric_view_fallback(
                    fq_quoted, fq_lower,
                    spark=spark, w=w, warehouse_id=warehouse_id,
                    catalog=catalog, schema=schema, exec_sql=_exec,
                )
                if isinstance(retry_envelope, dict):
                    rt_type = str(retry_envelope.get("type") or "").strip().upper()
                    rt_lang = str(retry_envelope.get("language") or "").strip().upper()
                    rt_view_text = str(retry_envelope.get("view_text") or "")
                    rt_cols = retry_envelope.get("columns") or []
                    if not isinstance(rt_cols, list):
                        rt_cols = []
                    rt_is_measure = any(
                        isinstance(c, dict) and bool(c.get("is_measure"))
                        for c in rt_cols
                    )
                    if rt_type == "METRIC_VIEW":
                        outcome_code = OUTCOME_DETECTED_VIA_TYPE
                    elif rt_lang == "YAML" and rt_view_text.strip():
                        outcome_code = OUTCOME_DETECTED_VIA_LANGUAGE
                    elif rt_is_measure:
                        outcome_code = OUTCOME_DETECTED_VIA_IS_MEASURE
                    if outcome_code is not None:
                        # Adopt the retry envelope's column / view_text
                        # signals so the YAML synthesizer below can
                        # populate measures/dimensions from the
                        # legacy DESCRIBE.
                        envelope = retry_envelope
                        cols = rt_cols
                        view_text = rt_view_text
                        logger.info(
                            "MV catalog detection: structural-signal retry "
                            "via legacy DESCRIBE classified %s as %s",
                            fq_lower, outcome_code,
                        )

        if outcome_code is None:
            # No structural signal — fall through to the same
            # non-detect outcomes as before so existing diagnostics keep
            # working. Order matters: the *first* missing input wins.
            if not view_text.strip():
                outcomes[fq_lower] = OUTCOME_NO_VIEW_TEXT
                if diagnostic_samples is not None:
                    diagnostic_samples[fq_lower] = (
                        "no view_text and no structural MV signal in envelope"
                    )
                # Regular tables and non-view objects always land here,
                # so this is the *expected* outcome for the majority of
                # refs. Keep at DEBUG to avoid log spam on table-heavy
                # spaces; the aggregated summary line at the call site
                # still surfaces the count.
                logger.debug(
                    "MV catalog detection: no view_text for %s "
                    "(treating as non-MV)",
                    fq_lower,
                )
            elif yaml_parse_failed:
                outcomes[fq_lower] = OUTCOME_YAML_PARSE_ERROR
                if diagnostic_samples is not None:
                    diagnostic_samples[fq_lower] = (
                        f"view_text[:120]={view_text[:120]!r}"
                    )
            else:
                outcomes[fq_lower] = OUTCOME_NOT_MV_SHAPE
                if diagnostic_samples is not None:
                    diagnostic_samples[fq_lower] = (
                        f"view_text[:120]={view_text[:120]!r}"
                    )
                logger.debug(
                    "MV catalog detection: YAML present but not metric-view "
                    "shape for %s (treating as non-MV)",
                    fq_lower,
                )
            continue

        # Settle on a YAML payload to cache. Prefer the real parsed
        # YAML when it round-trips; otherwise emit a synthetic skeleton
        # built from envelope columns so downstream consumers
        # (``build_metric_view_measures``, ``has_metric_view`` trait,
        # MEASURE auto-wrap) keep working without YAML.
        if yaml_shape_ok:
            chosen_yaml = yaml_doc
        else:
            chosen_yaml = _synthesize_yaml_skeleton(
                envelope=envelope,
                cols=cols,
                fq_lower=fq_lower,
            )
            if outcome_code != OUTCOME_DETECTED_VIA_YAML:
                logger.info(
                    "MV catalog detection: %s classified as MV via %s; "
                    "synthesized YAML skeleton (measures=%d, dimensions=%d)",
                    fq_lower, outcome_code,
                    len(chosen_yaml.get("measures") or []),
                    len(chosen_yaml.get("dimensions") or []),
                )

        outcomes[fq_lower] = outcome_code
        detected.add(fq_lower)
        yamls[fq_lower] = chosen_yaml

    return detected, yamls, outcomes


def _synthesize_yaml_skeleton(
    *,
    envelope: dict,
    cols: list,
    fq_lower: str,
) -> dict:
    """Build a synthetic YAML skeleton from a DESCRIBE JSON envelope (PR 24).

    Used when an MV is classified by a structural signal
    (``type=METRIC_VIEW`` / ``language=YAML`` / ``is_measure``) but
    ``view_text`` is missing, malformed, or otherwise unable to yield a
    parseable YAML body. Marks the result with ``_source =
    "structural_signal"`` so downstream consumers can tell synthetic from
    YAML-derived definitions if they care, but keeps the same top-level
    shape (``source``, ``dimensions``, ``measures``) the rest of the
    codebase already knows how to consume.
    """
    measures: list[dict] = []
    dimensions: list[dict] = []
    for c in cols:
        if not isinstance(c, dict):
            continue
        name = str(c.get("name") or c.get("col_name") or "").strip()
        if not name:
            continue
        if c.get("is_measure"):
            measures.append({"name": name, "expr": name})
        else:
            dimensions.append({"name": name, "expr": name})

    source = (
        envelope.get("source")
        or envelope.get("table_name")
        or fq_lower
    )
    return {
        "_source": "structural_signal",
        "source": source,
        "dimensions": dimensions,
        "measures": measures,
    }


def detect_metric_views_via_catalog(
    spark: "SparkSession",
    refs: list[tuple[str, str, str]],
    *,
    w: Any = None,
    warehouse_id: str = "",
    catalog: str = "",
    schema: str = "",
    exec_sql: Any = None,
) -> tuple[set[str], dict[str, dict]]:
    """Catalog-level metric-view detection.

    For each ``(catalog, schema, name)`` triple, runs ``DESCRIBE TABLE
    EXTENDED <fq> AS JSON`` and parses the JSON envelope. A ref is
    classified as a metric view when the response contains a
    ``view_text`` (or equivalent field) whose YAML payload has the
    metric-view top-level shape — a ``source`` plus at least one of
    ``dimensions`` / ``measures``.

    Returns ``(detected, yamls)`` where:

    * ``detected`` is a set of fully-qualified, lower-cased identifiers
      for refs classified as MVs.
    * ``yamls`` maps each detected identifier to its parsed YAML dict so
      downstream callers (MV-aware data profiling, prompt building, the
      MEASURE auto-wrap rewriter) can inspect dimensions and measures
      without re-running DESCRIBE.

    Backward-compatible 2-tuple façade over
    :func:`detect_metric_views_via_catalog_with_outcomes`. New call sites
    that want per-ref outcome breakdowns should use the ``_with_outcomes``
    variant directly.

    The optional ``exec_sql`` lets tests inject a stub; production
    callers leave it ``None`` and the helper resolves the canonical
    ``evaluation._exec_sql`` lazily.
    """
    detected, yamls, _outcomes = detect_metric_views_via_catalog_with_outcomes(
        spark, refs,
        w=w, warehouse_id=warehouse_id,
        catalog=catalog, schema=schema, exec_sql=exec_sql,
    )
    return detected, yamls


def summarize_outcomes(outcomes: dict[str, str]) -> dict[str, int]:
    """Count occurrences of each ``OUTCOME_*`` code (PR 23 + 24).

    Returns a dict with one key per known outcome code (always present,
    zero when no refs hit that path) so banner/summary lines have a
    consistent column layout regardless of input. PR 24 — the per-signal
    ``detected_via_*`` codes are also surfaced so callers can read the
    classification breakdown when desired; the umbrella ``detected``
    count rolls them up so existing log lines (``detected=N``) keep
    working unchanged. The literal string ``"detected"`` is treated as
    a legacy alias for ``detected_via_yaml`` to keep older fixtures and
    callers that pivoted on the umbrella string working.
    """
    detected_signal_keys = (
        OUTCOME_DETECTED_VIA_TYPE,
        OUTCOME_DETECTED_VIA_LANGUAGE,
        OUTCOME_DETECTED_VIA_YAML,
        OUTCOME_DETECTED_VIA_IS_MEASURE,
    )
    other_keys = (
        OUTCOME_DESCRIBE_ERROR,
        OUTCOME_EMPTY_RESULT,
        OUTCOME_NO_ENVELOPE,
        OUTCOME_NO_VIEW_TEXT,
        OUTCOME_YAML_PARSE_ERROR,
        OUTCOME_NOT_MV_SHAPE,
    )
    counts: dict[str, int] = {k: 0 for k in detected_signal_keys}
    counts.update({k: 0 for k in other_keys})
    for code in outcomes.values():
        if code == "detected":
            # Legacy alias from pre-PR-24 callers and test fixtures.
            counts[OUTCOME_DETECTED_VIA_YAML] += 1
        elif code in counts:
            counts[code] += 1
    counts["detected"] = sum(counts[k] for k in detected_signal_keys)
    return counts


# PR 25 — non-AS-JSON Spark-path fallback. Belt-and-suspenders for
# environments where ``DESCRIBE ... AS JSON`` is unavailable (older
# Spark in unit-test contexts, future runtime regressions, or cluster
# modes that haven't picked up the syntax). The fallback runs plain
# ``DESCRIBE EXTENDED`` and parses the legacy key-value rows.

_AS_JSON_UNSUPPORTED_INDICATORS: tuple[str, ...] = (
    "as json",
    "parse_syntax_error",
    "parseexception",
    "syntax error",
    "feature_not_supported",
    "unsupported syntax",
    "extraneous input 'json'",
    "mismatched input 'json'",
    "mismatched input 'as'",
)


def _is_as_json_unsupported_error(exc: BaseException) -> bool:
    """Heuristic: does this DESCRIBE failure indicate ``AS JSON`` is
    unsupported on the executing engine?

    True for parse-error / syntax-error / feature-not-supported messages
    so the caller can attempt the legacy ``DESCRIBE EXTENDED`` fallback.
    False for permission / network / table-not-found errors so the
    caller records ``describe_error`` immediately without a noisy second
    DESCRIBE.
    """
    msg = str(exc).lower()
    type_name = type(exc).__name__.lower()
    if "parseexception" in type_name or "analysisexception" in type_name:
        # AnalysisException can wrap legitimate permission errors too;
        # gate on the message text below to avoid false-positive
        # fallbacks against permission failures.
        pass
    return any(ind in msg for ind in _AS_JSON_UNSUPPORTED_INDICATORS)


def _describe_metric_view_fallback(
    fq_quoted: str,
    fq_lower: str,
    *,
    spark: Any,
    w: Any,
    warehouse_id: str,
    catalog: str,
    schema: str,
    exec_sql: Any,
) -> dict[str, Any] | None:
    """Run ``DESCRIBE EXTENDED <fq>`` (no ``AS JSON``) and return an
    envelope-shaped dict compatible with the JSON path.

    Best-effort — sets ``spark.databricks.metadata.metricview.enabled``
    so MV-aware columns (``Type``, ``Language``, ``View Text``) are
    surfaced when the engine honours that flag, then parses the standard
    three-column ``[col_name, data_type, comment]`` result. Anything we
    can't parse degrades gracefully: the caller treats a ``None`` return
    as a fallback failure and falls through to the regular
    ``describe_error`` path.

    Returns the envelope dict on success, or ``None`` when the fallback
    DESCRIBE itself fails or yields nothing actionable.
    """
    # Best-effort metric-view metadata flag. Swallow on failure — the
    # fallback DESCRIBE itself will still return the standard structural
    # rows even without it (just minus ``Type`` / ``Language``).
    try:
        if spark is not None and hasattr(spark, "conf"):
            spark.conf.set(
                "spark.databricks.metadata.metricview.enabled", "true",
            )
    except Exception:  # noqa: BLE001 — best-effort knob, never fatal
        logger.debug(
            "MV catalog detection: could not set "
            "spark.databricks.metadata.metricview.enabled (non-fatal)",
            exc_info=True,
        )

    try:
        df = exec_sql(
            f"DESCRIBE EXTENDED {fq_quoted}",
            spark, w=w, warehouse_id=warehouse_id,
            catalog=catalog, schema=schema,
        )
    except Exception as exc:  # noqa: BLE001
        logger.info(
            "MV catalog detection: fallback DESCRIBE EXTENDED failed for "
            "%s — %s: %s",
            fq_lower, type(exc).__name__, str(exc)[:200],
        )
        return None

    if df is None or getattr(df, "empty", True):
        return None

    columns: list[dict[str, Any]] = []
    detail_section = False
    view_text: str | None = None
    type_str: str | None = None
    language_str: str | None = None

    try:
        rows_iter = df.iterrows()
    except Exception:  # noqa: BLE001
        return None

    for _, row in rows_iter:
        try:
            col_name = str(row.iloc[0] or "").strip()
            data_type = str(row.iloc[1] or "").strip()
        except Exception:  # noqa: BLE001
            continue

        # Section markers. Standard ``DESCRIBE EXTENDED`` output has a
        # blank-line / ``# Detailed Table Information`` separator
        # between the column list and the detail key/value rows.
        if col_name == "" or col_name.startswith("#"):
            if "detailed table information" in col_name.lower():
                detail_section = True
            continue

        if not detail_section:
            columns.append({"name": col_name, "data_type": data_type})
            continue

        key_lower = col_name.lower()
        if key_lower in ("view text", "view_text"):
            view_text = data_type or None
        elif key_lower in ("type", "table type"):
            type_str = data_type or None
        elif key_lower == "language":
            language_str = data_type or None

    envelope: dict[str, Any] = {"columns": columns}
    if view_text is not None:
        envelope["view_text"] = view_text
    if type_str:
        envelope["type"] = type_str
    if language_str:
        envelope["language"] = language_str
    return envelope
