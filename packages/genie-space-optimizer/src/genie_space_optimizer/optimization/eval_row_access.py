"""Canonical accessors for MLflow/benchmark eval row dictionaries.

The optimizer sees rows in several shapes:

* MLflow slash keys: ``inputs/question_id``, ``outputs/response``
* dotted keys: ``inputs.question_id``, ``outputs.predictions.sql``
* nested dicts: ``{"inputs": {"question_id": ...}}``
* request/response payloads, sometimes as JSON strings

All control-plane stages must use this module instead of local row parsers.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Iterable, Iterator
from typing import Any

logger = logging.getLogger(__name__)

IDENT_RE = re.compile(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\b")
DOTTED_IDENT_RE = re.compile(
    r"[a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)+"
)


def normalize_token(value: Any) -> str:
    return str(value or "").strip().lower()


def _json_dict(value: Any) -> dict:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip().startswith("{"):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def nested_get(row: dict, *paths: str, default: Any = "") -> Any:
    for path in paths:
        if path in row and row.get(path) not in (None, ""):
            return row.get(path)
        cur: Any = row
        ok = True
        for part in path.replace("/", ".").split("."):
            if not isinstance(cur, dict) or part not in cur:
                ok = False
                break
            cur = cur.get(part)
        if ok and cur not in (None, ""):
            return cur
    return default


def request_kwargs(row: dict) -> dict:
    request = _json_dict(row.get("request"))
    kwargs = request.get("kwargs")
    if isinstance(kwargs, dict):
        return kwargs
    return request if isinstance(request, dict) else {}


def response_payload(row: dict) -> dict:
    return _json_dict(row.get("response"))


def row_qid(row: dict) -> str:
    """Return the canonical QID for ``row``, or ``""`` if none found.

    Trial 12 consolidation: thin wrapper over
    :func:`_qid_extraction.extract_question_id` so the QID extraction
    ladder has exactly one source of truth. Both modules previously
    maintained independent lookup chains, which is precisely the
    "QID projection is not evidence projection" divergence the Trial
    11 RCA called out.

    Callers that need to distinguish canonical hits from
    ``client_request_id`` trace-id fallbacks should call
    :func:`_qid_extraction.extract_question_id` directly and inspect
    the ``source`` tag.
    """
    from genie_space_optimizer.optimization._qid_extraction import (
        extract_question_id,
    )

    qid, _source = extract_question_id(row or {})
    return qid


def row_question_with_source(row: dict) -> tuple[str, str]:
    """Return ``(text, source_path)`` for the natural-language question.

    See :func:`row_question` for the path priority. The companion
    function records which path won so the Stage 1 contract can name
    the producing row path in its ``field_sources`` map.
    """
    text = nested_get(
        row,
        "inputs/question",
        "inputs.question",
        "question",
        default="",
    )
    if str(text or "").strip():
        # Walk the same path order to identify which one hit.
        for path in ("inputs/question", "inputs.question", "question"):
            if str(nested_get(row, path, default="") or "").strip():
                return str(text).strip(), path
        return str(text).strip(), "inputs/question"
    request = _json_dict(row.get("request"))
    request_top_q = (
        request.get("question") if isinstance(request, dict) else None
    )
    if str(request_top_q or "").strip():
        return str(request_top_q).strip(), "request.question"
    kwargs = request_kwargs(row)
    kw_q = kwargs.get("question") if isinstance(kwargs, dict) else None
    if str(kw_q or "").strip():
        return str(kw_q).strip(), "request.kwargs.question"
    return "", "absent"


def row_question(row: dict) -> str:
    """Return the natural-language question text for an eval row.

    Trial 13 (Phase 2) widening: production rows carry the question at
    ``row["request"]["question"]`` — a top-level sibling of
    ``request.kwargs`` (which holds ``question_id`` but not
    ``question``). The Trial 12 ladder only looked at
    ``inputs/question``, ``inputs.question``, ``question`` and
    ``request.kwargs.question``, so every canonical hard QID failed the
    Stage 1 pre-flight with ``question_text_empty``. The new
    ``request.question`` lookup precedes the kwargs fallback so an
    accidentally-populated kwarg cannot mask the canonical path.
    """
    text, _path = row_question_with_source(row)
    return text


def row_expected_sql(row: dict) -> str:
    kwargs = request_kwargs(row)
    return str(
        kwargs.get("expected_sql")
        or kwargs.get("ground_truth_sql")
        or nested_get(
            row,
            "inputs/expected_sql",
            "inputs.expected_sql",
            "inputs/ground_truth_sql",
            "inputs.ground_truth_sql",
            "inputs/expected_response",
            "inputs.expected_response",
            "expectations/expected_response",
            "expected_sql",
            "ground_truth_sql",
            "expected_response/value",
            "expected_response",
            default="",
        )
        or ""
    ).strip()


def row_generated_sql(row: dict) -> str:
    response = response_payload(row)
    return str(
        nested_get(
            row,
            "outputs/response",
            "outputs.response",
            "outputs/predictions/sql",
            "outputs.predictions.sql",
            "outputs/predictions/query",
            "outputs.predictions.query",
            "generated_sql",
            "genie_sql",
            default="",
        )
        or response.get("response")
        or response.get("sql")
        or response.get("query")
        or ""
    ).strip()


def row_response_text(row: dict) -> str:
    response = response_payload(row)
    return str(
        nested_get(
            row,
            "outputs/predictions/response_text",
            "outputs.predictions.response_text",
            "nl_response",
            default="",
        )
        or response.get("response_text")
        or response.get("text")
        or ""
    ).strip()


def iter_text_values(value: Any) -> Iterator[str]:
    if isinstance(value, str):
        if value.strip():
            yield value
    elif isinstance(value, dict):
        for child in value.values():
            yield from iter_text_values(child)
    elif isinstance(value, Iterable):
        for child in value:
            yield from iter_text_values(child)


def token_terms(value: Any) -> set[str]:
    terms: set[str] = set()
    for text in iter_text_values(value):
        normalized = normalize_token(text)
        if normalized:
            terms.add(normalized)
        for token in IDENT_RE.findall(text):
            terms.add(normalize_token(token))
        for dotted in DOTTED_IDENT_RE.findall(text):
            dotted_norm = normalize_token(dotted)
            terms.add(dotted_norm)
            terms.update(part for part in dotted_norm.split(".") if part)
    return {term for term in terms if term}


def iter_asi_metadata(row: dict) -> Iterator[tuple[str, dict]]:
    for key, value in (row or {}).items():
        if not isinstance(key, str) or not isinstance(value, dict):
            continue
        if key.endswith("/metadata"):
            yield key.rsplit("/", 1)[0].replace("feedback/", ""), value
        elif key.endswith(".metadata"):
            yield key.rsplit(".", 1)[0].replace("feedback.", ""), value


def _sql_surface(sql: str) -> set[str]:
    surface: set[str] = set()
    if not sql:
        return surface
    try:
        import sqlglot
        from sqlglot import exp as sql_exp
    except Exception:
        sqlglot = None  # type: ignore[assignment]
        sql_exp = None  # type: ignore[assignment]

    if sqlglot is not None:
        try:
            parsed = sqlglot.parse_one(sql, read="databricks")
            if parsed is not None:
                for col in parsed.find_all(sql_exp.Column):
                    if getattr(col, "name", None):
                        surface.add(normalize_token(col.name))
                for table in parsed.find_all(sql_exp.Table):
                    if getattr(table, "name", None):
                        surface.add(normalize_token(table.name))
                for fn in parsed.find_all(sql_exp.Func):
                    try:
                        name = fn.sql_name()
                    except Exception:
                        name = ""
                    if name:
                        surface.add(normalize_token(name))
        except Exception:
            logger.debug("sqlglot parse failed; using regex fallback", exc_info=True)

    surface |= token_terms(sql)
    return surface


ASI_SURFACE_KEYS: tuple[str, ...] = (
    "failure_type",
    "wrong_clause",
    "blame_set",
    # Trial 14 — typed blame surface keys. Stringification by
    # ``token_terms`` is intentional: failure-surface clustering
    # tokenises whatever it gets, so a structured-list value
    # contributes its FQN ``ref`` and ``description`` substrings to
    # the cluster keys just like the legacy free-text field used to.
    "blame_set_structured",
    "blame_rationale",
    "counterfactual_fix",
    "expected_objects",
    "actual_objects",
    "rca_kind",
    "patch_family",
)


def asi_metadata_surface(row: dict, *, ignored_judges: set[str] | frozenset[str] = frozenset()) -> set[str]:
    surface: set[str] = set()
    for judge, metadata in iter_asi_metadata(row):
        if judge in ignored_judges:
            continue
        for key in ASI_SURFACE_KEYS:
            surface |= token_terms(metadata.get(key))
    return surface


def extract_failure_surface(row: dict, *, ignored_judges: set[str] | frozenset[str] = frozenset()) -> set[str]:
    surface: set[str] = set()
    surface |= _sql_surface(row_expected_sql(row))
    surface |= _sql_surface(row_generated_sql(row))
    surface |= token_terms(row_question(row))
    surface |= token_terms(row_response_text(row))
    surface |= asi_metadata_surface(row, ignored_judges=ignored_judges)
    return surface


_FLAT_METADATA_PREFIX = "metadata/"


def _strip_one_pair_of_quotes(value: str) -> str:
    """Strip a single matching pair of surrounding ``"``/``'`` from ``value``.

    Helper for :func:`_parse_bracketed_string_list` fallback path so the
    legacy ``"[\"a\", \"b\"]"`` and Python-repr ``"['a', 'b']"`` shapes
    survive the comma-split path with the quote characters removed.
    """
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        return value[1:-1]
    return value


def _parse_bracketed_string_list(raw: str) -> list[str]:
    """Parse a bracketed-string blame set like ``"[A, B]"`` into ``["A", "B"]``.

    Production rows emit ``metadata/<judge>/blame_set`` as a stringified
    list. Two production-wild shapes are accepted:

    * Strict JSON arrays — ``'["DEST_AIRPORT_CD", "ORIG_AIRPORT_CD"]'``.
      Parsed via :func:`json.loads` so embedded commas and escaped
      characters inside quoted elements survive intact
      (Trial 13k headline fix — the legacy comma-split path returned
      tokens with the surrounding ``"`` characters still attached,
      which made ``_is_bare_identifier`` reject every element and
      ``_normalize_seeds_to_fqn`` drop the entire seed set).
    * Legacy bracketed CSV — ``"[DEST_AIRPORT_CD, ORIG_AIRPORT_CD]"`` or
      Python-repr ``"['a', 'b']"``. Fallback path strips the outer
      brackets, splits on ``,``, and removes at most one matching pair
      of surrounding ``'``/``"`` from each piece.

    Other recognized shapes:

    * ``"[]"`` → ``[]``
    * ``"a, b"`` (no brackets, comma-separated) → ``["a", "b"]``
    * unrecognized → single-element list with original text

    The parser remains intentionally permissive because the upstream
    blame_set carries arbitrary identifier substrings, SQL fragments,
    and free text.
    """
    text = (raw or "").strip()
    if not text:
        return []
    if text.startswith("[") and text.endswith("]"):
        try:
            parsed = json.loads(text)
        except (ValueError, TypeError):
            parsed = None
        if isinstance(parsed, list):
            return [
                str(piece).strip()
                for piece in parsed
                if str(piece or "").strip()
            ]
        inner = text[1:-1].strip()
        if not inner:
            return []
        return [
            _strip_one_pair_of_quotes(piece.strip())
            for piece in inner.split(",")
            if piece.strip()
        ]
    if "," in text:
        return [piece.strip() for piece in text.split(",") if piece.strip()]
    return [text]


def _iter_flat_metadata_judges(row: dict) -> Iterator[tuple[str, dict[str, Any]]]:
    """Yield ``(judge, synthetic_metadata_dict)`` from flat ``metadata/<judge>/<field>`` keys.

    Production rows carry ASI fields in two co-existing shapes:

    1. ``<judge>/metadata`` — a nested dict (handled by
       :func:`iter_asi_metadata`).
    2. ``metadata/<judge>/<field>`` — flat keys at the row root, each
       mapping to a string scalar (the actual production shape from
       Trial 12 evidence).

    This helper materializes shape (2) into a synthetic ``judge -> dict``
    mapping so downstream consumers can use a single iteration loop.
    Bracketed-list fields are post-parsed by the consumer.
    """
    grouped: dict[str, dict[str, Any]] = {}
    for key, value in (row or {}).items():
        if not isinstance(key, str) or not key.startswith(_FLAT_METADATA_PREFIX):
            continue
        rest = key[len(_FLAT_METADATA_PREFIX):]
        parts = rest.split("/", 1)
        if len(parts) != 2:
            continue
        judge, field = parts[0], parts[1]
        if not judge or not field:
            continue
        grouped.setdefault(judge, {})[field] = value
    for judge, metadata in grouped.items():
        yield judge, metadata


def _collect_blame_entries_from_asi(row: dict) -> list[Any]:
    """Trial 14 — flat-union of ``blame_set_structured`` entries
    across all ASI metadata judges.

    Walks both the nested ``<judge>/metadata.blame_set_structured``
    surface and the flat ``metadata/<judge>/blame_set_structured`` key
    (the latter is JSON-encoded as ``list[dict]`` by the workbench
    capture writer and by the Trial 14 ``_ASI_FLAT_FIELDS`` flatten
    pass). Every payload goes through
    :func:`coerce_blame_entries` so:

    * judges that emit clean ``list[dict]`` -> trusted entries
    * judges that drift to ``list[str]`` -> heuristically classified
      via the production-wild Trial 13k shapes
    * judges that emit a JSON-encoded string -> parsed and classified

    The returned list is order-preserving and de-duplicated on
    ``(kind, ref, description)``.
    """
    from genie_space_optimizer.optimization.blame_entry import (
        BlameEntry,
        coerce_blame_entries,
    )

    out: list[BlameEntry] = []
    seen: set[tuple[str, str | None, str | None]] = set()

    def _push(entries: list[BlameEntry]) -> None:
        for entry in entries:
            key = (entry.kind, entry.ref, entry.description)
            if key in seen:
                continue
            seen.add(key)
            out.append(entry)

    for _judge, metadata in iter_asi_metadata(row or {}):
        raw = metadata.get("blame_set_structured")
        if raw:
            _push(coerce_blame_entries(raw_structured=raw))
    for _judge, metadata in _iter_flat_metadata_judges(row or {}):
        raw = metadata.get("blame_set_structured")
        if raw:
            # Flat keys are stringified by the production eval row
            # writer and by workbench v2 capture; the coercer accepts
            # either ``str`` (JSON-encoded) or ``list[dict]`` natively.
            _push(coerce_blame_entries(raw_structured=raw))
    return out


def _collect_blame_set_from_asi(row: dict) -> list[str]:
    """Flat-union of ``blame_set`` entries across all ASI metadata judges.

    Order-preserving and de-duplicated. Returns the blame set the
    Stage 1 LLM uses to seed its reasoning.

    Trial 14 — prefer the structured surface first. When any judge
    emits ``blame_set_structured`` the schema-resolvable entries
    (``kind in {column, table, join}``) contribute their ``ref``
    values directly to the seed list. We deliberately do NOT pick up
    ``filter`` / ``instruction`` refs here — those are non-schema
    blame and must not be fed to the FQN normalizer. They surface via
    the marker's ``blame_kind_distribution`` and the
    ``seeds_all_filter_kind`` contract tag instead.

    Falls back to the legacy free-text path (``<judge>/metadata.blame_set``
    and flat ``metadata/<judge>/blame_set``) when no structured entries
    exist. Trial 11 root cause: all three Stage 1 input builders
    hardcoded ``blame_set_seed=[]``. Trial 13k JSON-quoted parser fix
    still applies via :func:`_parse_bracketed_string_list` on the
    legacy fallback.
    """
    from genie_space_optimizer.optimization.blame_entry import SCHEMA_RESOLVABLE_KINDS

    structured = _collect_blame_entries_from_asi(row)
    if structured:
        seen: set[str] = set()
        out: list[str] = []
        for entry in structured:
            if entry.kind not in SCHEMA_RESOLVABLE_KINDS:
                continue
            ref = (entry.ref or "").strip()
            if not ref or ref in seen:
                continue
            seen.add(ref)
            out.append(ref)
        if out:
            return out
        # Schema-resolvable entries existed but none had a usable ref
        # (e.g. all collapsed to filter/instruction during coercion).
        # Return an empty list and let the contract emit
        # ``seeds_all_filter_kind`` rather than back-filling from the
        # legacy field — that would lose the typed signal.
        return []

    # Legacy fallback (pre-Trial-14 rows / judges that did not emit a
    # structured field).
    seen = set()
    out = []

    def _ingest(raw: Any) -> None:
        if isinstance(raw, str):
            entries: Iterable[Any] = _parse_bracketed_string_list(raw)
        elif isinstance(raw, Iterable) and not isinstance(raw, (bytes, bytearray)):
            entries = raw
        else:
            entries = ()
        for entry in entries:
            text = str(entry or "").strip()
            if text and text not in seen:
                seen.add(text)
                out.append(text)

    for _judge, metadata in iter_asi_metadata(row or {}):
        _ingest(metadata.get("blame_set"))
    for _judge, metadata in _iter_flat_metadata_judges(row or {}):
        _ingest(metadata.get("blame_set"))
    return out


def _first_non_empty_asi_field(row: dict, key: str) -> str:
    """Return the first non-empty stringified value for ``key`` across
    every ASI metadata judge attached to ``row``.

    Used by :func:`build_stage1_evidence_card` to derive
    ``rca_evidence.*`` subfields from upstream evidence when typed
    RCA evidence is absent.

    Trial 13 widening: walks both the nested ``<judge>/metadata`` dict
    surface and the production flat-key surface ``metadata/<judge>/<field>``.
    Values that stringify to the sentinel ``"None"`` (common in flat
    metadata) are treated as empty.
    """

    def _norm(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            text = value.strip()
        else:
            text = str(value).strip()
        if text.lower() == "none":
            return ""
        return text

    for _judge, metadata in iter_asi_metadata(row or {}):
        text = _norm(metadata.get(key))
        if text:
            return text
    for _judge, metadata in _iter_flat_metadata_judges(row or {}):
        text = _norm(metadata.get(key))
        if text:
            return text
    return ""


# Canonical judge ordering used when concatenating per-judge rationales
# and when selecting a primary rationale fallback for Stage 1.
_JUDGE_RATIONALE_ORDER: tuple[str, ...] = (
    "arbiter",
    "result_correctness",
    "logical_accuracy",
    "completeness",
    "semantic_equivalence",
    "schema_accuracy",
    "response_quality",
)


def collect_judge_rationales(row: dict) -> dict[str, str]:
    """Return a ``judge -> rationale`` mapping for the eval row.

    Production rows publish per-judge narrative at flat keys
    ``row["<judge>/rationale"]``; there is no top-level
    ``judge_rationale``. The Trial 12 ladder looked only at
    ``row["judge_rationale"]``, which is never populated in the wild.
    """
    out: dict[str, str] = {}
    for key in (row or {}).keys():
        if not isinstance(key, str) or not key.endswith("/rationale"):
            continue
        judge = key.rsplit("/", 1)[0]
        if not judge or "/" in judge:
            continue
        value = row.get(key)
        text = str(value or "").strip()
        if text:
            out[judge] = text
    return out


def derive_judge_rationale(row: dict, *, top_level: str = "") -> str:
    """Derive the Stage 1 ``judge_rationale`` field from per-judge keys.

    Strategy (Trial 13, Phase 2):

    1. Use the explicit top-level ``judge_rationale`` if provided.
    2. Else select the first non-empty per-judge rationale in the
       canonical order: ``arbiter`` → ``result_correctness`` →
       ``logical_accuracy`` → others (alphabetical for stability).
    3. Else return ``""``.

    The Stage 1 LLM expects a single coherent narrative; concatenating
    every judge would balloon the prompt and dilute the signal, so we
    surface the most authoritative single rationale. The full per-judge
    map is available via :func:`collect_judge_rationales` for consumers
    that need it.
    """
    if isinstance(top_level, str) and top_level.strip():
        return top_level.strip()
    rationales = collect_judge_rationales(row or {})
    if not rationales:
        return ""
    for judge in _JUDGE_RATIONALE_ORDER:
        text = rationales.get(judge, "").strip()
        if text:
            return text
    # Fallback: return the first remaining rationale in sorted order so
    # the choice is deterministic even when no canonical judge matched.
    for judge in sorted(rationales):
        text = rationales[judge].strip()
        if text:
            return text
    return ""


def _summarize_generated_sql_issue_from_asi(row: dict) -> str:
    """Derive a human-readable ``generated_sql_issue`` summary from ASI
    metadata when typed evidence is unavailable.

    Combines ``failure_type`` and ``wrong_clause`` when present. The
    Stage 1 LLM uses this as the "what went wrong" hint that Plan 3's
    deterministic classifier would otherwise provide.
    """
    failure_type = _first_non_empty_asi_field(row, "failure_type")
    wrong_clause = _first_non_empty_asi_field(row, "wrong_clause")
    if failure_type and wrong_clause:
        return f"{failure_type} (wrong_clause={wrong_clause})"
    return failure_type or wrong_clause


def build_stage1_evidence_card(
    qid: str,
    row: dict | None,
    *,
    typed_evidence: Any = None,
    schema_columns: tuple[str, ...] | list[str] | None = None,
) -> dict:
    """Hydrate the Stage 1 ``diagnose_failing_qids`` input card from a
    canonical eval row and optional typed RCA evidence.

    Single source of truth for the three Stage 1 input builders:

    * :func:`diagnose_llm._build_failing_qid_payload`
    * :func:`optimizer._build_plan11_failing_qids_from_raw`
    * :func:`optimizer._build_plan11_failing_qids_from_typed_evidence`

    Trial 11 root cause: all three sites previously used flat
    ``row.get(...)`` against production rows (which carry data under
    ``inputs/...``, ``inputs.*``, or ``request.kwargs.*``) and
    hardcoded empty ``rca_evidence`` / ``blame_set_seed`` regardless
    of upstream ASI evidence. The Stage 1 LLM correctly declined
    every call with ``missing_schema_context``.

    Override order (when both are present, the typed value wins):

    1. ``typed_evidence`` — Plan 3's per-QID RCA classifier output.
    2. Row-derived ASI metadata via :func:`iter_asi_metadata` over
       :data:`ASI_SURFACE_KEYS`.
    3. ``judge_rationale`` (top-level narrative).

    Trial 13i — ``schema_columns`` is the run-level FQN universe; when
    provided, the post-ASI seed list is run through
    :func:`schema_columns._normalize_seeds_to_fqn`. The normalizer
    swaps bare identifiers (``"DEST_AIRPORT_CD"``) for FQNs whose
    column-leaf matches and drops compound text (``"LIMIT 10 vs
    RANK() <= 10"``). The card carries a ``_seed_normalization``
    diagnostic dict that the SM lane emits onto
    ``GSO_PLAN11_STAGE1_INPUT_QUALITY_V1``. When ``schema_columns`` is
    None / empty the normalizer is skipped and the field reports
    ``seeds_pre_normalize == seeds_post_normalize`` with zero
    swap/drop counts (legacy callers stay byte-stable).
    """
    safe_row: dict = row or {}
    judge_rationale = derive_judge_rationale(
        safe_row,
        top_level=str(safe_row.get("judge_rationale") or "").strip(),
    )
    if not judge_rationale and typed_evidence is not None:
        judge_rationale = str(
            getattr(typed_evidence, "observed_failure", "") or ""
        ).strip()
    blame_set_seed: list[str]
    if typed_evidence is not None:
        typed_blame = getattr(typed_evidence, "blame_set", ())
        blame_set_seed = [str(b) for b in (typed_blame or ()) if str(b).strip()]
    else:
        blame_set_seed = []
    if not blame_set_seed:
        blame_set_seed = _collect_blame_set_from_asi(safe_row)

    # Trial 14 — capture the typed structured entries before
    # FQN normalization so the contract validator can see WHY the
    # seed list is empty (no entries at all vs all-non-schema-kind
    # vs all-dropped-by-resolver). The Stage 1 contract emits
    # ``seeds_all_filter_kind`` when the pre-normalization seed list
    # is empty but the structured payload was non-empty and entirely
    # ``filter``/``instruction``; the marker carries the per-kind
    # distribution for postmortem triage.
    _blame_entries = _collect_blame_entries_from_asi(safe_row)
    _blame_structured_payload = [entry.to_dict() for entry in _blame_entries]

    # Trial 13i — normalize free-text ASI seed tokens to 4-part FQNs
    # against the run-level schema_columns universe.
    seeds_pre_normalize = len(blame_set_seed)
    seeds_normalized_count = 0
    seeds_dropped_count = 0
    schema_columns_tuple: tuple[str, ...] = tuple(
        s for s in (schema_columns or ()) if isinstance(s, str) and s.strip()
    )
    if schema_columns_tuple and blame_set_seed:
        from genie_space_optimizer.optimization.schema_columns import (
            _normalize_seeds_to_fqn,
        )
        (
            blame_set_seed,
            seeds_normalized_count,
            seeds_dropped_count,
        ) = _normalize_seeds_to_fqn(blame_set_seed, schema_columns_tuple)
    seeds_post_normalize = len(blame_set_seed)

    def _typed_or_row(field: str, row_fallback: str = "") -> str:
        if typed_evidence is not None:
            text = str(getattr(typed_evidence, field, "") or "").strip()
            if text:
                return text
        return row_fallback

    observed_failure = _typed_or_row("observed_failure", judge_rationale)
    generated_sql_issue = _typed_or_row(
        "generated_sql_issue",
        _summarize_generated_sql_issue_from_asi(safe_row),
    )
    expected_sql_shape = _typed_or_row(
        "expected_sql_shape",
        _first_non_empty_asi_field(safe_row, "counterfactual_fix"),
    )
    suggested_repair_family = _typed_or_row(
        "suggested_repair_family",
        _first_non_empty_asi_field(safe_row, "patch_family"),
    )
    confidence = (
        str(getattr(typed_evidence, "confidence", "") or "").strip()
        if typed_evidence is not None
        else ""
    )

    question_text, question_source_path = row_question_with_source(safe_row)
    return {
        "qid": str(qid),
        "question_text": question_text,
        "ground_truth_sql": row_expected_sql(safe_row),
        "generated_sql": row_generated_sql(safe_row),
        "judge_rationale": judge_rationale,
        "blame_set_seed": blame_set_seed,
        "rca_evidence": {
            "observed_failure": observed_failure,
            "generated_sql_issue": generated_sql_issue,
            "expected_sql_shape": expected_sql_shape,
            "suggested_repair_family": suggested_repair_family,
            "confidence": confidence,
        },
        # Trial 13 — per-field provenance trail consumed by
        # ``Stage1InputEvidenceContract.field_sources``. The contract
        # prefers these typed paths over the legacy
        # ``"present"`` / ``"absent"`` enum so postmortems can name
        # the exact production-row path that drifted when the next
        # row-shape regression lands.
        "_source_paths": {
            "question_text": question_source_path,
        },
        # Trial 13i — seed normalization diagnostics consumed by the
        # ``GSO_PLAN11_STAGE1_INPUT_QUALITY_V1`` marker. Underscore-
        # prefixed so the Stage 1 contract's existing card walk ignores
        # it (the contract only checks named top-level fields).
        "_seed_normalization": {
            "seeds_pre_normalize": int(seeds_pre_normalize),
            "seeds_post_normalize": int(seeds_post_normalize),
            "seeds_normalized": int(seeds_normalized_count),
            "seeds_dropped": int(seeds_dropped_count),
        },
        # Trial 14 — typed blame surface stamped onto the card for the
        # contract (``seeds_all_filter_kind`` arm) and the
        # ``GSO_PLAN11_STAGE1_INPUT_QUALITY_V1`` marker
        # (``blame_kind_distribution`` field). The payload is the
        # ``list[dict]`` form so consumers don't need to import
        # ``BlameEntry``; the typed dataclass round-trips via
        # ``BlameEntry.from_json`` if needed.
        "_blame_structured": _blame_structured_payload,
    }


def rows_by_qid(rows: Iterable[dict]) -> dict[str, dict]:
    indexed: dict[str, dict] = {}
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        qid = row_qid(row)
        if qid:
            indexed[qid] = row
    return indexed


def rows_for_qids(rows: Iterable[dict], qids: Iterable[str]) -> list[dict]:
    indexed = rows_by_qid(rows)
    out: list[dict] = []
    for qid in qids or []:
        row = indexed.get(str(qid))
        if row is not None:
            out.append(row)
    return out
