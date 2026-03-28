"""
Pydantic v2 models for the Genie Space ``serialized_space`` structure.

Used to validate config payloads **before** PATCH requests so that
malformed data never reaches the Databricks API.  All models use
``extra="allow"`` for forward-compatibility with new API fields.

Validation modes
~~~~~~~~~~~~~~~~
* **lenient** (``strict=False``, default) — structural / type checks only.
  Safe for configs read from the API that may contain legacy data.
* **strict** (``strict=True``) — additionally enforces all official
  Databricks API rules: 32-char hex IDs, sort order, uniqueness
  constraints, size limits, and structural requirements.
  Used in ``patch_space_config()`` before sending to the API.

Reference: https://docs.databricks.com/aws/en/genie/conversation-api#validation-rules-for-serialized_space
"""

from __future__ import annotations

import datetime
import logging
import random
import re
from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

logger = logging.getLogger(__name__)

_HEX32_RE = re.compile(r"[0-9a-f]{32}")
_THREE_LEVEL_RE = re.compile(r"^[^.]+\.[^.]+\.[^.]+$")

MAX_STRING_LENGTH = 25_000
MAX_ARRAY_SIZE = 10_000
MAX_INSTRUCTION_SLOTS = 100


# ── ID generation utility ────────────────────────────────────────────


def generate_genie_id() -> str:
    """Generate a valid 32-char lowercase hex ID (time-ordered UUID).

    Follows the Databricks recommendation for Genie space IDs:
    time-ordered so that IDs generated in sequence sort alphabetically.
    """
    epoch = datetime.datetime(1582, 10, 15)
    t = int((datetime.datetime.now() - epoch).total_seconds() * 1e7)
    hi = (t & 0xFFFF_FFFF_FFFF_0000) | (1 << 12) | ((t & 0xFFFF) >> 4)
    lo = random.getrandbits(62) | 0x8000_0000_0000_0000
    return f"{hi:016x}{lo:016x}"


def _is_metric_view_identifier(identifier: str, config: dict | None = None) -> bool:
    """Check if a table identifier is a metric view.

    When *config* is provided, checks ``data_sources.metric_views``
    deterministically.  Falls back to name-prefix heuristic (``mv_``,
    ``metric_``) when no config is available.
    """
    if config:
        ds = config.get("data_sources", {})
        if isinstance(ds, dict):
            mv_identifiers = {
                (mv.get("identifier") or "").lower()
                for mv in (ds.get("metric_views", []) or [])
                if isinstance(mv, dict)
            }
            if mv_identifiers:
                return identifier.lower() in mv_identifiers
    short = identifier.rsplit(".", 1)[-1].lower() if identifier else ""
    return short.startswith("mv_") or short.startswith("metric_")


def ensure_join_spec_fields(spec: dict, config: dict | None = None) -> dict:
    """Ensure a join spec dict has all required fields (alias, id, instruction).

    Mutates and returns the spec for convenience. Always derives ``alias``
    from the last segment of ``identifier`` (overriding any LLM-provided
    alias to prevent rejected short aliases like ``jt``), and generates a
    new ``id`` when absent.  Also normalises ``sql`` predicates so that
    table references match ``left.alias`` / ``right.alias``.

    When *config* is provided, metric view detection uses
    ``data_sources.metric_views`` deterministically.  When either side
    is a metric view and no ``instruction`` is set, auto-populates it
    with CTE-first guidance to prevent METRIC_VIEW_JOIN_NOT_SUPPORTED.
    """
    for side_key in ("left", "right"):
        side = spec.get(side_key)
        if isinstance(side, dict) and "identifier" in side:
            side["alias"] = side["identifier"].rsplit(".", 1)[-1]
    if not spec.get("id"):
        spec["id"] = generate_genie_id()
    spec = normalize_join_spec_sql(spec)

    left_id = (spec.get("left") or {}).get("identifier", "")
    right_id = (spec.get("right") or {}).get("identifier", "")
    left_is_mv = _is_metric_view_identifier(left_id, config)
    right_is_mv = _is_metric_view_identifier(right_id, config)

    if (left_is_mv or right_is_mv) and not spec.get("instruction"):
        mv_name = left_id.rsplit(".", 1)[-1] if left_is_mv else right_id.rsplit(".", 1)[-1]
        dim_name = right_id.rsplit(".", 1)[-1] if left_is_mv else left_id.rsplit(".", 1)[-1]
        spec["instruction"] = [
            f"METRIC VIEW JOIN: Do NOT use a direct JOIN between {mv_name} and {dim_name}. "
            f"Instead, materialize {mv_name} in a CTE (WITH clause) using MEASURE() and GROUP BY ALL, "
            f"then JOIN the CTE result to {dim_name}. "
            f"Direct JOINs on metric views cause METRIC_VIEW_JOIN_NOT_SUPPORTED errors."
        ]

    if spec.get("instruction") and isinstance(spec["instruction"], str):
        spec["instruction"] = [spec["instruction"]]

    return spec


# ── Join spec SQL normalisation ──────────────────────────────────────

_TABLE_DOT_COL_RE = re.compile(r"`?(\w+)`?\s*\.\s*`?(\w+)`?")


def _alias_match_score(short: str, canonical: str) -> int:
    """Score how well *short* abbreviates *canonical* (higher = better)."""
    s, c = short.lower(), canonical.lower()
    if s == c:
        return 100
    parts = c.split("_")
    if parts and parts[0] in ("dim", "fact", "mv"):
        parts = parts[1:]
    if parts and "".join(p[0] for p in parts if p) == s:
        return 80
    if c.startswith(s):
        return 50
    if s in c:
        return 20
    return 0


def _map_old_to_canonical(
    old_aliases: list[str], left_alias: str, right_alias: str,
) -> dict[str, str]:
    """Map two old SQL aliases to the spec's canonical left/right aliases."""
    a, b = old_aliases

    for old, other in ((a, b), (b, a)):
        if old == left_alias:
            return {old: left_alias, other: right_alias}
        if old == right_alias:
            return {old: right_alias, other: left_alias}

    score_ab = _alias_match_score(a, left_alias) + _alias_match_score(b, right_alias)
    score_ba = _alias_match_score(a, right_alias) + _alias_match_score(b, left_alias)
    if score_ab >= score_ba:
        return {a: left_alias, b: right_alias}
    return {a: right_alias, b: left_alias}


def _rewrite_predicate_aliases(
    predicate: str, left_alias: str, right_alias: str,
) -> str:
    """Rewrite table references in a join predicate to use canonical aliases.

    Replaces arbitrary SQL aliases (e.g. ``jt``, ``j``) with the spec's
    ``left.alias`` / ``right.alias`` and ensures backtick quoting so the
    Genie API proto parser can resolve them.
    """
    refs = _TABLE_DOT_COL_RE.findall(predicate)
    if not refs:
        return predicate

    old_aliases = list(dict.fromkeys(tbl for tbl, _ in refs))
    canonical = {left_alias, right_alias}

    if set(old_aliases) == canonical:
        return _TABLE_DOT_COL_RE.sub(r"`\1`.`\2`", predicate)

    if len(old_aliases) != 2:
        return predicate

    mapping = _map_old_to_canonical(old_aliases, left_alias, right_alias)

    def _replace(m: re.Match) -> str:
        tbl, col = m.group(1), m.group(2)
        return f"`{mapping.get(tbl, tbl)}`.`{col}`"

    return _TABLE_DOT_COL_RE.sub(_replace, predicate)


def normalize_join_spec_sql(spec: dict) -> dict:
    """Rewrite ``sql[]`` predicates so table refs use left/right aliases.

    The Genie API requires that equijoin predicates in the ``sql`` field
    reference the canonical aliases declared in ``left.alias`` and
    ``right.alias``.  Execution-proven SQL and LLM-generated conditions
    often use arbitrary short aliases (e.g. ``jt``, ``j``) that the API
    proto parser rejects.
    """
    left_alias = (spec.get("left") or {}).get("alias", "")
    right_alias = (spec.get("right") or {}).get("alias", "")
    if not left_alias or not right_alias:
        return spec

    sql_parts = spec.get("sql", [])
    if isinstance(sql_parts, str):
        sql_parts = [sql_parts]
    if not sql_parts:
        return spec

    split_parts: list[str] = []
    for part in sql_parts:
        if isinstance(part, str) and not part.startswith("--rt="):
            split_parts.extend(p.strip() for p in re.split(r"\bAND\b", part, flags=re.IGNORECASE) if p.strip())
        else:
            split_parts.append(part)
    sql_parts = split_parts

    rewritten: list[str] = []
    for part in sql_parts:
        if not isinstance(part, str):
            rewritten.append(part)
            continue
        if part.startswith("--rt="):
            rewritten.append(part)
            continue
        rewritten.append(_rewrite_predicate_aliases(part, left_alias, right_alias))
    spec["sql"] = rewritten
    return spec


# ── Leaf-level models ─────────────────────────────────────────────────


class ColumnConfig(BaseModel):
    """A single column configuration within a table or metric view."""

    model_config = ConfigDict(extra="allow")

    column_name: str
    description: list[str] | None = None
    synonyms: list[str] | None = None
    enable_format_assistance: bool | None = None
    enable_entity_matching: bool | None = None
    exclude: bool | None = None

    @field_validator("description", "synonyms", mode="before")
    @classmethod
    def _coerce_str_to_list(cls, v: Any) -> Any:
        """Accept a bare string and wrap it in a list."""
        if isinstance(v, str):
            return [v]
        return v


class TableDataSource(BaseModel):
    """A table or metric view entry in ``data_sources``."""

    model_config = ConfigDict(extra="allow")

    identifier: str
    column_configs: list[ColumnConfig] | None = None
    description: list[str] | None = None

    @field_validator("description", mode="before")
    @classmethod
    def _coerce_str_to_list(cls, v: Any) -> Any:
        if isinstance(v, str):
            return [v]
        return v


class JoinSpecSide(BaseModel):
    """One side (left / right) of a join specification."""

    model_config = ConfigDict(extra="allow")

    identifier: str
    alias: str


class JoinSpec(BaseModel):
    """A join specification linking two data sources."""

    model_config = ConfigDict(extra="allow")

    left: JoinSpecSide
    right: JoinSpecSide
    sql: list[str]
    id: str | None = None
    comment: list[str] | None = None
    instruction: list[str] | None = None

    @field_validator("sql", "comment", "instruction", mode="before")
    @classmethod
    def _coerce_str_to_list(cls, v: Any) -> Any:
        if isinstance(v, str):
            return [v]
        return v


class SampleQuestion(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: str
    question: list[str]

    @field_validator("question", mode="before")
    @classmethod
    def _coerce_str_to_list(cls, v: Any) -> Any:
        if isinstance(v, str):
            return [v]
        return v


# ── Instructions sub-models ──────────────────────────────────────────


class TextInstruction(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: str
    content: list[str]

    @field_validator("content", mode="before")
    @classmethod
    def _coerce_str_to_list(cls, v: Any) -> Any:
        if isinstance(v, str):
            return [v]
        return v


class ParameterDefaultValue(BaseModel):
    model_config = ConfigDict(extra="allow")

    values: list[str]

    @field_validator("values", mode="before")
    @classmethod
    def _coerce_str_to_list(cls, v: Any) -> Any:
        if isinstance(v, str):
            return [v]
        return v


class ExampleSqlParameter(BaseModel):
    model_config = ConfigDict(extra="allow")

    name: str
    type_hint: str | None = None
    description: list[str] | None = None
    default_value: ParameterDefaultValue | None = None

    @field_validator("description", mode="before")
    @classmethod
    def _coerce_str_to_list(cls, v: Any) -> Any:
        if isinstance(v, str):
            return [v]
        return v

    @field_validator("default_value", mode="before")
    @classmethod
    def _coerce_default_value(cls, v: Any) -> Any:
        if isinstance(v, str):
            return {"values": [v]}
        return v


class ExampleQuestionSql(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: str
    question: list[str]
    sql: list[str]
    parameters: list[ExampleSqlParameter] | None = None
    usage_guidance: list[str] | None = None

    @field_validator("question", "sql", mode="before")
    @classmethod
    def _coerce_str_to_list(cls, v: Any) -> Any:
        if isinstance(v, str):
            return [v]
        return v

    @field_validator("usage_guidance", mode="before")
    @classmethod
    def _coerce_guidance(cls, v: Any) -> Any:
        if isinstance(v, str):
            return [v]
        return v


class SqlFunction(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: str
    identifier: str


# ── SQL Snippet sub-models ───────────────────────────────────────────


class SqlSnippetFilter(BaseModel):
    """A reusable SQL filter snippet."""

    model_config = ConfigDict(extra="allow")

    id: str
    sql: list[str]
    display_name: str | None = None
    synonyms: list[str] | None = None
    comment: list[str] | None = None
    instruction: list[str] | None = None

    @field_validator("sql", "synonyms", "comment", "instruction", mode="before")
    @classmethod
    def _coerce_str_to_list(cls, v: Any) -> Any:
        if isinstance(v, str):
            return [v]
        return v


class SqlSnippetExpression(BaseModel):
    """A reusable SQL expression snippet."""

    model_config = ConfigDict(extra="allow")

    id: str
    alias: str
    sql: list[str]
    display_name: str | None = None
    synonyms: list[str] | None = None
    comment: list[str] | None = None
    instruction: list[str] | None = None

    @field_validator("sql", "synonyms", "comment", "instruction", mode="before")
    @classmethod
    def _coerce_str_to_list(cls, v: Any) -> Any:
        if isinstance(v, str):
            return [v]
        return v


class SqlSnippetMeasure(BaseModel):
    """A reusable SQL measure snippet."""

    model_config = ConfigDict(extra="allow")

    id: str
    alias: str
    sql: list[str]
    display_name: str | None = None
    synonyms: list[str] | None = None
    comment: list[str] | None = None
    instruction: list[str] | None = None

    @field_validator("sql", "synonyms", "comment", "instruction", mode="before")
    @classmethod
    def _coerce_str_to_list(cls, v: Any) -> Any:
        if isinstance(v, str):
            return [v]
        return v


class SqlSnippets(BaseModel):
    """Container for reusable SQL snippets (filters, expressions, measures)."""

    model_config = ConfigDict(extra="allow")

    filters: list[SqlSnippetFilter] | None = None
    expressions: list[SqlSnippetExpression] | None = None
    measures: list[SqlSnippetMeasure] | None = None


# ── Benchmark sub-models ─────────────────────────────────────────────


class BenchmarkAnswer(BaseModel):
    """A ground-truth answer for a benchmark question (typically SQL)."""

    model_config = ConfigDict(extra="allow")

    format: str
    content: list[str]

    @field_validator("content", mode="before")
    @classmethod
    def _coerce_str_to_list(cls, v: Any) -> Any:
        if isinstance(v, str):
            return [v]
        return v


class BenchmarkQuestion(BaseModel):
    """A single benchmark question entry in the Genie Space config."""

    model_config = ConfigDict(extra="allow")

    id: str | None = None
    question: list[str]
    answer: list[BenchmarkAnswer] | None = None

    @field_validator("question", mode="before")
    @classmethod
    def _coerce_str_to_list(cls, v: Any) -> Any:
        if isinstance(v, str):
            return [v]
        return v


class Benchmarks(BaseModel):
    """The ``benchmarks`` block inside ``serialized_space``."""

    model_config = ConfigDict(extra="allow")

    questions: list[BenchmarkQuestion] | None = None


# ── Composite models ─────────────────────────────────────────────────


class Instructions(BaseModel):
    model_config = ConfigDict(extra="allow")

    text_instructions: list[TextInstruction] | None = None
    example_question_sqls: list[ExampleQuestionSql] | None = None
    sql_functions: list[SqlFunction] | None = None
    join_specs: list[JoinSpec] | None = None
    sql_snippets: SqlSnippets | None = None


class DataSources(BaseModel):
    model_config = ConfigDict(extra="allow")

    tables: list[TableDataSource] | None = None
    metric_views: list[TableDataSource] | None = None


class SpaceConfig(BaseModel):
    """The ``config`` block inside ``serialized_space``."""

    model_config = ConfigDict(extra="allow")

    sample_questions: list[SampleQuestion] | None = None


class SerializedSpace(BaseModel):
    """Top-level model for the ``serialized_space`` payload."""

    model_config = ConfigDict(extra="allow")

    version: int | None = None
    config: SpaceConfig | None = None
    data_sources: DataSources | None = None
    instructions: Instructions | None = None
    benchmarks: Benchmarks | None = None

    @model_validator(mode="after")
    def _check_has_data_sources(self) -> "SerializedSpace":
        if self.data_sources is None:
            raise ValueError("serialized_space must contain 'data_sources'")
        return self


# ── Strict validation helpers ────────────────────────────────────────


def _validate_id_format(id_value: str, location: str, errors: list[str]) -> None:
    """Check that an ID is a 32-char lowercase hex string."""
    if not _HEX32_RE.fullmatch(id_value):
        errors.append(
            f"{location}: ID '{id_value}' is not a valid 32-char lowercase hex string"
        )


def _check_sorted(
    items: list[Any],
    key_fn: Any,
    collection_name: str,
    errors: list[str],
) -> None:
    """Verify a list is sorted by the given key function."""
    keys = [key_fn(item) for item in items]
    for i in range(len(keys) - 1):
        if keys[i] > keys[i + 1]:
            errors.append(
                f"{collection_name} is not sorted: "
                f"'{keys[i]}' comes before '{keys[i + 1]}'"
            )
            return


def _check_string_lengths(obj: Any, path: str, errors: list[str]) -> None:
    """Recursively check that no individual string exceeds MAX_STRING_LENGTH."""
    if isinstance(obj, str):
        if len(obj) > MAX_STRING_LENGTH:
            errors.append(
                f"{path}: string length {len(obj)} exceeds limit {MAX_STRING_LENGTH}"
            )
    elif isinstance(obj, list):
        if len(obj) > MAX_ARRAY_SIZE:
            errors.append(
                f"{path}: array size {len(obj)} exceeds limit {MAX_ARRAY_SIZE}"
            )
        for i, item in enumerate(obj):
            _check_string_lengths(item, f"{path}[{i}]", errors)
    elif isinstance(obj, dict):
        for k, v in obj.items():
            _check_string_lengths(v, f"{path}.{k}", errors)


def count_instruction_slots(config: dict) -> int:
    """Count the number of Genie API instruction slots consumed by *config*.

    The Genie API enforces a limit of 100 slots total.  The API counts
    **all** of the following toward that budget:

    - Each ``example_question_sqls`` entry = 1 slot
    - Each ``sql_functions`` entry = 1 slot
    - The entire ``text_instructions`` block = 1 slot (regardless of length)
    - Each table in ``data_sources.tables`` with a non-empty ``description`` = 1 slot
    - Each metric view in ``data_sources.metric_views`` with a non-empty ``description`` = 1 slot

    ``join_specs`` and ``sql_snippets`` do **not** count.
    """
    instructions = config.get("instructions") or {}
    text_count = min(len(instructions.get("text_instructions") or []), 1)
    example_count = len(instructions.get("example_question_sqls") or [])
    function_count = len(instructions.get("sql_functions") or [])

    ds = config.get("data_sources") or {}
    table_desc_count = sum(
        1 for t in (ds.get("tables") or [])
        if isinstance(t, dict) and t.get("description")
    )
    mv_desc_count = sum(
        1 for m in (ds.get("metric_views") or [])
        if isinstance(m, dict) and m.get("description")
    )

    return text_count + example_count + function_count + table_desc_count + mv_desc_count


def _strict_validate(config: dict) -> list[str]:
    """Run all strict validation rules against a parsed config dict.

    Returns a list of error messages (empty = valid).
    """
    errors: list[str] = []

    # ── Version ──────────────────────────────────────────────────
    version = config.get("version")
    if version is None:
        errors.append("version: required field is missing")
    elif version not in (1, 2):
        errors.append(f"version: must be 1 or 2, got {version}")

    # ── Collect and validate IDs ─────────────────────────────────
    sample_q_ids: list[str] = []
    cfg = config.get("config") or {}
    for sq in cfg.get("sample_questions") or []:
        sid = sq.get("id", "")
        sample_q_ids.append(sid)
        _validate_id_format(sid, "config.sample_questions", errors)

    benchmark_q_ids: list[str] = []
    bm = config.get("benchmarks") or {}
    for bq in bm.get("questions") or []:
        bid = bq.get("id", "")
        if not bid:
            errors.append("benchmarks.questions: id is required")
        else:
            benchmark_q_ids.append(bid)
            _validate_id_format(bid, "benchmarks.questions", errors)

    instruction_ids: list[str] = []
    inst = config.get("instructions") or {}

    for ti in inst.get("text_instructions") or []:
        tid = ti.get("id", "")
        instruction_ids.append(tid)
        _validate_id_format(tid, "instructions.text_instructions", errors)

    for eq in inst.get("example_question_sqls") or []:
        eid = eq.get("id", "")
        instruction_ids.append(eid)
        _validate_id_format(eid, "instructions.example_question_sqls", errors)

    for sf in inst.get("sql_functions") or []:
        sfid = sf.get("id", "")
        instruction_ids.append(sfid)
        _validate_id_format(sfid, "instructions.sql_functions", errors)

    for js in inst.get("join_specs") or []:
        jsid = js.get("id", "")
        if not jsid:
            errors.append("instructions.join_specs: id is required")
        else:
            instruction_ids.append(jsid)
            _validate_id_format(jsid, "instructions.join_specs", errors)

    snippets = inst.get("sql_snippets") or {}
    for snippet_type in ("filters", "expressions", "measures"):
        for item in snippets.get(snippet_type) or []:
            snip_id = item.get("id", "")
            instruction_ids.append(snip_id)
            _validate_id_format(
                snip_id, f"instructions.sql_snippets.{snippet_type}", errors
            )
            sql_val = item.get("sql", [])
            if not sql_val or (isinstance(sql_val, list) and all(not s for s in sql_val)):
                errors.append(
                    f"instructions.sql_snippets.{snippet_type}: sql must not be empty"
                )

    # ── Uniqueness: question IDs ─────────────────────────────────
    all_question_ids = sample_q_ids + benchmark_q_ids
    seen_q: set[str] = set()
    for qid in all_question_ids:
        if qid in seen_q:
            errors.append(f"Duplicate question ID across sample_questions/benchmarks: '{qid}'")
        seen_q.add(qid)

    # ── Uniqueness: instruction IDs ──────────────────────────────
    seen_i: set[str] = set()
    for iid in instruction_ids:
        if iid in seen_i:
            errors.append(f"Duplicate instruction ID: '{iid}'")
        seen_i.add(iid)

    # ── Uniqueness: (table_identifier, column_name) ──────────────
    ds = config.get("data_sources") or {}
    col_tuples: set[tuple[str, str]] = set()
    for source_key in ("tables", "metric_views"):
        for tbl in ds.get(source_key) or []:
            tbl_id = tbl.get("identifier", "")
            for cc in tbl.get("column_configs") or []:
                col_name = cc.get("column_name", "")
                key = (tbl_id, col_name)
                if key in col_tuples:
                    errors.append(
                        f"Duplicate column config: ({tbl_id}, {col_name})"
                    )
                col_tuples.add(key)

    # ── Sorting ──────────────────────────────────────────────────
    for source_key in ("tables", "metric_views"):
        items = ds.get(source_key) or []
        _check_sorted(
            items,
            lambda x: x.get("identifier", ""),
            f"data_sources.{source_key}",
            errors,
        )
        for tbl in items:
            ccs = tbl.get("column_configs") or []
            if ccs:
                tbl_id = tbl.get("identifier", "")
                _check_sorted(
                    ccs,
                    lambda x: x.get("column_name", ""),
                    f"data_sources.{source_key}[{tbl_id}].column_configs",
                    errors,
                )

    sqs = cfg.get("sample_questions") or []
    _check_sorted(sqs, lambda x: x.get("id", ""), "config.sample_questions", errors)

    for key in ("text_instructions", "example_question_sqls", "join_specs"):
        items = inst.get(key) or []
        _check_sorted(items, lambda x: x.get("id", ""), f"instructions.{key}", errors)

    sf_items = inst.get("sql_functions") or []
    _check_sorted(
        sf_items,
        lambda x: (x.get("id", ""), x.get("identifier", "")),
        "instructions.sql_functions",
        errors,
    )

    for snippet_type in ("filters", "expressions", "measures"):
        items = snippets.get(snippet_type) or []
        _check_sorted(
            items,
            lambda x: x.get("id", ""),
            f"instructions.sql_snippets.{snippet_type}",
            errors,
        )

    bm_questions = bm.get("questions") or []
    _check_sorted(
        bm_questions, lambda x: x.get("id", ""), "benchmarks.questions", errors
    )

    # ── Text instructions limit ──────────────────────────────────
    ti_list = inst.get("text_instructions") or []
    if len(ti_list) > 1:
        errors.append(
            f"instructions.text_instructions: at most 1 allowed, got {len(ti_list)}"
        )

    # ── Instruction slot budget (100 total) ──────────────────────
    slot_count = count_instruction_slots(config)
    if slot_count > MAX_INSTRUCTION_SLOTS:
        _text_ct = min(len(ti_list), 1)
        _eq_ct = len(inst.get("example_question_sqls") or [])
        _fn_ct = len(inst.get("sql_functions") or [])
        _tbl_ct = sum(1 for t in (ds.get("tables") or []) if isinstance(t, dict) and t.get("description"))
        _mv_ct = sum(1 for m in (ds.get("metric_views") or []) if isinstance(m, dict) and m.get("description"))
        errors.append(
            f"Instruction slot budget exceeded: {slot_count}/{MAX_INSTRUCTION_SLOTS} "
            f"(example_sqls={_eq_ct}, sql_functions={_fn_ct}, "
            f"text_instructions={_text_ct}, table_descs={_tbl_ct}, mv_descs={_mv_ct})"
        )

    # ── Table identifier format ──────────────────────────────────
    for source_key in ("tables", "metric_views"):
        for tbl in ds.get(source_key) or []:
            ident = tbl.get("identifier", "")
            if not _THREE_LEVEL_RE.match(ident):
                errors.append(
                    f"data_sources.{source_key}: identifier '{ident}' "
                    "must use three-level namespace (catalog.schema.table)"
                )

    # ── Benchmark answer validation ──────────────────────────────
    for bq in bm.get("questions") or []:
        answers = bq.get("answer") or []
        q_text = (bq.get("question") or ["?"])[0][:40]
        if len(answers) != 1:
            errors.append(
                f"benchmarks.questions['{q_text}...']: "
                f"must have exactly 1 answer, got {len(answers)}"
            )
        elif answers[0].get("format") != "SQL":
            errors.append(
                f"benchmarks.questions['{q_text}...']: "
                f"answer format must be 'SQL', got '{answers[0].get('format')}'"
            )

    # ── Size / length limits ─────────────────────────────────────
    _check_string_lengths(config, "root", errors)

    return errors


# ── Public validation entry point ────────────────────────────────────


def validate_serialized_space(
    config: dict,
    *,
    strict: bool = False,
) -> tuple[bool, list[str]]:
    """Validate a config dict against the Genie ``serialized_space`` schema.

    Parameters
    ----------
    config:
        The parsed ``serialized_space`` dict.
    strict:
        When True, enforces all official Databricks API rules (ID format,
        sorting, uniqueness, size limits).  When False (default), only
        structural / type checks are performed.

    Returns ``(True, [])`` on success or ``(False, [error, ...])`` on failure.
    """
    errors: list[str] = []

    if not isinstance(config, dict):
        return False, [f"Expected dict, got {type(config).__name__}"]

    try:
        SerializedSpace.model_validate(config)
    except Exception as exc:
        for line in str(exc).split("\n"):
            line = line.strip()
            if line:
                errors.append(line)

    if strict:
        errors.extend(_strict_validate(config))

    if errors:
        logger.warning("Genie config validation errors: %s", errors)
        return False, errors

    return True, []
