"""Metric view advisor persistence (MV-D7): DDL registration and round-trips.

The three stores have no live Spark in this suite, so each round-trip runs the
real ``MERGE`` builder, captures the statement a Spark session would have
received, replays it into an in-memory table, and reads it back through the
real accessor. That exercises the column names, the JSON transport, and the
upsert key rather than asserting against a mock.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
from typing import Any

import pandas as pd
import pytest
import sqlglot

from genie_space_optimizer.common.config import (
    TABLE_MV_CANDIDATES,
    TABLE_MV_CONSENTS,
    TABLE_MV_CREATED_OBJECTS,
)
from genie_space_optimizer.optimization import mv_state
from genie_space_optimizer.optimization.ddl import (
    _ALL_DDL,
    ADDITIVE_COLUMN_MIGRATIONS,
)


# ── A MERGE-replaying fake Delta table ───────────────────────────────────

_MERGE_RE = re.compile(
    r"MERGE INTO (?P<fqn>\S+) AS t "
    r"USING \(SELECT (?P<source>.*?)\) AS s "
    r"ON (?P<on>.*?) "
    r"WHEN MATCHED THEN UPDATE SET (?P<set>.*?) "
    r"WHEN NOT MATCHED THEN INSERT \((?P<cols>.*?)\) "
    r"VALUES \((?P<vals>.*)\)$",
    re.DOTALL,
)

_UNBASE64_RE = re.compile(r"^CAST\(unbase64\('(?P<b64>[^']*)'\) AS STRING\)$")

_DDL_COLUMN_RE = re.compile(
    r"^\s{4}(\w+)\s+(?:STRING|INT|DOUBLE|BOOLEAN|TIMESTAMP)\b", re.MULTILINE,
)


def _ddl_columns(table: str) -> list[str]:
    """Column names a table accepts: its CREATE statement plus additive columns.

    Additive migrations (MV-D23 ``yaml_text``, MV-D24 ``provenance``) land via
    ``ADDITIVE_COLUMN_MIGRATIONS`` rather than the CREATE body, but a writer may
    legitimately write them, so the fake's INSERT-column check must accept them
    exactly as a migrated live table would.
    """
    columns = _DDL_COLUMN_RE.findall(_ALL_DDL[table])
    columns.extend(col for tbl, col, _decl in ADDITIVE_COLUMN_MIGRATIONS if tbl == table)
    return columns


def _decode_literal(literal: str) -> Any:
    """Invert the SQL literal rendering in ``delta_helpers._sql_literal``."""
    literal = literal.strip()
    encoded = _UNBASE64_RE.match(literal)
    if encoded:
        return base64.b64decode(encoded.group("b64")).decode("utf-8")
    if literal == "NULL":
        return None
    if literal in ("True", "False"):
        return literal == "True"
    if literal.startswith("'") and literal.endswith("'"):
        return literal[1:-1].replace("''", "'").replace("\\\\", "\\")
    try:
        return int(literal)
    except ValueError:
        pass
    try:
        return float(literal)
    except ValueError:
        return literal


def _split_top_level(text: str) -> list[str]:
    """Split on commas that are not inside quotes or parentheses."""
    parts: list[str] = []
    depth = 0
    in_quote = False
    current: list[str] = []
    index = 0
    while index < len(text):
        char = text[index]
        if char == "'":
            if in_quote and index + 1 < len(text) and text[index + 1] == "'":
                current.append("''")
                index += 2
                continue
            in_quote = not in_quote
        elif not in_quote and char == "(":
            depth += 1
        elif not in_quote and char == ")":
            depth -= 1
        elif not in_quote and depth == 0 and char == ",":
            parts.append("".join(current))
            current = []
            index += 1
            continue
        current.append(char)
        index += 1
    if current:
        parts.append("".join(current))
    return [p.strip() for p in parts if p.strip()]


class FakeDeltaSpark:
    """Applies ``merge_row`` statements to in-memory rows and serves SELECTs."""

    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []
        self.statements: list[str] = []

    def sql(self, statement: str):
        self.statements.append(statement)
        stripped = statement.strip()
        if stripped.upper().startswith("MERGE INTO"):
            self._apply_merge(stripped)
            return _EmptyResult()
        if stripped.upper().startswith("SELECT"):
            return _RowsResult(self._apply_select(stripped))
        return _EmptyResult()

    def _apply_merge(self, statement: str) -> None:
        match = _MERGE_RE.search(statement)
        assert match, f"merge_row emitted an unparseable statement: {statement}"

        keys: dict[str, Any] = {}
        for item in _split_top_level(match.group("source")):
            literal, _, column = item.rpartition(" AS ")
            keys[column.strip()] = _decode_literal(literal)

        updates: dict[str, Any] = {}
        for item in _split_top_level(match.group("set")):
            target, _, literal = item.partition(" = ")
            updates[target.strip().removeprefix("t.")] = _decode_literal(literal)

        existing = next(
            (r for r in self.rows if all(r.get(k) == v for k, v in keys.items())),
            None,
        )
        if existing is not None:
            existing.update(updates)
            return

        columns = [c.strip() for c in match.group("cols").split(",")]
        values = _split_top_level(match.group("vals"))
        assert len(columns) == len(values), "INSERT column/value arity mismatch"

        table = match.group("fqn").rpartition(".")[2]
        declared = _ddl_columns(table)
        unknown = set(columns) - set(declared)
        assert not unknown, f"{table} has no such column(s): {sorted(unknown)}"

        # Real Delta materializes every declared column and returns NULL for the
        # ones an INSERT omitted; the fake must do the same or readers see KeyError
        # where production sees None.
        row: dict[str, Any] = dict.fromkeys(declared)
        for column, value in zip(columns, values, strict=True):
            row[column] = (
                keys[column]
                if value.strip().startswith("s.")
                else _decode_literal(value)
            )
        self.rows.append(row)

    def _apply_select(self, statement: str) -> pd.DataFrame:
        where = statement.partition(" WHERE ")[2].partition(" ORDER BY ")[0]
        predicates = [p.strip() for p in where.split(" AND ") if p.strip()]
        selected = [r for r in self.rows if self._matches(r, predicates)]
        return pd.DataFrame(selected)

    @staticmethod
    def _matches(row: dict[str, Any], predicates: list[str]) -> bool:
        for predicate in predicates:
            column, _, literal = predicate.partition(" = ")
            expected = _decode_literal(literal)
            if isinstance(expected, str) and expected in ("true", "false"):
                expected = expected == "true"
            if row.get(column.strip()) != expected:
                return False
        return True


class _EmptyResult:
    def toPandas(self) -> pd.DataFrame:  # noqa: N802 - Spark API name
        return pd.DataFrame()

    def collect(self) -> list:
        return []


class _RowsResult:
    def __init__(self, df: pd.DataFrame) -> None:
        self._df = df

    def toPandas(self) -> pd.DataFrame:  # noqa: N802 - Spark API name
        return self._df

    def collect(self) -> list:
        return self._df.to_dict(orient="records")


# ── DDL registration and conventions ────────────────────────────────────


@pytest.mark.parametrize(
    "table",
    [TABLE_MV_CANDIDATES, TABLE_MV_CONSENTS, TABLE_MV_CREATED_OBJECTS],
)
def test_mv_tables_are_registered_in_all_ddl(table: str) -> None:
    assert table in _ALL_DDL
    ddl = _ALL_DDL[table]
    assert f"CREATE TABLE IF NOT EXISTS {{catalog}}.{{schema}}.{table}" in ddl
    assert "USING DELTA" in ddl
    assert "delta.enableChangeDataFeed" in ddl


def test_candidates_ddl_carries_the_full_proposal_payload() -> None:
    ddl = _ALL_DDL[TABLE_MV_CANDIDATES]
    for column in (
        "suggestion_id", "candidate_type", "confidence_score", "tier",
        "target_space_id", "proposed_object", "score_components_json",
        "evidence_json", "provenance_json", "dedup_fingerprint",
        "alternatives_json", "conflicts_json", "run_id", "requested_mode",
        "effective_mode", "created_at", "updated_at", "decided_by", "decision",
        "decided_at", "suppressed_until", "approved_for_rerun",
    ):
        assert column in ddl, f"{column} missing from mv candidates DDL"
    # MV-D1 hinges on approved_for_rerun defaulting closed.
    assert "approved_for_rerun  BOOLEAN       DEFAULT false" in ddl
    assert "delta.feature.allowColumnDefaults" in ddl
    # A candidate outlives its proposing run, so it cannot be run_id-partitioned.
    assert "PARTITIONED BY (target_space_id)" in ddl


def test_consents_ddl_is_unpartitioned_because_run_id_arrives_late() -> None:
    ddl = _ALL_DDL[TABLE_MV_CONSENTS]
    for column in (
        "probe_id", "run_id", "granted_by", "granted_at", "target_catalog",
        "target_schema", "materialize_consented", "probe_results_json",
        "verdict", "reverified_at_trigger", "downgrade_reason",
    ):
        assert column in ddl, f"{column} missing from mv consents DDL"
    assert "PARTITIONED BY" not in ddl
    assert "reverified_at_trigger TIMESTAMP" in ddl
    assert "run_id              STRING                 COMMENT" in ddl


def test_created_objects_ddl_covers_the_attach_lifecycle() -> None:
    ddl = _ALL_DDL[TABLE_MV_CREATED_OBJECTS]
    for column in (
        "run_id", "suggestion_id", "full_name", "created_by", "created_at",
        "attach_patch_id", "baseline_eval_run_id", "post_attach_eval_run_id",
        "status", "on_regression_action",
    ):
        assert column in ddl, f"{column} missing from mv created-objects DDL"
    assert "CREATED|ATTACHED|DETACHED|DROPPED" in ddl
    assert "PARTITIONED BY (run_id)" in ddl


# ── Idempotency key ─────────────────────────────────────────────────────


def test_fingerprint_matches_the_documented_sha256_recipe() -> None:
    fingerprint = mv_state.mv_candidate_fingerprint(
        "space-1",
        "SUM(l_extendedprice * (1 - l_discount))",
        ["samples.tpch.lineitem", "samples.tpch.orders"],
    )
    expected = hashlib.sha256(
        "space-1|SUM(l_extendedprice * (1 - l_discount))|"
        "samples.tpch.lineitem,samples.tpch.orders".encode("utf-8")
    ).hexdigest()
    assert fingerprint == expected


def test_fingerprint_ignores_source_order_and_duplicates() -> None:
    a = mv_state.mv_candidate_fingerprint("s", "SUM(x)", ["b.c.d", "a.b.c"])
    b = mv_state.mv_candidate_fingerprint("s", "SUM(x)", ["a.b.c", "b.c.d", "a.b.c"])
    assert a == b


def test_fingerprint_separates_different_spaces_and_expressions() -> None:
    base = mv_state.mv_candidate_fingerprint("s1", "SUM(x)", ["t"])
    assert base != mv_state.mv_candidate_fingerprint("s2", "SUM(x)", ["t"])
    assert base != mv_state.mv_candidate_fingerprint("s1", "SUM(y)", ["t"])
    assert base != mv_state.mv_candidate_fingerprint("s1", "SUM(x)", ["t", "u"])


@pytest.mark.parametrize(
    ("space_id", "expr"), [("", "SUM(x)"), ("s", "")],
)
def test_fingerprint_rejects_empty_inputs(space_id: str, expr: str) -> None:
    with pytest.raises(ValueError):
        mv_state.mv_candidate_fingerprint(space_id, expr, ["t"])


# ── Candidates round-trip ───────────────────────────────────────────────


def _upsert_candidate(spark: FakeDeltaSpark, **overrides: Any) -> str:
    kwargs: dict[str, Any] = {
        "catalog": "cat",
        "schema": "sch",
        "run_id": "run-1",
        "target_space_id": "space-1",
        "suggestion_id": "sug_9f2a",
        "dedup_fingerprint": "fp-abc",
        "candidate_type": "NEW_METRIC_VIEW",
        "confidence_score": 80.0,
        "tier": "HIGH",
        "proposed_object": "finance.sales.discounted_revenue_metrics",
        "score_components": {"L": 0.9, "weights": {"L": 0.35}},
        "evidence": {"benchmark_questions": ["bmk_12", "bmk_31"]},
        "provenance": {"generated_by": "gwb-mv-advisor@1.0"},
        "alternatives": [],
        "conflicts": [],
        "requested_mode": "create_and_attach",
        "effective_mode": "suggest_only",
    }
    kwargs.update(overrides)
    return mv_state.upsert_mv_candidate(spark, **kwargs)


def test_candidate_round_trips_with_pov_field_names_at_the_api() -> None:
    spark = FakeDeltaSpark()
    fingerprint = _upsert_candidate(spark)

    assert fingerprint == "fp-abc"
    candidates = mv_state.load_mv_candidates(
        spark, "cat", "sch", target_space_id="space-1",
    )
    assert len(candidates) == 1
    row = candidates[0]

    assert row["suggestion_id"] == "sug_9f2a"
    assert row["candidate_type"] == "NEW_METRIC_VIEW"
    assert row["dedup_fingerprint"] == "fp-abc"
    assert row["confidence_score"] == 80.0
    assert row["tier"] == "HIGH"
    assert row["requested_mode"] == "create_and_attach"
    assert row["effective_mode"] == "suggest_only"
    # Storage columns carry the _json suffix; the accessor exposes POV Part 4 names.
    assert row["score_components"] == {"L": 0.9, "weights": {"L": 0.35}}
    assert row["evidence"] == {"benchmark_questions": ["bmk_12", "bmk_31"]}
    assert row["provenance"] == {"generated_by": "gwb-mv-advisor@1.0"}
    assert row["alternatives"] == []
    assert row["conflicts"] == []
    assert "score_components_json" not in row
    assert row["approved_for_rerun"] is False
    assert row["created_at"] == row["updated_at"]


def test_candidate_json_uses_byte_preserving_transport() -> None:
    spark = FakeDeltaSpark()
    evidence = {"note": "O'Brien\\nSnowman: ☃", "expr": r"path = 'C:\\tmp'"}
    _upsert_candidate(spark, evidence=evidence)

    merge = spark.statements[0]
    assert "unbase64(" in merge
    assert json.dumps(evidence, default=str, sort_keys=True) not in merge

    row = mv_state.load_mv_candidates(spark, "cat", "sch", target_space_id="space-1")[0]
    assert row["evidence"] == evidence


def test_hostile_strings_cannot_break_out_of_the_rendered_merge() -> None:
    """`_sql_literal` is load-bearing for insert_row, update_row and merge_row alike.

    Everything a candidate carries is derived from user-supplied SQL, table
    comments and LLM output, so the renderer is the only thing standing between
    that text and a statement executed on a warehouse. This pushes quotes,
    backslashes, a real newline, non-ASCII text and a statement-terminator
    payload through both escaping paths at once: a plain STRING column and a
    base64-encoded JSON column.
    """
    plain = "O'Brien said: path = 'C:\\tmp\\new'; DROP TABLE cat.sch.x; -- ☃\nsecond line"
    encoded = {
        "note": "it's a \\ backslash\nand a real newline",
        "unicode": "重要 · ☃ · café",
        "payload": "'); DROP TABLE cat.sch.genie_opt_mv_candidates; --",
    }

    spark = FakeDeltaSpark()
    _upsert_candidate(spark, proposed_object=plain, tier=plain, evidence=encoded)
    statement = spark.statements[0]

    # The injection claim: one statement in, one statement out. If any payload had
    # escaped its literal, sqlglot would see the terminator and hand back two.
    assert len(sqlglot.parse(statement, dialect="databricks")) == 1
    assert isinstance(
        sqlglot.parse_one(statement, dialect="databricks"), sqlglot.exp.Merge
    )
    assert "DROP TABLE cat.sch.genie_opt_mv_candidates" not in statement

    # The encoded column travels as bytes, so its payload never appears as SQL text.
    assert "unbase64(" in statement
    assert encoded["payload"] not in statement

    row = mv_state.load_mv_candidates(spark, "cat", "sch", target_space_id="space-1")[0]
    assert row["proposed_object"] == plain
    assert row["tier"] == plain
    assert row["evidence"] == encoded

    # Byte-identical, not merely equal-after-normalization: assert the characters
    # most likely to be silently eaten by an escaping bug are all still there.
    assert "\n" in row["proposed_object"]
    assert "\\tmp" in row["proposed_object"]
    assert "O'Brien" in row["proposed_object"]
    assert "☃" in row["proposed_object"]
    assert row["evidence"]["unicode"] == "重要 · ☃ · café"
    assert "\n" in row["evidence"]["note"]


def test_re_upserting_the_same_fingerprint_refreshes_one_row() -> None:
    spark = FakeDeltaSpark()
    _upsert_candidate(spark)
    _upsert_candidate(spark, run_id="run-2", confidence_score=91.5, tier="HIGH")

    candidates = mv_state.load_mv_candidates(
        spark, "cat", "sch", target_space_id="space-1",
    )
    assert len(candidates) == 1
    assert candidates[0]["run_id"] == "run-2"
    assert candidates[0]["confidence_score"] == 91.5


def test_a_different_fingerprint_is_a_different_candidate() -> None:
    spark = FakeDeltaSpark()
    _upsert_candidate(spark)
    _upsert_candidate(spark, dedup_fingerprint="fp-def", suggestion_id="sug_2")

    assert len(
        mv_state.load_mv_candidates(spark, "cat", "sch", target_space_id="space-1")
    ) == 2


def test_re_proposing_does_not_resurrect_a_rejected_candidate() -> None:
    spark = FakeDeltaSpark()
    _upsert_candidate(spark)
    mv_state.record_mv_candidate_decision(
        spark,
        catalog="cat",
        schema="sch",
        target_space_id="space-1",
        dedup_fingerprint="fp-abc",
        decision="rejected",
        decided_by="reviewer@example.com",
        suppressed_until="2026-12-01T00:00:00+00:00",
    )
    _upsert_candidate(spark, run_id="run-2")

    row = mv_state.load_mv_candidates(spark, "cat", "sch", target_space_id="space-1")[0]
    assert row["run_id"] == "run-2"
    assert row["decision"] == "rejected"
    assert row["approved_for_rerun"] is False
    assert row["suppressed_until"] == "2026-12-01T00:00:00+00:00"


def test_approval_sets_approved_for_rerun_for_mv_d1() -> None:
    spark = FakeDeltaSpark()
    _upsert_candidate(spark)
    mv_state.record_mv_candidate_decision(
        spark,
        catalog="cat",
        schema="sch",
        target_space_id="space-1",
        dedup_fingerprint="fp-abc",
        decision="approved",
        decided_by="owner@example.com",
    )

    approved = mv_state.load_mv_candidates(
        spark, "cat", "sch", target_space_id="space-1", approved_for_rerun=True,
    )
    assert len(approved) == 1
    assert approved[0]["decision"] == "approved"
    assert approved[0]["decided_by"] == "owner@example.com"
    assert approved[0]["decided_at"]


def test_candidate_writers_reject_unknown_vocabulary() -> None:
    spark = FakeDeltaSpark()
    with pytest.raises(ValueError, match="candidate_type"):
        _upsert_candidate(spark, candidate_type="SOMETHING_ELSE")
    with pytest.raises(ValueError, match="decision"):
        mv_state.record_mv_candidate_decision(
            spark,
            catalog="cat",
            schema="sch",
            target_space_id="space-1",
            dedup_fingerprint="fp-abc",
            decision="maybe",
            decided_by="x@example.com",
        )


def test_loading_candidates_requires_a_scope() -> None:
    with pytest.raises(ValueError, match="target_space_id or run_id"):
        mv_state.load_mv_candidates(FakeDeltaSpark(), "cat", "sch")


# ── Consents round-trip ─────────────────────────────────────────────────


def _upsert_consent(spark: FakeDeltaSpark, **overrides: Any) -> str:
    kwargs: dict[str, Any] = {
        "catalog": "cat",
        "schema": "sch",
        "probe_id": "probe_7f21",
        "granted_by": "owner@example.com",
        "target_catalog": "finance",
        "target_schema": "sales",
        "verdict": "SUFFICIENT",
        "probe_results": {"checked_as": "owner@example.com", "missing": []},
    }
    kwargs.update(overrides)
    return mv_state.upsert_mv_consent(spark, **kwargs)


def test_consent_round_trips_and_starts_unverified() -> None:
    spark = FakeDeltaSpark()
    probe_id = _upsert_consent(spark)

    assert probe_id == "probe_7f21"
    row = mv_state.load_mv_consent(spark, "probe_7f21", "cat", "sch")
    assert row is not None
    assert row["granted_by"] == "owner@example.com"
    assert row["target_catalog"] == "finance"
    assert row["target_schema"] == "sales"
    assert row["verdict"] == "SUFFICIENT"
    assert row["probe_results"] == {"checked_as": "owner@example.com", "missing": []}
    assert "probe_results_json" not in row
    # Materialization is a separate consent and defaults closed.
    assert row["materialize_consented"] is False
    # run_id arrives at trigger time; re-verification has not happened yet.
    assert row["run_id"] is None
    assert row["reverified_at_trigger"] is None


def test_consent_reverification_stamps_a_timestamp_and_binds_the_run() -> None:
    spark = FakeDeltaSpark()
    _upsert_consent(spark)
    mv_state.mark_mv_consent_reverified(
        spark, catalog="cat", schema="sch", probe_id="probe_7f21", run_id="run-2",
    )

    row = mv_state.load_mv_consent(spark, "probe_7f21", "cat", "sch")
    assert row is not None
    assert row["run_id"] == "run-2"
    assert row["reverified_at_trigger"] is not None
    assert row["verdict"] == "SUFFICIENT"


def test_reverification_can_record_a_downgrade() -> None:
    spark = FakeDeltaSpark()
    _upsert_consent(spark)
    mv_state.mark_mv_consent_reverified(
        spark,
        catalog="cat",
        schema="sch",
        probe_id="probe_7f21",
        run_id="run-2",
        verdict="INSUFFICIENT",
        downgrade_reason="CREATE TABLE on finance.sales revoked between config and trigger",
    )

    row = mv_state.load_mv_consent(spark, "probe_7f21", "cat", "sch")
    assert row is not None
    assert row["verdict"] == "INSUFFICIENT"
    assert "revoked" in row["downgrade_reason"]


def test_consent_upsert_is_keyed_on_probe_id() -> None:
    spark = FakeDeltaSpark()
    _upsert_consent(spark)
    _upsert_consent(spark, materialize_consented=True)
    _upsert_consent(spark, probe_id="probe_other")

    assert mv_state.load_mv_consent(spark, "probe_7f21", "cat", "sch")[
        "materialize_consented"
    ] is True
    assert len(spark.rows) == 2


def test_consent_writers_reject_unknown_verdicts() -> None:
    spark = FakeDeltaSpark()
    with pytest.raises(ValueError, match="verdict"):
        _upsert_consent(spark, verdict="PROBABLY")
    with pytest.raises(ValueError, match="verdict"):
        mv_state.mark_mv_consent_reverified(
            spark,
            catalog="cat",
            schema="sch",
            probe_id="probe_7f21",
            verdict="PROBABLY",
        )


def test_missing_consent_reads_as_none() -> None:
    assert mv_state.load_mv_consent(FakeDeltaSpark(), "nope", "cat", "sch") is None


# ── Created objects round-trip ──────────────────────────────────────────


def _upsert_created(spark: FakeDeltaSpark, **overrides: Any) -> str:
    kwargs: dict[str, Any] = {
        "catalog": "cat",
        "schema": "sch",
        "run_id": "run-2",
        "suggestion_id": "sug_9f2a",
        "full_name": "finance.sales.discounted_revenue_metrics",
        "created_by": "owner@example.com",
    }
    kwargs.update(overrides)
    return mv_state.upsert_mv_created_object(spark, **kwargs)


def test_created_object_round_trips_as_created() -> None:
    spark = FakeDeltaSpark()
    full_name = _upsert_created(spark)

    assert full_name == "finance.sales.discounted_revenue_metrics"
    rows = mv_state.load_mv_created_objects(spark, "run-2", "cat", "sch")
    assert len(rows) == 1
    row = rows[0]
    assert row["suggestion_id"] == "sug_9f2a"
    assert row["created_by"] == "owner@example.com"
    assert row["status"] == "CREATED"
    assert row["on_regression_action"] == "DETACH_ONLY_NEVER_DROP"
    assert row["attach_patch_id"] is None
    assert row["created_at"] == row["updated_at"]


def test_attach_then_detach_keeps_one_row_and_both_eval_run_ids() -> None:
    spark = FakeDeltaSpark()
    _upsert_created(spark)

    mv_state.update_mv_created_object_status(
        spark,
        catalog="cat",
        schema="sch",
        run_id="run-2",
        suggestion_id="sug_9f2a",
        status="ATTACHED",
        attach_patch_id="patch_88c1",
        baseline_eval_run_id="e1ef3471",
        post_attach_eval_run_id="a77c02be",
    )
    mv_state.update_mv_created_object_status(
        spark,
        catalog="cat",
        schema="sch",
        run_id="run-2",
        suggestion_id="sug_9f2a",
        status="DETACHED",
    )

    rows = mv_state.load_mv_created_objects(spark, "run-2", "cat", "sch")
    assert len(rows) == 1
    row = rows[0]
    assert row["status"] == "DETACHED"
    # A status-only update must not blank the lift evidence.
    assert row["attach_patch_id"] == "patch_88c1"
    assert row["baseline_eval_run_id"] == "e1ef3471"
    assert row["post_attach_eval_run_id"] == "a77c02be"


def test_created_objects_are_scoped_per_run() -> None:
    spark = FakeDeltaSpark()
    _upsert_created(spark)
    _upsert_created(spark, run_id="run-9")

    assert len(mv_state.load_mv_created_objects(spark, "run-2", "cat", "sch")) == 1
    assert len(mv_state.load_mv_created_objects(spark, "run-9", "cat", "sch")) == 1


def test_created_objects_can_be_filtered_by_status() -> None:
    spark = FakeDeltaSpark()
    _upsert_created(spark)
    _upsert_created(spark, suggestion_id="sug_2", full_name="finance.sales.other")
    mv_state.update_mv_created_object_status(
        spark,
        catalog="cat",
        schema="sch",
        run_id="run-2",
        suggestion_id="sug_2",
        status="DETACHED",
    )

    detached = mv_state.load_mv_created_objects(
        spark, "run-2", "cat", "sch", status="DETACHED",
    )
    assert [r["suggestion_id"] for r in detached] == ["sug_2"]


def test_lookup_by_uc_name_supports_the_explicit_drop_gate() -> None:
    spark = FakeDeltaSpark()
    _upsert_created(spark)
    mv_state.update_mv_created_object_status(
        spark,
        catalog="cat",
        schema="sch",
        run_id="run-2",
        suggestion_id="sug_9f2a",
        status="DETACHED",
    )

    row = mv_state.load_mv_created_object_by_name(
        spark, "finance.sales.discounted_revenue_metrics", "cat", "sch",
    )
    assert row is not None
    assert row["status"] == "DETACHED"
    assert mv_state.load_mv_created_object_by_name(
        spark, "finance.sales.absent", "cat", "sch",
    ) is None


def test_created_object_writers_reject_unknown_vocabulary() -> None:
    spark = FakeDeltaSpark()
    with pytest.raises(ValueError, match="status"):
        _upsert_created(spark, status="HALF_ATTACHED")
    with pytest.raises(ValueError, match="on_regression_action"):
        _upsert_created(spark, on_regression_action="AUTO_DROP")
    with pytest.raises(ValueError, match="status"):
        mv_state.update_mv_created_object_status(
            spark,
            catalog="cat",
            schema="sch",
            run_id="run-2",
            suggestion_id="sug_9f2a",
            status="GONE",
        )
