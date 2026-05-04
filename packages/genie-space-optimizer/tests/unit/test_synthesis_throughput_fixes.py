"""Phase 5 tests for the fix-example-sql-synthesis-throughput plan.

Each phase of the plan produces a distinct observable: Phase 0 unblocks
description enrichment; Phase 1 broadens archetype eligibility and
column-type detection; Phase 2 wires the data profile and column
descriptions into the synthesis prompt; Phase 3 softens the
EMPTY_RESULT gate and adds one retry with profile feedback; Phase 4
surfaces retries and under-target warnings in the pretty summary.

The 14 tests below are grouped by phase so a regression in any one
phase points straight at the failing cluster.
"""

from __future__ import annotations

import random
from unittest.mock import patch

import pytest

from genie_space_optimizer.optimization.archetypes import (
    ARCHETYPES,
    _col_type,
    schema_traits,
)
from genie_space_optimizer.optimization.preflight_synthesis import (
    AssetSlice,
    _build_empty_result_feedback,
    _eligible_archetypes,
    _format_slice_data_profile,
    plan_asset_coverage,
    render_preflight_prompt,
    run_preflight_example_synthesis,
)
from genie_space_optimizer.optimization.synthesis import (
    GateResult,
    _gate_execute,
    _sql_has_where_or_join,
)


# ═══════════════════════════════════════════════════════════════════════
# Shared fixtures
# ═══════════════════════════════════════════════════════════════════════


def _mk_table(
    identifier: str,
    columns: list[dict] | None = None,
    description: str = "",
) -> dict:
    return {
        "identifier": identifier,
        "name": identifier.split(".")[-1],
        "description": description,
        "column_configs": columns or [
            {"column_name": "id", "type_text": "BIGINT", "description": "PK"},
            {"column_name": "amount", "type_text": "DECIMAL", "description": "value"},
            {"column_name": "region", "type_text": "STRING", "description": "region"},
            {"column_name": "sale_date", "type_text": "DATE", "description": "date"},
        ],
    }


def _all_gates_pass(*_args, **_kwargs):
    return True, [
        GateResult(True, "parse"),
        GateResult(True, "execute"),
        GateResult(True, "structural"),
        GateResult(True, "arbiter", "skipped_no_arbiter"),
        GateResult(True, "firewall"),
    ]


def _gate_fails_at(gate: str, reason: str = ""):
    def _stub(*_args, **_kwargs):
        results = []
        for name in ("parse", "execute", "structural", "arbiter", "firewall"):
            if name == gate:
                results.append(GateResult(False, name, reason or f"{gate} failed"))
                return False, results
            results.append(GateResult(True, name))
        return True, results
    return _stub


# ═══════════════════════════════════════════════════════════════════════
# Phase 0 — AttributeError fix
# ═══════════════════════════════════════════════════════════════════════


class TestPhase0ListReturnNormalized:
    """The LLM sometimes drops the ``{"changes": [...]}`` envelope and
    returns a bare list. ``_enrich_blank_descriptions`` must treat that
    as a well-formed payload rather than crashing with
    ``AttributeError: 'list' object has no attribute 'get'``.
    """

    def test_list_return_is_normalized_to_changes_dict(self, monkeypatch):
        """Drive through ``_enrich_blank_descriptions`` with a stub LLM
        whose JSON extraction returns a bare list. The function must
        still produce the expected patch (no AttributeError).
        """
        from genie_space_optimizer.optimization import optimizer as opt_mod

        snap = {
            "data_sources": {
                "tables": [
                    {
                        "identifier": "cat.sch.t",
                        "name": "t",
                        "column_configs": [
                            {"column_name": "amount", "type_text": "DECIMAL"},
                        ],
                    },
                ],
                "metric_views": [],
            },
            "instructions": {"join_specs": []},
            "tables": [
                {
                    "identifier": "cat.sch.t",
                    "name": "t",
                    "column_configs": [
                        {"column_name": "amount", "type_text": "DECIMAL"},
                    ],
                },
            ],
            "metric_views": [],
        }

        bare_list = [
            {
                "table": "cat.sch.t",
                "column": "amount",
                "entity_type": "measure",
                "sections": {"description": "ok"},
            },
        ]

        monkeypatch.setattr(
            opt_mod, "_traced_llm_call",
            lambda *a, **kw: ("[{...}]", object()),
        )
        monkeypatch.setattr(
            "genie_space_optimizer.optimization.evaluation._extract_json",
            lambda text: bare_list,
        )
        monkeypatch.setattr(
            opt_mod, "_collect_blank_columns",
            lambda _snap: [
                {
                    "table": "cat.sch.t",
                    "column": "amount",
                    "data_type": "DECIMAL",
                    "entity_type": "measure",
                    "table_description": "",
                    "sibling_columns": [],
                },
            ],
        )

        patches = opt_mod._enrich_blank_descriptions(snap, w=None)
        assert isinstance(patches, list)
        assert len(patches) == 1
        assert patches[0]["table"] == "cat.sch.t"
        assert patches[0]["column"] == "amount"


# ═══════════════════════════════════════════════════════════════════════
# Phase 1.R1 — centralised column-type reader
# ═══════════════════════════════════════════════════════════════════════


class TestPhase1ColType:
    """``_col_type`` normalises ``data_type`` / ``type_text`` / ``type``
    into a lowercase string; ``schema_traits`` reads through it so a
    production-shaped snapshot with ``data_type`` (but no ``type_text``)
    still fires every trait.
    """

    def test_col_type_reads_data_type_first(self):
        assert _col_type({"data_type": "BIGINT"}) == "bigint"
        assert _col_type({"type_text": "STRING"}) == "string"
        assert _col_type({"type": "DATE"}) == "date"
        assert _col_type({}) == ""

    def test_schema_traits_reads_data_type_only(self):
        """Production snapshot: UC only populates ``data_type``. Every
        trait must still fire so the planner sees eligible archetypes.
        """
        snap = {
            "data_sources": {
                "tables": [
                    {
                        "identifier": "cat.sch.a",
                        "column_configs": [
                            {"column_name": "amount", "data_type": "DECIMAL"},
                            {"column_name": "region", "data_type": "STRING"},
                            {"column_name": "sale_date", "data_type": "DATE"},
                        ],
                    },
                    {
                        "identifier": "cat.sch.b",
                        "column_configs": [
                            {"column_name": "id", "data_type": "BIGINT"},
                        ],
                    },
                ],
                "metric_views": [],
            },
        }
        traits = schema_traits(snap)
        assert "has_numeric" in traits
        assert "has_date" in traits
        assert "has_categorical" in traits
        assert "has_joinable" in traits


# ═══════════════════════════════════════════════════════════════════════
# Phase 1.R3 — filter_compose is not preflight-eligible
# ═══════════════════════════════════════════════════════════════════════


class TestPhase1FilterComposePreflight:
    def test_filter_compose_marked_ineligible(self):
        fc = next(a for a in ARCHETYPES if a.name == "filter_compose")
        assert fc.preflight_eligible is False

    def test_eligible_archetypes_excludes_filter_compose(self):
        eligible = {a.name for a in _eligible_archetypes(set())}
        assert "filter_compose" not in eligible


# ═══════════════════════════════════════════════════════════════════════
# Phase 1.R4 — simple_enumerate is always eligible
# ═══════════════════════════════════════════════════════════════════════


class TestPhase1SimpleEnumerateSafetyNet:
    def test_simple_enumerate_registered(self):
        names = {a.name for a in ARCHETYPES}
        assert "simple_enumerate" in names

    def test_simple_enumerate_eligible_with_empty_traits(self):
        """Even with zero detected traits and filter_compose excluded,
        the planner must still have the safety-net archetype available.
        """
        eligible = {a.name for a in _eligible_archetypes(set())}
        assert "simple_enumerate" in eligible


# ═══════════════════════════════════════════════════════════════════════
# Phase 1.R7 — adaptive per_archetype cap
# ═══════════════════════════════════════════════════════════════════════


class TestPhase1AdaptivePerArchetypeCap:
    def test_single_archetype_space_reaches_target(self):
        """A snapshot with no numeric/date/categorical columns + no MVs
        + no join specs has essentially one eligible archetype. With the
        adaptive cap, the planner should still get close to target.
        """
        snap = {
            "data_sources": {
                "tables": [
                    {
                        "identifier": "cat.sch.only",
                        "name": "only",
                        "column_configs": [
                            {"column_name": "id", "data_type": "BIGINT"},
                        ],
                    },
                ],
                "metric_views": [],
            },
            "instructions": {"join_specs": []},
        }
        plans = plan_asset_coverage(snap, need=20, rng=random.Random(5))
        assert len(plans) >= 5, (
            f"adaptive cap didn't trigger — got only {len(plans)} plans "
            "with 1-eligible-archetype snapshot"
        )


# ═══════════════════════════════════════════════════════════════════════
# Phase 2 — data profile + column descriptions in the prompt
# ═══════════════════════════════════════════════════════════════════════


def _profile_fixture() -> dict:
    return {
        "cat.sch.t": {
            "columns": {
                "region": {
                    "cardinality": 3,
                    "distinct_values": ["CA", "JP", "TW"],
                },
                "amount": {
                    "cardinality": 1000,
                    "min": 0,
                    "max": 99999,
                },
            },
        },
    }


class TestPhase2PromptGrounding:
    def test_prompt_renders_data_profile_section(self):
        arch = next(a for a in ARCHETYPES if a.name == "top_n_by_metric")
        slice_ = AssetSlice(
            tables=[_mk_table("cat.sch.t")],
            columns=[("cat.sch.t", "region"), ("cat.sch.t", "amount")],
        )
        prompt = render_preflight_prompt(
            arch, slice_, [], data_profile=_profile_fixture(),
        )
        assert "Column value profile" in prompt
        assert "'CA'" in prompt
        assert "range=[0, 99999]" in prompt

    def test_prompt_falls_back_when_profile_missing(self):
        """No crash + no fabricated values when data_profile=None."""
        arch = next(a for a in ARCHETYPES if a.name == "top_n_by_metric")
        slice_ = AssetSlice(
            tables=[_mk_table("cat.sch.t")],
            columns=[("cat.sch.t", "region")],
        )
        prompt = render_preflight_prompt(arch, slice_, [])
        assert "Column value profile" in prompt
        assert "'CA'" not in prompt

    def test_prompt_includes_column_descriptions_when_present(self):
        arch = next(a for a in ARCHETYPES if a.name == "top_n_by_metric")
        tbl = _mk_table(
            "cat.sch.t",
            columns=[
                {
                    "column_name": "amount",
                    "data_type": "DECIMAL",
                    "description": "Transaction value in USD",
                },
                {
                    "column_name": "region",
                    "data_type": "STRING",
                    "description": "ISO country code",
                },
            ],
        )
        slice_ = AssetSlice(
            tables=[tbl],
            columns=[("cat.sch.t", "amount"), ("cat.sch.t", "region")],
        )
        prompt = render_preflight_prompt(arch, slice_, [])
        assert "Transaction value in USD" in prompt
        assert "ISO country code" in prompt

    def test_prompt_falls_back_when_descriptions_missing(self):
        """Missing descriptions must render bare ``tbl.col`` without
        crashing. This is the Phase 0 coupling safeguard."""
        arch = next(a for a in ARCHETYPES if a.name == "top_n_by_metric")
        tbl = _mk_table(
            "cat.sch.t",
            columns=[
                {"column_name": "amount", "data_type": "DECIMAL"},
            ],
        )
        slice_ = AssetSlice(
            tables=[tbl], columns=[("cat.sch.t", "amount")],
        )
        prompt = render_preflight_prompt(arch, slice_, [])
        assert "cat.sch.t.amount" in prompt

    def test_cluster_driven_prompt_inherits_data_profile(self):
        from genie_space_optimizer.optimization.cluster_driven_synthesis import (
            ClusterContext,
            render_cluster_driven_prompt,
        )
        arch = next(a for a in ARCHETYPES if a.name == "top_n_by_metric")
        slice_ = AssetSlice(
            tables=[_mk_table("cat.sch.t")],
            columns=[("cat.sch.t", "region")],
        )
        ctx = ClusterContext(
            afs={"cluster_id": "C1"},
            asset_slice=slice_,
            data_profile=_profile_fixture(),
        )
        prompt = render_cluster_driven_prompt(arch, ctx, [])
        assert "Column value profile" in prompt
        assert "'CA'" in prompt


class TestPhase2TokenBudget:
    def test_value_cap_and_length_cap_applied(self):
        """High-cardinality categoricals get truncated to the configured
        per-column value cap, each string clipped to the char cap, and
        the tail is marked ``+N more``."""
        from genie_space_optimizer.common.config import (
            PREFLIGHT_PROFILE_VALUE_LEN_CAP,
            PREFLIGHT_PROFILE_VALUES_CAP,
        )
        values_cap = PREFLIGHT_PROFILE_VALUES_CAP
        val_len_cap = PREFLIGHT_PROFILE_VALUE_LEN_CAP

        long_vals = [f"v{i:03d}" + ("x" * (val_len_cap + 20)) for i in range(50)]
        profile = {
            "cat.sch.t": {
                "columns": {
                    "chatty": {
                        "cardinality": 50,
                        "distinct_values": long_vals,
                    },
                },
            },
        }
        slice_ = AssetSlice(
            tables=[_mk_table("cat.sch.t")],
            columns=[("cat.sch.t", "chatty")],
        )
        rendered = _format_slice_data_profile(slice_, profile)

        # Exactly ``values_cap`` values rendered, the rest collapsed.
        assert rendered.count("'") >= values_cap * 2
        assert f"+{50 - values_cap} more" in rendered
        # Length cap enforced (truncated values end with an ellipsis).
        assert "…" in rendered


# ═══════════════════════════════════════════════════════════════════════
# Phase 3.R5 — EMPTY_RESULT classified by WHERE/JOIN
# ═══════════════════════════════════════════════════════════════════════


class TestPhase3ExecuteGateClassification:
    def test_sql_has_where_or_join_detects_tokens(self):
        assert _sql_has_where_or_join("SELECT * FROM t WHERE x=1")
        assert _sql_has_where_or_join("SELECT * FROM a JOIN b ON a.id=b.id")
        assert _sql_has_where_or_join("select * from t where foo='where'")
        # No WHERE/JOIN at all:
        assert not _sql_has_where_or_join("SELECT * FROM t LIMIT 10")
        # Literal-only WHERE token does not count.
        assert not _sql_has_where_or_join(
            "SELECT 'WHERE stuff lives' AS note FROM t LIMIT 5",
        )

    def test_empty_with_no_where_hard_fails(self):
        proposal = {"example_sql": "SELECT a FROM cat.sch.t LIMIT 10"}
        with patch(
            "genie_space_optimizer.optimization.benchmarks.validate_ground_truth_sql",
            return_value=(False, "EMPTY_RESULT: Query returned 0 rows"),
        ):
            result = _gate_execute(
                proposal, spark=None, w=object(), warehouse_id="wh",
            )
        assert result.passed is False
        assert result.gate == "execute"
        assert proposal.get("_execute_empty") is not True

    def test_empty_with_where_soft_accepts(self):
        proposal = {
            "example_sql": "SELECT a FROM cat.sch.t WHERE a = 'missing'",
        }
        with patch(
            "genie_space_optimizer.optimization.benchmarks.validate_ground_truth_sql",
            return_value=(False, "EMPTY_RESULT: Query returned 0 rows"),
        ):
            result = _gate_execute(
                proposal, spark=None, w=object(), warehouse_id="wh",
            )
        assert result.passed is True
        assert "empty_result_soft_accept" in result.reason
        assert proposal["_execute_empty"] is True

    def test_empty_with_join_soft_accepts(self):
        proposal = {
            "example_sql": "SELECT a FROM cat.sch.t JOIN cat.sch.u ON t.id=u.id",
        }
        with patch(
            "genie_space_optimizer.optimization.benchmarks.validate_ground_truth_sql",
            return_value=(False, "EMPTY_RESULT: Query returned 0 rows"),
        ):
            result = _gate_execute(
                proposal, spark=None, w=object(), warehouse_id="wh",
            )
        assert result.passed is True
        assert proposal["_execute_empty"] is True

    def test_hard_error_never_soft_accepts(self):
        """A CAST/syntax/unresolved-column error must hard-fail even
        when the SQL contains a WHERE clause — only EMPTY_RESULT is
        eligible for soft-accept.
        """
        proposal = {
            "example_sql": "SELECT CAST(x AS INT) FROM cat.sch.t WHERE y=1",
        }
        with patch(
            "genie_space_optimizer.optimization.benchmarks.validate_ground_truth_sql",
            return_value=(False, "EXECUTION_ERROR: cannot cast"),
        ):
            result = _gate_execute(
                proposal, spark=None, w=object(), warehouse_id="wh",
            )
        assert result.passed is False
        assert "_execute_empty" not in proposal


# ═══════════════════════════════════════════════════════════════════════
# Phase 3.R6 — retry on EMPTY_RESULT
# ═══════════════════════════════════════════════════════════════════════


class _RetryValidatorStub:
    """Validator stub with a scripted sequence of (passed, gate_results)
    return values. Used to simulate ``first attempt EMPTY_RESULT → retry
    succeeds`` and similar flows without running SQL.
    """

    def __init__(self, script: list[tuple[bool, list[GateResult]]]):
        self._script = list(script)
        self.calls = 0

    def __call__(self, *_a, **_kw):
        self.calls += 1
        if self._script:
            return self._script.pop(0)
        return _all_gates_pass()


def _rich_preflight_snapshot() -> dict:
    tables = [
        _mk_table("cat.sch.fact_sales"),
        _mk_table(
            "cat.sch.dim_region",
            columns=[
                {"column_name": "region_id", "data_type": "STRING"},
                {"column_name": "region_name", "data_type": "STRING"},
            ],
        ),
    ]
    joins = [
        {
            "left": {"identifier": "cat.sch.fact_sales", "alias": "fact_sales"},
            "right": {"identifier": "cat.sch.dim_region", "alias": "dim_region"},
            "sql": ["fact_sales.region = dim_region.region_id"],
        },
    ]
    return {
        "data_sources": {"tables": tables, "metric_views": []},
        "instructions": {"join_specs": joins, "example_question_sqls": []},
        "_data_profile": {
            "cat.sch.fact_sales": {
                "columns": {
                    "region": {
                        "cardinality": 3,
                        "distinct_values": ["CA", "JP", "TW"],
                    },
                },
            },
        },
    }


class _ScriptedLLM:
    """LLM stub that returns a sequence of scripted responses and records
    every prompt it was called with."""

    def __init__(self, responses: list[str]):
        self._responses = list(responses)
        self.prompts: list[str] = []

    def __call__(self, prompt: str) -> str:
        self.prompts.append(prompt)
        if not self._responses:
            return self._responses_default()
        return self._responses.pop(0)

    def _responses_default(self) -> str:
        return (
            '{"example_question": "Regions list?", '
            '"example_sql": "SELECT region FROM cat.sch.fact_sales WHERE region = \'CA\'", '
            '"rationale": "demo"}'
        )


_EMPTY_SQL = (
    '{"example_question": "Regions list?", '
    '"example_sql": "SELECT region FROM cat.sch.fact_sales WHERE region = \'ZZ\'", '
    '"rationale": "will return empty"}'
)
_RECOVER_SQL = (
    '{"example_question": "Regions list redone?", '
    '"example_sql": "SELECT region FROM cat.sch.fact_sales WHERE region = \'CA\'", '
    '"rationale": "uses profile value"}'
)


class TestPhase3RetryPreflight:
    def test_retry_fires_on_empty_result_and_succeeds(self, monkeypatch):
        snap = _rich_preflight_snapshot()
        monkeypatch.setattr(
            "genie_space_optimizer.optimization.preflight_synthesis."
            "_apply_preflight_proposals",
            lambda proposals, **kw: len(proposals),
        )

        validator = _RetryValidatorStub([
            # First proposal: execute gate fails with EMPTY_RESULT.
            (False, [
                GateResult(True, "parse"),
                GateResult(
                    False, "execute",
                    "EMPTY_RESULT: Query returned 0 rows",
                ),
            ]),
            # Retry: all gates pass.
            _all_gates_pass(),
        ])
        llm = _ScriptedLLM([_EMPTY_SQL, _RECOVER_SQL])

        with patch(
            "genie_space_optimizer.optimization.preflight_synthesis."
            "validate_synthesis_proposal",
            side_effect=validator,
        ):
            result = run_preflight_example_synthesis(
                w=None, spark=None, run_id="r", space_id="s", config={},
                metadata_snapshot=snap,
                benchmarks=[], catalog="c", schema="sch",
                llm_caller=llm,
                target=1, rng=random.Random(0),
            )

        assert result["retries_fired"] >= 1
        assert result["retries_succeeded"] >= 1
        assert result["applied"] >= 1

        # Second prompt must carry the Retry-feedback block driven from
        # the _data_profile value list.
        retry_prompts = [p for p in llm.prompts if "## Retry feedback" in p]
        assert retry_prompts, "retry prompt should include feedback section"
        assert "'CA'" in retry_prompts[0]

    def test_retry_still_empty_soft_accepts_when_where_present(self, monkeypatch):
        """First attempt EMPTY_RESULT → retry also EMPTY_RESULT → since
        both SQLs include a WHERE clause, the retry's R5 classifier
        soft-accepts and the proposal still passes.
        """
        snap = _rich_preflight_snapshot()
        monkeypatch.setattr(
            "genie_space_optimizer.optimization.preflight_synthesis."
            "_apply_preflight_proposals",
            lambda proposals, **kw: len(proposals),
        )

        validator = _RetryValidatorStub([
            # First: EMPTY_RESULT, hard-fail to trigger R6.
            (False, [
                GateResult(True, "parse"),
                GateResult(False, "execute", "EMPTY_RESULT: 0 rows"),
            ]),
            # Retry: soft-accept (passed=True) + subsequent gates ok.
            (True, [
                GateResult(True, "parse"),
                GateResult(True, "execute", "empty_result_soft_accept"),
                GateResult(True, "structural"),
                GateResult(True, "arbiter", "skipped_no_arbiter"),
                GateResult(True, "firewall"),
            ]),
        ])
        llm = _ScriptedLLM([_EMPTY_SQL, _RECOVER_SQL])

        with patch(
            "genie_space_optimizer.optimization.preflight_synthesis."
            "validate_synthesis_proposal",
            side_effect=validator,
        ):
            result = run_preflight_example_synthesis(
                w=None, spark=None, run_id="r", space_id="s", config={},
                metadata_snapshot=snap,
                benchmarks=[], catalog="c", schema="sch",
                llm_caller=llm,
                target=1, rng=random.Random(1),
            )

        assert result["retries_fired"] >= 1
        assert result["applied"] >= 1
        # retries_still_empty tracks the retry gate sequence; soft-
        # accept is ``passed=True``, so the counter should stay at 0.
        assert result["retries_still_empty"] == 0

    def test_retry_second_empty_no_where_hard_rejects(self, monkeypatch):
        """First attempt EMPTY_RESULT → retry also EMPTY_RESULT, retry
        SQL has no WHERE/JOIN → hard-reject; retry counter shows one
        fired with zero successes."""
        snap = _rich_preflight_snapshot()
        monkeypatch.setattr(
            "genie_space_optimizer.optimization.preflight_synthesis."
            "_apply_preflight_proposals",
            lambda proposals, **kw: len(proposals),
        )

        validator = _RetryValidatorStub([
            (False, [
                GateResult(True, "parse"),
                GateResult(False, "execute", "EMPTY_RESULT: 0 rows"),
            ]),
            (False, [
                GateResult(True, "parse"),
                GateResult(False, "execute", "EMPTY_RESULT: 0 rows"),
            ]),
        ] * 20)  # repeat for every attempted plan
        plain_sql = (
            '{"example_question": "Regions?", '
            '"example_sql": "SELECT region FROM cat.sch.fact_sales LIMIT 5", '
            '"rationale": "plain"}'
        )
        llm = _ScriptedLLM([plain_sql] * 40)

        with patch(
            "genie_space_optimizer.optimization.preflight_synthesis."
            "validate_synthesis_proposal",
            side_effect=validator,
        ):
            result = run_preflight_example_synthesis(
                w=None, spark=None, run_id="r", space_id="s", config={},
                metadata_snapshot=snap,
                benchmarks=[], catalog="c", schema="sch",
                llm_caller=llm,
                target=1, rng=random.Random(2),
            )

        assert result["applied"] == 0
        assert result["retries_fired"] >= 1
        assert result["retries_still_empty"] >= 1


class TestPhase3RetryClusterDriven:
    def test_retry_fires_and_succeeds_on_cluster_path(self):
        """The cluster-driven path has its own retry wiring; simulate
        first-EMPTY → retry-success and assert the retry prompt carries
        the feedback block derived from _data_profile.
        """
        from genie_space_optimizer.optimization.cluster_driven_synthesis import (
            run_cluster_driven_synthesis_for_single_cluster,
        )

        snap = _rich_preflight_snapshot()
        snap["_space_id"] = "SP1"
        snap["_cluster_synthesis_count"] = 0
        snap["tables"] = snap["data_sources"]["tables"]
        snap["metric_views"] = []

        cluster = {
            "cluster_id": "C1",
            "root_cause": "missing_filter",
            "affected_judge": "answer_correctness",
            "asi_blame_set": ["cat.sch.fact_sales"],
            "question_ids": ["q1"],
        }

        validator = _RetryValidatorStub([
            (False, [
                GateResult(True, "parse"),
                GateResult(False, "execute", "EMPTY_RESULT: 0 rows"),
            ]),
            _all_gates_pass(),
        ])
        llm = _ScriptedLLM([_EMPTY_SQL, _RECOVER_SQL])

        from genie_space_optimizer.optimization.synthesis import GateResult as _GR
        with patch(
            "genie_space_optimizer.optimization.cluster_driven_synthesis."
            "validate_synthesis_proposal",
            side_effect=validator,
        ), patch(
            "genie_space_optimizer.optimization.preflight_synthesis."
            "_gate_genie_agreement",
            side_effect=lambda *a, **kw: _GR(
                True, "genie_agreement", "both_correct",
            ),
        ):
            synthesis = run_cluster_driven_synthesis_for_single_cluster(
                cluster, snap, benchmarks=[], llm_caller=llm,
            )

        result = synthesis.proposal
        assert result is not None
        assert validator.calls == 2, (
            f"validator called {validator.calls} times; expected exactly "
            "one retry on the cluster-driven path"
        )
        retry_prompts = [p for p in llm.prompts if "## Retry feedback" in p]
        assert retry_prompts, "cluster-driven retry must carry feedback block"


# ═══════════════════════════════════════════════════════════════════════
# Phase 4 / 5 — retry feedback helper sanity
# ═══════════════════════════════════════════════════════════════════════


class TestEmptyResultFeedbackHelper:
    def test_feedback_contains_prior_sql_and_profile(self):
        proposal = {
            "example_sql": "SELECT region FROM cat.sch.t WHERE region = 'ZZ'",
        }
        slice_ = AssetSlice(
            tables=[_mk_table("cat.sch.t")],
            columns=[("cat.sch.t", "region")],
        )
        profile = {
            "cat.sch.t": {
                "columns": {
                    "region": {
                        "cardinality": 3,
                        "distinct_values": ["CA", "JP", "TW"],
                    },
                },
            },
        }
        feedback = _build_empty_result_feedback(proposal, profile, slice_)
        assert "returned 0 rows" in feedback
        assert "region = 'ZZ'" in feedback
        assert "'CA'" in feedback

    def test_feedback_empty_when_nothing_to_say(self):
        proposal = {"example_sql": ""}
        slice_ = AssetSlice(tables=[], columns=[])
        assert _build_empty_result_feedback(proposal, None, slice_) == ""


# ═══════════════════════════════════════════════════════════════════════
# F2 — Description-enrichment batch-failure logging (structured, no traceback)
# ═══════════════════════════════════════════════════════════════════════


class TestEnrichmentBatchFailureLogging:
    """When an enrichment batch fails JSON parsing even after PR 1's
    validator retries exhaust, the warning must be a single-line
    structured message with a response preview — NOT an ``exc_info``
    traceback that buries the operator's signal-to-noise ratio.
    """

    _SNAPSHOT = {
        "data_sources": {
            "tables": [
                {
                    "identifier": "cat.sch.t",
                    "name": "t",
                    "column_configs": [
                        {"column_name": "amount", "type_text": "DECIMAL"},
                    ],
                },
            ],
            "metric_views": [],
        },
        "instructions": {"join_specs": []},
        "tables": [
            {
                "identifier": "cat.sch.t",
                "name": "t",
                "column_configs": [
                    {"column_name": "amount", "type_text": "DECIMAL"},
                ],
            },
        ],
        "metric_views": [],
    }

    def _mock_blank_columns(self, monkeypatch, opt_mod):
        monkeypatch.setattr(
            opt_mod, "_collect_blank_columns",
            lambda _snap: [
                {
                    "table": "cat.sch.t",
                    "column": "amount",
                    "data_type": "DECIMAL",
                    "entity_type": "measure",
                    "table_description": "",
                    "sibling_columns": [],
                },
            ],
        )

    def test_column_batch_failure_logs_preview_no_traceback(self, monkeypatch, caplog):
        import logging

        from genie_space_optimizer.optimization import optimizer as opt_mod

        self._mock_blank_columns(monkeypatch, opt_mod)

        def _fail(*_a, **_k):
            raise ValueError("Expecting value: line 1 column 1 (char 0)")

        monkeypatch.setattr(opt_mod, "_traced_llm_call", _fail)

        with caplog.at_level(logging.WARNING, logger="genie_space_optimizer"):
            patches = opt_mod._enrich_blank_descriptions(self._SNAPSHOT, w=None)

        assert patches == []
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert warnings, "expected at least one warning"
        # Preview key is present; traceback (exc_info) is not.
        assert any("preview=" in r.getMessage() for r in warnings)
        assert all(r.exc_info is None for r in warnings)

    def test_column_batch_preview_truncated_to_300_chars(self, monkeypatch, caplog):
        import logging

        from genie_space_optimizer.optimization import optimizer as opt_mod

        self._mock_blank_columns(monkeypatch, opt_mod)

        long_text = "x" * 500

        def _llm(*_a, **_k):
            return long_text, object()

        monkeypatch.setattr(opt_mod, "_traced_llm_call", _llm)
        monkeypatch.setattr(
            "genie_space_optimizer.optimization.evaluation._extract_json",
            lambda _t: (_ for _ in ()).throw(ValueError("junk")),
        )

        with caplog.at_level(logging.WARNING, logger="genie_space_optimizer"):
            opt_mod._enrich_blank_descriptions(self._SNAPSHOT, w=None)

        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert warnings
        # Preview surfaces the text but is bounded — the long response must
        # not dump the entire response into the log line.
        msg = warnings[0].getMessage()
        assert "preview=" in msg
        # At most 300 x-chars in the preview plus framing.
        preview_len = msg.count("x")
        assert preview_len <= 300

    def test_column_batch_preview_reads_last_response_text_from_exc(
        self, monkeypatch, caplog,
    ):
        """F6 — when ``_traced_llm_call`` raises with ``last_response_text``
        attached, the warning's preview must come from the exception
        attribute, not the empty local ``text`` variable.

        This is the bug observed in the field: the caller initialises
        ``text = ""`` and never gets past the raising call, so the old
        code logged ``preview=''`` no matter what the LLM returned.
        """
        import logging

        from genie_space_optimizer.optimization import optimizer as opt_mod

        self._mock_blank_columns(monkeypatch, opt_mod)

        def _raising(*_a, **_k):
            exc = ValueError("Expecting value: line 1 column 1 (char 0)")
            # Exactly what the real _traced_llm_call stamps on exhaustion.
            exc.last_response_text = '[{"partial":'
            exc.last_response_chars = len('[{"partial":')
            raise exc

        monkeypatch.setattr(opt_mod, "_traced_llm_call", _raising)

        with caplog.at_level(logging.WARNING, logger="genie_space_optimizer"):
            opt_mod._enrich_blank_descriptions(self._SNAPSHOT, w=None)

        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert warnings
        msg = warnings[0].getMessage()
        assert "preview=" in msg
        # The body the model returned is surfaced — not the empty string.
        assert '[{"partial":' in msg
        assert "preview=''" not in msg
        # response_chars reflects the attached length, not 0.
        assert "response_chars=12" in msg

    def test_table_batch_preview_reads_last_response_text_from_exc(
        self, monkeypatch, caplog,
    ):
        """F6 (table variant) — the same attribute-first preview lookup
        applies to _enrich_table_descriptions."""
        import logging

        from genie_space_optimizer.optimization import optimizer as opt_mod

        monkeypatch.setattr(
            opt_mod, "_collect_insufficient_tables",
            lambda _snap: [
                {
                    "table": "cat.sch.t",
                    "current_description": "",
                    "columns": [{"column_name": "c", "type_text": "STRING"}],
                },
            ],
        )

        def _raising(*_a, **_k):
            exc = ValueError("Expecting value: line 1 column 1 (char 0)")
            exc.last_response_text = "<refusal>I cannot help"
            exc.last_response_chars = len("<refusal>I cannot help")
            raise exc

        monkeypatch.setattr(opt_mod, "_traced_llm_call", _raising)

        with caplog.at_level(logging.WARNING, logger="genie_space_optimizer"):
            opt_mod._enrich_table_descriptions(self._SNAPSHOT, w=None)

        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert warnings
        msg = warnings[0].getMessage()
        assert "<refusal>I cannot help" in msg
        assert "preview=''" not in msg


# ═══════════════════════════════════════════════════════════════════════
# F7 — Per-batch column/table caps in description enrichment
# ═══════════════════════════════════════════════════════════════════════


class TestEnrichmentBatchCapChunking:
    """F7 — _chunk_enrichment_batches splits oversized per-table batches
    into fixed-size chunks so a single 88-column table can't blow the
    LLM output budget and return an empty HTTP 200 body.
    """

    def test_chunks_are_bounded_by_max_size(self):
        from genie_space_optimizer.optimization.optimizer import (
            _chunk_enrichment_batches,
        )
        batch = [{"i": i} for i in range(88)]
        out = _chunk_enrichment_batches([batch], max_size=25)
        assert len(out) == 4, (
            "88 / 25 = 3 full chunks + 1 remainder = 4 chunks"
        )
        assert [len(c) for c in out] == [25, 25, 25, 13]
        # Order is preserved.
        assert out[0][0]["i"] == 0
        assert out[-1][-1]["i"] == 87

    def test_small_batches_pass_through_unchanged(self):
        from genie_space_optimizer.optimization.optimizer import (
            _chunk_enrichment_batches,
        )
        small = [{"i": 0}, {"i": 1}]
        out = _chunk_enrichment_batches([small], max_size=25)
        assert out == [small]

    def test_exactly_at_max_size_not_split(self):
        from genie_space_optimizer.optimization.optimizer import (
            _chunk_enrichment_batches,
        )
        batch = [{"i": i} for i in range(25)]
        out = _chunk_enrichment_batches([batch], max_size=25)
        assert len(out) == 1
        assert len(out[0]) == 25

    def test_preserves_table_affinity_across_multiple_batches(self):
        """Rows from different input batches never get merged."""
        from genie_space_optimizer.optimization.optimizer import (
            _chunk_enrichment_batches,
        )
        t1 = [{"table": "t1", "i": i} for i in range(30)]
        t2 = [{"table": "t2", "i": i} for i in range(20)]
        out = _chunk_enrichment_batches([t1, t2], max_size=15)
        # t1 splits into 2 chunks (15 + 15); t2 splits into 2 chunks (15 + 5).
        assert len(out) == 4
        # Each chunk is homogeneous — no cross-table contamination.
        for chunk in out:
            tables_in_chunk = {row["table"] for row in chunk}
            assert len(tables_in_chunk) == 1, (
                f"chunk mixed tables: {tables_in_chunk}"
            )

    def test_invalid_max_size_returns_input_unchanged(self):
        from genie_space_optimizer.optimization.optimizer import (
            _chunk_enrichment_batches,
        )
        batches = [[{"i": 0}], [{"i": 1}]]
        assert _chunk_enrichment_batches(batches, max_size=0) == batches
        assert _chunk_enrichment_batches(batches, max_size=-5) == batches


class TestColumnEnrichmentAppliesCap:
    """_enrich_blank_descriptions wires _chunk_enrichment_batches so a
    wide metric view (e.g. the 88-column mv_esr_store_sales seen in the
    field) is broken into ≤ _MAX_COLUMNS_PER_BATCH chunks before any
    LLM call fires.
    """

    def _snapshot_with_wide_table(self, n_columns: int) -> dict:
        columns = [
            {"column_name": f"col{i}", "type_text": "DOUBLE"}
            for i in range(n_columns)
        ]
        return {
            "data_sources": {
                "tables": [],
                "metric_views": [
                    {
                        "identifier": "cat.sch.mv_wide",
                        "name": "mv_wide",
                        "column_configs": columns,
                    },
                ],
            },
            "instructions": {"join_specs": []},
            "tables": [],
            "metric_views": [
                {
                    "identifier": "cat.sch.mv_wide",
                    "name": "mv_wide",
                    "column_configs": columns,
                },
            ],
        }

    def test_wide_table_splits_into_capped_chunks(self, monkeypatch):
        """88 blank columns → ceil(88/25)=4 LLM calls, each ≤ 25 rows."""
        from genie_space_optimizer.optimization import optimizer as opt_mod

        blanks = [
            {
                "table": "cat.sch.mv_wide",
                "column": f"col{i}",
                "data_type": "DOUBLE",
                "entity_type": "measure",
                "table_description": "",
                "sibling_columns": [],
            }
            for i in range(88)
        ]
        monkeypatch.setattr(
            opt_mod, "_collect_blank_columns", lambda _snap: blanks,
        )

        observed_batch_sizes: list[int] = []

        def _fake_llm(w, system_msg, prompt, **kwargs):
            # Count rows from the prompt indirectly via span_name — we
            # capture the actual context length via the prompt length
            # heuristic instead, since the prompt embeds one row per
            # column. Easier: count how many times we're called by
            # stashing the batch slice via a closure variable in the
            # caller; here we just record '1' per call. The len check
            # happens via the number of calls.
            observed_batch_sizes.append(1)
            return '{"changes": []}', object()

        monkeypatch.setattr(opt_mod, "_traced_llm_call", _fake_llm)

        opt_mod._enrich_blank_descriptions(
            self._snapshot_with_wide_table(88), w=None,
        )

        # 88 columns / 25 cap = 4 chunks. Pre-F7 this was 1 call.
        assert len(observed_batch_sizes) == 4

    def test_threshold_small_single_batch_not_split(self, monkeypatch):
        """A small run (≤ _ENRICHMENT_BATCH_THRESHOLD total) stays as
        one batch — the cap only bites when table-grouping produces
        an oversized single-table batch."""
        from genie_space_optimizer.optimization import optimizer as opt_mod

        blanks = [
            {
                "table": "cat.sch.t",
                "column": f"col{i}",
                "data_type": "STRING",
                "entity_type": "dimension",
                "table_description": "",
                "sibling_columns": [],
            }
            for i in range(20)  # ≤ _ENRICHMENT_BATCH_THRESHOLD = 30
        ]
        monkeypatch.setattr(
            opt_mod, "_collect_blank_columns", lambda _snap: blanks,
        )

        call_count = [0]

        def _fake_llm(w, system_msg, prompt, **kwargs):
            call_count[0] += 1
            return '{"changes": []}', object()

        monkeypatch.setattr(opt_mod, "_traced_llm_call", _fake_llm)

        opt_mod._enrich_blank_descriptions(
            self._snapshot_with_wide_table(20), w=None,
        )

        # 20 rows ≤ cap 25 → single batch → exactly one LLM call.
        assert call_count[0] == 1


class TestTableEnrichmentAppliesCap:
    """_enrich_table_descriptions applies _MAX_TABLES_PER_BATCH = 15
    when the single-batch path is active (≤ _ENRICHMENT_BATCH_THRESHOLD
    tables in total).
    """

    def test_single_batch_with_20_tables_splits_into_two(self, monkeypatch):
        from genie_space_optimizer.optimization import optimizer as opt_mod

        # 20 tables, all with insufficient descriptions. The
        # pre-existing code would put all 20 in one batch since
        # 20 <= _ENRICHMENT_BATCH_THRESHOLD (30); F7's cap of 15
        # forces a split into 2 chunks (15 + 5).
        tables = [
            {
                "table": f"cat.sch.t{i}",
                "current_description": "",
                "columns": [{"column_name": "c", "type_text": "STRING"}],
            }
            for i in range(20)
        ]
        monkeypatch.setattr(
            opt_mod, "_collect_insufficient_tables", lambda _snap: tables,
        )

        call_count = [0]

        def _fake_llm(w, system_msg, prompt, **kwargs):
            call_count[0] += 1
            return '{"changes": []}', object()

        monkeypatch.setattr(opt_mod, "_traced_llm_call", _fake_llm)

        opt_mod._enrich_table_descriptions(
            {"data_sources": {"tables": [], "metric_views": []},
             "tables": [], "metric_views": []},
            w=None,
        )

        assert call_count[0] == 2  # ceil(20/15) = 2


# ═══════════════════════════════════════════════════════════════════════
# F3 — Honest total_eligible accounting in _run_description_enrichment
# ═══════════════════════════════════════════════════════════════════════


class TestDescriptionEnrichmentAccounting:
    """When some batches silently drop (LLM returned unparseable JSON
    even after validator retries), ``total_eligible`` must reflect the
    ORIGINAL number of blank columns — not the number of patches that
    survived. Otherwise the stage banner misreports "30/30 enriched"
    when 20 columns were actually dropped.
    """

    def test_total_eligible_counts_blanks_not_patches(self, monkeypatch):
        """Classic prod scenario: 50 blanks, LLM parses 30 of them."""
        from genie_space_optimizer.optimization import harness as harness_mod

        blanks = [
            {
                "table": f"cat.sch.t_{i // 10}",
                "column": f"c_{i}",
                "data_type": "STRING",
                "entity_type": "column_dim",
                "table_description": "",
                "sibling_columns": [],
            }
            for i in range(50)
        ]
        patches = [
            {
                "type": "update_column_description",
                "table": b["table"],
                "column": b["column"],
                "structured_sections": {"definition": "d"},
                "column_entity_type": b["entity_type"],
                "lever": 0,
                "risk_level": "low",
                "source": "proactive_enrichment",
            }
            for b in blanks[:30]
        ]

        monkeypatch.setattr(
            harness_mod, "_collect_blank_columns", lambda _snap: blanks,
        )
        monkeypatch.setattr(
            harness_mod, "_enrich_blank_descriptions",
            lambda *_a, **_k: patches,
        )
        monkeypatch.setattr(
            harness_mod, "_collect_insufficient_tables", lambda _snap: [],
        )
        monkeypatch.setattr(
            harness_mod, "_enrich_table_descriptions",
            lambda *_a, **_k: [],
        )
        # No-op SDK side effects: write_stage / write_patch /
        # patch_space_config are all wired to swallow calls.
        monkeypatch.setattr(
            harness_mod, "write_stage", lambda *a, **k: None,
        )
        monkeypatch.setattr(
            harness_mod, "write_patch", lambda *a, **k: None,
        )

        class _FakePatchClient:
            def __call__(self, *a, **k):
                return None

        monkeypatch.setattr(
            "genie_space_optimizer.common.genie_client.patch_space_config",
            _FakePatchClient(),
        )
        monkeypatch.setattr(
            "genie_space_optimizer.common.genie_client.fetch_space_config",
            lambda *a, **k: {},
        )

        # Minimal snapshot with distinct tables so patches can be applied.
        snapshot = {
            "data_sources": {
                "tables": [
                    {
                        "identifier": f"cat.sch.t_{i}",
                        "name": f"t_{i}",
                        "column_configs": [
                            {"column_name": f"c_{i*10 + j}", "type_text": "STRING"}
                            for j in range(10)
                        ],
                    }
                    for i in range(5)
                ],
                "metric_views": [],
            },
            "instructions": {"join_specs": []},
        }

        result = harness_mod._run_description_enrichment(
            w=None, spark=None, run_id="r1", space_id="s1",
            config={"_parsed_space": snapshot}, metadata_snapshot=snapshot,
            catalog="c", schema="s",
        )

        assert result["total_eligible"] == 50
        assert result["total_patches_generated"] == 30
        assert result["total_failed_llm"] == 20

    def test_total_failed_llm_is_zero_when_all_batches_parse(self, monkeypatch):
        """Happy path: 3 blanks, LLM returns 3 patches → failed_llm=0."""
        from genie_space_optimizer.optimization import harness as harness_mod

        blanks = [
            {
                "table": "cat.sch.t",
                "column": f"c_{i}",
                "data_type": "STRING",
                "entity_type": "column_dim",
                "table_description": "",
                "sibling_columns": [],
            }
            for i in range(3)
        ]
        patches = [
            {
                "type": "update_column_description",
                "table": "cat.sch.t",
                "column": b["column"],
                "structured_sections": {"definition": "d"},
                "column_entity_type": "column_dim",
                "lever": 0,
                "risk_level": "low",
                "source": "proactive_enrichment",
            }
            for b in blanks
        ]

        monkeypatch.setattr(
            harness_mod, "_collect_blank_columns", lambda _snap: blanks,
        )
        monkeypatch.setattr(
            harness_mod, "_enrich_blank_descriptions",
            lambda *_a, **_k: patches,
        )
        monkeypatch.setattr(
            harness_mod, "_collect_insufficient_tables", lambda _snap: [],
        )
        monkeypatch.setattr(
            harness_mod, "_enrich_table_descriptions",
            lambda *_a, **_k: [],
        )
        monkeypatch.setattr(harness_mod, "write_stage", lambda *a, **k: None)
        monkeypatch.setattr(harness_mod, "write_patch", lambda *a, **k: None)
        monkeypatch.setattr(
            "genie_space_optimizer.common.genie_client.patch_space_config",
            lambda *a, **k: None,
        )

        snapshot = {
            "data_sources": {
                "tables": [
                    {
                        "identifier": "cat.sch.t",
                        "name": "t",
                        "column_configs": [
                            {"column_name": f"c_{i}", "type_text": "STRING"}
                            for i in range(3)
                        ],
                    },
                ],
                "metric_views": [],
            },
            "instructions": {"join_specs": []},
        }

        result = harness_mod._run_description_enrichment(
            w=None, spark=None, run_id="r1", space_id="s1",
            config={"_parsed_space": snapshot}, metadata_snapshot=snapshot,
            catalog="c", schema="s",
        )

        assert result["total_eligible"] == 3
        assert result["total_patches_generated"] == 3
        assert result["total_failed_llm"] == 0
        assert result["total_enriched"] == 3

    def test_tables_accounting_mirrors_column_accounting(self, monkeypatch):
        """Table eligibility accounting works the same way: 5 eligible,
        2 patches generated → tables_failed_llm=3."""
        from genie_space_optimizer.optimization import harness as harness_mod

        insufficient = [
            {"table": f"cat.sch.t_{i}", "is_metric_view": False}
            for i in range(5)
        ]
        tbl_patches = [
            {
                "type": "update_description",
                "table": t["table"],
                "structured_sections": {"purpose": "x"},
                "table_entity_type": "table",
                "lever": 0,
                "risk_level": "low",
                "source": "proactive_enrichment",
            }
            for t in insufficient[:2]
        ]

        monkeypatch.setattr(
            harness_mod, "_collect_blank_columns", lambda _snap: [],
        )
        monkeypatch.setattr(
            harness_mod, "_enrich_blank_descriptions",
            lambda *_a, **_k: [],
        )
        monkeypatch.setattr(
            harness_mod, "_collect_insufficient_tables",
            lambda _snap: insufficient,
        )
        monkeypatch.setattr(
            harness_mod, "_enrich_table_descriptions",
            lambda *_a, **_k: tbl_patches,
        )
        monkeypatch.setattr(harness_mod, "write_stage", lambda *a, **k: None)
        monkeypatch.setattr(harness_mod, "write_patch", lambda *a, **k: None)
        monkeypatch.setattr(
            "genie_space_optimizer.common.genie_client.patch_space_config",
            lambda *a, **k: None,
        )

        snapshot = {
            "data_sources": {
                "tables": [
                    {"identifier": f"cat.sch.t_{i}", "name": f"t_{i}",
                     "column_configs": []}
                    for i in range(5)
                ],
                "metric_views": [],
            },
            "instructions": {"join_specs": []},
        }

        result = harness_mod._run_description_enrichment(
            w=None, spark=None, run_id="r1", space_id="s1",
            config={"_parsed_space": snapshot}, metadata_snapshot=snapshot,
            catalog="c", schema="s",
        )

        assert result["tables_eligible"] == 5
        assert result["tables_patches_generated"] == 2
        assert result["tables_failed_llm"] == 3
        assert result["tables_enriched"] == 2
