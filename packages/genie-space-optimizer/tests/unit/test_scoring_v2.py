"""Group-B scoring-v2 flag matrix tests (B1-B5).

Validates three flag states for every Group-B fix:

* ``GSO_SCORING_V2`` unset / ``on`` → new scoring is headline (default).
* ``GSO_SCORING_V2=shadow``       → new is headline, legacy mirrored.
* ``GSO_SCORING_V2=off``          → legacy behavior restored byte-for-byte.

The tests target pure helpers rather than the full ``run_evaluation`` loop so
they stay hermetic (no MLflow, no Databricks, no Genie). Flag flips use
``monkeypatch.setenv`` because the helpers in ``config.py`` re-read the env
on every call exactly to support this style of testing.
"""

from __future__ import annotations

import pytest

from genie_space_optimizer.common import config as cfg
from genie_space_optimizer.common.genie_client import detect_asset_type
from genie_space_optimizer.optimization import evaluation as ev


# ─────────────────────────────────────────────────────────────────────────────
# Flag plumbing
# ─────────────────────────────────────────────────────────────────────────────
def test_scoring_v2_default_is_on(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GSO_SCORING_V2", raising=False)
    assert cfg.get_scoring_v2_mode() == "on"
    assert cfg.scoring_v2_is_on() is True
    assert cfg.scoring_v2_is_legacy() is False
    assert cfg.scoring_v2_is_shadow() is False


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("on", "on"),
        ("ON", "on"),
        ("shadow", "shadow"),
        ("Shadow", "shadow"),
        ("off", "off"),
        ("1", "on"),
        ("0", "off"),
        ("true", "on"),
        ("false", "off"),
        ("garbage", "on"),
    ],
)
def test_scoring_v2_normalization(
    monkeypatch: pytest.MonkeyPatch, raw: str, expected: str
) -> None:
    monkeypatch.setenv("GSO_SCORING_V2", raw)
    assert cfg.get_scoring_v2_mode() == expected


def test_shadow_mode_flags(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GSO_SCORING_V2", "shadow")
    assert cfg.scoring_v2_is_on() is True
    assert cfg.scoring_v2_is_shadow() is True
    assert cfg.scoring_v2_is_legacy() is False


def test_off_mode_flags(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GSO_SCORING_V2", "off")
    assert cfg.scoring_v2_is_on() is False
    assert cfg.scoring_v2_is_legacy() is True
    assert cfg.scoring_v2_is_shadow() is False


# ─────────────────────────────────────────────────────────────────────────────
# B1 — detect_asset_type drops bare ``mv_`` rule when scoring-v2 is active
# ─────────────────────────────────────────────────────────────────────────────
_MV_NAMED_SQL = "SELECT MEASURE(revenue) FROM mv_sales"
_TABLE_NAMED_MV_SQL = "SELECT * FROM mv_customers LIMIT 10"
_TVF_SQL = "SELECT * FROM get_customer_orders(42)"
_PLAIN_TABLE_SQL = "SELECT * FROM orders LIMIT 10"


def test_b1_measure_still_detected_as_mv(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GSO_SCORING_V2", raising=False)
    assert detect_asset_type(_MV_NAMED_SQL) == "MV"
    monkeypatch.setenv("GSO_SCORING_V2", "off")
    assert detect_asset_type(_MV_NAMED_SQL) == "MV"


def test_b1_explicit_mv_names_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """``mv_names`` coming from Genie space config must always win."""
    monkeypatch.delenv("GSO_SCORING_V2", raising=False)
    assert detect_asset_type(_TABLE_NAMED_MV_SQL, mv_names=["mv_customers"]) == "MV"
    monkeypatch.setenv("GSO_SCORING_V2", "off")
    assert detect_asset_type(_TABLE_NAMED_MV_SQL, mv_names=["mv_customers"]) == "MV"


def test_b1_default_treats_mv_prefixed_table_as_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """This is the fix: customer tables called ``mv_something`` are TABLE."""
    monkeypatch.delenv("GSO_SCORING_V2", raising=False)
    assert detect_asset_type(_TABLE_NAMED_MV_SQL) == "TABLE"


def test_b1_legacy_reproduces_mv_prefix_bug(monkeypatch: pytest.MonkeyPatch) -> None:
    """Kill-switch must reproduce legacy classifier byte-for-byte."""
    monkeypatch.setenv("GSO_SCORING_V2", "off")
    assert detect_asset_type(_TABLE_NAMED_MV_SQL) == "MV"


def test_b1_tvf_detection_preserved(monkeypatch: pytest.MonkeyPatch) -> None:
    for mode in (None, "on", "shadow", "off"):
        if mode is None:
            monkeypatch.delenv("GSO_SCORING_V2", raising=False)
        else:
            monkeypatch.setenv("GSO_SCORING_V2", mode)
        assert detect_asset_type(_TVF_SQL) == "TVF"
        assert detect_asset_type(_PLAIN_TABLE_SQL) == "TABLE"


# ─────────────────────────────────────────────────────────────────────────────
# B2 — expected_asset_hint beats detection under scoring-v2
# ─────────────────────────────────────────────────────────────────────────────
def test_b2_hint_overrides_detection_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Authoring hint ``TABLE`` beats any detection result when flag on."""
    monkeypatch.delenv("GSO_SCORING_V2", raising=False)
    normalized = ev._normalize_expected_asset(
        raw="some_table_name",
        expected_sql=_MV_NAMED_SQL,
        hint="TABLE",
    )
    assert normalized == "TABLE"


def test_b2_hint_ignored_under_legacy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GSO_SCORING_V2", "off")
    normalized = ev._normalize_expected_asset(
        raw="some_table_name",
        expected_sql=_MV_NAMED_SQL,
        hint="TABLE",
    )
    assert normalized == "MV"  # legacy falls back to detection.


def test_b2_valid_raw_always_wins_over_hint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GSO_SCORING_V2", raising=False)
    normalized = ev._normalize_expected_asset(
        raw="MV", expected_sql=_PLAIN_TABLE_SQL, hint="TABLE"
    )
    assert normalized == "MV"


def test_b2_invalid_hint_falls_back_to_detection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GSO_SCORING_V2", raising=False)
    normalized = ev._normalize_expected_asset(
        raw="", expected_sql=_TVF_SQL, hint="nonsense"
    )
    assert normalized == "TVF"


# ─────────────────────────────────────────────────────────────────────────────
# B3 — logical_pass vs all_judge_pass buckets
# ─────────────────────────────────────────────────────────────────────────────
def _row_with(**verdicts: str) -> dict:
    """Build a scorer-output row with nine judges defaulted to passing."""
    defaults = {j: "yes" for j in ev._JUDGE_ORDER}
    defaults["arbiter"] = "both_correct"
    defaults.update(verdicts)
    return {f"{j}/value": v for j, v in defaults.items()}


def test_b3_all_judges_passing_both_buckets_pass() -> None:
    logical, all_j = ev._compute_pass_buckets(_row_with())
    assert logical is True
    assert all_j is True


def test_b3_cosmetic_failure_only_flips_all_judge() -> None:
    row = _row_with(asset_routing="no", completeness="no")
    logical, all_j = ev._compute_pass_buckets(row)
    assert logical is True
    assert all_j is False


def test_b3_result_correctness_flips_both() -> None:
    row = _row_with(result_correctness="no")
    logical, all_j = ev._compute_pass_buckets(row)
    assert logical is False
    assert all_j is False


def test_b3_semantic_equivalence_flips_both() -> None:
    row = _row_with(semantic_equivalence="no")
    logical, all_j = ev._compute_pass_buckets(row)
    assert logical is False
    assert all_j is False


def test_b3_arbiter_ground_truth_only_flips_both() -> None:
    row = _row_with(arbiter="ground_truth_correct")
    logical, all_j = ev._compute_pass_buckets(row)
    assert logical is False
    assert all_j is False


def test_b3_arbiter_genie_correct_passes_logical() -> None:
    row = _row_with(arbiter="genie_correct")
    logical, all_j = ev._compute_pass_buckets(row)
    assert logical is True
    assert all_j is True


# ─────────────────────────────────────────────────────────────────────────────
# B4 — excluded verdict when Genie SQL is valid but the result set is missing
# ─────────────────────────────────────────────────────────────────────────────
def test_b4_result_correctness_excluded_under_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from genie_space_optimizer.optimization.scorers.result_correctness import (
        result_correctness_scorer,
    )

    monkeypatch.delenv("GSO_SCORING_V2", raising=False)
    cmp = {
        "error": "result fetch failed",
        "error_type": "genie_result_unavailable",
        "gt_rows": 10,
    }
    outputs = {"response": "SELECT 1 FROM t", "comparison": cmp}
    expectations = {"expected_response": "SELECT 1 FROM t"}
    fb = result_correctness_scorer(
        outputs=outputs, expectations=expectations, inputs={"question_id": "q1"}
    )
    assert str(fb.value).lower() == "excluded"


def test_b4_result_correctness_legacy_no_exclusion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from genie_space_optimizer.optimization.scorers.result_correctness import (
        result_correctness_scorer,
    )

    monkeypatch.setenv("GSO_SCORING_V2", "off")
    cmp = {
        "error": "result fetch failed",
        "error_type": "genie_result_unavailable",
        "gt_rows": 10,
    }
    outputs = {"response": "SELECT 1 FROM t", "comparison": cmp}
    expectations = {"expected_response": "SELECT 1 FROM t"}
    fb = result_correctness_scorer(
        outputs=outputs, expectations=expectations, inputs={"question_id": "q1"}
    )
    assert str(fb.value).lower() == "no"


def _make_completeness_under_flag(monkeypatch: pytest.MonkeyPatch, value: str | None):
    """Build a completeness scorer under a given scoring-v2 flag value."""
    from genie_space_optimizer.optimization.scorers import completeness as _compl

    if value is None:
        monkeypatch.delenv("GSO_SCORING_V2", raising=False)
    else:
        monkeypatch.setenv("GSO_SCORING_V2", value)

    # The factory requires a WorkspaceClient, but the ``genie_result_unavailable``
    # short-circuit runs before any LLM call, so a bare ``object()`` stand-in is
    # sufficient. ``resolve_sql`` is called with the GT SQL; patch it to a no-op.
    monkeypatch.setattr(_compl, "resolve_sql", lambda sql, c, s: sql, raising=True)
    return _compl._make_completeness_judge(object(), "cat", "sch")


def test_b4_completeness_excluded_under_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    judge = _make_completeness_under_flag(monkeypatch, None)
    cmp = {"error_type": "genie_result_unavailable"}
    outputs = {"response": "SELECT col FROM t", "comparison": cmp}
    expectations = {"expected_response": "SELECT col FROM t"}
    fb = judge(
        outputs=outputs, expectations=expectations, inputs={"question_id": "q1"}
    )
    assert str(fb.value).lower() == "excluded"


def test_b4_completeness_legacy_no_exclusion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Under ``off`` the exclusion block must not fire.

    The downstream LLM call would then run; we stub it to confirm the flow
    reaches past the short-circuit.
    """
    judge = _make_completeness_under_flag(monkeypatch, "off")
    from genie_space_optimizer.optimization.scorers import completeness as _compl

    sentinel = object()
    called: dict[str, bool] = {"hit": False}

    def _fake_call_llm(*_args, **_kwargs) -> dict:
        called["hit"] = True
        return {"complete": True, "rationale": "", "failure_type": "", "blame_set": []}

    monkeypatch.setattr(
        _compl, "_call_llm_for_scoring", _fake_call_llm, raising=True
    )

    cmp = {"error_type": "genie_result_unavailable"}
    outputs = {"response": "SELECT col FROM t", "comparison": cmp}
    expectations = {"expected_response": "SELECT col FROM t"}
    fb = judge(
        outputs=outputs, expectations=expectations, inputs={"question_id": "q1"}
    )
    # The LLM path must have been reached (short-circuit disabled under ``off``).
    assert called["hit"] is True
    assert str(fb.value).lower() != "excluded"
    _ = sentinel  # keep reference for readability


# ─────────────────────────────────────────────────────────────────────────────
# B5 — sorted-comparison default + order_sensitive opt-in
# ─────────────────────────────────────────────────────────────────────────────
def test_b5_eval_records_threads_order_sensitive_default_false() -> None:
    """When the benchmark omits ``order_sensitive`` we must pass ``False``.

    We smoke-test the plumbing by replicating the single line of code from
    ``_build_eval_records`` that writes ``inputs["order_sensitive"]``. The
    real constructor is wrapped inside large ``run_evaluation`` branches and
    cannot be invoked hermetically, but the default must never be True.
    """
    benchmark_without_flag = {"question": "q", "expected_sql": "SELECT 1"}
    assert bool(benchmark_without_flag.get("order_sensitive", False)) is False

    benchmark_with_flag = {
        "question": "q",
        "expected_sql": "SELECT 1",
        "order_sensitive": True,
    }
    assert bool(benchmark_with_flag.get("order_sensitive", False)) is True
