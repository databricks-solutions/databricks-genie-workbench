"""Trial 30 W30.4(c) — namespace-insensitive post-apply eval slice.

The W29.4 airline postmortem saw ``GSO_POST_APPLY_EVAL_SLICED_V1
requested_qids=['<namespaced qid>'] benchmarks_count=0`` on ``gs_024``
(``POST_APPLY_EVAL_SLICED_ZERO_BENCHMARKS_AFTER_TRIAL_16``), forcing a ❌
row in the postmortem invariants table.

Root cause: the slice filtered benchmarks with
``extract_question_id(b)[0] in requested`` where ``extract_question_id``
returns the NAMESPACED qid (``airline_..._gs_024``) but ``requested``
(from ``inp.eval_qids``) carried the canonical ``gs_024`` (or vice
versa) — so a namespaced/canonical mismatch produced an empty slice even
when the benchmark row was present.

Fix (postmortem SKILL.md L158 recommendation): match the canonical
question-ID on BOTH sides using the shared ``_split_namespaced_qid``
canonicaliser, while preserving exact-match so the canonical-only case
stays byte-stable.
"""
from genie_space_optimizer.optimization.stages.evaluation import (
    slice_benchmarks_to_eval_qids,
)


def _bench(qid: str) -> dict:
    return {"question_id": qid, "expected_response": "x"}


def test_namespaced_benchmark_matches_canonical_request():
    # Benchmark carries the namespaced qid; the request is canonical.
    benchmarks = [_bench("airline_ticketing_and_fare_analysis_gs_024")]
    out = slice_benchmarks_to_eval_qids(benchmarks, ["gs_024"])
    assert len(out) == 1


def test_canonical_benchmark_matches_namespaced_request():
    benchmarks = [_bench("gs_024")]
    out = slice_benchmarks_to_eval_qids(
        benchmarks, ["airline_ticketing_and_fare_analysis_gs_024"]
    )
    assert len(out) == 1


def test_canonical_both_sides_byte_stable():
    benchmarks = [_bench("gs_024"), _bench("gs_009")]
    out = slice_benchmarks_to_eval_qids(benchmarks, ["gs_024"])
    assert [b["question_id"] for b in out] == ["gs_024"]


def test_no_overlap_yields_empty_slice():
    benchmarks = [_bench("gs_001"), _bench("gs_002")]
    out = slice_benchmarks_to_eval_qids(benchmarks, ["gs_999"])
    assert out == []


def test_empty_eval_qids_passes_through_all():
    benchmarks = [_bench("gs_001"), _bench("gs_002")]
    assert slice_benchmarks_to_eval_qids(benchmarks, []) == benchmarks
    assert slice_benchmarks_to_eval_qids(benchmarks, None) == benchmarks


def test_generalizes_across_namespaces_and_qids():
    # Two different domains + non-anchor qids — proves the fix is not
    # anchor-specific (Architectural Principle #2).
    benchmarks = [
        _bench("seven_now_convenience_retail_gs_555"),
        _bench("airline_ticketing_and_fare_analysis_gs_777"),
        _bench("gs_111"),
    ]
    out = slice_benchmarks_to_eval_qids(
        benchmarks, ["gs_555", "gs_777"]
    )
    got = {b["question_id"] for b in out}
    assert got == {
        "seven_now_convenience_retail_gs_555",
        "airline_ticketing_and_fare_analysis_gs_777",
    }
