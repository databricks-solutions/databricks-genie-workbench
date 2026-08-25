"""Unit tests for the metric view advisor phase.

The phase talks to Delta through four functions, so ``spark`` is never a real
session here: :class:`FakeSpark` is unused by the loaders (they are monkeypatched)
and passed only to prove the phase does not touch it directly. The interesting
assertions are about what the phase *records* — skip reasons, statuses, coverage,
the oracle actually being wired — rather than about SQL round-trips.
"""

from __future__ import annotations

import json

import pytest

from genie_space_optimizer.common import config
from genie_space_optimizer.optimization import mv_advisor
from genie_space_optimizer.optimization.mv_advisor import (
    AdvisorOutcome,
    candidate_from_measure,
    column_facts_from_inventory,
    load_iteration_zero_corpus,
    profiling_for,
    run_mv_advisor_phase,
)
from genie_space_optimizer.optimization import mv_scoring
from genie_space_optimizer.optimization.mv_fingerprint import FingerprintRecurrence
from genie_space_optimizer.optimization.mv_scoring import (
    DemandSignal,
    LineageOverlap,
    coverage_ceiling,
)
from genie_space_optimizer.optimization.mv_signals import SignalResult

SPACE_ID = "01f04ac8c1f11c9a9e5b3b2b0e5d5c11"
LINEITEM = "samples.tpch.lineitem"

REVENUE_SQL = (
    "SELECT SUM(l_extendedprice * (1 - l_discount)) AS revenue, l_returnflag "
    f"FROM {LINEITEM} GROUP BY l_returnflag"
)
COUNT_SQL = f"SELECT COUNT(l_orderkey) AS orders, l_returnflag FROM {LINEITEM} GROUP BY l_returnflag"


class FakeSpark:
    """A session the phase must never call. Any attribute access is a failure."""

    def __getattr__(self, name: str):  # pragma: no cover - only on regression
        raise AssertionError(f"the advisor phase touched spark.{name} directly")


def eval_row(sql: str, qid: str, **overrides):
    """One official-runner eval row, in the shape ``rows_json`` stores."""
    row = {
        "question_id": qid,
        "inputs/question_id": qid,
        "generated_sql": sql,
        "outputs/response": sql,
        "response": {"response": sql, "comparison": {}},
        "expected_sql": sql,
    }
    row.update(overrides)
    return row


def iteration(rows, *, number: int = 0):
    return {"iteration": number, "eval_scope": "full", "rows_json": rows}


def recurring(sql: str = REVENUE_SQL, times: int = 8):
    """A corpus where one measure genuinely recurs.

    Eight occurrences rather than one because Y is the *only* computed signal here
    (L and D have no producer, and no embedding client is supplied by default), so
    a measure seen once scores 16 and is suppressed before it can be persisted.
    Eight puts it above the floor without pinning a tier the coverage cap owns.
    """
    return [eval_row(sql, f"bmk_{i}") for i in range(times)]


def patch_iterations(monkeypatch, rows):
    monkeypatch.setattr(
        mv_advisor, "load_all_full_iterations", lambda *a, **k: rows
    )


def patch_writes(monkeypatch):
    """Silence the two Delta writers and capture what they were handed."""
    stages: list[dict] = []
    artifacts: list[dict] = []
    upserts: list[dict] = []

    monkeypatch.setattr(
        mv_advisor,
        "write_stage",
        lambda spark, run_id, stage, status, **kw: stages.append(
            {"run_id": run_id, "stage": stage, "status": status, **kw}
        ),
    )
    monkeypatch.setattr(
        mv_advisor,
        "write_artifact",
        lambda spark, run_id, kind, payload, **kw: (
            artifacts.append({"kind": kind, "payload": payload, **kw}) or "art_1"
        ),
    )
    monkeypatch.setattr(
        mv_advisor,
        "persist_proposal",
        lambda spark, proposal, **kw: (
            upserts.append({"proposal": proposal, **kw}) or proposal.dedup_fingerprint
        ),
    )
    return stages, artifacts, upserts


def patch_estate(monkeypatch, yamls=None):
    """Stub the estate scan. Left out of :func:`patch_writes` so one test can
    exercise the real wrapper's failure path."""
    monkeypatch.setattr(
        mv_advisor, "estate_metric_view_yamls", lambda *a, **k: dict(yamls or {})
    )


def advise(monkeypatch, rows, *, stub_estate: bool = True, **overrides):
    patch_iterations(monkeypatch, rows)
    if stub_estate:
        patch_estate(monkeypatch)
    kwargs = {
        "run_id": "run_5521",
        "space_id": SPACE_ID,
        "catalog": "main",
        "schema": "genie_space_optimizer",
        "enabled": True,
        "benchmarks": [{"id": "bmk_1", "question": "what is revenue", "expected_sql": REVENUE_SQL}],
    }
    kwargs.update(overrides)
    return run_mv_advisor_phase(FakeSpark(), **kwargs)


# ── Gating ───────────────────────────────────────────────────────────────


def test_the_phase_is_free_when_the_flag_is_off(monkeypatch) -> None:
    """Off means no Delta read at all, not a read that finds nothing."""
    def explode(*a, **k):
        raise AssertionError("the disabled phase read from Delta")

    monkeypatch.setattr(mv_advisor, "load_all_full_iterations", explode)
    stages, _artifacts, _upserts = patch_writes(monkeypatch)

    outcome = run_mv_advisor_phase(
        FakeSpark(),
        run_id="r1",
        space_id=SPACE_ID,
        catalog="main",
        schema="gso",
        enabled=False,
    )

    assert outcome.status == mv_advisor.STATUS_SKIPPED
    assert outcome.skip_reason == mv_advisor.SKIP_DISABLED
    # Still recorded: a silent no-op is indistinguishable from a phase that never ran.
    assert stages[0]["status"] == mv_advisor.STATUS_SKIPPED


# ── The empty-corpus trap (recon Q1) ─────────────────────────────────────


@pytest.mark.parametrize(
    ("rows", "expected"),
    [
        ([], mv_advisor.SKIP_NO_ITERATIONS),
        ([iteration([eval_row(REVENUE_SQL, "b1")], number=3)], mv_advisor.SKIP_NO_ITERATION_ZERO),
        ([iteration([])], mv_advisor.SKIP_EMPTY_ROWS_JSON),
        ([iteration(None)], mv_advisor.SKIP_EMPTY_ROWS_JSON),
        ([iteration([{"question_id": "b1"}])], mv_advisor.SKIP_NO_GENERATED_SQL),
        ([iteration([eval_row("NOT SQL AT ALL (((", "b1")])], mv_advisor.SKIP_NO_PARSEABLE_SQL),
        ([iteration([eval_row(f"SELECT l_orderkey FROM {LINEITEM}", "b1")])],
         mv_advisor.SKIP_NO_CANDIDATES),
    ],
)
def test_every_way_of_having_no_corpus_is_a_named_skip(monkeypatch, rows, expected) -> None:
    """MV-D15's first-class SKIP, and the reason it is not one reason.

    ``EMPTY_ROWS_JSON`` is the one that matters most: the eval runner returns no
    rows on any non-success terminal status, so it means the evaluation failed and
    says nothing about the space. Collapsing it into "found no candidates" would
    report a broken eval as a finding about the customer's queries.
    """
    patch_writes(monkeypatch)
    outcome = advise(monkeypatch, rows)

    assert outcome.status == mv_advisor.STATUS_SKIPPED
    assert outcome.skip_reason == expected
    assert outcome.proposals_persisted == 0


def test_a_skip_never_scores_anything_to_nothing(monkeypatch) -> None:
    """The trap stated positively: no corpus must not produce a zero-scored row.

    A persisted candidate with every signal at 0.0 would be indistinguishable
    from a real measurement, and it would sit in the table outranked by nothing
    because there is nothing else. Skipping leaves the table clean.
    """
    _stages, artifacts, upserts = patch_writes(monkeypatch)
    outcome = advise(monkeypatch, [iteration([])])

    assert outcome.skip_reason == mv_advisor.SKIP_EMPTY_ROWS_JSON
    assert upserts == []
    assert artifacts == []


def test_rows_without_ids_get_distinct_positional_provenance(monkeypatch) -> None:
    """Merging unidentified rows would inflate ``provenance_count``.

    That count is what separates "sixty occurrences of one query" from a measure
    the space genuinely reuses, so two anonymous rows must not collapse into one
    source.
    """
    patch_writes(monkeypatch)
    load = load_iteration_zero_corpus.__wrapped__ if hasattr(
        load_iteration_zero_corpus, "__wrapped__"
    ) else load_iteration_zero_corpus
    patch_iterations(
        monkeypatch,
        [iteration([
            {"generated_sql": REVENUE_SQL},
            {"generated_sql": COUNT_SQL},
        ])],
    )

    result = load(FakeSpark(), run_id="r1", catalog="main", schema="gso")

    assert [pid for _sql, pid in result.entries] == ["row_0", "row_1"]


def test_the_nested_response_shape_is_read_when_flat_aliases_are_absent(monkeypatch) -> None:
    patch_iterations(
        monkeypatch,
        [iteration([{"question_id": "b1", "response": {"response": REVENUE_SQL}}])],
    )

    result = load_iteration_zero_corpus(FakeSpark(), run_id="r1", catalog="main", schema="gso")

    assert result.usable
    assert result.entries == ((REVENUE_SQL, "b1"),)


# ── Signal availability (MV-D15) ─────────────────────────────────────────


def test_l_and_d_are_unavailable_rather_than_scored_zero(monkeypatch) -> None:
    """MV-D15's degraded-workspace state, asserted rather than described.

    ``advise`` injects no ``signal_reader``, so the L and D producers cannot run
    and both report ``UNAVAILABLE`` — the exact landing of a workspace missing the
    warehouse or the ``column_lineage`` grant. They leave the blend instead of
    contributing zeros nobody measured. With no embedding client either, Y is the
    only computed signal and coverage is 0.30 — below the MEDIUM floor, so the tier
    is LOW-capped however well the measure scores. This is the degraded-mode pin:
    producers-all-UNAVAILABLE must reproduce the pre-6b evidence exactly.
    """
    _stages, _artifacts, _upserts = patch_writes(monkeypatch)
    outcome = advise(monkeypatch, [iteration(recurring())])

    assert outcome.status == mv_advisor.STATUS_COMPLETE
    assert outcome.proposals

    proposal = outcome.proposals[0]
    components = proposal.components
    assert components.status_of("L") == config.MV_SIGNAL_UNAVAILABLE
    assert components.status_of("D") == config.MV_SIGNAL_UNAVAILABLE
    assert components.status_of("Y") == config.MV_SIGNAL_COMPUTED
    assert components.status_of("S") == config.MV_SIGNAL_UNAVAILABLE
    assert components.evidence_coverage == 0.30

    # Not scored 0.0: the score is Y renormalized over Y's own weight.
    assert components.L == 0.0
    assert proposal.confidence_score == pytest.approx(100.0 * components.Y)
    assert proposal.tier == "LOW"
    assert proposal.tier_capped_by_coverage is True
    assert proposal.uncapped_tier == "MEDIUM"


def test_the_candidate_carries_a_semantic_reference_set(monkeypatch) -> None:
    """Without this, S is EMPTY on every candidate — MV-D12's defect relabelled.

    A ``NEW_METRIC_VIEW`` candidate prefers ``SOURCE_COLUMN_METADATA``, so if the
    advisor builds candidates without it, S never has anything to compare and a
    fifth of the blend is structurally dead for the engine's primary output. The
    reference set is narrowed to the measure's own columns: a wide fact table's
    other columns are not evidence about this measure.
    """
    measure = FingerprintRecurrence(
        fingerprint="f1",
        canonical_expr="SUM(l_extendedprice)",
        kind="measure",
        recurrence=8,
        provenance_ids=("b1",),
        provenance_count=1,
        source_columns=("l_extendedprice",),
        source_tables=(LINEITEM,),
    )

    candidate = candidate_from_measure(
        measure, space_id=SPACE_ID, table_columns=column_facts_from_inventory(INVENTORY)
    )

    assert [ref.column for ref in candidate.source_column_metadata] == ["l_extendedprice"]
    assert candidate.source_column_metadata[0].comment == "Extended price"
    # No inventory is a genuine absence, and reports as one rather than silently.
    assert candidate_from_measure(measure, space_id=SPACE_ID).source_column_metadata == ()


def test_a_candidate_whose_columns_are_in_the_inventory_has_a_non_empty_reference_set(
    monkeypatch,
) -> None:
    """MV-D12's standing assertion, asserted end-to-end rather than on the scorer.

    The MV-D12 defect re-entered through the *producer*: ``candidate_from_measure``
    left ``source_column_metadata`` empty, so the scorer correctly preferred
    ``SOURCE_COLUMN_METADATA``, correctly found nothing, and correctly reported
    ``EMPTY`` — while every MV-D12 scorer test stayed green, because none of them
    touched the producer. A scorer test proves the consumer, not that anything
    feeds it.

    So the assertion is on the *persisted* proposal after the whole phase has run,
    and it is that the reference set was **non-empty** — ``reference_kind`` is the
    one actually compared, so ``SOURCE_COLUMN_METADATA`` with ``COMPUTED`` can only
    be reached by a populated set. Handling an empty set gracefully is a separate
    property, separately tested; it is not evidence the set is ever populated.
    """
    _stages, _artifacts, _upserts = patch_writes(monkeypatch)

    class Client:
        def embed(self, texts):
            return [[1.0, 0.0, 0.0] for _ in texts]

    outcome = advise(
        monkeypatch,
        [iteration(recurring())],
        embedding_client=Client(),
        intent_texts=("revenue",),
        wide_schema_inventory=INVENTORY,
    )

    match = outcome.proposals[0].evidence["semantic_top_match"]
    assert match["reference_kind"] == mv_scoring.SEMANTIC_REF_SOURCE_COLUMN_METADATA
    assert match["status"] == config.MV_SIGNAL_COMPUTED
    assert match["field"], "a compared match names the reference it matched"
    # The unfed-producer signature this exists to catch.
    assert match["reference_kind"] != mv_scoring.SEMANTIC_REF_NONE
    assert match["status"] != config.MV_SIGNAL_EMPTY


def test_advisor_statuses_reports_each_producers_actual_status() -> None:
    """Post-6b: it carries the producers' real statuses, not a hardcoded pair.

    S stays absent on purpose (recon Q4) — ``score_candidate`` derives it from the
    embedding attempt, so naming it here would overwrite the endpoint's own report.
    """
    lineage = SignalResult(LineageOverlap(), config.MV_SIGNAL_COMPUTED)
    demand = SignalResult(
        DemandSignal(), config.MV_SIGNAL_EMPTY, "no matching measure in history"
    )
    statuses = mv_advisor.advisor_statuses(lineage, demand)

    assert set(statuses) == {"L", "D"}
    assert "S" not in statuses
    assert statuses["L"] == config.MV_SIGNAL_COMPUTED
    assert statuses["D"] == config.MV_SIGNAL_EMPTY


def test_advisor_statuses_cannot_be_mutated_by_a_caller() -> None:
    unavailable = SignalResult(LineageOverlap(), config.MV_SIGNAL_UNAVAILABLE, "no reader")
    mv_advisor.advisor_statuses(unavailable, unavailable)["L"] = "COMPUTED"
    assert (
        mv_advisor.advisor_statuses(unavailable, unavailable)["L"]
        == config.MV_SIGNAL_UNAVAILABLE
    )


def test_a_reachable_endpoint_raises_coverage_to_one_half(monkeypatch) -> None:
    """With S computed but no signal reader, coverage is Y + S = 0.50.

    The second assertion is the one that matters: with L and D still UNAVAILABLE
    (no ``signal_reader`` injected), 0.50 clears the MEDIUM floor and stays under
    the HIGH one, so this degraded configuration cannot present a HIGH candidate
    no matter how strong the two available signals are. It is the ceiling a
    computed L lifts — see ``test_the_signal_reader_lifts_coverage_and_unlocks_high``
    for the wired case — and it should fail here if this degraded path moves.
    """
    _stages, _artifacts, _upserts = patch_writes(monkeypatch)

    class Client:
        def embed(self, texts):
            return [[1.0, 0.0, 0.0] for _ in texts]

    outcome = advise(
        monkeypatch,
        [iteration(recurring())],
        embedding_client=Client(),
        intent_texts=("revenue",),
        wide_schema_inventory=INVENTORY,
    )

    proposal = outcome.proposals[0]
    assert proposal.components.status_of("S") == config.MV_SIGNAL_COMPUTED
    assert proposal.components.evidence_coverage == 0.50
    assert proposal.tier != "HIGH"
    assert coverage_ceiling(0.50) == "MEDIUM"


# ── Wired producers (Prompt 6b) ──────────────────────────────────────────


def _footprint_rows(columns):
    """``column_lineage`` rows in the shape the L producer reads."""
    return [
        {"source_table_full_name": LINEITEM, "source_column_name": column}
        for column in columns
    ]


def _history_rows(statements, *, users=None, duration_ms=2000, start_time="2026-08-20T00:00:00Z"):
    """``query.history`` rows in the shape the D producer reads."""
    users = users or []
    return [
        {
            "statement_id": f"h{i}",
            "executed_by": users[i] if i < len(users) else f"u{i}",
            "start_time": start_time,
            "total_duration_ms": duration_ms,
            "statement_text": sql,
        }
        for i, sql in enumerate(statements)
    ]


def dispatching_reader(*, footprint, history):
    """A fake :data:`RunQuery` that answers by which system table the SQL names.

    The producers are injected with a reader, not mocked, so this drives *real*
    ``lineage_signal`` / ``demand_signal`` over fixture rows — proving the
    producer-to-consumer shape the recon says fixtures alone cannot.
    """
    def _run(sql):
        if "column_lineage" in sql:
            return list(footprint)
        if "query.history" in sql:
            return list(history)
        return []

    return _run


def test_the_signal_reader_lifts_coverage_and_unlocks_high(monkeypatch) -> None:
    """The integration assertion 6a owes: a real producer reaches the scorer.

    A dispatching reader feeds the L footprint and the D history through the
    genuine producers. With all four signals COMPUTED, ``evidence_coverage`` is
    1.0, the coverage cap no longer binds, and HIGH — unreachable in every
    degraded pin above — is the tier a strong candidate earns.
    """
    _stages, _artifacts, _upserts = patch_writes(monkeypatch)

    class Client:
        def embed(self, texts):
            return [[1.0, 0.0, 0.0] for _ in texts]

    reader = dispatching_reader(
        footprint=_footprint_rows(["l_extendedprice", "l_discount"]),
        history=_history_rows([REVENUE_SQL] * 12, users=[f"u{i}" for i in range(12)]),
    )

    outcome = advise(
        monkeypatch,
        [iteration(recurring(times=60))],
        embedding_client=Client(),
        intent_texts=("revenue",),
        wide_schema_inventory=INVENTORY,
        signal_reader=reader,
    )

    proposal = outcome.proposals[0]
    components = proposal.components
    # Real L and D reached the scorer in the shapes mv_scoring declares.
    assert components.status_of("L") == config.MV_SIGNAL_COMPUTED
    assert components.status_of("D") == config.MV_SIGNAL_COMPUTED
    assert components.status_of("S") == config.MV_SIGNAL_COMPUTED
    assert components.status_of("Y") == config.MV_SIGNAL_COMPUTED
    assert components.L == pytest.approx(1.0)
    assert components.D > 0.0
    # Coverage flip: every signal counted, nothing capped by coverage, HIGH earned.
    assert components.evidence_coverage == pytest.approx(1.0)
    assert coverage_ceiling(1.0) == "HIGH"
    assert proposal.tier_capped_by_coverage is False
    assert proposal.tier == proposal.uncapped_tier
    assert proposal.tier == "HIGH"
    # Statuses (and their reasons) ride in the evidence payload per producer.
    assert proposal.evidence["signal_status"]["L"]["status"] == config.MV_SIGNAL_COMPUTED
    assert proposal.evidence["signal_status"]["D"]["status"] == config.MV_SIGNAL_COMPUTED


def test_a_reader_that_always_raises_reproduces_the_no_reader_baseline(monkeypatch) -> None:
    """Producers-all-UNAVAILABLE must land identical to no reader at all (MV-D15).

    A workspace missing the grant raises on the read; one with no warehouse
    injects no reader. Both must degrade to the same coverage and statuses — the
    upgrade is additive or it is a regression. The one legible difference is the
    named reason: ``missing_grant`` rather than ``no_scope``.
    """
    patch_writes(monkeypatch)

    def boom(sql):
        raise RuntimeError("PERMISSION_DENIED: SELECT on system.access.column_lineage")

    with_raiser = advise(monkeypatch, [iteration(recurring())], signal_reader=boom)
    no_reader = advise(monkeypatch, [iteration(recurring())])

    raised = with_raiser.proposals[0].components
    absent = no_reader.proposals[0].components
    assert raised.evidence_coverage == absent.evidence_coverage == 0.30
    for key in ("L", "Y", "S", "D"):
        assert raised.status_of(key) == absent.status_of(key)
    assert raised.status_of("L") == config.MV_SIGNAL_UNAVAILABLE
    assert raised.status_of("D") == config.MV_SIGNAL_UNAVAILABLE
    # The difference the reason carries: a named cause, not a silent gap.
    assert "missing_grant" in with_raiser.proposals[0].evidence["signal_status"]["L"]["reason"]
    assert "no_scope" in no_reader.proposals[0].evidence["signal_status"]["L"]["reason"]


def test_demand_history_text_never_reaches_the_evidence(monkeypatch) -> None:
    """The D producer reads raw query history; no literal may reach a surface.

    ``statement_text`` enters only as fingerprint input, where canonicalization
    erases every literal (MV-D10(b)). A PII literal in the history a candidate
    matches must not surface in the proposal evidence or the recorded stage row.
    """
    stages, _artifacts, _upserts = patch_writes(monkeypatch)
    pii = "top-secret-user@example.com"
    history_sql = REVENUE_SQL + f" HAVING l_returnflag = '{pii}'"
    reader = dispatching_reader(
        footprint=_footprint_rows(["l_extendedprice", "l_discount"]),
        history=_history_rows([history_sql] * 3),
    )

    outcome = advise(monkeypatch, [iteration(recurring())], signal_reader=reader)

    proposal = outcome.proposals[0]
    assert proposal.components.status_of("D") == config.MV_SIGNAL_COMPUTED
    blob = json.dumps(proposal.evidence) + json.dumps(stages, default=str)
    assert pii not in blob


# ── The oracle (recon Q5) ────────────────────────────────────────────────


def test_the_oracle_is_wired_so_the_echo_check_actually_compares(monkeypatch) -> None:
    """Without this the firewall reports NOT_COMPARED on every production run.

    B4 made the vacuous case visible; this is what makes the non-vacuous case
    happen. The assertion is on ``echo_check`` rather than on a rejection, because
    a run whose comments legitimately echo nothing must still prove it looked.
    """
    _stages, _artifacts, _upserts = patch_writes(monkeypatch)
    outcome = advise(
        monkeypatch,
        [iteration(recurring())],
        wide_schema_inventory=INVENTORY,
    )

    assert outcome.echo_checks
    assert set(outcome.echo_checks) == {config.MV_ECHO_CHECK_COMPARED}
    assert outcome.proposals[0].evidence["comment_echo_check"] == config.MV_ECHO_CHECK_COMPARED


def test_an_empty_benchmark_list_still_builds_an_oracle(monkeypatch) -> None:
    """An empty corpus is a corpus. The check runs and finds nothing to reject,
    which is a different outcome from never having compared."""
    _stages, _artifacts, _upserts = patch_writes(monkeypatch)
    outcome = advise(
        monkeypatch,
        [iteration(recurring())],
        benchmarks=[],
        wide_schema_inventory=INVENTORY,
    )

    assert set(outcome.echo_checks) == {config.MV_ECHO_CHECK_COMPARED}


# ── Artifact and MV-D7 cross-reference ───────────────────────────────────


def test_the_ddl_artifact_carries_the_dedup_fingerprint_as_its_content_hash(
    monkeypatch,
) -> None:
    """MV-D7's cross-reference, which was false until ``content_hash`` was passable.

    A content hash could not serve here: Prompt 9 regenerates the YAML under probe
    capabilities the job does not have, so the text changes while the candidate
    does not. Keying on the fingerprint is what keeps the two stores joinable.
    """
    _stages, artifacts, _upserts = patch_writes(monkeypatch)
    outcome = advise(
        monkeypatch,
        [iteration(recurring())],
        wide_schema_inventory=INVENTORY,
    )

    assert artifacts, "no DDL artifact was written"
    written = artifacts[0]
    assert written["kind"] == "mv_candidate_ddl"
    assert written["content_hash"] == outcome.proposals[0].dedup_fingerprint
    assert written["payload"]["dedup_fingerprint"] == outcome.proposals[0].dedup_fingerprint


def test_the_artifact_kind_is_registered() -> None:
    """An unregistered kind only warns, so the write would succeed unnoticed."""
    from genie_space_optimizer.optimization.state import ARTIFACT_KINDS

    assert "mv_candidate_ddl" in ARTIFACT_KINDS


def test_the_ddl_comes_from_mv_yaml_not_from_this_module(monkeypatch) -> None:
    """The advisor never hand-builds a statement — sole-renderer, per Prompt 6."""
    _stages, artifacts, _upserts = patch_writes(monkeypatch)
    advise(
        monkeypatch,
        [iteration(recurring())],
        wide_schema_inventory=INVENTORY,
    )

    ddl = artifacts[0]["payload"]["ddl"]
    assert ddl.startswith("CREATE VIEW ")
    assert "WITH METRICS" in ddl
    assert "LANGUAGE YAML" in ddl


def test_the_rendered_body_carries_the_literal_not_a_placeholder(monkeypatch) -> None:
    """MV-D29 golden case: the POV's headline measure
    ``SUM(l_extendedprice * (1 - l_discount))`` — the exact expression Scenario
    D's BYO leg could not create — now renders an executable body. The
    structural constant ``1`` survives; the ``?n`` placeholder that made the
    canonical form unrenderable is gone."""
    _stages, artifacts, _upserts = patch_writes(monkeypatch)
    advise(
        monkeypatch,
        [iteration(recurring())],
        wide_schema_inventory=INVENTORY,
    )

    ddl = artifacts[0]["payload"]["ddl"]
    # Source columns are `source.`-qualified by the renderer; the structural
    # constant `1` is what MV-D29 had to preserve.
    assert "SUM(source.l_extendedprice * (1 - source.l_discount))" in ddl
    assert "?n" not in ddl
    assert "?s" not in ddl


def test_a_leaking_representative_drops_the_candidate(monkeypatch) -> None:
    """MV-D29's firewall gate. Because the render source now preserves literals,
    a representative carrying a benchmark/PII value could reach a shipped body —
    so it must clear the leakage oracle first, and a match DROPS the candidate
    rather than shipping it. Erasure-by-construction no longer stands in for the
    gate. Modelled with an oracle that flags every expression, so the single
    recurring measure is dropped and nothing is scored or persisted."""

    class LeakyOracle:
        def __init__(self, *a, **k) -> None:
            pass

        def contains_sql(self, sql, *, w=None) -> bool:
            return True

        def contains_question(self, *a, **k) -> bool:
            return False

    monkeypatch.setattr(mv_advisor, "LeakageOracle", LeakyOracle)
    _stages, _artifacts, upserts = patch_writes(monkeypatch)

    outcome = advise(monkeypatch, [iteration(recurring())])

    assert outcome.status == mv_advisor.STATUS_COMPLETE
    assert outcome.candidates_dropped_for_leakage >= 1
    assert outcome.candidates_scored == 0
    assert outcome.proposals_persisted == 0
    assert not upserts
    assert outcome.detail()["candidates_dropped_for_leakage"] >= 1


def test_join_strategy_and_its_evidence_are_persisted_per_candidate(monkeypatch) -> None:
    """``genie_opt_mv_candidates`` has no column for either, so they ride in evidence."""
    _stages, _artifacts, upserts = patch_writes(monkeypatch)
    outcome = advise(
        monkeypatch,
        [iteration(recurring())],
        wide_schema_inventory=INVENTORY,
    )

    evidence = outcome.proposals[0].evidence
    assert evidence["join_strategy"] == config.MV_JOIN_STRATEGY_DIRECT
    assert "join_strategy_reason" in evidence
    assert "join_strategy_evidence" in evidence
    assert upserts[0]["proposal"].evidence["join_strategy"] == config.MV_JOIN_STRATEGY_DIRECT


# ── Isolation ────────────────────────────────────────────────────────────


def test_an_advisor_exception_does_not_propagate(monkeypatch) -> None:
    """The isolation contract. A phase added to the optimize task must cost its
    own output and nothing else when it breaks."""
    stages, _artifacts, _upserts = patch_writes(monkeypatch)

    def explode(*a, **k):
        raise RuntimeError("corpus load blew up")

    monkeypatch.setattr(mv_advisor, "load_all_full_iterations", explode)

    outcome = run_mv_advisor_phase(
        FakeSpark(),
        run_id="r1",
        space_id=SPACE_ID,
        catalog="main",
        schema="gso",
        enabled=True,
    )

    assert outcome.status == mv_advisor.STATUS_FAILED
    assert "corpus load blew up" in (outcome.error or "")
    assert stages[0]["status"] == mv_advisor.STATUS_FAILED
    assert stages[0]["error_message"] == outcome.error


def test_a_failing_stage_write_still_does_not_propagate(monkeypatch) -> None:
    """Reporting the failure must not become a second way to fail the task."""
    monkeypatch.setattr(mv_advisor, "load_all_full_iterations", lambda *a, **k: [])

    def explode(*a, **k):
        raise RuntimeError("delta unavailable")

    monkeypatch.setattr(mv_advisor, "write_stage", explode)

    outcome = run_mv_advisor_phase(
        FakeSpark(), run_id="r1", space_id=SPACE_ID, catalog="main", schema="gso", enabled=True
    )

    assert outcome.status == mv_advisor.STATUS_SKIPPED


def test_a_failed_estate_scan_costs_the_reference_set_and_not_the_run(monkeypatch) -> None:
    """A DESCRIBE the SP cannot run is not evidence that the estate is empty.

    The real wrapper is exercised here rather than the stub, because the whole
    point is that its own try/except absorbs the failure and returns ``{}``.
    """
    _stages, _artifacts, _upserts = patch_writes(monkeypatch)

    def explode(*a, **k):
        raise RuntimeError("DESCRIBE denied")

    monkeypatch.setattr(
        "genie_space_optimizer.common.metric_view_catalog.detect_metric_views_via_catalog",
        explode,
    )

    outcome = advise(
        monkeypatch,
        [iteration(recurring())],
        stub_estate=False,
        wide_schema_inventory=INVENTORY,
    )

    assert outcome.status == mv_advisor.STATUS_COMPLETE
    assert outcome.proposals


def test_a_two_part_table_name_is_never_described() -> None:
    """No catalog to DESCRIBE against, and guessing one would point the scan at a
    different securable than the corpus referenced."""
    assert mv_advisor.estate_metric_view_yamls(FakeSpark(), ["tpch.lineitem"]) == {}
    assert mv_advisor.estate_metric_view_yamls(FakeSpark(), []) == {}


# ── Assembly details ─────────────────────────────────────────────────────

INVENTORY = {
    "assets": [
        {
            "asset_key": ["samples", "tpch", "lineitem"],
            "asset_type": "table",
            "columns": [
                {"name": "l_extendedprice", "data_type": "DECIMAL(18,2)",
                 "description": "Extended price"},
                {"name": "l_discount", "data_type": "DECIMAL(18,2)",
                 "description": "Discount rate"},
                {"name": "l_returnflag", "data_type": "STRING", "description": "Return flag"},
                {"name": "l_orderkey", "data_type": "BIGINT", "description": "Order key"},
            ],
        }
    ]
}


def test_column_facts_come_from_the_inventory_the_intake_task_already_built() -> None:
    """Reuse rather than a second round of catalog reads."""
    facts = column_facts_from_inventory(INVENTORY)

    assert set(facts) == {LINEITEM}
    assert {f.name for f in facts[LINEITEM]} == {
        "l_extendedprice", "l_discount", "l_returnflag", "l_orderkey"
    }
    assert facts[LINEITEM][0].comment == "Extended price"


def test_no_inventory_yields_no_facts_rather_than_raising() -> None:
    assert column_facts_from_inventory(None) == {}
    assert column_facts_from_inventory({}) == {}


def test_the_proposed_object_lands_beside_its_source_data() -> None:
    """Not in GSO's own schema: users of the space have no reason to be granted it."""
    measure = FingerprintRecurrence(
        fingerprint="f1",
        canonical_expr="SUM(l_extendedprice)",
        kind="measure",
        recurrence=5,
        provenance_ids=("b1",),
        provenance_count=1,
        source_columns=("l_extendedprice",),
        source_tables=(LINEITEM,),
    )

    candidate = candidate_from_measure(measure, space_id=SPACE_ID)

    assert candidate.proposed_object is not None
    assert candidate.proposed_object.startswith("samples.tpch.")


def test_a_two_part_table_name_yields_no_proposed_object() -> None:
    """Guessing a catalog would point the proposal at a different securable."""
    measure = FingerprintRecurrence(
        fingerprint="f1",
        canonical_expr="SUM(x)",
        kind="measure",
        recurrence=2,
        provenance_ids=("b1",),
        provenance_count=1,
        source_tables=("tpch.lineitem",),
    )

    assert candidate_from_measure(measure, space_id=SPACE_ID).proposed_object is None


def test_in_job_profiling_reports_no_capabilities(monkeypatch) -> None:
    """MV-D13 plus MV-D15: no probe in the job, so every capability is UNKNOWN and
    multi-hop candidates land on rung 3. Prompt 9 must regenerate, not replay."""
    measure = FingerprintRecurrence(
        fingerprint="f1",
        canonical_expr="SUM(l_extendedprice)",
        kind="measure",
        recurrence=5,
        provenance_ids=("b1",),
        provenance_count=1,
        source_columns=("l_extendedprice",),
        source_tables=(LINEITEM,),
    )
    candidate = candidate_from_measure(measure, space_id=SPACE_ID)

    profiling = profiling_for(
        candidate, table_columns=column_facts_from_inventory(INVENTORY)
    )

    assert profiling.capabilities == {}
    assert profiling.source_table == LINEITEM
    assert profiling.measures[0].expr == "SUM(l_extendedprice)"


def test_the_candidate_cut_is_bounded(monkeypatch) -> None:
    """The corpus scan's tail is long and its members cannot clear suppression."""
    _stages, _artifacts, _upserts = patch_writes(monkeypatch)
    rows = [
        eval_row(f"SELECT SUM(l_extendedprice + {i}) AS m FROM {LINEITEM}", f"b{i}")
        for i in range(6)
    ]

    outcome = advise(
        monkeypatch, [iteration(rows)], max_candidates=2, wide_schema_inventory=INVENTORY
    )

    assert outcome.candidates_scored <= 2
    assert outcome.measures_found >= outcome.candidates_scored


def test_the_stage_detail_carries_no_text_from_the_corpus(monkeypatch) -> None:
    """A stage row is operator-facing and is not a leakage exemption."""
    _stages, _artifacts, _upserts = patch_writes(monkeypatch)
    outcome = advise(
        monkeypatch,
        [iteration(recurring())],
        wide_schema_inventory=INVENTORY,
    )

    detail = outcome.detail()
    flat = repr(detail)
    assert "SELECT" not in flat
    assert "l_extendedprice" not in flat
    assert detail["phase"] == config.MV_ADVISOR_PHASE_NAME


def test_the_outcome_detail_is_json_serializable(monkeypatch) -> None:
    import json

    _stages, _artifacts, _upserts = patch_writes(monkeypatch)
    outcome = advise(
        monkeypatch,
        [iteration(recurring())],
        wide_schema_inventory=INVENTORY,
    )

    assert json.dumps(outcome.detail())


def test_a_skipped_outcome_always_names_a_reason() -> None:
    """A caller reading only the return value must be able to tell a clean skip
    from a swallowed exception."""
    assert AdvisorOutcome(status=mv_advisor.STATUS_SKIPPED, skip_reason="X").skip_reason == "X"
    assert AdvisorOutcome(status=mv_advisor.STATUS_COMPLETE).skip_reason is None


# ── Trusted assets reach the gate (POV Part 5 step 3) ────────────────────


CURATED_DIVERGENT_SQL = (
    f"SELECT SUM(l_extendedprice - l_discount) AS revenue FROM {LINEITEM}"
)


def _with_config(rows, config, *, champion: bool = True):
    """Attach an applied config to the iteration rows, as Delta stores it."""
    out = []
    for index, row in enumerate(rows):
        enriched = dict(row)
        enriched["config_json"] = json.dumps(config)
        enriched["is_champion"] = champion and index == len(rows) - 1
        out.append(enriched)
    return out


def test_a_proposal_contradicting_a_curated_answer_reaches_conflict(monkeypatch) -> None:
    """The wiring, not just the gate.

    ``score_candidate`` was called without ``instructions`` at all, so the whole
    conflict path was unreachable however well it worked in isolation. This drives
    the phase end to end: the corpus proposes discounted revenue, the space's own
    curated answer defines it differently, and the proposal must be flagged rather
    than shipped as a second answer.
    """
    _stages, _artifacts, upserts = patch_writes(monkeypatch)
    outcome = advise(
        monkeypatch,
        _with_config(
            [iteration(recurring())],
            {
                "instructions": {
                    "example_question_sqls": [
                        {"id": "eq_3", "question": "revenue?", "sql": CURATED_DIVERGENT_SQL}
                    ]
                }
            },
        ),
    )

    assert outcome.status == mv_advisor.STATUS_COMPLETE
    conflicted = [p for p in outcome.proposals if p.verdict == "CONFLICT"]
    assert conflicted, [p.verdict for p in outcome.proposals]
    entry = conflicted[0].conflicts[0]
    assert entry["authoritative"] == "trusted_asset:eq_3"
    # Persisted for adjudication (CONFLICT is a persistable verdict), never as a
    # suggestion.
    assert any(u["proposal"].verdict == "CONFLICT" for u in upserts)
    assert not conflicted[0].is_suggestion


def test_an_agreeing_curated_answer_leaves_the_proposal_alone(monkeypatch) -> None:
    patch_writes(monkeypatch)
    outcome = advise(
        monkeypatch,
        _with_config(
            [iteration(recurring())],
            {
                "instructions": {
                    "example_question_sqls": [{"id": "eq_3", "sql": REVENUE_SQL}]
                }
            },
        ),
    )

    assert [p.verdict for p in outcome.proposals] == ["PROPOSE"]


def test_a_space_with_no_curated_answers_is_unaffected(monkeypatch) -> None:
    """The common case must not change: no assets, no conflicts, no extra reads."""
    patch_writes(monkeypatch)
    outcome = advise(monkeypatch, [iteration(recurring())])

    assert [p.verdict for p in outcome.proposals] == ["PROPOSE"]


# ── The curated half of the corpus (Prompt 6c / MV-D17) ──────────────────


def test_trusted_asset_sql_reaches_the_corpus_as_a_curated_occurrence(
    monkeypatch,
) -> None:
    """POV Part 5 read it for conflicts; 6c also feeds it to the scan.

    The curated answer restates the measure the generated corpus already recurs,
    so it lands in the same bucket and raises the curated count the MV-D17
    up-weight reads — proof the trusted-asset SQL reached ``corpus_scan`` through
    the same reader the conflict surface uses, not a second one that could drift.
    """
    patch_writes(monkeypatch)
    outcome = advise(
        monkeypatch,
        _with_config(
            [iteration(recurring())],
            {"instructions": {"example_question_sqls": [{"id": "eq_3", "sql": REVENUE_SQL}]}},
        ),
    )

    assert outcome.proposals[0].evidence["ast_curated_provenance_count"] == 1


def test_curated_sql_snippets_reach_the_corpus(monkeypatch) -> None:
    """``sql_snippets.measures`` is the substitute for the bodyless
    ``sql_functions`` (MV-D17). Its inline SQL restates the recurring measure, so
    it merges into that bucket and raises the curated count."""
    _stages, _artifacts, _upserts = patch_writes(monkeypatch)
    outcome = advise(
        monkeypatch,
        _with_config(
            [iteration(recurring())],
            {
                "instructions": {
                    "sql_snippets": {
                        "measures": [
                            {"id": "m1", "sql": ["SUM(l_extendedprice * (1 - l_discount))"]}
                        ]
                    }
                }
            },
        ),
        wide_schema_inventory=INVENTORY,
    )

    assert outcome.status == mv_advisor.STATUS_COMPLETE
    assert outcome.proposals[0].evidence["ast_curated_provenance_count"] >= 1


def test_gso_applied_patches_reach_the_corpus_as_curated(monkeypatch) -> None:
    """A measure GSO itself patched in counts as curated, not generated (6c).

    ``load_patches`` is stubbed with one SQL-bearing patch whose fragment restates
    the recurring measure; it must reach the scan and raise the curated count.
    """
    patch_writes(monkeypatch)
    monkeypatch.setattr(
        mv_advisor,
        "load_patches",
        lambda *a, **k: [
            {
                "patch_type": "add_sql_snippet_measure",
                "iteration": 1,
                "lever": 6,
                "patch_index": 0,
                "patch_json": json.dumps({"sql": "SUM(l_extendedprice * (1 - l_discount))"}),
            }
        ],
    )
    outcome = advise(monkeypatch, [iteration(recurring())])

    assert outcome.proposals[0].evidence["ast_curated_provenance_count"] == 1


def test_a_non_sql_patch_type_is_not_harvested(monkeypatch) -> None:
    """Only SQL-bearing patch types are read; a description patch's text must not
    reach the scan (and its ``new_text`` prose would not parse anyway)."""
    patch_writes(monkeypatch)
    monkeypatch.setattr(
        mv_advisor,
        "load_patches",
        lambda *a, **k: [
            {
                "patch_type": "add_description",
                "iteration": 1,
                "lever": 1,
                "patch_index": 0,
                "patch_json": json.dumps({"new_text": "this table holds line items"}),
            }
        ],
    )
    outcome = advise(monkeypatch, [iteration(recurring())])

    assert outcome.proposals[0].evidence["ast_curated_provenance_count"] == 0


def test_a_bodyless_sql_function_is_skipped_not_harvested(monkeypatch) -> None:
    """``sql_functions`` was dropped from 6c (no body in ``serialized_space``).

    A bodyless entry — the shape the synthetic-data path appends — must be ignored
    rather than crash the loader, and must contribute nothing curated.
    """
    patch_writes(monkeypatch)
    outcome = advise(
        monkeypatch,
        _with_config(
            [iteration(recurring())],
            {"instructions": {"sql_functions": [{"id": "fn_1", "identifier": "cat.sch.fn"}]}},
        ),
    )

    assert outcome.status == mv_advisor.STATUS_COMPLETE
    assert outcome.proposals[0].evidence["ast_curated_provenance_count"] == 0


def test_a_governed_measure_is_excluded_from_the_seed_set(monkeypatch) -> None:
    """MV-D17 / blocker 4: governed metric-view measures are evidence, not seeds.

    Seeding one would only produce a candidate the dedup gate blocks with the very
    MV it came from, so the exclusion happens post-scan at the assembly site. Here
    the only recurring measure is already governed, so the run is a clean
    ``NO_CANDIDATES`` skip rather than a proposal that would be blocked.
    """
    _stages, _artifacts, upserts = patch_writes(monkeypatch)
    patch_estate(
        monkeypatch,
        {
            "samples.tpch.revenue_metrics": {
                "source": LINEITEM,
                "measures": [
                    {
                        "name": "discounted_revenue",
                        "expr": "SUM(l_extendedprice * (1 - l_discount))",
                    }
                ],
            }
        },
    )
    outcome = advise(monkeypatch, [iteration(recurring())], stub_estate=False)

    assert outcome.skip_reason == mv_advisor.SKIP_NO_CANDIDATES
    assert upserts == []


def test_the_applied_config_prefers_the_champion_row(monkeypatch) -> None:
    """The conflict surface compares against the config the run stands behind."""
    patch_iterations(
        monkeypatch,
        [
            {
                **iteration(recurring()),
                "config_json": json.dumps({"instructions": {"example_question_sqls": []}}),
                "is_champion": False,
            },
            {
                **iteration(recurring(), number=1),
                "config_json": json.dumps(
                    {
                        "instructions": {
                            "example_question_sqls": [
                                {"id": "eq_9", "sql": CURATED_DIVERGENT_SQL}
                            ]
                        }
                    }
                ),
                "is_champion": True,
            },
        ],
    )
    patch_estate(monkeypatch)
    patch_writes(monkeypatch)

    load = load_iteration_zero_corpus(
        FakeSpark(), run_id="r1", catalog="main", schema="gso",
    )

    assert load.applied_config is not None
    assert load.applied_config["instructions"]["example_question_sqls"][0]["id"] == "eq_9"


def test_the_applied_config_falls_back_to_the_last_iteration(monkeypatch) -> None:
    """A run that failed mid-loop never stamped a champion, and its curated
    answers are still the ones the space ships."""
    patch_iterations(
        monkeypatch,
        _with_config(
            [iteration(recurring()), iteration(recurring(), number=1)],
            {"instructions": {"example_question_sqls": [{"id": "eq_1", "sql": REVENUE_SQL}]}},
            champion=False,
        ),
    )

    load = load_iteration_zero_corpus(
        FakeSpark(), run_id="r1", catalog="main", schema="gso",
    )

    assert load.applied_config is not None
    assert load.applied_config["instructions"]["example_question_sqls"][0]["id"] == "eq_1"


def test_the_observed_config_wins_over_the_submitted_one(monkeypatch) -> None:
    """Matching how ``integration/revert.py`` resolves the same pair."""
    patch_iterations(
        monkeypatch,
        [
            {
                **iteration(recurring()),
                "config_json": json.dumps({"instructions": {"example_question_sqls": []}}),
                "observed_config_json": json.dumps(
                    {"instructions": {"example_question_sqls": [{"id": "eq_live", "sql": REVENUE_SQL}]}}
                ),
                "is_champion": True,
            }
        ],
    )

    load = load_iteration_zero_corpus(
        FakeSpark(), run_id="r1", catalog="main", schema="gso",
    )

    assert load.applied_config["instructions"]["example_question_sqls"][0]["id"] == "eq_live"


def test_an_unusable_applied_config_is_not_a_failure(monkeypatch) -> None:
    patch_iterations(
        monkeypatch,
        [{**iteration(recurring()), "config_json": "not json at all", "is_champion": True}],
    )

    load = load_iteration_zero_corpus(
        FakeSpark(), run_id="r1", catalog="main", schema="gso",
    )

    assert load.usable
    assert load.applied_config is None


# ── MV-D22: the artifact persists the raw rendered body ──────────────────


def test_ddl_artifact_persists_raw_yaml_text(monkeypatch) -> None:
    """MV-D22: the backend replays ``yaml_text``, so it must be its own field.

    Without this the backend would have to string-slice the body out of the
    wrapped ``ddl`` (coupling it to ``create_ddl``'s fence) or regenerate it
    (impossible — ``generate()``'s inputs are not persisted). The field is the
    seam that lets the create path re-wrap for the consented target.
    """
    from types import SimpleNamespace

    captured: list[dict] = []
    monkeypatch.setattr(
        mv_advisor, "write_artifact",
        lambda spark, run_id, kind, payload, **kw: captured.append(payload) or "art_1",
    )
    monkeypatch.setattr(
        mv_advisor, "validate",
        lambda text: SimpleNamespace(
            ok=True, errors=(), warnings=(), downgrade_to=None, echo_check="NOT_COMPARED",
        ),
    )

    proposal = SimpleNamespace(
        proposed_object="sales.core.revenue_metrics",
        suggestion_id="sug_abc123",
        dedup_fingerprint="fp_abc123",
        target_space_id=SPACE_ID,
    )
    rendered = SimpleNamespace(
        yaml_text="version: 0.1\nsource: sales.core.orders\n",
        join_strategy="subquery_source",
    )

    mv_advisor._write_ddl_artifact(
        FakeSpark(), proposal, rendered, run_id="r1", catalog="main", schema="gso",
    )

    assert len(captured) == 1
    payload = captured[0]
    assert payload["yaml_text"] == rendered.yaml_text
    # The wrapped DDL still ships too; yaml_text is additive, not a replacement.
    assert "ddl" in payload
    assert rendered.yaml_text in payload["ddl"]


# ── View-grained consolidation + fan-out suppression (MV-D30, Prompt 15.3) ──

ORDERS_SQL = (
    "SELECT SUM(o_totalprice) AS total_order_value, o_orderpriority "
    "FROM samples.tpch.orders GROUP BY o_orderpriority"
)
AVG_QTY_SQL = (
    "SELECT AVG(l_quantity) AS avg_quantity, l_returnflag "
    f"FROM {LINEITEM} GROUP BY l_returnflag"
)


def _rows(*specs):
    """Flatten ``(sql, prefix, times)`` specs into distinct-id eval rows."""
    rows = []
    for sql, prefix, times in specs:
        rows.extend(eval_row(sql, f"{prefix}_{i}") for i in range(times))
    return rows


def test_bundle_grain_resolves_a_three_part_source() -> None:
    obj, concept = mv_advisor._bundle_grain(["cat.sch.orders"])
    assert obj == "cat.sch.orders_metrics"
    assert concept == "orders_metrics"


def test_bundle_grain_without_a_catalog_is_dropped_not_rendered_blank() -> None:
    """MV-D30 validate-don't-render: a two-part source has no catalog to place
    the view in, so the bundle has no resolvable object and ``_build_bundle``
    drops it rather than surfacing a blank-named card."""
    obj, _concept = mv_advisor._bundle_grain(["sales.orders"])
    assert obj is None


def test_measures_on_one_source_set_consolidate_into_one_bundle(monkeypatch) -> None:
    """MV-D30: two recurring measures over one table become ONE view proposal."""
    _stages, _artifacts, upserts = patch_writes(monkeypatch)
    rows = _rows((REVENUE_SQL, "rev", 8), (COUNT_SQL, "cnt", 8))
    outcome = advise(monkeypatch, [iteration(rows)])

    assert outcome.status == mv_advisor.STATUS_COMPLETE
    assert len(outcome.proposals) == 1
    bundle = outcome.proposals[0]
    assert bundle.evidence["bundle"] is True
    assert bundle.evidence["measure_count"] == 2
    assert len(bundle.evidence["measures"]) == 2
    assert bundle.proposed_object.endswith("lineitem_metrics")
    assert len(upserts) == 1
    # Each member carries its own per-measure fingerprint — the grain suppression
    # cross-references — and the view key is not any member's key.
    member_fps = {m["dedup_fingerprint"] for m in bundle.evidence["measures"]}
    assert len(member_fps) == 2
    assert bundle.dedup_fingerprint not in member_fps


def test_measures_on_different_source_sets_are_separate_bundles(monkeypatch) -> None:
    _stages, _artifacts, upserts = patch_writes(monkeypatch)
    rows = _rows((REVENUE_SQL, "rev", 8), (ORDERS_SQL, "ord", 8))
    outcome = advise(monkeypatch, [iteration(rows)])

    assert len(outcome.proposals) == 2
    objects = sorted(p.proposed_object for p in outcome.proposals)
    assert any(o.endswith("lineitem_metrics") for o in objects)
    assert any(o.endswith("orders_metrics") for o in objects)
    assert len(upserts) == 2


def test_a_suppressed_member_is_dropped_before_bundling(monkeypatch) -> None:
    """A rejected measure never enters a bundle, and dropping it changes the
    view key (membership drives the MV-D30 bundle fingerprint)."""
    patch_writes(monkeypatch)
    rows = _rows((REVENUE_SQL, "rev", 8), (COUNT_SQL, "cnt", 8))
    baseline = advise(monkeypatch, [iteration(rows)])
    members = baseline.proposals[0].evidence["measures"]
    victim = members[0]["dedup_fingerprint"]

    monkeypatch.setattr(
        mv_advisor, "load_mv_suppressed_fingerprints", lambda *a, **k: {victim}
    )
    outcome = advise(monkeypatch, [iteration(rows)])

    assert len(outcome.proposals) == 1
    remaining = {m["dedup_fingerprint"] for m in outcome.proposals[0].evidence["measures"]}
    assert victim not in remaining
    assert outcome.proposals[0].dedup_fingerprint != baseline.proposals[0].dedup_fingerprint
    assert outcome.candidates_dropped_suppressed == 1


def test_rejected_members_never_return_when_a_new_measure_joins_the_grain(
    monkeypatch,
) -> None:
    """The load-bearing MV-D30 invariant: reject a bundle, then a later scan finds
    a new recurring measure on the same grain — the resurfaced bundle contains
    only the unsuppressed member, never the rejected ones back (the view-grained
    reincarnation of test_re_proposing_does_not_resurrect_a_rejected_candidate)."""
    patch_writes(monkeypatch)
    rows1 = _rows((REVENUE_SQL, "rev", 8), (COUNT_SQL, "cnt", 8))
    first = advise(monkeypatch, [iteration(rows1)])
    rejected = {m["dedup_fingerprint"] for m in first.proposals[0].evidence["measures"]}
    assert len(rejected) == 2

    # The reject fan-out suppressed both members; the next scan adds a 3rd measure.
    monkeypatch.setattr(
        mv_advisor, "load_mv_suppressed_fingerprints", lambda *a, **k: set(rejected)
    )
    rows2 = rows1 + _rows((AVG_QTY_SQL, "avg", 8))
    second = advise(monkeypatch, [iteration(rows2)])

    assert len(second.proposals) == 1
    survivors = {m["dedup_fingerprint"] for m in second.proposals[0].evidence["measures"]}
    assert survivors.isdisjoint(rejected)
    assert len(survivors) == 1


def test_when_every_member_is_suppressed_no_bundle_surfaces(monkeypatch) -> None:
    patch_writes(monkeypatch)
    rows = _rows((REVENUE_SQL, "rev", 8), (COUNT_SQL, "cnt", 8))
    first = advise(monkeypatch, [iteration(rows)])
    all_fps = {m["dedup_fingerprint"] for m in first.proposals[0].evidence["measures"]}

    monkeypatch.setattr(
        mv_advisor, "load_mv_suppressed_fingerprints", lambda *a, **k: set(all_fps)
    )
    outcome = advise(monkeypatch, [iteration(rows)])

    assert outcome.proposals == ()
    assert outcome.candidates_dropped_suppressed == 2


def test_the_in_job_advisor_injects_the_suppression_reader(monkeypatch) -> None:
    """MV-D30: the Spark (in-job) caller MUST pass a decisions-reader, or the job
    would re-propose what the IQ Scan user rejected and the two surfaces would
    disagree about what 'rejected' means."""
    captured: dict = {}

    def fake_advise(**kwargs):
        captured.update(kwargs)
        return AdvisorOutcome(status=mv_advisor.STATUS_COMPLETE)

    monkeypatch.setattr(mv_advisor, "advise_from_corpus", fake_advise)
    patch_writes(monkeypatch)
    advise(monkeypatch, [iteration(recurring())])

    assert callable(captured.get("read_suppressed_fingerprints"))


# ── Prompt 15.5: servable-body invariant (Tier-2 Scenario A) ─────────────

RATIO_SQL = (
    "SELECT SUM(CASE WHEN o_orderstatus = 'F' THEN o_totalprice ELSE 0 END) "
    "/ SUM(o_totalprice) AS finished_share, o_orderpriority "
    "FROM samples.tpch.orders GROUP BY o_orderpriority"
)


def _advise_from_corpus_directly(*, persist_proposal, write_ddl_artifact, **overrides):
    """Drive ``advise_from_corpus`` the way the backend suggest route does —
    SparkSession-free, with injected persistence — over a recurring lineitem
    corpus that bundles into one view."""
    corpus = [(REVENUE_SQL, f"b{i}") for i in range(8)] + [
        (COUNT_SQL, f"c{i}") for i in range(8)
    ]
    kwargs = dict(
        space_id=SPACE_ID,
        run_id="run_backend",
        corpus_entries=corpus,
        applied_config=None,
        benchmarks=(),
        wide_schema_inventory=None,
        metric_view_reader=lambda tables: [],
        embedding_client=None,
        signal_reader=None,
        intent_texts=(),
        domain="",
        max_candidates=None,
        persist_proposal=persist_proposal,
        write_ddl_artifact=write_ddl_artifact,
        read_suppressed_fingerprints=None,
    )
    kwargs.update(overrides)
    return mv_advisor.advise_from_corpus(**kwargs)


def assert_every_surfaced_proposal_is_servable(outcome, *, has_body) -> None:
    """OUTCOME INVARIANT (Prompt 15.5 §4b), for ANY persistence writer.

    Walks every proposal a scenario surfaces and asserts the FULL serving
    contract — a resolvable DDL body (``has_body(proposal)``, which the caller
    binds to its writer's evidence: the run artifact for the in-job writer, the
    ``rendered.ok`` ``yaml_text`` for the backend writer), a ``proposed_object``,
    a tier, and the evidence the justification is assembled from — and that no
    render-failed suggestion appears at all. The guard lives in the shared
    ``advise_from_corpus`` body, so both writer suites call this same helper;
    a third writer added later inherits the pin by construction. A proposal that
    persists but cannot be served fails offline from here on.
    """
    failed_ids = {sid for sid, _verdict in outcome.render_failures}
    surfaced = {p.suggestion_id for p in outcome.proposals}
    assert failed_ids.isdisjoint(surfaced), "a render-failed suggestion surfaced"
    assert outcome.candidates_render_failed == len(failed_ids)
    for proposal in outcome.proposals:
        assert has_body(proposal), f"{proposal.suggestion_id} surfaced with no servable body"
        assert proposal.proposed_object, "surfaced proposal has no proposed_object"
        assert proposal.tier is not None, "surfaced proposal has no tier"
        assert proposal.evidence, "surfaced proposal carries no justification evidence"


def test_a_recurring_ratio_shape_renders_a_servable_body(monkeypatch) -> None:
    """Scenario A, named and pinned. A recurring RATIO shape stored its numerator
    and denominator in canonical (``?n``/``?s``) form, so the metric-view body
    rendered a placeholder, MV-D29 validation rejected it, and the bundle
    persisted with no servable body — /mv-ddl then 404'd. With
    ``ShapeMatch.render_components`` the shape renders literal-preserving, so the
    bundle carries an artifact again (``artifacts_written`` was 0 pre-fix)."""
    _stages, artifacts, upserts = patch_writes(monkeypatch)
    outcome = advise(monkeypatch, [iteration(_rows((RATIO_SQL, "ratio", 8)))])

    assert outcome.status == mv_advisor.STATUS_COMPLETE
    assert outcome.candidates_render_failed == 0
    assert outcome.proposals_persisted >= 1
    assert outcome.artifacts_written >= 1
    assert artifacts, "the ratio bundle rendered no servable DDL body"
    assert upserts, "the ratio bundle was not persisted"
    served = {a["content_hash"] for a in artifacts}
    assert_every_surfaced_proposal_is_servable(
        outcome, has_body=lambda p: p.dedup_fingerprint in served
    )


def test_a_bundle_that_fails_to_render_never_persists_and_rides_the_outcome(
    monkeypatch,
) -> None:
    """The structural guard, cause-independent: force the generator to fail on a
    bundle and it is DROPPED, not persisted as a bodyless card — the drop is
    loud (a count + an ``(id, verdict)`` pair on the outcome), never silent."""
    from genie_space_optimizer.optimization.mv_scoring import VERDICT_CONFLICT
    from genie_space_optimizer.optimization.mv_yaml import GeneratedMetricView

    _stages, artifacts, upserts = patch_writes(monkeypatch)
    monkeypatch.setattr(
        mv_advisor, "generate",
        lambda *a, **k: GeneratedMetricView(
            verdict=VERDICT_CONFLICT, rejections=("forced render failure",)
        ),
    )
    outcome = advise(
        monkeypatch, [iteration(_rows((REVENUE_SQL, "rev", 8), (COUNT_SQL, "cnt", 8)))]
    )

    assert outcome.proposals == ()
    assert outcome.proposals_persisted == 0
    assert outcome.artifacts_written == 0
    assert upserts == []
    assert artifacts == []
    assert outcome.candidates_render_failed == 1
    assert len(outcome.render_failures) == 1
    _sid, verdict = outcome.render_failures[0]
    assert verdict == VERDICT_CONFLICT
    # No surfaced proposals to serve, and the render-failed id is disjoint.
    assert_every_surfaced_proposal_is_servable(outcome, has_body=lambda p: False)


def test_the_backend_writer_also_never_persists_a_bodyless_bundle(monkeypatch) -> None:
    """The invariant on the SECOND writer. Driven directly (as the suggest route
    drives it), a render-failed bundle is never handed to ``persist_proposal`` or
    ``write_ddl_artifact`` — the backend warehouse writer inherits the guard from
    the shared body, so a re-scan cannot leave a bodyless candidate row."""
    from genie_space_optimizer.optimization.mv_scoring import VERDICT_CONFLICT
    from genie_space_optimizer.optimization.mv_yaml import GeneratedMetricView

    monkeypatch.setattr(
        mv_advisor, "generate",
        lambda *a, **k: GeneratedMetricView(
            verdict=VERDICT_CONFLICT, rejections=("forced render failure",)
        ),
    )
    persisted: list = []
    artifacts: list = []
    outcome = _advise_from_corpus_directly(
        persist_proposal=lambda proposal, rendered: persisted.append(proposal) or True,
        write_ddl_artifact=lambda proposal, rendered: artifacts.append(proposal) or True,
    )

    assert persisted == []
    assert artifacts == []
    assert outcome.candidates_render_failed >= 1
    assert_every_surfaced_proposal_is_servable(outcome, has_body=lambda p: False)


def test_the_backend_writer_serves_every_surfaced_bundle(monkeypatch) -> None:
    """The backend warehouse writer's positive serving contract. Driven directly
    with the real generator, every surfaced bundle carries a ``rendered.ok``
    body the candidate row would persist as ``yaml_text`` — the same shared
    invariant the in-job writer satisfies, so a re-scan hydrates a servable
    card."""
    persisted: list = []

    def _persist(proposal, rendered) -> bool:
        # Mirrors mv_suggest._persist: the body rides the row only when ok.
        persisted.append((proposal, bool(getattr(rendered, "ok", False) and rendered.yaml_text)))
        return True

    outcome = _advise_from_corpus_directly(
        persist_proposal=_persist,
        write_ddl_artifact=lambda proposal, rendered: False,  # MV-D23: no artifact
    )

    assert outcome.proposals, "the backend writer surfaced no bundle to serve"
    served = {p.dedup_fingerprint for p, has_yaml in persisted if has_yaml}
    assert_every_surfaced_proposal_is_servable(
        outcome, has_body=lambda p: p.dedup_fingerprint in served
    )


def test_a_shape_whose_render_form_matches_the_benchmark_is_dropped(monkeypatch) -> None:
    """MV-D29 shape firewall. A shape's components now render literal-preserving,
    so a predicate/arithmetic literal could reach a shipped body just like a
    primary measure — and the generator's only leakage check is the BEST FOR
    comment echo, which never inspects the body. The caller gates the shapes
    through the SAME oracle: a shape with a leaking render fragment is dropped,
    the clean base measure on another grain still surfaces."""

    class _CaseFlaggingOracle:
        """A stand-in oracle that flags any SQL carrying a CASE predicate — the
        RATIO numerator here — and nothing else, so the arithmetic revenue
        measure on the other table passes untouched."""

        def __init__(self, *a, **k) -> None:
            pass

        def contains_sql(self, sql, *, w=None) -> bool:
            return "case when" in (sql or "").lower()

    monkeypatch.setattr(mv_advisor, "LeakageOracle", _CaseFlaggingOracle)
    _stages, _artifacts, _upserts = patch_writes(monkeypatch)
    outcome = advise(
        monkeypatch, [iteration(_rows((REVENUE_SQL, "rev", 8), (RATIO_SQL, "ratio", 8)))]
    )

    # The CASE-bearing ratio shape (and the CASE primary measure) were dropped;
    # the clean arithmetic measures — lineitem revenue and the plain
    # SUM(o_totalprice) denominator — still surface.
    assert outcome.candidates_dropped_for_leakage >= 1
    assert any(p.proposed_object.endswith("lineitem_metrics") for p in outcome.proposals)
    for proposal in outcome.proposals:
        for member in proposal.evidence.get("measures", []):
            assert "case when" not in (member.get("expr") or "").lower()
