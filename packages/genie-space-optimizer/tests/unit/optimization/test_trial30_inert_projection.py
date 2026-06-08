"""Trial 30 W30.3 — pure projection of kit_forced_inert_reroute SM states
into (a) typed ``Trial29InertPatchDiagnostic`` records for JSONL bundle
persistence and (b) ``genie_eval_lever_loop_decisions`` rows.

These pin the W30.3 evidence-bundle-completeness contract: every inert
reroute must yield exactly one diagnostic and one decision row, and the
projection must be byte-stable (empty) when no reroute happened.
"""
from genie_space_optimizer.optimization.inert_patch_diagnostic import (
    Trial29InertPatchDiagnostic,
)
from genie_space_optimizer.optimization.trial30_inert_projection import (
    InertDecisionRow,
    build_inert_decision_rows,
    build_inert_patch_diagnostics,
)
from genie_space_optimizer.optimization.state_machine.records import (
    AcceptanceDecisionRecord,
)


def _reroute_record(rejected: str, *, rca: str) -> AcceptanceDecisionRecord:
    return AcceptanceDecisionRecord(
        decision="kit_forced_inert_reroute",
        arbiter_reason=f"kit_forced_inert_reroute:rca={rca}:behavior=unchanged",
        target_fixed=False,
        collateral_regressions=(),
        behavioral_diff="unchanged",
        insufficient_repair_signature=(
            f"{rejected}:add_example_sql:kit_forced_inert:"
            f"rca={rca}:behavior=unchanged"
        ),
        rejected_mechanism=rejected,
    )


class _Diag:
    def __init__(self, label: str) -> None:
        self.rca_kind_label = label


class _Eval:
    def __init__(self, pre: float, post: float) -> None:
        self.pre_apply_score = pre
        self.post_apply_score = post


class _Applied:
    def __init__(self) -> None:
        self.applied_intent_ids = ("intent-1",)
        self.proposal_attempt_index = 0


class _Proposal:
    def __init__(self, patch_type: str) -> None:
        self.patch_type = patch_type


class _State:
    """Duck-typed QuestionStateInIteration mock (same convention as the
    existing harvest-wire test)."""

    def __init__(
        self,
        *,
        qid: str,
        rca_label: str,
        accepted: AcceptanceDecisionRecord | None,
        iteration: int = 2,
        pre: float = 0.0,
        post: float = 0.0,
        patch_type: str = "instructions_text",
    ) -> None:
        self.qid = qid
        self.iteration = iteration
        self.diagnosed = _Diag(rca_label)
        self.accepted = accepted
        self.evaluated = _Eval(pre, post)
        self.applied = _Applied()
        self.proposals = (_Proposal(patch_type),)


def test_diagnostic_built_for_reroute_state():
    st = _State(
        qid="gs_026",
        rca_label="top-n cardinality collapse",
        accepted=_reroute_record("lever-5", rca="top_n_cardinality_collapse"),
        iteration=3,
        pre=0.0,
        post=0.0,
    )
    diags = build_inert_patch_diagnostics([st])
    assert len(diags) == 1
    d = diags[0]
    assert isinstance(d, Trial29InertPatchDiagnostic)
    assert d.qid == "gs_026"
    # RCA canonicalized to the RCA_CANONICAL_KEY_SET form.
    assert d.rca_kind == "top_n_cardinality_collapse"
    assert d.rejected_mechanism == "lever-5"
    assert d.behavioral_diff == "unchanged"
    assert d.iteration == 3
    assert d.trial == "trial30"
    assert d.signature.startswith("lever-5:")
    # patch_json carries a light forensic descriptor (not the full body).
    assert d.patch_json.get("patch_type") == "instructions_text"


def test_non_reroute_states_contribute_nothing():
    accepted = AcceptanceDecisionRecord(
        decision="accepted",
        arbiter_reason="",
        target_fixed=True,
        collateral_regressions=(),
    )
    st = _State(qid="gs_001", rca_label="wrong column", accepted=accepted)
    none_st = _State(qid="gs_002", rca_label="wrong column", accepted=None)
    assert build_inert_patch_diagnostics([st, none_st]) == ()
    assert build_inert_decision_rows(
        [st, none_st], run_id="run-x", iteration=1
    ) == ()


def test_decision_row_projects_to_table_contract():
    st = _State(
        qid="gs_013",
        rca_label="wrong column",
        accepted=_reroute_record("lever-5", rca="wrong_column"),
        iteration=2,
        pre=0.5,
        post=0.5,
    )
    rows = build_inert_decision_rows([st], run_id="run-7now", iteration=2)
    assert len(rows) == 1
    row = rows[0]
    assert isinstance(row, InertDecisionRow)
    assert row.qid == "gs_013"
    assert row.rca_kind == "wrong_column"
    assert row.rejected_mechanism == "lever-5"

    # to_decision_row() must satisfy write_lever_loop_decisions's contract:
    # non-empty run_id, gate_name, decision.
    d = row.to_decision_row()
    assert d["run_id"] == "run-7now"
    assert d["gate_name"]
    assert d["decision"] == "kit_forced_inert_reroute"
    assert d["affected_qids"] == ["gs_013"]
    assert d["metrics"]["rejected_mechanism"] == "lever-5"
    assert d["metrics"]["rca_kind"] == "wrong_column"


def test_generalizes_across_qids_and_rcas():
    # Two different (qid, rca) families in one batch — proves no per-QID
    # or per-anchor branch (Architectural Principle #2). Neither qid is an
    # anchor space-id; both are generic question ids.
    states = [
        _State(
            qid="gs_009",
            rca_label="top-n cardinality collapse",
            accepted=_reroute_record(
                "lever-5", rca="top_n_cardinality_collapse"
            ),
        ),
        _State(
            qid="gs_777",
            rca_label="wrong column",
            accepted=_reroute_record("lever-1", rca="wrong_column"),
        ),
    ]
    diags = build_inert_patch_diagnostics(states)
    rows = build_inert_decision_rows(states, run_id="run-air", iteration=4)
    assert {d.qid for d in diags} == {"gs_009", "gs_777"}
    assert {d.rca_kind for d in diags} == {
        "top_n_cardinality_collapse",
        "wrong_column",
    }
    assert {r.rejected_mechanism for r in rows} == {"lever-5", "lever-1"}
