"""Trial 22 postmortem-replay regression suite — the new merge gate.

Replays the two production lever_loop postmortems through the Trial 22
Evidence Actuator and asserts eight bright-line conditions from the
Trial 22 plan:

1. Bundle survival (d139, SC-1)
2. Bundle cohesion cascade (d139, SC-1)
3. Typed drop round-trip into harness terminal-reason verdict (SC-2)
4. Closed-vocab terminal reason (no colon suffix; structured fields) (SC-2)
5. H001 positive control: e943 stays under cap, no split fires (SC-3 guard)
6. RCA-subcluster slice for d139 105k payload (SC-3) — PR-2 (xfail in PR-1)
7. Lineage orphan stamping (e943, SC-3) — PR-2 (xfail in PR-1)
8. Retry feedback durability on iteration terminal-state ledger (SC-2) — PR-2 (xfail in PR-1)

Tests 1-5 are the PR-1 merge gate and MUST go GREEN as PR-1 lands W1+W2+W4.
Tests 6-8 carry pytest.mark.xfail until PR-2 lands W3, W5, W7. The xfail
markers come off in PR-2.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

FIXTURE_DIR = Path(__file__).parent / "fixtures"
RUN_D139_FIXTURE = FIXTURE_DIR / "run_d139_322426313992436.json"
RUN_E943_FIXTURE = FIXTURE_DIR / "run_e943_231749822620014.json"


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


@pytest.fixture(scope="module")
def run_d139() -> dict[str, Any]:
    return _load(RUN_D139_FIXTURE)


@pytest.fixture(scope="module")
def run_e943() -> dict[str, Any]:
    return _load(RUN_E943_FIXTURE)


@pytest.fixture(autouse=True)
def _trial22_flags_on(monkeypatch):
    monkeypatch.setenv("GSO_TRIAL21_ACTUATOR", "1")
    monkeypatch.setenv("GSO_TRIAL22_SLATE_REPAIR", "1")
    monkeypatch.setenv("GSO_TRIAL22_BUNDLE_GROUP_CHECK", "1")
    monkeypatch.setenv("GSO_TRIAL22_BUNDLE_COHESION_SWEEP", "1")
    monkeypatch.setenv("GSO_TRIAL22_TERMINAL_REASON_HELPER", "1")
    yield


def _make_proposal_from_fixture(p: dict[str, Any]) -> Any:
    """Build a RepairProposal in the wire-shape Stage 3 emits."""
    from genie_space_optimizer.optimization.repair_intent import (
        PatchType,
        RepairShape,
    )
    from genie_space_optimizer.optimization.repair_proposal_typed import (
        RepairProposal,
    )

    return RepairProposal(
        intent_id=str(p["intent_id"]),
        intent_name=str(p.get("intent_name") or p["intent_id"]),
        intent_description="",
        repair_shape=RepairShape.OTHER,
        patch_type=PatchType(p["patch_type"]),
        rationale=str(p.get("rationale", "r")),
        confidence="medium",
        patch_body=dict(p.get("patch_body") or {}),
        blame_set=tuple(p.get("target_qids") or ()),
        target_qids=tuple(p.get("target_qids") or ()),
        selected_lever=str(p.get("selected_lever") or ""),
        selected_levers=tuple(p.get("selected_levers") or ()),
        bundle_id=str(p.get("bundle_id") or ""),
    )


# ---------------------------------------------------------------------
# Bright-line #1 — Bundle survival (d139, SC-1, PR-1 GATE)
# ---------------------------------------------------------------------


def test_w2_d139_bundle_survives_when_group_carries_two_distinct_levers(
    run_d139,
) -> None:
    """W2 bright-line #1 — compile_slate admits both members of a
    production-shape bundle where the lever union across siblings has
    cardinality >= 2.

    This is the headline contract Trial 21 violated: production emits
    bundles as N sibling proposals sharing bundle_id with one lever
    each. The group invariant must be evaluated on the union of
    selected_levers across members, NOT per proposal.
    """
    from genie_space_optimizer.optimization.proposal_slate_compiler import (
        SlateCompilerContext,
        compile_slate,
    )

    f = run_d139["h001_bundle_fixture"]
    proposals = [_make_proposal_from_fixture(p) for p in f["proposals"]]
    ctx = SlateCompilerContext(
        cluster_id=f["cluster_id"],
        iteration=int(f["iteration"]),
    )
    result = compile_slate(proposals, ctx)

    surviving_intents = {p.intent_id for p in result.surviving_proposals}
    expected_intents = {str(p["intent_id"]) for p in f["proposals"]}
    assert surviving_intents == expected_intents, (
        "W2 bright-line #1: H001 bundle with lever-1 + lever-5 across "
        "2 siblings must have BOTH members survive Phase 2. The "
        "group-aware invariant evaluates the union across the bundle, "
        f"not each proposal in isolation. surviving={surviving_intents} "
        f"expected={expected_intents} dropped={result.dropped_proposals}"
    )


# ---------------------------------------------------------------------
# Bright-line #2 — Bundle cohesion cascade (d139, SC-1, PR-1 GATE)
# ---------------------------------------------------------------------


def test_w2_d139_bundle_cohesion_cascade_drops_sibling_with_typed_reason(
    run_d139,
) -> None:
    """W2 bright-line #2 — when Phase 1 drops one sibling for
    MISSING_IMPLICATED_ASSETS, Phase 1.5 cascades the OTHER sibling
    with DropReason.BUNDLE_MEMBER_DROPPED_CASCADE (NOT
    BUNDLE_INVARIANT_VIOLATED).

    This is the W6 x W2 cascade interaction the design review caught:
    a naive Phase 1 -> Phase 2 pipeline would re-create the Trial 21
    failure mode because the surviving lever-5 sibling would be a
    one-member bundle.
    """
    from genie_space_optimizer.optimization.proposal_slate_compiler import (
        DropReason,
        SlateCompilerContext,
        compile_slate,
    )

    f = run_d139["h001_bundle_cohesion_cascade_fixture"]
    proposals = [_make_proposal_from_fixture(p) for p in f["proposals"]]
    # Simulate W6 wiring: the lever-1 add_column_description member has
    # NO implicated_assets from Stage 1 diagnosis. The lever-5
    # add_example_sql member is otherwise valid.
    assets_by_id: dict[str, list[str]] = {}
    just_by_id: dict[str, str] = {}
    for p in f["proposals"]:
        intent_id = str(p["intent_id"])
        assets_by_id[intent_id] = list(
            p.get("implicated_assets_from_diagnosis") or []
        )
        # Provide non-empty justification for the add_example_sql sibling
        # so it survives Phase 1's _check_required_assets.
        if p["patch_type"] != "add_column_description":
            just_by_id[intent_id] = "exemplar paired with column doc"
        else:
            just_by_id[intent_id] = ""

    ctx = SlateCompilerContext(
        implicated_assets_by_proposal_id=assets_by_id,
        justification_by_proposal_id=just_by_id,
    )
    result = compile_slate(proposals, ctx)

    dropped_by_intent = {
        p.intent_id: r for p, r in result.dropped_proposals
    }

    originating = "h001_cohesion_member_lever1_assets_missing"
    cascaded = "h001_cohesion_member_lever5_otherwise_valid"

    assert dropped_by_intent.get(originating) == (
        DropReason.MISSING_IMPLICATED_ASSETS
    ), (
        "W2 bright-line #2 setup: the lever-1 sibling with empty "
        "implicated_assets must drop in Phase 1 with "
        f"MISSING_IMPLICATED_ASSETS; got {dropped_by_intent}"
    )

    assert dropped_by_intent.get(cascaded) == (
        DropReason.BUNDLE_MEMBER_DROPPED_CASCADE
    ), (
        "W2 bright-line #2: the otherwise-valid lever-5 sibling must "
        "be cascaded by Phase 1.5 with BUNDLE_MEMBER_DROPPED_CASCADE, "
        f"NOT BUNDLE_INVARIANT_VIOLATED; got {dropped_by_intent}"
    )

    # The cascade feedback must name the originating sibling and reason
    cascaded_feedback = [
        fb
        for fb in result.typed_feedback_for_retry
        if fb.proposal_id == cascaded
    ]
    assert cascaded_feedback, (
        "W2 bright-line #2: cascaded sibling must have typed feedback"
    )
    text = cascaded_feedback[0].feedback_text
    assert originating in text, (
        "W2 bright-line #2: cascade feedback must name the originating "
        f"sibling; got {text!r}"
    )
    assert "missing_implicated_assets" in text, (
        "W2 bright-line #2: cascade feedback must name the originating "
        f"drop reason; got {text!r}"
    )


# ---------------------------------------------------------------------
# Bright-line #3 — Typed drop round-trip into harness verdict (SC-2, PR-1 GATE)
# ---------------------------------------------------------------------


def test_w4_d139_compiler_drops_round_trip_into_terminal_verdict(
    run_d139,
) -> None:
    """W4 bright-line #3 — harness.compute_iteration_terminal_reason
    returns a typed verdict that reflects compiler drops: the
    structured fields (top_drop_reason, drop_reason_counts) match the
    compiler's drop summary.
    """
    from genie_space_optimizer.optimization.harness import (
        compute_iteration_terminal_reason,
    )
    from genie_space_optimizer.optimization.terminal_reason import (
        TerminalReason,
    )

    f = run_d139["terminal_reason_closed_vocab_fixture"]
    verdict = compute_iteration_terminal_reason(
        stage3_proposal_count=int(f["stage3_proposal_count"]),
        compiler_surviving_count=0,
        compiler_top_drop_reason=str(f["compiler_result_top_drop_reason"]),
        compiler_drop_reason_counts=dict(
            f["compiler_result_drop_reason_counts"]
        ),
        compiler_first_originating_intent_id="",
        applied_outcome_count=len(f["applied_outcomes"]),
    )

    assert verdict.terminal_reason == TerminalReason.SLATE_COMPILER_EMPTY, (
        "W4 bright-line #3: when Stage 3 returned proposals and the "
        "compiler dropped them all, the terminal reason MUST be the "
        "closed-vocab SLATE_COMPILER_EMPTY enum value, NOT "
        f"NO_APPLIED_PATCHES; got {verdict.terminal_reason}"
    )
    assert verdict.top_drop_reason == "bundle_invariant_violated", (
        "W4 bright-line #3: top_drop_reason structured field must "
        f"carry the compiler's top drop reason; got {verdict}"
    )
    assert verdict.drop_reason_counts == {
        "bundle_invariant_violated": 2,
    }, (
        "W4 bright-line #3: drop_reason_counts structured field must "
        f"carry the compiler's counts; got {verdict}"
    )


# ---------------------------------------------------------------------
# Bright-line #4 — Closed-vocab terminal reason (no colon suffix) (SC-2, PR-1 GATE)
# ---------------------------------------------------------------------


def test_w4_d139_terminal_reason_is_closed_vocab_enum_no_string_suffix(
    run_d139,
) -> None:
    """W4 bright-line #4 — the TerminalReason taxonomy stays a
    closed-vocabulary enum: no dynamic string suffixes (no
    `slate_compiler_empty:<reason>` strings). Root-cause attribution
    lives in structured fields on the verdict, not in the enum value.
    """
    from genie_space_optimizer.optimization.harness import (
        compute_iteration_terminal_reason,
    )
    from genie_space_optimizer.optimization.terminal_reason import (
        TerminalReason,
    )

    f = run_d139["terminal_reason_closed_vocab_fixture"]
    verdict = compute_iteration_terminal_reason(
        stage3_proposal_count=int(f["stage3_proposal_count"]),
        compiler_surviving_count=0,
        compiler_top_drop_reason=str(f["compiler_result_top_drop_reason"]),
        compiler_drop_reason_counts=dict(
            f["compiler_result_drop_reason_counts"]
        ),
        compiler_first_originating_intent_id="",
        applied_outcome_count=len(f["applied_outcomes"]),
    )

    # The enum value itself must NOT contain a colon suffix.
    assert ":" not in str(verdict.terminal_reason.value), (
        "W4 bright-line #4: TerminalReason enum values are closed "
        "vocabulary; no `slate_compiler_empty:<reason>` colon-suffix "
        f"strings allowed. got {verdict.terminal_reason!r}"
    )

    # And it must be one of the recognized enum members, never the
    # NO_APPLIED_PATCHES catch-all when Stage 3 actually returned
    # proposals.
    assert verdict.terminal_reason != TerminalReason.NO_APPLIED_PATCHES, (
        "W4 bright-line #4: Stage 3 returned 2 proposals; the terminal "
        "reason MUST NOT be NO_APPLIED_PATCHES"
    )

    # Anti-assertion: STAGE3_RETURNED_NONE is also forbidden because
    # Stage 3 DID return proposals (the compiler dropped them).
    if hasattr(TerminalReason, "STAGE3_RETURNED_NONE"):
        assert verdict.terminal_reason != TerminalReason.STAGE3_RETURNED_NONE, (
            "W4 bright-line #4: Stage 3 returned 2 proposals; the "
            "terminal reason MUST NOT be STAGE3_RETURNED_NONE"
        )


# ---------------------------------------------------------------------
# Bright-line #5 — H001 positive control: e943 stays under cap (SC-3 guard, PR-1 GATE)
# ---------------------------------------------------------------------


def test_w7_e943_h001_positive_control_stays_under_cap_no_split(
    run_e943,
) -> None:
    """W7 bright-line #5 (positive control) — the e943 H001 cluster
    Stage 3 prompt path already produces ~7k token prompts and works
    correctly. W7's scope is the RCA-subcluster builder only; the
    H001 cluster builder MUST stay below the cap and MUST NOT fire
    GSO_TRIAL22_STAGE3_SUBCLUSTER_SPLIT_V1.

    This guards against the scope creep flagged in design review:
    any regression on H001 fails the merge gate.
    """
    from genie_space_optimizer.optimization.stage3_prompt_sizer import (
        STAGE3_PROMPT_TOTAL_CAP,
        slice_segments,
    )

    f = run_e943["h001_positive_control_fixture"]
    bd = f["sample_in_budget_breakdown"]
    sliced = slice_segments(
        system_msg_tokens=int(bd["system_msg_tokens"]),
        user_prompt_tokens=int(bd["user_prompt_tokens"]),
        cacheable_block_tokens=int(bd["cacheable_block_tokens"]),
        cap=STAGE3_PROMPT_TOTAL_CAP,
    )
    expected = f["expected_after_trial22_w7"]

    assert sliced["over_cap"] is False, (
        "W7 bright-line #5: e943 H001 prompt is ~7k tokens; over_cap "
        f"must be false; got {sliced}"
    )
    assert sliced["sub_cluster_split_needed"] is False, (
        "W7 bright-line #5: H001 prompt is under budget; "
        f"sub_cluster_split_needed must be false; got {sliced}"
    )
    assert sliced["total_tokens"] <= int(expected["total_tokens_max"]), (
        "W7 bright-line #5: H001 total_tokens must stay <= "
        f"{expected['total_tokens_max']}; got {sliced['total_tokens']}"
    )


# ---------------------------------------------------------------------
# Bright-line #6 — RCA-subcluster slice (d139, SC-3) — PR-2 xfail
# ---------------------------------------------------------------------


def test_w7_d139_rca_subcluster_oversize_payload_slices_into_sub_batches(
    run_d139,
) -> None:
    """W7 bright-line #6 — d139's 105k-token RCA-subcluster Stage 3
    request must slice into >= 3 sub-batches each <= 40k tokens. The
    builder field on the split marker pins this to the subcluster
    builder.
    """
    import json
    import math

    from genie_space_optimizer.optimization.stage3_prompt_sizer import (
        STAGE3_PROMPT_TOTAL_CAP,
        partition_rca_subcluster_by_token_budget,
        stage3_subcluster_split_marker,
    )

    f = run_d139["rca_subcluster_oversize_fixture"]
    bd = f["sample_over_cap_breakdown"]
    expected = f["expected_after_trial22_w7"]
    qids = [f"qid_{i:02d}" for i in range(int(f["qid_count"]))]

    partitions = partition_rca_subcluster_by_token_budget(
        qids=qids,
        user_prompt_tokens=int(bd["user_prompt_tokens"]),
        system_msg_tokens=int(bd["system_msg_tokens"]),
        cacheable_block_tokens=int(bd["cacheable_block_tokens"]),
        cap=int(bd["cap"]),
    )

    # >= 3 sub-batches.
    assert len(partitions) >= int(expected["min_batch_count"]), (
        "W7 bright-line #6: the 98.6k-token RCA-subcluster payload must "
        f"split into >= {expected['min_batch_count']} batches; got "
        f"{len(partitions)} ({[len(p) for p in partitions]})"
    )

    # Every QID is covered exactly once (no loss, no dup).
    flat = [q for p in partitions for q in p]
    assert sorted(flat) == sorted(qids), (
        "W7 bright-line #6: the partition must cover every QID exactly "
        f"once; got {flat}"
    )

    # Each batch's projected user-prompt tokens stay under the per-call
    # user budget so the assembled prompt is <= cap.
    user_budget = int(bd["cap"]) - (
        int(bd["system_msg_tokens"]) + int(bd["cacheable_block_tokens"])
    )
    per_qid = int(bd["user_prompt_tokens"]) / len(qids)
    for p in partitions:
        projected_user = math.ceil(per_qid * len(p))
        projected_total = (
            projected_user
            + int(bd["system_msg_tokens"])
            + int(bd["cacheable_block_tokens"])
        )
        assert projected_user <= user_budget, (
            "W7 bright-line #6: each sub-batch's projected user-prompt "
            f"tokens ({projected_user}) must stay <= user_budget "
            f"({user_budget})"
        )
        assert projected_total <= int(expected["max_tokens_per_batch"]), (
            "W7 bright-line #6: each sub-batch total must be <= "
            f"{expected['max_tokens_per_batch']}; got {projected_total}"
        )

    # The split marker pins the builder to the RCA-subcluster path.
    marker = stage3_subcluster_split_marker(
        optimization_run_id="d139",
        iteration=int(f["iteration"]),
        cluster_id=str(f["cluster_id"]),
        builder=str(f["builder"]),
        partitions=partitions,
        user_prompt_tokens=int(bd["user_prompt_tokens"]),
        user_budget=user_budget,
        cap=STAGE3_PROMPT_TOTAL_CAP,
    )
    assert marker.startswith("GSO_TRIAL22_STAGE3_SUBCLUSTER_SPLIT_V1 ")
    payload = json.loads(marker.split(" ", 1)[1])
    assert payload["builder"] == "rca_subcluster", (
        "W7 bright-line #6: split marker must pin builder=rca_subcluster "
        f"so it is distinguishable from the H001 path; got {payload}"
    )
    assert payload["batch_count"] == len(partitions)


# ---------------------------------------------------------------------
# Bright-line #7 — Lineage orphan stamping (e943, SC-3) — PR-2 xfail
# ---------------------------------------------------------------------


def test_w5_e943_full_eval_accepted_without_patch_outcome_is_orphan(
    run_e943,
) -> None:
    """W5.1 bright-line #7 — the e943 full-eval accept with
    candidate_accuracy=95.8 must be stamped provenance=orphan_acceptance
    because there is no matching GSO_PATCH_OUTCOME_V1 or
    GSO_ADMISSION_DECISION_V1 row. scoreboard.best_accuracy stays at
    baseline (87.5). The positive control (matching patch+admission)
    must NOT be orphaned and DOES raise best_accuracy to 95.8.
    """
    from genie_space_optimizer.optimization.lineage_invariants import (
        CANONICAL_LINEAGE_KEY,
        ORPHAN_ACCEPTANCE,
        canonical_key,
        enforce_full_eval_lineage,
    )

    # W5.0 — the canonical key audit must NOT include intent_id (the
    # design-review trap: full-eval rows are not keyed on intent_id).
    assert CANONICAL_LINEAGE_KEY == (
        "optimization_run_id",
        "ag_id",
        "iteration",
    )
    assert "intent_id" not in CANONICAL_LINEAGE_KEY

    # --- Orphan path (the actual e943 contradiction) ---
    contradiction = run_e943["lineage_contradiction_fixture"]
    recon = enforce_full_eval_lineage(
        full_eval_rows=contradiction["full_eval_rows"],
        patch_outcome_rows=contradiction["patch_outcome_rows"],
        admission_decision_rows=contradiction["admission_decision_rows"],
        baseline_accuracy=contradiction["full_eval_rows"][0][
            "baseline_accuracy"
        ],
    )
    fe_key = canonical_key(contradiction["full_eval_rows"][0])
    assert recon.provenance_for(fe_key) == ORPHAN_ACCEPTANCE
    assert recon.orphan_count == 1
    # scoreboard.best_accuracy stays at baseline — the 95.8 orphan is
    # excluded, never counted as an optimizer win.
    assert recon.best_accuracy == pytest.approx(87.5)
    audit = [
        m for m in recon.markers
        if m.startswith("GSO_TRIAL22_LINEAGE_KEY_AUDIT_V1")
    ]
    violations = [
        m for m in recon.markers
        if m.startswith("GSO_TRIAL22_LINEAGE_VIOLATION_V1")
    ]
    assert len(audit) == 1, "W5.0 key-audit marker must be emitted"
    assert len(violations) == 1, "one orphan must emit one violation"

    # --- Positive control (must NOT be orphaned) ---
    positive = run_e943["lineage_positive_control_fixture"]
    recon_ok = enforce_full_eval_lineage(
        full_eval_rows=positive["full_eval_rows"],
        patch_outcome_rows=positive["patch_outcome_rows"],
        admission_decision_rows=positive["admission_decision_rows"],
        baseline_accuracy=positive["full_eval_rows"][0][
            "baseline_accuracy"
        ],
    )
    ok_key = canonical_key(positive["full_eval_rows"][0])
    assert recon_ok.provenance_for(ok_key) == ""
    assert recon_ok.orphan_count == 0
    assert recon_ok.best_accuracy == pytest.approx(95.8)
    assert not [
        m for m in recon_ok.markers
        if m.startswith("GSO_TRIAL22_LINEAGE_VIOLATION_V1")
    ]


# ---------------------------------------------------------------------
# Bright-line #8 — Retry feedback durability (SC-2) — PR-2 xfail
# ---------------------------------------------------------------------


def test_w3_d139_compiler_drop_summary_lives_on_iteration_terminal_state_ledger(
    run_d139,
    tmp_path,
) -> None:
    """W3 bright-line #8 — after d139 iteration 1's slate is fully
    dropped, compiler_drop_summary is persisted on the iteration
    terminal-state ledger (NOT only on the transient
    ClusterSynthesisResult). Iteration 2's Stage 3 _build_request
    reads it and the rendered prompt contains the iteration-1 drop
    strings.

    The durable boundary is the ``IterationCandidateLedgerEntry``: we
    write iteration 1's summary, read it back from the JSONL (proving
    it survived the AG/cluster transition the way the harness advances
    past it), then feed the read-back summary through the exact
    renderer ``_build_request`` injects into the Stage 3 N+1 prompt.
    """
    from genie_space_optimizer.optimization.candidate_ledger import (
        IterationCandidateLedgerEntry,
        read_ledger,
        write_ledger_entry,
    )
    from genie_space_optimizer.optimization.proposal_slate_compiler import (
        build_compiler_drop_summary,
        compile_slate,
        render_prior_iteration_drops,
        SlateCompilerContext,
    )

    # Drive a REAL full drop through the compiler: the cohesion-cascade
    # fixture drops one sibling for MISSING_IMPLICATED_ASSETS in Phase 1
    # and cascades the other in Phase 1.5, so survivor_count == 0.
    f = run_d139["h001_bundle_cohesion_cascade_fixture"]
    proposals = [_make_proposal_from_fixture(p) for p in f["proposals"]]
    assets_by_id: dict[str, list[str]] = {}
    just_by_id: dict[str, str] = {}
    for p in f["proposals"]:
        iid = str(p["intent_id"])
        assets_by_id[iid] = list(p.get("implicated_assets_from_diagnosis") or [])
        just_by_id[iid] = (
            "" if p["patch_type"] == "add_column_description"
            else "exemplar paired with column doc"
        )
    ctx = SlateCompilerContext(
        implicated_assets_by_proposal_id=assets_by_id,
        justification_by_proposal_id=just_by_id,
    )
    result = compile_slate(proposals, ctx)
    assert not result.surviving_proposals, (
        "W3 #8 setup: the cohesion-cascade fixture must fully drop the "
        f"slate; surviving={result.surviving_proposals}"
    )

    summary = build_compiler_drop_summary(result)
    assert summary["drop_reason_counts"], "summary must carry drop counts"

    # --- Durable boundary: write iteration 1, read it back from disk ---
    ledger_path = str(tmp_path / "iteration_candidate_ledger.jsonl")
    entry = IterationCandidateLedgerEntry(
        iteration=1,
        ag_id="ag_d139",
        cluster_ids=("h001_cohesion",),
        target_qids=tuple(f["proposals"][0].get("target_qids") or ()),
        root_cause="bundle_cohesion",
        requested_levers=(1, 5),
        rca_card_id_or_provisional="prov_d139",
        proposal_attempts=2,
        selected_proposal_id="",
        terminal_reason="slate_compiler_empty",
        terminal_outcome="info",
        best_of_n_size=1,
        patches_applied=0,
        subset_isolation_run=False,
        subset_isolation_kept=(),
        subset_isolation_dropped=(),
        protected_dependents=(),
        narrow_replacement_attempted=False,
        narrow_replacement_succeeded=False,
        accuracy_delta_pp=0.0,
        acceptance_tier="reject_loss",
        retire_signature="",
        compiler_drop_summary=summary,
    )
    write_ledger_entry(entry, path=ledger_path)
    rows = read_ledger(ledger_path)

    assert rows[-1].compiler_drop_summary is not None, (
        "W3 #8: compiler_drop_summary MUST survive the durable ledger "
        "round-trip (NOT only live on the transient ClusterSynthesisResult)"
    )
    persisted = rows[-1].compiler_drop_summary
    assert persisted["drop_reason_counts"] == summary["drop_reason_counts"]

    # --- Stage 3 N+1 reads the durable summary into the prompt ---
    section = render_prior_iteration_drops(persisted)
    assert "<prior_iteration_drops>" in section
    for reason in persisted["top_drop_reasons"]:
        assert reason in section, (
            "W3 #8: iteration 2's Stage 3 prompt MUST surface the "
            f"iteration-1 drop reason {reason!r}; section={section}"
        )
