"""P4 C2 unit tests — PatchMechanism taxonomy + mechanism-repeat guard."""
from __future__ import annotations

import json

import pytest

from genie_space_optimizer.optimization.patch_mechanism import (
    UNPRODUCTIVE_OUTCOMES,
    MechanismAttempt,
    PatchMechanism,
    behavior_delta_hash,
    check_mechanism_repeat_guard,
    mechanism_for_patch_type,
    mechanism_repeat_guard_marker,
)


def test_mechanism_enum_has_six_values():
    assert {m.value for m in PatchMechanism} == {
        "instruction_text",
        "example_sql",
        "sql_snippet",
        "metadata_description",
        "metadata_join",
        "routing",
    }


@pytest.mark.parametrize(
    "patch_type,expected",
    [
        ("add_instruction", PatchMechanism.INSTRUCTION_TEXT),
        ("update_instruction", PatchMechanism.INSTRUCTION_TEXT),
        ("add_example_sql", PatchMechanism.EXAMPLE_SQL),
        ("add_example_sql_negative", PatchMechanism.EXAMPLE_SQL),
        ("add_sql_snippet_filter", PatchMechanism.SQL_SNIPPET),
        ("add_sql_snippet_expression", PatchMechanism.SQL_SNIPPET),
        ("add_sql_snippet_measure", PatchMechanism.SQL_SNIPPET),
        ("update_column_description", PatchMechanism.METADATA_DESCRIPTION),
        ("add_column_synonym", PatchMechanism.METADATA_DESCRIPTION),
        ("add_join_spec", PatchMechanism.METADATA_JOIN),
        ("update_join_spec", PatchMechanism.METADATA_JOIN),
        ("add_table", PatchMechanism.ROUTING),
        ("remove_table", PatchMechanism.ROUTING),
    ],
)
def test_mechanism_for_patch_type_known(patch_type, expected):
    assert mechanism_for_patch_type(patch_type) == expected


def test_mechanism_for_patch_type_unknown_returns_none():
    assert mechanism_for_patch_type("not_a_patch_type") is None
    assert mechanism_for_patch_type("") is None


def test_mechanism_for_patch_type_case_insensitive_and_trims():
    assert mechanism_for_patch_type("  ADD_INSTRUCTION  ") == (
        PatchMechanism.INSTRUCTION_TEXT
    )


def test_behavior_delta_hash_is_stable_and_normalized():
    h1 = behavior_delta_hash("Top 3 by amount")
    h2 = behavior_delta_hash("top 3 by amount")
    h3 = behavior_delta_hash("  top   3   by amount  ")
    assert h1 == h2 == h3
    assert len(h1) == 8


def test_unproductive_outcomes_set_pinned():
    assert UNPRODUCTIVE_OUTCOMES == frozenset(
        {"kept_insufficient", "no_applied_patches"}
    )


def test_guard_allowed_when_no_prior_attempts():
    verdict = check_mechanism_repeat_guard(
        qid="gs_009",
        behavior_delta="rank top 3",
        proposed_mechanisms=(PatchMechanism.EXAMPLE_SQL,),
        mechanism_change_justification="",
        prior_attempts=(),
    )
    assert verdict.outcome == "allowed"
    assert verdict.forbidden_mechanism is None


def test_guard_blocks_pure_repeat_after_kept_insufficient():
    bdh = behavior_delta_hash("rank top 3")
    prior = (
        MechanismAttempt(
            qid="gs_009",
            behavior_delta_hash=bdh,
            mechanism=PatchMechanism.EXAMPLE_SQL,
            outcome="kept_insufficient",
        ),
    )
    verdict = check_mechanism_repeat_guard(
        qid="gs_009",
        behavior_delta="rank top 3",
        proposed_mechanisms=(PatchMechanism.EXAMPLE_SQL,),
        mechanism_change_justification="",
        prior_attempts=prior,
    )
    assert verdict.outcome == "blocked"
    assert verdict.forbidden_mechanism == PatchMechanism.EXAMPLE_SQL
    assert verdict.prior_unproductive_outcome == "kept_insufficient"
    assert "switch mechanism" in verdict.feedback


def test_guard_blocks_pure_repeat_after_no_applied_patches():
    bdh = behavior_delta_hash("rank top 3")
    prior = (
        MechanismAttempt(
            qid="gs_009",
            behavior_delta_hash=bdh,
            mechanism=PatchMechanism.SQL_SNIPPET,
            outcome="no_applied_patches",
        ),
    )
    verdict = check_mechanism_repeat_guard(
        qid="gs_009",
        behavior_delta="rank top 3",
        proposed_mechanisms=(PatchMechanism.SQL_SNIPPET,),
        mechanism_change_justification="",
        prior_attempts=prior,
    )
    assert verdict.outcome == "blocked"
    assert verdict.prior_unproductive_outcome == "no_applied_patches"


def test_guard_allows_paired_new_mechanism():
    """The d139 fix: after example_sql -> kept_insufficient, a new
    proposal that ADDS sql_snippet alongside example_sql is allowed."""
    bdh = behavior_delta_hash("rank top 3")
    prior = (
        MechanismAttempt(
            qid="gs_009",
            behavior_delta_hash=bdh,
            mechanism=PatchMechanism.EXAMPLE_SQL,
            outcome="kept_insufficient",
        ),
    )
    verdict = check_mechanism_repeat_guard(
        qid="gs_009",
        behavior_delta="rank top 3",
        proposed_mechanisms=(
            PatchMechanism.EXAMPLE_SQL,
            PatchMechanism.SQL_SNIPPET,  # new mechanism paired in
        ),
        mechanism_change_justification="adding sql_snippet to anchor top-3",
        prior_attempts=prior,
    )
    assert verdict.outcome == "allowed"


def test_guard_allows_switching_to_new_mechanism():
    bdh = behavior_delta_hash("rank top 3")
    prior = (
        MechanismAttempt(
            qid="gs_009",
            behavior_delta_hash=bdh,
            mechanism=PatchMechanism.EXAMPLE_SQL,
            outcome="kept_insufficient",
        ),
    )
    verdict = check_mechanism_repeat_guard(
        qid="gs_009",
        behavior_delta="rank top 3",
        proposed_mechanisms=(PatchMechanism.METADATA_DESCRIPTION,),
        mechanism_change_justification="trying column-meaning fix",
        prior_attempts=prior,
    )
    assert verdict.outcome == "allowed"


def test_guard_isolates_qids():
    """Same mechanism+behavior on a DIFFERENT qid must not block."""
    bdh = behavior_delta_hash("rank top 3")
    prior = (
        MechanismAttempt(
            qid="gs_001",
            behavior_delta_hash=bdh,
            mechanism=PatchMechanism.EXAMPLE_SQL,
            outcome="kept_insufficient",
        ),
    )
    verdict = check_mechanism_repeat_guard(
        qid="gs_002",  # different qid
        behavior_delta="rank top 3",
        proposed_mechanisms=(PatchMechanism.EXAMPLE_SQL,),
        mechanism_change_justification="",
        prior_attempts=prior,
    )
    assert verdict.outcome == "allowed"


def test_guard_isolates_behavior_deltas():
    """Same mechanism but DIFFERENT behavior_delta must not block."""
    prior = (
        MechanismAttempt(
            qid="gs_009",
            behavior_delta_hash=behavior_delta_hash("rank top 3"),
            mechanism=PatchMechanism.EXAMPLE_SQL,
            outcome="kept_insufficient",
        ),
    )
    verdict = check_mechanism_repeat_guard(
        qid="gs_009",
        behavior_delta="filter by region",  # different delta
        proposed_mechanisms=(PatchMechanism.EXAMPLE_SQL,),
        mechanism_change_justification="",
        prior_attempts=prior,
    )
    assert verdict.outcome == "allowed"


def test_marker_payload_pins_required_fields():
    bdh = behavior_delta_hash("rank top 3")
    prior = (
        MechanismAttempt(
            qid="gs_009",
            behavior_delta_hash=bdh,
            mechanism=PatchMechanism.EXAMPLE_SQL,
            outcome="kept_insufficient",
        ),
    )
    verdict = check_mechanism_repeat_guard(
        qid="gs_009",
        behavior_delta="rank top 3",
        proposed_mechanisms=(PatchMechanism.EXAMPLE_SQL,),
        mechanism_change_justification="",
        prior_attempts=prior,
    )
    line = mechanism_repeat_guard_marker(
        optimization_run_id="run_xyz",
        iteration=3,
        qid="gs_009",
        behavior_delta="rank top 3",
        proposed_mechanisms=(PatchMechanism.EXAMPLE_SQL,),
        verdict=verdict,
        mechanism_change_justification="",
    )
    name, _, payload_json = line.partition(" ")
    assert name == "GSO_MECHANISM_REPEAT_GUARD_V1"
    payload = json.loads(payload_json)
    assert payload["qid"] == "gs_009"
    assert payload["outcome"] == "blocked"
    assert payload["forbidden_mechanism"] == "example_sql"
    assert payload["prior_unproductive_outcome"] == "kept_insufficient"
    assert payload["behavior_delta_hash"] == bdh
    assert payload["proposed_mechanisms"] == ["example_sql"]


def test_d139_four_iteration_repeat_blocked_on_attempt_2():
    """Regression: d139 emitted add_example_sql four times in a row.
    The guard must block on the second emission."""
    bdh = behavior_delta_hash("ordering of top results")
    attempts_after_1 = (
        MechanismAttempt(
            qid="gs_009",
            behavior_delta_hash=bdh,
            mechanism=PatchMechanism.EXAMPLE_SQL,
            outcome="kept_insufficient",
        ),
    )
    second_attempt = check_mechanism_repeat_guard(
        qid="gs_009",
        behavior_delta="ordering of top results",
        proposed_mechanisms=(PatchMechanism.EXAMPLE_SQL,),
        mechanism_change_justification="",
        prior_attempts=attempts_after_1,
    )
    assert second_attempt.outcome == "blocked"
