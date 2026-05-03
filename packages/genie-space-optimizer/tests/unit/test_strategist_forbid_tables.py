"""TDD coverage for the strategist forbid-tables constraint (T5).

See `docs/2026-05-03-cycle9-burndown-blast-radius-recovery-and-decision-trace-plan.md`
T5 for the cycle-9 motivation: blast-radius drops weren't fed back to
the next strategist call, so the same patch shape against the same
table was re-proposed on every iteration.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.strategist_constraints import (
    StrategistConstraints,
    record_blast_radius_drop,
)


def test_constraints_default_empty():
    c = StrategistConstraints()
    assert c.forbid_tables_for_ag("AG_X") == set()


def test_record_blast_radius_drop_adds_table():
    c = StrategistConstraints()
    record_blast_radius_drop(
        constraints=c,
        ag_id="AG_DECOMPOSED_H001",
        dropped_patches=[
            {"target": "ucat.dev.tkt_payment", "type": "add_sql_snippet_filter"},
            {"target": "ucat.dev.tkt_payment", "type": "add_sql_snippet_measure"},
        ],
    )
    assert c.forbid_tables_for_ag("AG_DECOMPOSED_H001") == {
        "ucat.dev.tkt_payment"
    }


def test_record_skips_patches_without_target():
    c = StrategistConstraints()
    record_blast_radius_drop(
        constraints=c,
        ag_id="AG_X",
        dropped_patches=[{"type": "add_instruction"}],
    )
    assert c.forbid_tables_for_ag("AG_X") == set()


def test_constraints_serialize_for_strategist_context():
    c = StrategistConstraints()
    record_blast_radius_drop(
        constraints=c,
        ag_id="AG_X",
        dropped_patches=[{"target": "ucat.dev.t1"}],
    )
    payload = c.to_strategist_context()
    assert payload == {
        "AG_X": {"forbid_tables": ["ucat.dev.t1"]},
    }


def test_to_strategist_context_omits_empty_ags():
    c = StrategistConstraints()
    record_blast_radius_drop(
        constraints=c,
        ag_id="AG_X",
        dropped_patches=[{"type": "add_instruction"}],  # no target
    )
    assert c.to_strategist_context() == {}


def test_forbid_table_for_ag_handles_blank_inputs():
    c = StrategistConstraints()
    c.forbid_table_for_ag("", "ucat.dev.t1")
    c.forbid_table_for_ag("AG_X", "")
    c.forbid_table_for_ag("  ", "  ")
    assert c.to_strategist_context() == {}


def test_forbid_table_dedupes_across_repeated_calls():
    c = StrategistConstraints()
    c.forbid_table_for_ag("AG_X", "ucat.dev.t1")
    c.forbid_table_for_ag("AG_X", "ucat.dev.t1")
    c.forbid_table_for_ag("AG_X", "ucat.dev.t2")
    assert c.forbid_tables_for_ag("AG_X") == {
        "ucat.dev.t1",
        "ucat.dev.t2",
    }
    payload = c.to_strategist_context()
    assert payload == {
        "AG_X": {"forbid_tables": ["ucat.dev.t1", "ucat.dev.t2"]},
    }
