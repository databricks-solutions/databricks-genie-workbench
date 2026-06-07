from genie_space_optimizer.optimization.patch_mechanism import PatchMechanism
from genie_space_optimizer.optimization.rca_mechanism_routing import (
    mechanisms_for_rejected_levers,
)


def test_lever5_family_maps_to_example_sql_and_instruction():
    # lever-5 spans add_instruction (INSTRUCTION_TEXT) + add_example_sql
    # (EXAMPLE_SQL); both behavioral units must be in the normalized set.
    out = mechanisms_for_rejected_levers(("lever-5",))
    assert PatchMechanism.EXAMPLE_SQL in out
    assert PatchMechanism.INSTRUCTION_TEXT in out


def test_lever6_maps_to_sql_snippet():
    out = mechanisms_for_rejected_levers(("lever-6",))
    assert out == frozenset({PatchMechanism.SQL_SNIPPET})


def test_lever1_maps_to_metadata_and_routing():
    # lever-1 spans descriptions/synonyms (METADATA_DESCRIPTION) plus
    # add_table/remove_table (ROUTING).
    out = mechanisms_for_rejected_levers(("lever-1",))
    assert PatchMechanism.METADATA_DESCRIPTION in out
    assert PatchMechanism.ROUTING in out


def test_empty_and_unknown_are_safe():
    assert mechanisms_for_rejected_levers(()) == frozenset()
    assert mechanisms_for_rejected_levers(("lever-999",)) == frozenset()


def test_patch_type_token_form_normalizes():
    # rejected_mechanism may be a patch_type wire token when the lever
    # was inferred. The helper accepts those directly too.
    out = mechanisms_for_rejected_levers(("add_example_sql",))
    assert out == frozenset({PatchMechanism.EXAMPLE_SQL})


def test_alias_collapse_lever_and_patch_type_to_same_mechanism():
    # A lever-id and a patch_type token that resolve to the same
    # behavioral unit collapse to a single mechanism (no duplication,
    # caught by membership not by string identity).
    out = mechanisms_for_rejected_levers(("lever-6", "add_sql_snippet_filter"))
    assert out == frozenset({PatchMechanism.SQL_SNIPPET})
