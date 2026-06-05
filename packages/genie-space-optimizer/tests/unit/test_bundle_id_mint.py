"""Phase 3 P3.3 — deterministic bundle_id mint."""
from __future__ import annotations

from genie_space_optimizer.optimization.bundle_id_mint import (
    mint_bundle_id,
    mint_bundle_id_for_proposal,
    primary_family_for_patch_type,
)
from genie_space_optimizer.optimization.repair_intent import PatchType


def test_format_is_cluster_dot_iter_dot_family() -> None:
    assert (
        mint_bundle_id(
            cluster_id="c1",
            iteration=3,
            primary_family="add_example_sql",
        )
        == "c1.iter3.add_example_sql"
    )


def test_empty_cluster_id_falls_back_to_unknown() -> None:
    assert (
        mint_bundle_id(
            cluster_id="",
            iteration=1,
            primary_family="add_instruction",
        )
        == "unknown.iter1.add_instruction"
    )


def test_empty_family_falls_back_to_unknown_family() -> None:
    assert (
        mint_bundle_id(
            cluster_id="c0",
            iteration=0,
            primary_family="",
        )
        == "c0.iter0.unknown_family"
    )


def test_negative_iteration_for_tape_replay() -> None:
    assert (
        mint_bundle_id(
            cluster_id="c1",
            iteration=-1,
            primary_family="add_example_sql",
        )
        == "c1.iter-1.add_example_sql"
    )


def test_primary_family_for_patch_type_collapses_example_sql_variants() -> None:
    assert (
        primary_family_for_patch_type(PatchType.ADD_EXAMPLE_SQL)
        == "add_example_sql"
    )
    assert (
        primary_family_for_patch_type(PatchType.ADD_EXAMPLE_SQL_NEGATIVE)
        == "add_example_sql"
    )
    assert (
        primary_family_for_patch_type(PatchType.UPDATE_EXAMPLE_SQL)
        == "add_example_sql"
    )


def test_primary_family_for_patch_type_collapses_instruction_variants() -> None:
    for pt in (
        PatchType.ADD_INSTRUCTION,
        PatchType.UPDATE_INSTRUCTION,
        PatchType.UPDATE_INSTRUCTION_SECTION,
        PatchType.REWRITE_INSTRUCTION,
    ):
        assert primary_family_for_patch_type(pt) == "add_instruction"


def test_primary_family_for_patch_type_accepts_string() -> None:
    assert primary_family_for_patch_type("add_join_spec") == "add_join_spec"


def test_primary_family_for_unknown_string_returns_generic() -> None:
    assert (
        primary_family_for_patch_type("not_a_real_patch_type")
        == "generic_judge_guidance"
    )


def test_mint_bundle_id_for_proposal_round_trip() -> None:
    assert (
        mint_bundle_id_for_proposal(
            cluster_id="cluster_42",
            iteration=7,
            patch_type=PatchType.ADD_SQL_SNIPPET_FILTER,
        )
        == "cluster_42.iter7.add_sql_snippet_filter"
    )


def test_mint_bundle_id_collisions_avoided_across_iterations() -> None:
    # Two iterations producing the same family from the same
    # cluster MUST mint distinct bundle_ids — this is the property
    # that closes the fallback AG-collision class.
    a = mint_bundle_id(cluster_id="c1", iteration=1, primary_family="add_example_sql")
    b = mint_bundle_id(cluster_id="c1", iteration=2, primary_family="add_example_sql")
    assert a != b


def test_mint_bundle_id_collisions_avoided_across_clusters() -> None:
    a = mint_bundle_id(cluster_id="c1", iteration=1, primary_family="add_example_sql")
    b = mint_bundle_id(cluster_id="c2", iteration=1, primary_family="add_example_sql")
    assert a != b
