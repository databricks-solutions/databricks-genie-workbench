"""Phase 3 P3.3 — deterministic ``bundle_id`` minting.

Background — pre-P3.3 the ``bundle_id`` field on
:class:`RepairProposal` was sourced verbatim from the Stage 3 LLM
output. Two problems followed:

  1. The LLM could (and did) emit the same string across iterations
     (e.g. ``"bundle_1"``), causing the AG-collision
     (``ag_collision_with_forbidden_set``) detector to false-positive
     on legitimately new bundles on every fallback path that
     re-issued an old bundle id.
  2. There was no way to look at a candidate ledger row and tell
     "this bundle came from cluster X, iteration Y, family Z"
     without joining against the proposal store.

P3.3 mints the bundle_id deterministically in the state-machine
synthesize transformer as::

    f"{cluster_id}.iter{iteration}.{primary_family}"

The deterministic format guarantees:

  * Uniqueness across iterations (the ``iter{N}`` suffix prevents
    cross-iteration collisions).
  * Uniqueness across clusters in the same iteration (the
    ``{cluster_id}`` prefix prevents cross-cluster collisions even
    when two clusters happen to land on the same patch family).
  * One bundle per (cluster, iteration, family) triplet — so a
    Stage 3 LLM that returns a kit of 2-3 lever proposals for the
    same RCA gets all proposals into the SAME bundle.

This module exposes :func:`mint_bundle_id` and the family-derivation
helper :func:`primary_family_for_patch_type` so both the SM
synthesize transformer and the legacy harness path can produce
byte-identical bundle_ids without re-implementing the format.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.repair_intent import PatchType


# Map from PatchType to its representative ``patch_family`` string.
# ``patch_family`` is a finer-grained label than ``patch_type``: it
# collapses several related PatchTypes (e.g. ADD_EXAMPLE_SQL and
# ADD_EXAMPLE_SQL_NEGATIVE) into a single family for the purpose of
# bundle_id minting and pivot bookkeeping.
_PATCH_TYPE_TO_FAMILY: dict[PatchType, str] = {
    PatchType.ADD_INSTRUCTION: "add_instruction",
    PatchType.UPDATE_INSTRUCTION: "add_instruction",
    PatchType.UPDATE_INSTRUCTION_SECTION: "add_instruction",
    PatchType.REWRITE_INSTRUCTION: "add_instruction",
    PatchType.ADD_EXAMPLE_SQL: "add_example_sql",
    # Phase 2 P2.4 — negative variant collapses to the positive
    # family for bundle_id minting; the ``negative`` flag on the
    # patch_body discriminates the two at apply time.
    PatchType.ADD_EXAMPLE_SQL_NEGATIVE: "add_example_sql",
    PatchType.UPDATE_EXAMPLE_SQL: "add_example_sql",
    PatchType.ADD_DESCRIPTION: "add_description",
    PatchType.UPDATE_DESCRIPTION: "add_description",
    PatchType.ADD_COLUMN_DESCRIPTION: "add_column_description",
    PatchType.UPDATE_COLUMN_DESCRIPTION: "add_column_description",
    PatchType.ADD_JOIN_SPEC: "add_join_spec",
    PatchType.UPDATE_JOIN_SPEC: "add_join_spec",
    PatchType.REMOVE_JOIN_SPEC: "add_join_spec",
    PatchType.ADD_SQL_SNIPPET_EXPRESSION: "add_sql_snippet_expression",
    PatchType.ADD_SQL_SNIPPET_FILTER: "add_sql_snippet_filter",
    PatchType.ADD_SQL_SNIPPET_MEASURE: "add_sql_snippet_measure",
    PatchType.UPDATE_TVF_SQL: "add_sql_snippet_expression",
    PatchType.ADD_DEFAULT_FILTER: "add_sql_snippet_filter",
    PatchType.UPDATE_FILTER_CONDITION: "add_sql_snippet_filter",
    PatchType.ADD_MV_MEASURE: "add_sql_snippet_measure",
    PatchType.UPDATE_MV_MEASURE: "add_sql_snippet_measure",
    PatchType.ADD_MV_DIMENSION: "add_sql_snippet_measure",
}


def primary_family_for_patch_type(patch_type: PatchType | str) -> str:
    """Return the representative ``patch_family`` string for a
    :class:`PatchType` (or its ``.value`` string).

    Returns ``"generic_judge_guidance"`` — the same sentinel used by
    :func:`rca.patch_family_for_rca_kind` — when the patch_type is
    not in the mapping. This keeps the helper total so the
    bundle_id minter never raises on an unknown patch_type from a
    future enum addition.
    """
    if isinstance(patch_type, str):
        try:
            patch_type = PatchType(patch_type)
        except ValueError:
            return "generic_judge_guidance"
    return _PATCH_TYPE_TO_FAMILY.get(patch_type, "generic_judge_guidance")


def mint_bundle_id(
    *,
    cluster_id: str,
    iteration: int,
    primary_family: str,
) -> str:
    """Return the canonical bundle_id for the given coordinates.

    Empty / whitespace-only ``cluster_id`` falls back to the literal
    ``"unknown"`` token so the format remains parseable by
    postmortems even when the upstream cluster bookkeeping is
    incomplete; the SM emits a marker upstream when this happens.

    ``iteration`` is rendered without sign — negative iterations
    indicate test/replay scaffolding and produce ``iter-N`` (e.g.
    ``"c1.iter-1.add_example_sql"`` for tape-replay tests).

    ``primary_family`` is taken verbatim; callers MUST normalize
    via :func:`primary_family_for_patch_type` to keep the format
    stable across the codebase. The minter does NOT re-normalize
    here because the synthesize transformer also accepts a kit
    family (e.g. from the KIT_FOR_RCA companion table) that is not
    derived from a PatchType.
    """
    cid = (str(cluster_id or "").strip() or "unknown")
    fam = (str(primary_family or "").strip() or "unknown_family")
    return f"{cid}.iter{int(iteration)}.{fam}"


def mint_bundle_id_for_proposal(
    *,
    cluster_id: str,
    iteration: int,
    patch_type: PatchType | str,
) -> str:
    """Convenience wrapper — mint a bundle_id directly from a
    proposal's patch_type (most common call shape in the SM
    synthesize transformer). Equivalent to::

        mint_bundle_id(
            cluster_id=cluster_id,
            iteration=iteration,
            primary_family=primary_family_for_patch_type(patch_type),
        )
    """
    return mint_bundle_id(
        cluster_id=cluster_id,
        iteration=iteration,
        primary_family=primary_family_for_patch_type(patch_type),
    )
