"""Pydantic output contracts for the failure-clustering skill.

Two classes:

  * ``LlmClusterOutput`` — per-cluster shape. The LLM emits a list
    of these; the framework stamps a deterministic ``cluster_id``
    post-hoc so the LLM cannot hallucinate duplicate IDs.
  * ``LlmClusterSetOutput`` — envelope-bound full-list shape. Used
    as ``T`` in Plan 2's ``AbstainableEnvelope[LlmClusterSetOutput]``
    because Databricks JSON Schema strict mode forbids root-level
    arrays — the response_format must wrap the list in an object.
"""
from __future__ import annotations

from typing import Literal

from pydantic import Field

from genie_space_optimizer.optimization.prompt_io import LLMOutputContract
from genie_space_optimizer.optimization.repair_intent import RepairShape


class LlmClusterOutput(LLMOutputContract):
    """One semantic cluster the LLM identified.

    ``cluster_id`` is intentionally absent — the framework stamps it
    deterministically (e.g. ``H001``, ``H002``, …) after validation.
    """

    semantic_theme: str = Field(
        description=(
            "Short LLM-invented label naming the failure pattern. "
            "Examples: 'top-N collapse', 'missing join spec on "
            "customer_id', 'extra defensive WHERE filter dropped'."
        ),
    )
    member_qids: list[str] = Field(
        min_length=1,
        description=(
            "Qids that belong to this cluster. Must be a non-empty "
            "subset of the qids present in the input "
            "PerQidRcaEvidence batch."
        ),
    )
    unifying_evidence: str = Field(
        description=(
            "One paragraph (≤400 chars) explaining why these qids "
            "cluster together. Cite specific signals from each qid's "
            "PerQidRcaEvidence (observed_failure, blame_set, etc.)."
        ),
    )
    suggested_repair_shape: RepairShape = Field(
        description=(
            "Closed-enum repair shape from the Plan 1 catalog. Pick "
            "the closest match. ``OTHER`` is the documented "
            "escape-hatch for novel patterns the catalog does not "
            "enumerate."
        ),
    )
    primary_blame_set: list[str] = Field(
        description=(
            "Fully-qualified table.column references shared by ALL "
            "members of this cluster (intersection of each qid's "
            "blame_set). Empty list is acceptable when the failure "
            "is metadata-level."
        ),
    )
    confidence: Literal["high", "medium", "low"] = Field(
        description=(
            "How confident you are in this clustering. 'high' = the "
            "qids share an obvious unifying signal; 'low' = "
            "best-effort grouping."
        ),
    )


class LlmClusterSetOutput(LLMOutputContract):
    """Envelope-bound full-list shape — one object wrapping the list.

    Used as the ``T`` in Plan 2's
    ``AbstainableEnvelope[LlmClusterSetOutput]``. Empty list is
    acceptable (post-validation may have rejected every cluster);
    decline lives on the envelope, not here.
    """

    clusters: list[LlmClusterOutput] = Field(
        description=(
            "All semantic clusters the LLM identified. Empty list "
            "means 'no clusters found'; decline (insufficient signal) "
            "goes on the envelope's ``declined`` field, not in this "
            "list."
        ),
    )
