"""Lever-loop tape: deterministic recording of LLM calls + per-iteration inputs.

The tape lets the actual ``_run_lever_loop`` execute against frozen production
LLM responses (captured from MLflow traces or ``lever_loop_latest_export_*.json``)
without calling Genie, the LLM, the warehouse, or any Delta writer.

Keying:
    Each LLM call is keyed by ``TapeKey(stage, iteration, ag_id, cluster_id,
    prompt_sha256)``. ``stage`` is the ``span_name`` passed to
    ``optimizer._traced_llm_call`` (e.g. ``"adaptive_strategy"``,
    ``"cluster_driven_synthesis"``). ``ag_id``/``cluster_id`` are empty strings
    when the call is not bound to a specific AG or cluster (e.g. strategist).

Miss policy:
    Default ``"raise"``. ``"warn"`` is for prompt-text-drift tolerance where
    we match on ``(stage, iteration, ag_id, cluster_id)`` and accept the first
    matching entry regardless of ``prompt_sha256``.
"""
from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal


MissPolicy = Literal["raise", "warn"]


# ──────────────────────────────────────────────────────────────────────
# Phase 3.5 (2026-05-17) — closed stage vocabulary.
#
# Every production ``span_name`` literal in ``src/genie_space_optimizer``
# MUST appear in this frozenset. Tapes referencing an unknown stage
# load successfully (forward compat for new call sites added between
# captures and replays) but emit a WARNING. The CI gate
# (``tests/ci/test_known_stages_matches_source.py``) re-derives the
# production set via grep and refuses to merge if drift exceeds zero.
#
# Source-of-truth derivation:
#   grep -ro 'span_name="[^"]*"' src/genie_space_optimizer/
# ──────────────────────────────────────────────────────────────────────
_KNOWN_STAGES: frozenset[str] = frozenset({
    # Stage 1 (three-stage pipeline)
    "stage_1_discovery",
    # Stage 2 / per-lever (some are stale / future; kept for forward
    # compat against older or newer tapes)
    "lever_1_table_column_description",
    "lever_2_mv_column_refinement",
    "lever_3_tvf_routing",
    "lever_4_join_discovery",
    "lever_4_join_discovery_repair",
    "lever_5a_instructions",
    "lever_5b_example_sql",
    "lever_5b_example_sql_for_rca",
    "lever_6_sql_expression",
    # Synthesis-stage callers
    "cluster_driven_example_synthesis",
    "cluster_driven_example_synthesis_retry",
    "cluster_driven_synthesis",  # legacy/test alias
    "preflight_example_synthesis",
    "archetype_learning.synthesize_provisional",
    # Legacy strategist (fallback when Stage-1 returns zero picks)
    "adaptive_strategy",
    "monolithic_strategy_fallback",
    # Proposal-stage callers outside the three-stage pipeline
    "phase_1a_triage",
    "lever1_rca_proposal",
    "lever6_llm",
    "prose_rule_mining",
    "prose_rule_mining_retry",
    "sql_expression_seeding_llm",
    # Space-setup callers (create flow)
    "generate_space_description",
    "generate_sample_questions",
})


def prompt_sha256(prompt: str) -> str:
    """Return the canonical hex SHA256 of a prompt string."""
    return hashlib.sha256(prompt.encode("utf-8")).hexdigest()


class TapeMissError(LookupError):
    """Raised when a tape lookup finds no matching entry under ``raise`` policy."""


@dataclass(frozen=True)
class TapeKey:
    stage: str
    iteration: int
    ag_id: str
    cluster_id: str
    prompt_sha256: str

    def as_tuple(self) -> tuple[str, int, str, str, str]:
        return (
            self.stage,
            self.iteration,
            self.ag_id,
            self.cluster_id,
            self.prompt_sha256,
        )


@dataclass(frozen=True)
class TapeEntry:
    key: TapeKey
    prompt: str
    response_text: str
    response_metadata: dict


@dataclass
class LeverLoopTape:
    tape_id: str
    source_run_id: str
    captured_at: str
    entries: list[TapeEntry]
    evals_by_iteration: dict[int, list[dict]] = field(default_factory=dict)
    clusters_by_iteration: dict[int, list[dict]] = field(default_factory=dict)
    rca_cards_by_cluster: dict[str, dict] = field(default_factory=dict)
    miss_policy: MissPolicy = "raise"

    @classmethod
    def from_json_file(cls, path: Path | str) -> "LeverLoopTape":
        """Load a tape from a JSON file produced by the capture script."""
        path = Path(path)
        with path.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
        return cls._from_payload(payload)

    @classmethod
    def _from_payload(cls, payload: dict) -> "LeverLoopTape":
        entries: list[TapeEntry] = []
        for raw in payload.get("entries", []):
            k = raw["key"]
            entries.append(
                TapeEntry(
                    key=TapeKey(
                        stage=str(k["stage"]),
                        iteration=int(k["iteration"]),
                        ag_id=str(k.get("ag_id", "")),
                        cluster_id=str(k.get("cluster_id", "")),
                        prompt_sha256=str(k["prompt_sha256"]),
                    ),
                    prompt=str(raw.get("prompt", "")),
                    response_text=str(raw.get("response_text", "")),
                    response_metadata=dict(raw.get("response_metadata", {})),
                )
            )
        # Phase 3.5 (2026-05-17) — emit WARNING on unknown stages
        # (forward-compat: loading still succeeds, but capture-side
        # typos or new-call-site drift are visible at load time).
        unknown_stages: set[str] = {
            e.key.stage for e in entries if e.key.stage not in _KNOWN_STAGES
        }
        if unknown_stages:
            logging.getLogger(__name__).warning(
                "LeverLoopTape: unknown stage(s) in tape: %s "
                "(tape may be from a newer or stale capture; loading anyway)",
                sorted(unknown_stages),
            )
        return cls(
            tape_id=str(payload.get("tape_id", "")),
            source_run_id=str(payload.get("source_run_id", "")),
            captured_at=str(payload.get("captured_at", "")),
            entries=entries,
            evals_by_iteration={
                int(k): list(v)
                for k, v in (payload.get("evals_by_iteration") or {}).items()
            },
            clusters_by_iteration={
                int(k): list(v)
                for k, v in (payload.get("clusters_by_iteration") or {}).items()
            },
            rca_cards_by_cluster=dict(
                payload.get("rca_cards_by_cluster") or {}
            ),
            miss_policy=str(payload.get("miss_policy", "raise")),
        )

    def lookup(
        self,
        *,
        stage: str,
        iteration: int,
        ag_id: str,
        cluster_id: str,
        prompt: str,
    ) -> TapeEntry:
        """Return the matching entry or raise / log according to ``miss_policy``."""
        sha = prompt_sha256(prompt)
        for entry in self.entries:
            k = entry.key
            if (
                k.stage == stage
                and k.iteration == iteration
                and k.ag_id == ag_id
                and k.cluster_id == cluster_id
                and k.prompt_sha256 == sha
            ):
                return entry

        if self.miss_policy == "warn":
            for entry in self.entries:
                k = entry.key
                if (
                    k.stage == stage
                    and k.iteration == iteration
                    and k.ag_id == ag_id
                    and k.cluster_id == cluster_id
                ):
                    logging.getLogger(__name__).warning(
                        "tape drift: stage=%s iter=%d ag=%s cluster=%s "
                        "prompt_sha=%s did not match; returning first "
                        "stage/iteration/ag/cluster sibling.",
                        stage, iteration, ag_id, cluster_id, sha,
                    )
                    return entry

        raise TapeMissError(
            f"No tape entry for stage={stage!r} iteration={iteration} "
            f"ag_id={ag_id!r} cluster_id={cluster_id!r} prompt_sha256={sha}"
        )
