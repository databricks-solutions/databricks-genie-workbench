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
    Default ``"raise"``. ``"warn"`` is for prompt-text-drift tolerance
    where we match on ``(stage, iteration, ag_id, cluster_id)`` and
    accept the first matching entry regardless of ``prompt_sha256``.
    ``"prompt_sha_only"`` (Phase 3.6) is for historic tapes captured
    from MLflow traces without iteration/ag breadcrumbs: matches on
    ``(stage, prompt_sha256)`` alone, ignoring iteration/ag/cluster.
    Safe because prompt content is iteration-+-AG-unique.
"""
from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal


MissPolicy = Literal["raise", "warn", "prompt_sha_only"]


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


# Phase 3.6.2 E1 (2026-05-18) — tape format version. Bumped each
# time the on-disk schema gains a new field. ``from_json_file``
# tolerates older versions for back-compat read; capture writes
# the current version.
#   v1 — LLM entries + evals_by_iteration + clusters_by_iteration
#   v2 — Phase 3.5 adds llm_call_log shape + _KNOWN_STAGES vocab
#   v3 — Phase 3.6 E1 adds iteration_payloads (full per-iteration
#        row dicts from genie_opt_iterations, served by replay
#        stubs for state.load_latest_full_iteration et al.)
#   v4 — Phase 3.7 (2026-05-18) adds replay_mode_by_stage: a
#        per-stage dispatch hint consumed by TapeBackedLLMCaller.
#        Unset stages default to "rebuild_and_match" (SHA-based
#        lookup, the v1–v3 behaviour). The single use today is
#        {"lever6_llm": "historic_inject"} for anchor tapes
#        whose lever6 prompts cannot be byte-for-byte rebuilt
#        under replay (see
#        docs/architecture/stage-prompt-fidelity-audit.md).
TAPE_FORMAT_VERSION = 4
_SUPPORTED_FORMAT_VERSIONS: frozenset[int] = frozenset({1, 2, 3, 4})

# Phase 3.7 (2026-05-18) — typed replay-mode vocabulary. New modes
# require a code change here AND in TapeBackedLLMCaller's dispatch.
ReplayMode = Literal["rebuild_and_match", "historic_inject"]
_VALID_REPLAY_MODES: frozenset[str] = frozenset(
    {"rebuild_and_match", "historic_inject"}
)


@dataclass
class LeverLoopTape:
    tape_id: str
    source_run_id: str
    captured_at: str
    entries: list[TapeEntry]
    evals_by_iteration: dict[int, list[dict]] = field(default_factory=dict)
    clusters_by_iteration: dict[int, list[dict]] = field(default_factory=dict)
    rca_cards_by_cluster: dict[str, dict] = field(default_factory=dict)
    # Phase 3.6.2 E1 — per-iteration row dicts from
    # ``genie_opt_iterations`` (rows_json, scores_json,
    # soft_signal_qids, mlflow_run_id, evaluated_count, eval_scope,
    # rolled_back, ...). Replay stubs serve ``state.load_*`` calls
    # from this dict instead of letting them return None under a
    # MagicMock spark.
    iteration_payloads: dict[int, dict] = field(default_factory=dict)
    # Phase 3.7 (2026-05-18) — per-stage replay-mode hint. Keys are
    # ``span_name`` strings (subset of ``_KNOWN_STAGES``); values are
    # in ``_VALID_REPLAY_MODES``. Stages absent from this dict default
    # to ``"rebuild_and_match"``. See
    # ``docs/architecture/tape-replay-protocol.md``.
    replay_mode_by_stage: dict[str, str] = field(default_factory=dict)
    format_version: int = TAPE_FORMAT_VERSION
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
        # Phase 3.6.2 E1 (2026-05-18) — format-version gate. v1 and v2
        # tapes load with their pre-v3 fields; ``iteration_payloads``
        # defaults to empty when missing. v3 tapes MUST carry a
        # non-empty ``iteration_payloads`` dict.
        version_raw = payload.get("format_version")
        if version_raw is None:
            # Legacy capture (pre-Phase 3.6.2). Treat as v1/v2.
            inferred_version = 2 if "llm_call_log" not in payload else 2
            format_version = 2 if "iteration_payloads" not in payload else inferred_version
        else:
            format_version = int(version_raw)
        if format_version not in _SUPPORTED_FORMAT_VERSIONS:
            raise ValueError(
                f"LeverLoopTape: unsupported format_version "
                f"{format_version!r} (supported: "
                f"{sorted(_SUPPORTED_FORMAT_VERSIONS)}). Recapture "
                f"the tape with the current capture script."
            )

        evals_raw = payload.get("evals_by_iteration") or {}
        clusters_raw = payload.get("clusters_by_iteration") or {}
        iter_payloads_raw = payload.get("iteration_payloads") or {}

        # Phase 3.6.1 (2026-05-18) — tape side-tables MUST be
        # 0-indexed. The replay harness queries with ``_iter_num - 1``
        # (see ``harness.py:18933``); a 1-indexed tape silently
        # misses every lookup, leaving the lever loop with no eval
        # rows / no clusters and short-circuiting at
        # ``no_actionable_clusters`` before ever reaching the
        # iterations the postmortems documented. Refuse to load
        # the broken shape so the failure is loud at capture time,
        # not silent at replay time.
        evals_by_iteration = {
            int(k): list(v) for k, v in evals_raw.items()
        }
        clusters_by_iteration = {
            int(k): list(v) for k, v in clusters_raw.items()
        }
        iteration_payloads = {
            int(k): dict(v) for k, v in iter_payloads_raw.items()
            if isinstance(v, dict)
        }
        for label, table in (
            ("evals_by_iteration", evals_by_iteration),
            ("clusters_by_iteration", clusters_by_iteration),
            ("iteration_payloads", iteration_payloads),
        ):
            if not table:
                continue
            keys = sorted(table.keys())
            if keys[0] != 0:
                raise ValueError(
                    f"LeverLoopTape: {label} must be 0-indexed "
                    f"(got keys {keys}). Pre-Phase-3.6.1 tapes need "
                    f"recapture via "
                    f"``scripts/capture_tape_from_mlflow.py`` or "
                    f"``scripts/capture_lever_loop_tape_from_export.py``."
                )

        # Phase 3.6.2 E1 — v3 tapes assert non-empty iteration_payloads.
        # v1/v2 tapes are accepted with empty iteration_payloads (the
        # replay stubs will return None for state.load_* calls, which
        # is the pre-Phase-3.6.2 behavior).
        if format_version >= 3 and not iteration_payloads:
            raise ValueError(
                "LeverLoopTape: format_version 3 tapes must carry a "
                "non-empty ``iteration_payloads`` dict. Either bump "
                "the capture script to populate it or write the tape "
                "with ``format_version: 2``."
            )

        # Phase 3.7 (2026-05-18) — validate replay_mode_by_stage.
        # v1/v2/v3 tapes that omit the field load with the default
        # ``rebuild_and_match`` behaviour for every stage. v4 tapes
        # may carry the field; we enforce the typed vocabulary at
        # load time so a typo or stale capture is loud, not silent.
        replay_mode_raw = payload.get("replay_mode_by_stage") or {}
        if not isinstance(replay_mode_raw, dict):
            raise ValueError(
                f"LeverLoopTape: replay_mode_by_stage must be a dict, "
                f"got {type(replay_mode_raw).__name__}."
            )
        replay_mode_by_stage: dict[str, str] = {}
        for stage_name, mode in replay_mode_raw.items():
            mode_str = str(mode)
            if mode_str not in _VALID_REPLAY_MODES:
                raise ValueError(
                    f"LeverLoopTape: invalid replay_mode "
                    f"{mode_str!r} for stage {stage_name!r} "
                    f"(supported: {sorted(_VALID_REPLAY_MODES)})."
                )
            replay_mode_by_stage[str(stage_name)] = mode_str

        return cls(
            tape_id=str(payload.get("tape_id", "")),
            source_run_id=str(payload.get("source_run_id", "")),
            captured_at=str(payload.get("captured_at", "")),
            entries=entries,
            evals_by_iteration=evals_by_iteration,
            clusters_by_iteration=clusters_by_iteration,
            rca_cards_by_cluster=dict(
                payload.get("rca_cards_by_cluster") or {}
            ),
            iteration_payloads=iteration_payloads,
            replay_mode_by_stage=replay_mode_by_stage,
            format_version=format_version,
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

        # Phase 3.6 (2026-05-17) — historic tapes captured from MLflow
        # traces without iteration/ag breadcrumbs key calls only by
        # (stage, prompt_sha256). The prompt content includes
        # iteration's clusters / RCA cards / AG context, so the SHA is
        # iteration-+-AG-unique in practice and there is no ambiguity.
        if self.miss_policy == "prompt_sha_only":
            for entry in self.entries:
                k = entry.key
                if k.stage == stage and k.prompt_sha256 == sha:
                    return entry

        raise TapeMissError(
            f"No tape entry for stage={stage!r} iteration={iteration} "
            f"ag_id={ag_id!r} cluster_id={cluster_id!r} prompt_sha256={sha}"
        )

    def lookup_by_binding(
        self,
        *,
        stage: str,
        iteration: int,
        ag_id: str,
        cluster_id: str,
    ) -> TapeEntry:
        """Phase 3.7 — find the unique entry for (stage, iteration, ag_id,
        cluster_id) ignoring prompt_sha256.

        Used by ``TapeBackedLLMCaller`` under the ``historic_inject``
        replay mode, where the replay-time prompt cannot be rebuilt
        byte-for-byte and the historic prompt+response must be served
        from the tape directly.

        Raises ``TapeMissError`` if no entry matches. Multiple matches
        are not expected (each (stage, iteration, ag, cluster) tuple is
        unique per production run); the first match wins and a warning
        is emitted only on actual ambiguity.
        """
        matches: list[TapeEntry] = []
        for entry in self.entries:
            k = entry.key
            if (
                k.stage == stage
                and k.iteration == iteration
                and k.ag_id == ag_id
                and k.cluster_id == cluster_id
            ):
                matches.append(entry)
        if not matches:
            raise TapeMissError(
                f"No tape entry for binding stage={stage!r} "
                f"iteration={iteration} ag_id={ag_id!r} "
                f"cluster_id={cluster_id!r} (historic_inject mode)."
            )
        if len(matches) > 1:
            logging.getLogger(__name__).warning(
                "lookup_by_binding: %d entries match (stage=%s "
                "iter=%d ag=%s cluster=%s); returning the first.",
                len(matches), stage, iteration, ag_id, cluster_id,
            )
        return matches[0]
