"""Centralized MLflow run-name builder (Tier 4).

The v3 scheme is ``iter_NN / stage [/ detail] / run_xxxxxxxx`` — descriptive
context first, run id last. This keeps the most useful prefix visible in
MLflow's UI even when the run-name column is truncated, and makes runs
within a single iteration cluster together visually.

- ``iter_NN`` — zero-padded iteration index (``iter_00`` for baseline /
  enrichment / preflight / labeling that happen before the first AG cycle).
- ``stage`` — lifecycle stage (``baseline``, ``enrichment``, ``strategy``,
  ``slice_eval``, ``p0_eval``, ``full_eval``, ``finalize``, ``deploy``,
  ``preflight``, ``labeling_session``).
- ``detail`` — optional disambiguator (``AG_1``, ``pass_1``,
  ``pass_2_confirm``, ``held_out``, ``benchmark_generation``).
- ``run_xxxxxxxx`` — first 8 chars of the optimisation run_id; consistent
  trailing segment so cross-stage joins on ``genie.run_id`` are easy.

No timestamps in names (MLflow already records ``start_time``). Retries
append ``/retry_{k}`` so idempotent writes don't mint fresh names.

History: v1 = ad-hoc ``strategy_1_eval_20260424_132244``;
v2 = ``<run_short>/<stage>/<detail>`` (run id first, hard to scan when
truncated); v3 (current) flips that order so the descriptive segments
land in the visible portion of the column.
"""

from __future__ import annotations


def _short(run_id: str) -> str:
    """Return the first 8 characters of a run_id for grouping."""
    if not run_id:
        return "run"
    return str(run_id)[:8]


def _v3(iteration: int, stage: str, *details: str, run_id: str) -> str:
    """Canonical v3 name builder: ``iter_NN / stage [/ detail] / run_xxx``."""
    parts: list[str] = [f"iter_{int(iteration):02d}", stage]
    parts.extend(d for d in details if d)
    parts.append(f"run_{_short(run_id)}")
    return " / ".join(parts)


def _with_retry(name: str, retry: int) -> str:
    return name if not retry else f"{name} / retry_{retry}"


def baseline_run_name(run_id: str, *, retry: int = 0) -> str:
    return _with_retry(_v3(0, "baseline", run_id=run_id), retry)


def enrichment_run_name(run_id: str, *, detail: str = "snapshot", retry: int = 0) -> str:
    # ``detail`` historically defaulted to "snapshot"; keep behaviour but
    # surface it as a named segment between stage and run id.
    return _with_retry(_v3(0, "enrichment", detail, run_id=run_id), retry)


def strategy_run_name(run_id: str, iteration: int, ag_id: str, *, retry: int = 0) -> str:
    return _with_retry(_v3(iteration, "strategy", ag_id, run_id=run_id), retry)


def slice_eval_run_name(run_id: str, iteration: int, *, retry: int = 0) -> str:
    return _with_retry(_v3(iteration, "slice_eval", run_id=run_id), retry)


def p0_eval_run_name(run_id: str, iteration: int, *, retry: int = 0) -> str:
    return _with_retry(_v3(iteration, "p0_eval", run_id=run_id), retry)


def full_eval_run_name(
    run_id: str, iteration: int, *, pass_index: int = 1, retry: int = 0,
) -> str:
    detail = f"pass_{pass_index}" if pass_index == 1 else f"pass_{pass_index}_confirm"
    return _with_retry(_v3(iteration, "full_eval", detail, run_id=run_id), retry)


def finalize_run_name(
    run_id: str,
    *,
    detail: str = "repeat_pass_1",
    iteration: int = 0,
    retry: int = 0,
) -> str:
    return _with_retry(_v3(iteration, "finalize", detail, run_id=run_id), retry)


def deploy_run_name(
    run_id: str, *, detail: str = "uc", iteration: int = 0, retry: int = 0,
) -> str:
    return _with_retry(_v3(iteration, "deploy", detail, run_id=run_id), retry)


def iteration_outcome_run_name(
    run_id: str, iteration: int, outcome: str, ag_id: str, *, retry: int = 0,
) -> str:
    """Naming for the per-iteration outcome record (accepted vs rolled_back)."""
    return _with_retry(_v3(iteration, outcome, ag_id, run_id=run_id), retry)


def preflight_run_name(
    run_id: str, *, detail: str = "benchmark_generation", retry: int = 0,
) -> str:
    """Pre-iteration benchmark generation / preflight checks."""
    return _with_retry(_v3(0, "preflight", detail, run_id=run_id), retry)


def labeling_run_name(run_id: str, *, retry: int = 0) -> str:
    """Labeling session bootstrapped before the optimisation loop."""
    return _with_retry(_v3(0, "labeling_session", run_id=run_id), retry)


# Tags added to every run, in addition to the name, so operators can filter
# by field without parsing the name.
RUN_NAME_VERSION = "v3"


def default_tags(
    run_id: str,
    *,
    space_id: str = "",
    stage: str = "",
    iteration: int | None = None,
    ag_id: str = "",
) -> dict[str, str]:
    """Build the tag dict added alongside every v3-named run."""
    tags: dict[str, str] = {
        "genie.run_id": str(run_id or ""),
        "genie.run_name_version": RUN_NAME_VERSION,
    }
    if space_id:
        tags["genie.space_id"] = str(space_id)
    if stage:
        tags["genie.stage"] = str(stage)
    if iteration is not None:
        tags["genie.iteration"] = f"{int(iteration):02d}"
    if ag_id:
        tags["genie.ag_id"] = str(ag_id)
    return tags
