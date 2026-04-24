"""Centralized MLflow run-name builder (Tier 4).

The previous naming scheme mixed ad-hoc timestamps (``strategy_1_eval_20260424_132244``)
with short-hashed run IDs (``enrichment_snapshot_e9c0b491-ab``). MLflow's UI
sorts runs lexicographically by name, so timestamp suffixes actively fought
sort order, and run-ID hashes were truncated differently per call site.

The v2 scheme is ``<run_short>/<stage>/<detail>``:

- ``run_short`` = ``run_id[:8]`` — fixed-length unique prefix, groups all
  runs for a single optimisation run together in the UI.
- ``stage`` names encode lifecycle order with zero-padded iteration indices
  (``iter_01_strategy``, ``iter_02_full_eval``) so MLflow's lex sort gives
  the expected chronological order.
- ``detail`` disambiguates within a stage (``run_1``, ``run_2_confirm``,
  ``AG1``).

No timestamps in names (MLflow already records ``start_time``). Retries
append ``/retry_{k}`` so idempotent writes don't mint fresh names.
"""

from __future__ import annotations


def _short(run_id: str) -> str:
    """Return the first 8 characters of a run_id for grouping."""
    if not run_id:
        return "run"
    return str(run_id)[:8]


def baseline_run_name(run_id: str, *, retry: int = 0) -> str:
    name = f"{_short(run_id)}/baseline"
    return name if not retry else f"{name}/retry_{retry}"


def enrichment_run_name(run_id: str, *, detail: str = "snapshot", retry: int = 0) -> str:
    name = f"{_short(run_id)}/enrichment/{detail}"
    return name if not retry else f"{name}/retry_{retry}"


def strategy_run_name(run_id: str, iteration: int, ag_id: str, *, retry: int = 0) -> str:
    name = f"{_short(run_id)}/iter_{iteration:02d}_strategy/{ag_id}"
    return name if not retry else f"{name}/retry_{retry}"


def slice_eval_run_name(run_id: str, iteration: int, *, retry: int = 0) -> str:
    name = f"{_short(run_id)}/iter_{iteration:02d}_slice_eval"
    return name if not retry else f"{name}/retry_{retry}"


def p0_eval_run_name(run_id: str, iteration: int, *, retry: int = 0) -> str:
    name = f"{_short(run_id)}/iter_{iteration:02d}_p0_eval"
    return name if not retry else f"{name}/retry_{retry}"


def full_eval_run_name(
    run_id: str, iteration: int, *, pass_index: int = 1, retry: int = 0,
) -> str:
    detail = "run_1" if pass_index == 1 else f"run_{pass_index}_confirm"
    name = f"{_short(run_id)}/iter_{iteration:02d}_full_eval/{detail}"
    return name if not retry else f"{name}/retry_{retry}"


def finalize_run_name(run_id: str, *, detail: str = "repeat_pass_1", retry: int = 0) -> str:
    name = f"{_short(run_id)}/finalize/{detail}"
    return name if not retry else f"{name}/retry_{retry}"


def deploy_run_name(run_id: str, *, detail: str = "uc", retry: int = 0) -> str:
    name = f"{_short(run_id)}/deploy/{detail}"
    return name if not retry else f"{name}/retry_{retry}"


def iteration_outcome_run_name(
    run_id: str, iteration: int, outcome: str, ag_id: str, *, retry: int = 0,
) -> str:
    """Naming for the per-iteration outcome record (accepted vs rolled_back)."""
    name = f"{_short(run_id)}/iter_{iteration:02d}_{outcome}/{ag_id}"
    return name if not retry else f"{name}/retry_{retry}"


# Tags added to every run, in addition to the name, so operators can filter
# by field without parsing the name.
RUN_NAME_VERSION = "v2"


def default_tags(
    run_id: str,
    *,
    space_id: str = "",
    stage: str = "",
    iteration: int | None = None,
    ag_id: str = "",
) -> dict[str, str]:
    """Build the tag dict added alongside every v2-named run."""
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
