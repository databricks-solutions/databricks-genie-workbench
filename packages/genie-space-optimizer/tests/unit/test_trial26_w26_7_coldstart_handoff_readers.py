"""Trial 26 W26.7 — job notebooks must read cross-task values via the
Trial-25 compact-aware reader, never raw ``dbutils.jobs.taskValues.get``.

Root cause this guard locks: Trial 25 W25.2 collapsed every task's
``dbutils.jobs.taskValues.set`` fan-out into a single ``<task>_outputs``
JSON blob (``run_preflight.py`` → ``_handoff.publish_task_outputs``). But
the consumer notebooks (``run_baseline`` / ``run_enrichment`` /
``run_deploy`` / ``run_finalize``) still read upstream values with raw
``dbutils.jobs.taskValues.get(taskKey=…, key=…)``. A raw per-key read
returns nothing once the publisher is compact, so a COLD-START — which
actually runs ``baseline_eval`` (replays skip it, which is why this stayed
latent through 22 replays of the retired anchor parents) — died at
``baseline_eval`` with ``ValueError: No task values with key "run_id"``,
and ``lever_loop`` (where the Trial 26 kit fix lives) was skipped.

The fix routes every cross-task read through
``genie_space_optimizer.jobs._handoff._tv_get`` (compact blob → per-key
fallback → default). This deterministic source-scan guard fails if any
job notebook regresses to a raw cross-task read — the same shape of guard
``check_invariants.sh`` uses for hand-rolled QID extraction.
"""
from __future__ import annotations

import pathlib
import re

JOBS_DIR = (
    pathlib.Path(__file__).resolve().parents[2]
    / "src"
    / "genie_space_optimizer"
    / "jobs"
)

# A raw cross-task read. ``_handoff._tv_get`` / ``_tv_get_raw`` are the
# ONLY sanctioned call sites and live in ``_handoff.py`` (not ``run_*``),
# so scanning only ``run_*.py`` keeps the implementation exempt.
_RAW_GET = re.compile(r"dbutils\.jobs\.taskValues\.get\s*\(")


def _strip_comments(src: str) -> str:
    """Drop ``#`` comments (full-line and trailing) so a documented
    mention of the forbidden pattern in a docstring/comment does not trip
    the guard. The read sites never carry a ``#`` inside a string literal,
    so the crude split is sufficient for a source-scan guard."""
    return "\n".join(line.split("#", 1)[0] for line in src.splitlines())


def test_job_notebooks_use_compact_aware_reader_not_raw_taskvalues_get():
    offenders: dict[str, int] = {}
    scanned = 0
    for f in sorted(JOBS_DIR.glob("run_*.py")):
        scanned += 1
        code = _strip_comments(f.read_text())
        hits = _RAW_GET.findall(code)
        if hits:
            offenders[f.name] = len(hits)

    assert scanned >= 4, (
        f"guard precondition: expected to scan the job notebooks, found "
        f"only {scanned} run_*.py under {JOBS_DIR}"
    )
    assert not offenders, (
        "Job notebooks must read cross-task values via "
        "genie_space_optimizer.jobs._handoff._tv_get (Trial-25 compact-blob "
        "aware), NOT raw dbutils.jobs.taskValues.get(...). A raw per-key "
        "read silently breaks on a cold-start once the publisher emits the "
        "compact <task>_outputs blob (Trial 26 W26.7 RCA). Offending files: "
        f"{offenders}"
    )


def test_compact_aware_consumers_import_tv_get():
    """The four DAG consumers that read preflight/baseline/lever_loop
    outputs must import the compact-aware reader (or a getter that wraps
    it). Belt-and-suspenders alongside the negative scan above."""
    consumers = [
        "run_baseline.py",
        "run_enrichment.py",
        "run_deploy.py",
        "run_finalize.py",
    ]
    missing: list[str] = []
    for name in consumers:
        src = (JOBS_DIR / name).read_text()
        if "_tv_get" not in src and "get_run_context" not in src:
            missing.append(name)
    assert not missing, (
        "These DAG consumer notebooks must import the compact-aware handoff "
        f"reader (_tv_get or a get_*_state/context wrapper): {missing}"
    )
