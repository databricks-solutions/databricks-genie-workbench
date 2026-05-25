"""Live-LLM-only bug-discovery sweep over the production-replay corpus.

Loops the workbench over every committed sanitized fixture in
``tests/integration/fixtures/production_replay/``, runs ONE iteration
per QID through ``LLM_MODE_LIVE_LLM_ONLY``, applies the v1.7
invariant fuzzer to each run, and writes a markdown report to
``devtools/local_lever_workbench/runs/live-llm-<timestamp>/report.md``.

The sweep is the operator-facing entry point for hunting strategist /
lever prompt regressions. Tape replay cannot catch a regression
where the LLM emits a malformed envelope or a hallucinated patch
type — only a real model serving call exercises that path.

Usage (from the package root)::

    PYTHONPATH=devtools:src:tests \\
        DATABRICKS_AUTH_STORAGE=plaintext \\
        uv run python -m local_lever_workbench.sweeps.live_llm_sweep \\
        --profile fevm-prashanth

Options::

    --profile <name>   Databricks CLI profile (default: fevm-prashanth)
    --qid <suffix>     Filter to a single fixture suffix (e.g. gs_009).
                       Repeatable. Default: every committed fixture.
    --llm-model <name> Override LLM_MODEL env (default: respect existing
                       env / serving endpoint default).
    --output-dir <p>   Write under <p>/report.md and <p>/per_qid/<qid>.json.
                       Default: devtools/local_lever_workbench/runs/live-llm-<ts>/

Exit code is 0 if the sweep ran (regardless of whether bugs were
found — the report flags them) and non-zero only if the sweep
itself crashed.
"""
from __future__ import annotations

import argparse
import dataclasses
import json
import os
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

# Make sibling devtools packages importable when invoked via -m.
_DEVTOOLS_DIR = Path(__file__).resolve().parent.parent.parent
if str(_DEVTOOLS_DIR) not in sys.path:
    sys.path.insert(0, str(_DEVTOOLS_DIR))
_PKG_ROOT = _DEVTOOLS_DIR.parent
for extra in ("src", "tests"):
    candidate = _PKG_ROOT / extra
    if candidate.is_dir() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))


from local_lever_workbench.fuzzer import (  # noqa: E402
    InvariantResult,
    check_all_invariants,
)
from local_lever_workbench.input_bundle import from_production_replay  # noqa: E402
from local_lever_workbench.local_runner import (  # noqa: E402
    LLM_MODE_LIVE_LLM_ONLY,
    LocalRunArtifacts,
    run_workbench_iteration,
    summarize_stage_progress,
)
from local_lever_workbench.models import WorkbenchRunConfig  # noqa: E402


# ── Production replay fixture catalogue ───────────────────────────────


# Names match the suffix after ``__`` in the fixture filenames. We
# pin the full list explicitly so the sweep fails loudly if a fixture
# goes missing rather than silently producing a thinner report.
_DEFAULT_FIXTURES: tuple[str, ...] = (
    "gs_001",
    "gs_009",
    "gs_013",
    "gs_016",
    "gs_021",
    "gs_024",
    "gs_026",
)


# ── Per-QID result ────────────────────────────────────────────────────


@dataclasses.dataclass(frozen=True)
class _PerQidResult:
    """Aggregate of one QID's run through the live workbench."""

    fixture_id: str
    qid: str
    elapsed_seconds: float
    deepest_stage: str
    terminal_reason: str
    terminal_message: str
    invariant_ok: bool
    invariant_violations: tuple[dict, ...]
    crash: str  # empty unless the workbench itself raised

    def to_dict(self) -> dict:
        return {
            "fixture_id": self.fixture_id,
            "qid": self.qid,
            "elapsed_seconds": round(self.elapsed_seconds, 2),
            "deepest_stage": self.deepest_stage,
            "terminal_reason": self.terminal_reason,
            "terminal_message": self.terminal_message,
            "invariant_ok": self.invariant_ok,
            "invariant_violations": list(self.invariant_violations),
            "crash": self.crash,
        }


# ── Sweep core ────────────────────────────────────────────────────────


def _run_one_qid(
    *,
    fixture_id: str,
    profile: str,
    llm_model: str | None,
    output_dir: Path,
) -> _PerQidResult:
    """Run the workbench for a single fixture, capture artefacts + invariants.

    Workbench failures (e.g. profile misconfigured, serving endpoint
    timeout) are caught and surfaced in the report so one bad QID
    cannot abort the sweep. The fuzzer's invariants are then ALSO
    applied so we record both "what the workbench said" and "what
    the SM contract said" for the same run.
    """
    crash = ""
    artifacts: LocalRunArtifacts | None = None
    deepest = ""
    terminal_reason = ""
    terminal_message = ""

    qid_dir = output_dir / "per_qid" / fixture_id
    qid_dir.mkdir(parents=True, exist_ok=True)

    try:
        bundle = from_production_replay(qids=(fixture_id,))
        config = WorkbenchRunConfig(
            bundle_path=qid_dir / "bundle.json",
            output_dir=qid_dir,
            llm_mode=LLM_MODE_LIVE_LLM_ONLY,
            profile=profile,
            llm_model=llm_model,
            iteration=1,
        )
        artifacts = run_workbench_iteration(bundle, config)

        progress = summarize_stage_progress(artifacts)
        # One QID per fixture by construction. ``progress`` may be
        # empty if the runner short-circuited before dispatch (a bug
        # the invariants will also flag).
        if progress:
            deepest = progress[0].deepest_stage
            terminal_reason = progress[0].terminal_reason
            terminal_message = progress[0].terminal_message

        (qid_dir / "stdout.txt").write_text(artifacts.stdout_text)
    except Exception as exc:  # noqa: BLE001 — must not abort the sweep
        crash = f"{type(exc).__name__}: {exc}"
        (qid_dir / "traceback.txt").write_text(traceback.format_exc())

    inv_ok = True
    inv_violations: tuple[dict, ...] = ()
    if artifacts is not None:
        result: InvariantResult = check_all_invariants(artifacts)
        inv_ok = bool(result.ok)
        inv_violations = tuple(
            {
                "invariant_id": v.invariant_id,
                "invariant_name": v.invariant_name,
                "qid": v.qid,
                "detail": v.detail,
                "evidence": dict(v.evidence or {}),
            }
            for v in result.violations
        )

    return _PerQidResult(
        fixture_id=fixture_id,
        qid=fixture_id,  # we filter by suffix so the sanitized QID == fixture_id
        elapsed_seconds=float(getattr(artifacts, "elapsed_seconds", 0.0) or 0.0),
        deepest_stage=deepest,
        terminal_reason=terminal_reason,
        terminal_message=terminal_message,
        invariant_ok=inv_ok,
        invariant_violations=inv_violations,
        crash=crash,
    )


def _render_report(
    results: Iterable[_PerQidResult],
    *,
    profile: str,
    llm_model: str | None,
    started_at: str,
    elapsed_total: float,
) -> str:
    """Render the per-QID results as a single markdown report."""
    results = list(results)
    n_total = len(results)
    n_crashed = sum(1 for r in results if r.crash)
    n_inv_fail = sum(1 for r in results if not r.invariant_ok)
    n_terminated = sum(1 for r in results if r.deepest_stage == "terminated")
    n_accepted = sum(1 for r in results if r.deepest_stage == "accepted")

    lines: list[str] = []
    lines.append("# Live-LLM-Only Workbench Sweep — Bug Discovery Report")
    lines.append("")
    lines.append(f"- Started at: `{started_at}`")
    lines.append(f"- Total elapsed: `{elapsed_total:.1f}s`")
    lines.append(f"- Profile: `{profile}`")
    lines.append(f"- LLM model override: `{llm_model or '(env LLM_MODEL)'}`")
    lines.append(f"- Bundles run: **{n_total}**")
    lines.append(f"- Reached ACCEPTED: **{n_accepted}**")
    lines.append(f"- Reached TERMINATED: **{n_terminated}**")
    lines.append(f"- Workbench crashes: **{n_crashed}**")
    lines.append(f"- Invariant violations: **{n_inv_fail}**")
    lines.append("")
    lines.append("## Summary Table")
    lines.append("")
    lines.append(
        "| Fixture | Elapsed (s) | Deepest stage | Terminal reason | Invariants | Crash |"
    )
    lines.append(
        "|---|---:|---|---|---|---|"
    )
    for r in results:
        inv = "OK" if r.invariant_ok else f"FAIL ({len(r.invariant_violations)})"
        crash = "—" if not r.crash else r.crash[:60]
        terminal = r.terminal_reason or "—"
        lines.append(
            f"| `{r.fixture_id}` | {r.elapsed_seconds:.1f} | "
            f"`{r.deepest_stage or '—'}` | `{terminal}` | {inv} | {crash} |"
        )
    lines.append("")

    # Per-QID detail for any non-clean run.
    flagged = [r for r in results if r.crash or not r.invariant_ok]
    if flagged:
        lines.append("## Flagged Runs (potential new bugs)")
        lines.append("")
        for r in flagged:
            lines.append(f"### `{r.fixture_id}`")
            lines.append("")
            lines.append(f"- Elapsed: {r.elapsed_seconds:.1f}s")
            lines.append(f"- Deepest stage: `{r.deepest_stage or '—'}`")
            if r.terminal_reason:
                lines.append(f"- Terminal reason: `{r.terminal_reason}`")
            if r.terminal_message:
                lines.append(
                    f"- Terminal message: `{r.terminal_message[:200]}`"
                )
            if r.crash:
                lines.append(f"- Workbench crash: `{r.crash}`")
            if not r.invariant_ok:
                lines.append(f"- Invariant violations ({len(r.invariant_violations)}):")
                for v in r.invariant_violations:
                    lines.append(
                        f"  - `{v['invariant_id']}` ({v['invariant_name']}) "
                        f"on `qid={v['qid']}`: {v['detail']}"
                    )
            lines.append("")
    else:
        lines.append("## Flagged Runs")
        lines.append("")
        lines.append("None — every QID terminated cleanly and every invariant held.")
        lines.append("")

    return "\n".join(lines)


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--profile",
        default="fevm-prashanth",
        help="Databricks CLI profile (default: fevm-prashanth).",
    )
    parser.add_argument(
        "--qid",
        action="append",
        default=None,
        help=(
            "Filter to a single fixture suffix (e.g. gs_009). "
            "Repeatable. Default: every committed fixture."
        ),
    )
    parser.add_argument(
        "--llm-model",
        default=None,
        help="Override LLM_MODEL env var.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Output directory (default: runs/live-llm-<UTC timestamp>/).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    fixtures = tuple(args.qid) if args.qid else _DEFAULT_FIXTURES

    if args.output_dir:
        output_dir = Path(args.output_dir).resolve()
    else:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_dir = (
            _DEVTOOLS_DIR / "local_lever_workbench" / "runs" / f"live-llm-{ts}"
        )
    output_dir.mkdir(parents=True, exist_ok=True)

    # Defensive: live mode short-circuits in some integration paths
    # without a stable plaintext credential store.
    os.environ.setdefault("DATABRICKS_AUTH_STORAGE", "plaintext")
    os.environ.setdefault("GSO_WORKBENCH", "1")

    started_at = datetime.now(timezone.utc).isoformat()
    t0 = time.monotonic()

    results: list[_PerQidResult] = []
    for fixture_id in fixtures:
        print(f"[live-llm-sweep] Running fixture {fixture_id!r} ...", flush=True)
        r = _run_one_qid(
            fixture_id=fixture_id,
            profile=args.profile,
            llm_model=args.llm_model,
            output_dir=output_dir,
        )
        status = "OK" if (r.invariant_ok and not r.crash) else "FLAGGED"
        print(
            f"[live-llm-sweep] {fixture_id}: deepest={r.deepest_stage or '—'} "
            f"terminal={r.terminal_reason or '—'} elapsed={r.elapsed_seconds:.1f}s "
            f"-> {status}",
            flush=True,
        )
        results.append(r)
        # Persist per-QID JSON so partial sweeps survive a crash.
        (output_dir / "per_qid" / fixture_id / "result.json").write_text(
            json.dumps(r.to_dict(), indent=2, sort_keys=True)
        )

    elapsed_total = time.monotonic() - t0

    report = _render_report(
        results,
        profile=args.profile,
        llm_model=args.llm_model,
        started_at=started_at,
        elapsed_total=elapsed_total,
    )
    (output_dir / "report.md").write_text(report)
    (output_dir / "results.json").write_text(
        json.dumps(
            {
                "started_at": started_at,
                "elapsed_total_seconds": round(elapsed_total, 2),
                "profile": args.profile,
                "llm_model": args.llm_model,
                "results": [r.to_dict() for r in results],
            },
            indent=2,
            sort_keys=True,
        )
    )
    print(f"[live-llm-sweep] Report written to {output_dir / 'report.md'}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
