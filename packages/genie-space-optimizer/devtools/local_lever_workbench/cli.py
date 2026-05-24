"""Local lever-loop workbench CLI.

Invoked by file path so the package layout stays clearly outside the
deployable ``src/genie_space_optimizer`` tree::

    uv run python devtools/local_lever_workbench/cli.py prepare \\
        --source production-replay \\
        --output devtools/local_lever_workbench/runs/<run_id>/bundle.json

    uv run python devtools/local_lever_workbench/cli.py run \\
        --input devtools/local_lever_workbench/runs/<run_id>/bundle.json \\
        --llm-mode stage1-only

    uv run python devtools/local_lever_workbench/cli.py run \\
        --input devtools/local_lever_workbench/runs/<run_id>/bundle.json \\
        --llm-mode live-databricks \\
        --profile <databricks-cli-profile>

Four subcommands:

* ``capture`` — fetch ``genie_predict_fn`` traces from a production job
  run and write them as a workbench-shaped replay fixture.
* ``prepare`` — normalize a source into a workbench bundle JSON.
* ``probe`` — run only the Stage 1 preflight on a bundle.
* ``run`` — full workbench iteration with funnel report.

The CLI is the only file under ``devtools/local_lever_workbench/`` that
is meant to be executed directly; everything else is plain Python
modules other tools can import.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Make the ``local_lever_workbench`` package importable when this file
# is invoked directly. ``python devtools/local_lever_workbench/cli.py``
# sets sys.path[0] to ``devtools/local_lever_workbench`` itself, which
# is too narrow for absolute imports. Prepend the parent (``devtools/``)
# so ``import local_lever_workbench.x`` resolves.
_DEVTOOLS_DIR = Path(__file__).resolve().parent.parent
if str(_DEVTOOLS_DIR) not in sys.path:
    sys.path.insert(0, str(_DEVTOOLS_DIR))

# Also make the package's ``src/`` and ``tests/`` discoverable when the
# workbench is invoked outside of pytest. This mirrors the pyproject
# pytest pythonpath, but for the standalone CLI.
_PKG_ROOT = _DEVTOOLS_DIR.parent
for extra in ("src", "tests"):
    candidate = _PKG_ROOT / extra
    if candidate.is_dir() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))


# ── Subcommand handlers ──────────────────────────────────────────────


def _cmd_capture(args: argparse.Namespace) -> int:
    from local_lever_workbench.mlflow_eval_capture import (
        capture_eval_rows,
        default_output_path,
        write_capture,
    )

    spec, rows, schema_columns, serialized_space, schema_columns_source = (
        capture_eval_rows(
            job_run_id=args.job_run_id,
            task_key=args.task_key,
            profile=args.profile,
        )
    )
    if not rows:
        print(
            f"WARNING: 0 genie_predict_fn traces matched "
            f"job_id={spec.job_id} task_run_id={spec.task_run_id}. "
            f"Nothing written.",
            file=sys.stderr,
        )
        return 1

    if args.output:
        out = Path(args.output).resolve()
    else:
        docs_root = _PKG_ROOT / "docs"
        if not spec.optimization_run_id:
            print(
                "ERROR: --output is required when the job run carries no "
                "'optimization_run_id' parameter (otherwise the default "
                "docs/runid_analysis/<run_id>/evidence/... path cannot "
                "be resolved).",
                file=sys.stderr,
            )
            return 2
        out = default_output_path(
            docs_root=docs_root,
            optimization_run_id=spec.optimization_run_id,
            task_run_id=spec.task_run_id,
        )
    written = write_capture(
        spec=spec,
        rows=rows,
        output_path=out,
        serialized_space=serialized_space,
        schema_columns=schema_columns,
        schema_columns_source=schema_columns_source,
    )
    print(f"wrote eval capture: {written}")
    print(
        f"  experiment={spec.experiment_name}\n"
        f"  task={spec.task_key} task_run_id={spec.task_run_id} "
        f"optimization_run_id={spec.optimization_run_id}\n"
        f"  eval_rows={len(rows)}\n"
        f"  genie_space_id={spec.genie_space_id or '(unresolved)'}\n"
        f"  schema_columns={len(schema_columns)} (source={schema_columns_source or 'unavailable'})"
    )
    return 0


def _cmd_prepare(args: argparse.Namespace) -> int:
    from local_lever_workbench.input_bundle import (
        DEFAULT_FAKE_SPACE_ID,
        from_bundle_json,
        from_production_replay,
        from_run_analysis_dir,
    )

    if args.source == "production-replay":
        bundle = from_production_replay(
            run_tags=args.run_tag or None,
            qids=args.qid or None,
            space_id=args.space_id or DEFAULT_FAKE_SPACE_ID,
        )
    elif args.source == "run-analysis":
        if not args.bundle_dir:
            print(
                "ERROR: --bundle-dir is required for --source run-analysis",
                file=sys.stderr,
            )
            return 2
        bundle = from_run_analysis_dir(
            Path(args.bundle_dir),
            space_id=args.space_id or DEFAULT_FAKE_SPACE_ID,
        )
    elif args.source == "bundle-json":
        if not args.input:
            print(
                "ERROR: --input is required for --source bundle-json",
                file=sys.stderr,
            )
            return 2
        bundle = from_bundle_json(Path(args.input))
    else:
        print(f"ERROR: unsupported --source {args.source!r}", file=sys.stderr)
        return 2

    out = Path(args.output)
    bundle.to_json(out)
    print(f"wrote bundle: {out}")
    print(
        f"  source_kind={bundle.provenance.source_kind} "
        f"hard_cases={len(bundle.hard_cases)} "
        f"space_id={bundle.space_id}"
    )
    return 0


def _cmd_probe(args: argparse.Namespace) -> int:
    from local_lever_workbench.input_bundle import from_bundle_json
    from local_lever_workbench.stage1_probe import probe_bundle

    bundle = from_bundle_json(Path(args.input))
    result = probe_bundle(bundle)
    for finding in result.findings:
        status = "ok" if not finding.violations else "FAIL"
        print(
            f"[{status}] qid={finding.qid} violations="
            f"{list(finding.violations) or '[]'} "
            f"would_dispatch_llm={finding.would_dispatch_llm}"
        )
    print(f"all_pass={result.all_pass}")
    return 0 if result.all_pass else 1


def _cmd_run(args: argparse.Namespace) -> int:
    from local_lever_workbench.input_bundle import from_bundle_json
    from local_lever_workbench.local_runner import (
        SUPPORTED_APPLY_MODES,
        SUPPORTED_LLM_MODES,
        run_workbench_iteration,
    )
    from local_lever_workbench.models import WorkbenchRunConfig
    from local_lever_workbench.report import build_run_result, write_report
    from local_lever_workbench.stage1_probe import probe_bundle

    if args.llm_mode not in SUPPORTED_LLM_MODES:
        print(
            f"ERROR: --llm-mode must be one of {SUPPORTED_LLM_MODES}",
            file=sys.stderr,
        )
        return 2
    if args.apply_mode not in SUPPORTED_APPLY_MODES:
        print(
            f"ERROR: --apply-mode must be one of {SUPPORTED_APPLY_MODES}",
            file=sys.stderr,
        )
        return 2

    bundle = from_bundle_json(Path(args.input))
    output_dir = Path(args.output_dir).resolve()

    config = WorkbenchRunConfig(
        bundle_path=Path(args.input).resolve(),
        output_dir=output_dir,
        llm_mode=args.llm_mode,
        apply_mode=args.apply_mode,
        tape_path=Path(args.tape_path).resolve() if args.tape_path else None,
        profile=args.profile,
        llm_model=args.llm_model,
        iteration=args.iteration,
    )

    stage1 = probe_bundle(bundle)
    artifacts = run_workbench_iteration(bundle, config)
    result = build_run_result(
        bundle=bundle, config=config, stage1=stage1, artifacts=artifacts,
    )
    json_path, md_path = write_report(result=result, output_dir=output_dir)
    print(f"deepest_stage_reached: {result.deepest_stage_reached}")
    print(f"recorded_patches: {len(result.recorded_patches)}")
    if result.surprises:
        print("surprises:")
        for s in result.surprises:
            print(f"  - {s}")
    print(f"report.json: {json_path}")
    print(f"report.md:   {md_path}")
    # Non-zero exit when surprises are detected so a CI wrapper can
    # treat the workbench as a gate.
    return 1 if result.surprises else 0


# ── Argparse wiring ──────────────────────────────────────────────────


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="local_lever_workbench",
        description=(
            "Developer-only local lever-loop workbench. Not part of the "
            "deployable optimizer wheel. See "
            "docs/architecture/local-lever-loop-workbench.md."
        ),
    )
    subparsers = parser.add_subparsers(dest="cmd", required=True)

    # capture
    p_cap = subparsers.add_parser(
        "capture",
        help=(
            "Fetch genie_predict_fn traces for a production job run and "
            "write a workbench-shaped replay fixture."
        ),
    )
    p_cap.add_argument(
        "--job-run-id",
        required=True,
        help="Parent Databricks job run id (e.g. 538827243617302).",
    )
    p_cap.add_argument(
        "--task-key",
        default="enrichment",
        help=(
            "Task whose post-eval traces to capture "
            "(default: enrichment; lever_loop also valid)."
        ),
    )
    p_cap.add_argument(
        "--profile",
        default=None,
        help="Databricks CLI profile (defaults to DEFAULT).",
    )
    p_cap.add_argument(
        "--output",
        default=None,
        help=(
            "Override the default output path "
            "(docs/runid_analysis/<run_id>/evidence/replay_fixture_..."
            ".json). When omitted the default is used so "
            "'prepare --source run-analysis' picks the fixture up."
        ),
    )
    p_cap.set_defaults(handler=_cmd_capture)

    # prepare
    p_prep = subparsers.add_parser(
        "prepare",
        help="Normalize a source into a workbench bundle JSON.",
    )
    p_prep.add_argument(
        "--source",
        choices=("production-replay", "run-analysis", "bundle-json"),
        default="production-replay",
        help="Where to load eval rows from.",
    )
    p_prep.add_argument(
        "--bundle-dir",
        help="Path to docs/runid_analysis/<run_id> when --source run-analysis.",
    )
    p_prep.add_argument(
        "--input",
        help="Path to an existing bundle JSON when --source bundle-json.",
    )
    p_prep.add_argument(
        "--run-tag",
        action="append",
        default=None,
        help=(
            "Optional run_tag filter for --source production-replay "
            "(e.g. 98ec). Repeatable."
        ),
    )
    p_prep.add_argument(
        "--qid",
        action="append",
        default=None,
        help=(
            "Optional sanitized QID filter for --source production-replay "
            "(e.g. gs_009). Repeatable."
        ),
    )
    p_prep.add_argument(
        "--space-id",
        default="",
        help="Override the placeholder space_id. Must be 32 lowercase hex.",
    )
    p_prep.add_argument(
        "--output",
        required=True,
        help="Output path for the workbench bundle JSON.",
    )
    p_prep.set_defaults(handler=_cmd_prepare)

    # probe
    p_probe = subparsers.add_parser(
        "probe",
        help="Run only the Stage 1 evidence-card preflight on a bundle.",
    )
    p_probe.add_argument("--input", required=True, help="Bundle JSON path.")
    p_probe.set_defaults(handler=_cmd_probe)

    # run
    p_run = subparsers.add_parser(
        "run",
        help="Run one workbench SM iteration end-to-end.",
    )
    p_run.add_argument("--input", required=True, help="Bundle JSON path.")
    p_run.add_argument(
        "--output-dir",
        default=str(
            _DEVTOOLS_DIR
            / "local_lever_workbench"
            / "runs"
            / "latest"
        ),
        help="Directory to write result.json / result.md into.",
    )
    p_run.add_argument(
        "--llm-mode",
        default="stage1-only",
        help="One of: live-databricks, sm-tape, stage1-only.",
    )
    p_run.add_argument(
        "--apply-mode",
        default="fake-record",
        help="V1 supports only fake-record.",
    )
    p_run.add_argument(
        "--tape-path",
        default=None,
        help="JSONL tape path (required for --llm-mode sm-tape).",
    )
    p_run.add_argument(
        "--profile",
        default=None,
        help="Databricks CLI profile for --llm-mode live-databricks.",
    )
    p_run.add_argument(
        "--llm-model",
        default=None,
        help=(
            "Override the LLM_MODEL env var "
            "(e.g. databricks-claude-sonnet-4-6)."
        ),
    )
    p_run.add_argument("--iteration", type=int, default=1)
    p_run.set_defaults(handler=_cmd_run)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    return int(args.handler(args) or 0)


if __name__ == "__main__":
    # Default workbench env footprint — keep the run reproducible
    # across machines unless the operator overrides at the CLI layer.
    os.environ.setdefault("GSO_WORKBENCH", "1")
    raise SystemExit(main())
