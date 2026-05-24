"""Bundle normalization helpers for the local lever-loop workbench.

Three input shapes are supported:

* ``from_production_replay()`` — read the committed sanitized
  ``tests/integration/fixtures/production_replay/*.json`` corpus and
  emit a bundle. Each case carries a real-row hard QID with typed RCA
  evidence already attached.

* ``from_bundle_json()`` — load a previously serialised
  ``WorkbenchInputBundle`` JSON file.

* ``from_run_analysis_dir()`` — best-effort normalization of a
  ``docs/runid_analysis/<run_id>/`` postmortem bundle. Looks for
  ``evidence/replay_fixture_from_latest_export_*.json`` (the captured
  iter-0 eval rows) and falls back to per-iteration
  ``stages/01_input/input.json`` files. Returns an explicit error when
  no eval rows are recoverable.

All helpers are pure with respect to network and Databricks state.
They read only from the local filesystem.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, Sequence

from local_lever_workbench.models import (
    WorkbenchHardCase,
    WorkbenchInputBundle,
    WorkbenchProvenance,
)


# A 32-char lowercase hex space_id satisfies the Genie API ID-format
# validator the applier chains through. The workbench never actually
# patches a real space, so a stable placeholder is safe.
DEFAULT_FAKE_SPACE_ID = "deadbeefcafebabe1234567890abcdef"


def minimal_metadata_snapshot() -> dict:
    """Return the smallest ``serialized_space`` that survives the strict
    SerializedSpace pydantic model validator the applier runs.

    Mirrors ``tests/integration/fake_workspace_client.
    minimal_valid_metadata_snapshot``. Duplicated here (rather than
    imported) so the workbench module graph does not depend on
    ``tests/``. If the production validator changes, this snapshot
    must be updated in lockstep with the test fixture.
    """
    return {
        "version": 1,
        "data_sources": {
            "tables": [],
            "metric_views": [],
        },
        "instructions": {
            "example_question_sqls": [],
            "text_instructions": [],
        },
        "config": {
            "sample_questions": [],
        },
    }


# ── Source 1: production_replay corpus ────────────────────────────────


def _production_replay_dir() -> Path:
    """Return the absolute path to the committed sanitized corpus.

    The workbench resolves this relative to the package root so it
    works whether invoked via path (``python devtools/...``) or via
    pytest from any working directory.
    """
    # devtools/local_lever_workbench/input_bundle.py → parents[2] =
    # packages/genie-space-optimizer.
    pkg_root = Path(__file__).resolve().parents[2]
    return (
        pkg_root / "tests" / "integration" / "fixtures" / "production_replay"
    )


def from_production_replay(
    *,
    run_tags: Sequence[str] | None = None,
    qids: Sequence[str] | None = None,
    space_id: str = DEFAULT_FAKE_SPACE_ID,
) -> WorkbenchInputBundle:
    """Build a bundle from the committed sanitized production replay corpus.

    Filters: when ``run_tags`` or ``qids`` are supplied, the bundle
    only includes cases that match. The unsanitized source run id is
    preserved in the provenance so postmortems can cross-reference,
    but the row payloads themselves are the sanitized snapshots.
    """
    corpus_dir = _production_replay_dir()
    if not corpus_dir.exists():
        raise FileNotFoundError(
            f"Production replay corpus not found at {corpus_dir}. "
            f"The workbench requires the committed sanitized fixtures. "
            f"Run from the package root or check the corpus is present."
        )

    cases: list[WorkbenchHardCase] = []
    source_artifacts: list[str] = []
    source_run_id = ""
    run_tag_filter = frozenset(run_tags or ())
    qid_filter = frozenset(qids or ())

    for path in sorted(corpus_dir.glob("*.json")):
        stem = path.stem
        if "__" not in stem:
            continue
        run_tag, qid = stem.split("__", 1)
        if run_tag_filter and run_tag not in run_tag_filter:
            continue
        if qid_filter and qid not in qid_filter:
            continue

        payload = json.loads(path.read_text())
        if payload.get("_schema_version") != "production_case_v1":
            continue
        prov = payload.get("_provenance") or {}
        if not source_run_id:
            source_run_id = str(prov.get("source_run_id") or "")
        source_artifacts.append(str(path.relative_to(corpus_dir.parent.parent.parent)))

        cases.append(
            WorkbenchHardCase(
                qid=str(payload.get("qid") or ""),
                row=dict(payload.get("row") or {}),
                typed_evidence=(
                    dict(payload["typed_evidence"])
                    if isinstance(payload.get("typed_evidence"), dict)
                    else None
                ),
                expected_card_violations=tuple(
                    str(v)
                    for v in payload.get("expected_card_violations") or ()
                ),
            )
        )

    if not cases:
        raise ValueError(
            f"No production replay cases matched filters "
            f"run_tags={list(run_tag_filter) or 'ALL'} "
            f"qids={list(qid_filter) or 'ALL'}. Available cases live "
            f"under {corpus_dir}."
        )

    provenance = WorkbenchProvenance(
        source_kind="production_replay",
        source_run_id=source_run_id,
        source_artifacts=tuple(source_artifacts),
        notes=(
            "Cases were loaded from the committed sanitized production "
            "replay corpus. Row shapes mirror real production hard-QID "
            "rows; literals are sanitized per the corpus SCHEMA.md."
        ),
    )
    return WorkbenchInputBundle(
        provenance=provenance,
        space_id=space_id,
        hard_cases=tuple(cases),
        metadata_snapshot=minimal_metadata_snapshot(),
    )


# ── Source 2: previously-serialised bundle JSON ───────────────────────


def from_bundle_json(path: Path) -> WorkbenchInputBundle:
    """Load a bundle directly from a JSON file produced by ``prepare``.

    Useful for replaying an exact normalized input through different
    LLM modes without re-normalizing.
    """
    return WorkbenchInputBundle.from_json_file(Path(path))


# ── Source 3: docs/runid_analysis/<run_id>/ postmortem bundle ─────────


_WORKBENCH_CAPTURE_SCHEMA = "workbench_eval_capture_v1"


def _replay_fixture_sort_key(path: Path) -> tuple[int, float, str]:
    """Sort key prioritising the highest-fidelity ``replay_fixture_*`` artefact.

    Older fixtures in ``docs/runid_analysis/*/evidence/`` contain only a
    projected three-key row shape (``arbiter``, ``question_id``,
    ``result_correctness``) nested under ``iterations[].eval_rows``.
    The PR-1 ``capture`` CLI writes a much richer
    ``workbench_eval_capture_v1`` payload with full per-trace question
    text, SQL, and judge metadata at the top level.

    The legacy alphabetic sort tied to the trailing ``task_run_id``
    (a snowflake-style random integer) would non-deterministically
    pick either shape. We instead prefer:
      1. ``_schema_version == workbench_eval_capture_v1`` (rank 0)
      2. all other replay fixtures (rank 1)
    Ties fall back to file mtime (newest first) and the file name for
    determinism.
    """
    rank = 1
    try:
        with path.open("r", encoding="utf-8") as fh:
            # Peek only the leading bytes; the schema marker is in the
            # first object so this avoids parsing megabyte payloads.
            head = fh.read(2048)
        if _WORKBENCH_CAPTURE_SCHEMA in head:
            rank = 0
    except OSError:
        rank = 1
    try:
        mtime = path.stat().st_mtime
    except OSError:
        mtime = 0.0
    return (rank, -mtime, path.name)


def _iter_candidate_eval_row_files(bundle_dir: Path) -> Iterable[Path]:
    """Yield candidate JSON files inside a run-analysis bundle that may
    carry eval rows. Search order favours captured replay fixtures
    over per-iteration stage inputs because the former is closer to
    a faithful production row shape.

    ``replay_fixture_from_latest_export_*.json`` artefacts are sorted
    with the highest trailing task_run_id first so a fresh MLflow
    capture always wins over an older projected fixture committed
    alongside it.
    """
    evidence = bundle_dir / "evidence"
    if evidence.is_dir():
        for p in sorted(
            evidence.glob("replay_fixture_from_latest_export_*.json"),
            key=_replay_fixture_sort_key,
        ):
            yield p
        for p in sorted(evidence.glob("analysis_inputs_*.json")):
            yield p
        gso = evidence / "gso_postmortem_bundle"
        if gso.is_dir():
            for stage_input in sorted(gso.rglob("01_input/input.json")):
                yield stage_input


def _extract_rows_from_payload(payload: object) -> list[dict]:
    """Best-effort scan of a JSON document for a list of eval-row dicts.

    Postmortem artefacts pack eval rows under various keys
    (``eval_rows``, ``iterations[i].eval_rows``, ``rows``, ...). The
    workbench accepts any of them so the operator does not have to
    pre-shape the bundle. Returns an empty list if no recognisable
    eval-row list is found.
    """
    if isinstance(payload, list):
        rows = [r for r in payload if isinstance(r, dict) and r]
        return rows if rows else []
    if not isinstance(payload, dict):
        return []
    for key in ("eval_rows", "rows", "hard_rows", "production_rows"):
        v = payload.get(key)
        if isinstance(v, list):
            rows = [r for r in v if isinstance(r, dict) and r]
            if rows:
                return rows
    iters = payload.get("iterations")
    if isinstance(iters, list):
        for it in iters:
            if isinstance(it, dict) and isinstance(it.get("eval_rows"), list):
                rows = [r for r in it["eval_rows"] if isinstance(r, dict) and r]
                if rows:
                    return rows
    return []


def from_run_analysis_dir(
    bundle_dir: Path,
    *,
    space_id: str = DEFAULT_FAKE_SPACE_ID,
    max_hard_cases: int = 25,
) -> WorkbenchInputBundle:
    """Normalize a ``docs/runid_analysis/<run_id>/`` bundle into a workbench bundle.

    This is the "operator captured a real run, hand it to the workbench"
    path. The function inspects common artefact filenames and admits
    rows the same way the production state machine admits them
    (``admit_eval_rows`` from ``eval_row_admission``). Rows that are
    not hard failures are dropped so the workbench focuses on the
    same QIDs the production optimizer would have advanced.
    """
    from genie_space_optimizer.optimization.eval_row_admission import (
        admit_eval_rows,
    )

    bundle_dir = Path(bundle_dir).resolve()
    if not bundle_dir.is_dir():
        raise FileNotFoundError(
            f"Run-analysis bundle directory not found: {bundle_dir}. "
            f"Pass a path like docs/runid_analysis/<optimization_run_id>."
        )

    source_artifacts: list[str] = []
    seen_qids: set[str] = set()
    collected_rows: list[dict] = []
    captured_serialized_space: dict | None = None
    captured_schema_columns: list[str] = []
    captured_schema_columns_source: str = ""
    for path in _iter_candidate_eval_row_files(bundle_dir):
        try:
            payload = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        rows = _extract_rows_from_payload(payload)
        if not rows:
            continue
        source_artifacts.append(str(path.relative_to(bundle_dir.parent)))
        for row in rows:
            collected_rows.append(row)
        # Trial 13j — v2 capture payloads carry ``serialized_space`` +
        # ``schema_columns`` at top-level. Lift them so the loader can
        # populate ``metadata_snapshot["schema_columns"]`` for the
        # downstream Trial 13i ``_derive_schema_columns`` priority
        # chain (step 1 fires). v1 payloads have neither key and the
        # derivation chain falls back to step 3/4 as before.
        #
        # Trial 14 — accept both ``workbench_eval_capture_v2`` and
        # ``workbench_eval_capture_v2.1``. v2.1 adds typed
        # ``metadata/<judge>/blame_set_structured`` flat keys per
        # row; they are additive and the lift logic for
        # ``serialized_space`` / ``schema_columns`` is identical.
        if isinstance(payload, dict) and payload.get(
            "_schema_version"
        ) in ("workbench_eval_capture_v2", "workbench_eval_capture_v2.1"):
            ss = payload.get("serialized_space")
            if isinstance(ss, dict):
                captured_serialized_space = ss
            cols = payload.get("schema_columns")
            if isinstance(cols, list):
                captured_schema_columns = [str(c) for c in cols if c]
            prov = payload.get("_provenance") or {}
            if isinstance(prov, dict):
                captured_schema_columns_source = str(
                    prov.get("schema_columns_source") or ""
                )
        if collected_rows:
            break

    if not collected_rows:
        raise ValueError(
            f"No recoverable eval rows under {bundle_dir}. The workbench "
            f"requires either replay_fixture_from_latest_export_*.json or "
            f"a stages/01_input/input.json with an 'eval_rows' list. "
            f"V1 does not fall back to MLflow traces alone — see the "
            f"workbench architecture doc for why."
        )

    admitted = admit_eval_rows(collected_rows)
    hard_rows = list(admitted.hard_rows)[:max_hard_cases]

    cases: list[WorkbenchHardCase] = []
    for row in hard_rows:
        from genie_space_optimizer.optimization._qid_extraction import (
            extract_question_id,
        )
        qid, _src = extract_question_id(dict(row))
        if not qid or qid in seen_qids:
            continue
        seen_qids.add(qid)
        cases.append(
            WorkbenchHardCase(
                qid=qid,
                row=dict(row),
                typed_evidence=None,
                expected_card_violations=(),
            )
        )

    if not cases:
        raise ValueError(
            f"Bundle directory {bundle_dir} carried eval rows but none "
            f"were admitted as hard failures (row_is_hard_failure=False "
            f"for every row). Nothing for the workbench to do."
        )

    provenance = WorkbenchProvenance(
        source_kind="run_analysis",
        source_run_id=str(bundle_dir.name),
        source_artifacts=tuple(source_artifacts),
        notes=(
            "Eval rows extracted from a captured run-analysis bundle. "
            "Admission filters mirror the production state machine. "
            "Rows are NOT re-sanitized — keep the bundle on the local "
            "filesystem only."
        ),
    )
    # Trial 13j — merge v2 ``serialized_space`` (when present) over the
    # minimal placeholder so the strict ``SerializedSpace`` pydantic
    # validator the applier runs continues to accept the snapshot, AND
    # so the Trial 13i ``_derive_schema_columns`` chain finds the
    # ``data_sources.tables`` shape it expects at priority step 1.
    metadata_snapshot = minimal_metadata_snapshot()
    if captured_serialized_space:
        merged = dict(metadata_snapshot)
        for key, value in captured_serialized_space.items():
            if str(key).startswith("_"):
                # Drop convenience keys ``fetch_space_config`` adds
                # (``_parsed_space``, ``_tables``, ...); they are not
                # part of the ``SerializedSpace`` schema and would fail
                # the pydantic strict-extra validator.
                continue
            merged[str(key)] = value
        metadata_snapshot = merged
    if captured_schema_columns:
        metadata_snapshot["schema_columns"] = list(captured_schema_columns)
    if captured_schema_columns_source:
        metadata_snapshot["_schema_columns_source"] = captured_schema_columns_source
    return WorkbenchInputBundle(
        provenance=provenance,
        space_id=space_id,
        hard_cases=tuple(cases),
        metadata_snapshot=metadata_snapshot,
    )


__all__ = [
    "DEFAULT_FAKE_SPACE_ID",
    "from_bundle_json",
    "from_production_replay",
    "from_run_analysis_dir",
    "minimal_metadata_snapshot",
]
