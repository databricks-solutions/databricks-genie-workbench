#!/usr/bin/env python3
"""Rewrite stale ``expected_asset`` values post-B1.

Part of the Group-C data-hygiene migration from the ``baseline-eval-fix``
plan. Many benchmark rows were authored when
:func:`genie_space_optimizer.common.genie_client.detect_asset_type`
mis-classified ``mv_*``-prefixed tables as metric views. B1 tightens the
classifier; this script brings stored ``expected_asset`` values in line
with the corrected detection so ``asset_routing`` scoring reports the
right verdicts.

Strategy:

1. Read each benchmark row.
2. Re-run ``detect_asset_type(expected_sql, mv_names=known_mvs)`` — the
   B1-corrected classifier.
3. Compare with the stored ``expected_asset``.
4. If different and the stored value looks like a type category (not a
   table name), propose a rewrite. If it looks like a table name, set
   ``expected_asset_hint`` instead (authoring hint — B2 pathway) without
   touching the original ``expected_asset`` so the authoring intent is
   preserved for human review.
5. Emit a JSON + CSV diff for PR review. ``--apply`` writes back.

Read-only by default. Always run once without ``--apply`` and review the
diff before committing.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
from io import StringIO
from pathlib import Path
from typing import Any

from genie_space_optimizer.common.genie_client import detect_asset_type

logger = logging.getLogger("migrate_expected_asset")

_VALID_CATEGORIES = frozenset({"MV", "TVF", "TABLE", "NONE"})


def _is_category(value: Any) -> bool:
    return isinstance(value, str) and value.strip().upper() in _VALID_CATEGORIES


def migrate_expected_asset(
    benchmarks: list[dict[str, Any]],
    mv_names: list[str] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return ``(new_benchmarks, diff_entries)``.

    * ``new_benchmarks`` is shallow-copied with either ``expected_asset``
      or ``expected_asset_hint`` updated where the detector disagrees.
    * ``diff_entries`` lists every rewritten row.
    """
    new_rows: list[dict[str, Any]] = []
    diff: list[dict[str, Any]] = []

    for row in benchmarks:
        row_copy = dict(row)
        sql = row_copy.get("expected_sql") or row_copy.get("expected_response") or ""
        detected = detect_asset_type(sql, mv_names=mv_names)
        stored = row_copy.get("expected_asset", "")
        stored_hint = row_copy.get("expected_asset_hint", "")

        if not sql:
            new_rows.append(row_copy)
            continue
        if detected == (str(stored).strip().upper() or "") and not stored_hint:
            new_rows.append(row_copy)
            continue

        entry: dict[str, Any] = {
            "id": row_copy.get("id") or row_copy.get("question_id"),
            "expected_sql_snippet": sql.strip().splitlines()[0][:120] if sql else "",
            "stored_expected_asset": stored,
            "detected_expected_asset": detected,
        }

        if _is_category(stored):
            if detected != str(stored).strip().upper():
                entry["action"] = "rewrite_expected_asset"
                entry["new_expected_asset"] = detected
                row_copy["expected_asset"] = detected
                diff.append(entry)
        else:
            # Stored is a table name (legacy schema). Preserve it for human
            # review; set the hint so the B2 path takes over at eval time.
            hint = detected
            if stored_hint and str(stored_hint).strip().upper() in _VALID_CATEGORIES:
                hint = str(stored_hint).strip().upper()
            entry["action"] = "set_expected_asset_hint"
            entry["new_expected_asset_hint"] = hint
            if row_copy.get("expected_asset_hint") != hint:
                row_copy["expected_asset_hint"] = hint
                diff.append(entry)

        new_rows.append(row_copy)

    return new_rows, diff


def summarize_diff(diff: list[dict[str, Any]], total_rows: int) -> dict[str, Any]:
    by_action: dict[str, int] = {}
    for entry in diff:
        by_action[entry["action"]] = by_action.get(entry["action"], 0) + 1
    return {
        "total_rows": total_rows,
        "rewrites": len(diff),
        "by_action": by_action,
        "entries": diff,
    }


def diff_to_csv(diff: list[dict[str, Any]]) -> str:
    if not diff:
        return ""
    buf = StringIO()
    writer = csv.DictWriter(
        buf,
        fieldnames=[
            "id",
            "action",
            "stored_expected_asset",
            "detected_expected_asset",
            "new_expected_asset",
            "new_expected_asset_hint",
            "expected_sql_snippet",
        ],
        extrasaction="ignore",
    )
    writer.writeheader()
    writer.writerows(diff)
    return buf.getvalue()


# ─────────────────────────────────────────────────────────────────────────────
# I/O adapters (same pattern as dedupe_benchmark_qids.py)
# ─────────────────────────────────────────────────────────────────────────────
def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False))
            fh.write("\n")


def _read_uc(uc_schema: str, domain: str) -> list[dict[str, Any]]:
    from genie_space_optimizer.optimization.benchmarks import (
        load_benchmarks_from_dataset,
    )

    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:  # pragma: no cover
        raise SystemExit(
            "PySpark not importable — use --jsonl offline mode."
        ) from exc

    spark = SparkSession.builder.getOrCreate()
    return load_benchmarks_from_dataset(spark, uc_schema, domain)


def _write_uc(uc_schema: str, domain: str, rows: list[dict[str, Any]]) -> None:
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:  # pragma: no cover
        raise SystemExit("PySpark not importable — cannot --apply UC.") from exc

    spark = SparkSession.builder.getOrCreate()
    table = f"{uc_schema}.genie_benchmarks_{domain}"
    df = spark.createDataFrame(rows)
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    logger.info("Wrote %d rows to %s", len(rows), table)


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────
def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--uc-schema", help="catalog.schema holding genie_benchmarks_<domain>")
    p.add_argument("--domain", help="Benchmark domain (e.g. cost, booking)")
    p.add_argument("--jsonl", type=Path, help="Offline JSONL benchmark export")
    p.add_argument("--output", type=Path, help="Where to save the JSON diff")
    p.add_argument("--csv", type=Path, help="Optional CSV export (human-friendly diff)")
    p.add_argument("--mv-names", nargs="*", default=None, help="Known metric-view table names")
    p.add_argument("--apply", action="store_true", help="Write changes back (dry-run by default)")
    p.add_argument("--verbose", "-v", action="count", default=0)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=max(logging.INFO - 10 * args.verbose, logging.DEBUG),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if args.jsonl:
        src = _read_jsonl(args.jsonl)
    elif args.uc_schema and args.domain:
        src = _read_uc(args.uc_schema, args.domain)
    else:
        logger.error(
            "Must supply either --jsonl PATH or (--uc-schema AND --domain)."
        )
        return 2

    new_rows, diff = migrate_expected_asset(src, mv_names=args.mv_names)
    report = summarize_diff(diff, total_rows=len(src))

    report_json = json.dumps(report, indent=2, ensure_ascii=False)
    if args.output:
        args.output.write_text(report_json, encoding="utf-8")
        logger.info("Wrote JSON diff to %s", args.output)
    else:
        sys.stdout.write(report_json + "\n")

    if args.csv and diff:
        args.csv.write_text(diff_to_csv(diff), encoding="utf-8")
        logger.info("Wrote CSV diff to %s", args.csv)

    if not args.apply:
        logger.info("Dry-run only (pass --apply to write changes).")
        return 0

    if report["rewrites"] == 0:
        logger.info("No stale expected_asset rows — nothing to write.")
        return 0

    if args.jsonl:
        _write_jsonl(args.jsonl, new_rows)
    else:
        _write_uc(args.uc_schema, args.domain, new_rows)

    return 0


if __name__ == "__main__":  # pragma: no cover - thin CLI wrapper.
    raise SystemExit(main())
