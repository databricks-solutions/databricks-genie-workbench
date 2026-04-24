#!/usr/bin/env python3
"""Dedupe ``question_id`` (``id``) collisions in a benchmark table.

Part of the Group-C data-hygiene migration from the ``baseline-eval-fix``
plan. Collisions inflate aggregate counts, confuse the run-scoped scorer
cache, and make trace-recovery strategies (A3) ambiguous.

Strategy (deterministic, row-order stable):

1. Walk the benchmark rows in insertion order.
2. The *first* occurrence of a given ``question_id`` keeps the id as-is.
3. Subsequent duplicates get a suffix — ``:v2``, ``:v3``, … in visitation
   order.
4. Emit a JSON diff of ``{old_id, new_id, question_snippet}`` for every
   rewritten row, plus a totals header.

Read-only by default. Pass ``--apply`` to write the changes back; pass
``--output`` to save the JSON diff for PR review.

Target sources:
- Unity Catalog Delta table ``{uc_schema}.genie_benchmarks_{domain}``
  (the canonical production layout; see
  :func:`genie_space_optimizer.optimization.benchmarks.load_benchmarks_from_dataset`).
- Local JSON / JSONL file (for offline migrations and CI tests).

Usage examples::

    # Dry-run against UC, print diff to stdout.
    python scripts/dedupe_benchmark_qids.py \
        --uc-schema my_cat.my_schema --domain cost

    # Apply and save the diff.
    python scripts/dedupe_benchmark_qids.py \
        --uc-schema my_cat.my_schema --domain cost \
        --apply --output diff.json

    # Offline mode (JSONL file in -> JSONL file out).
    python scripts/dedupe_benchmark_qids.py \
        --jsonl benchmarks.jsonl --apply --output diff.json
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any, Iterable

logger = logging.getLogger("dedupe_benchmark_qids")


# ─────────────────────────────────────────────────────────────────────────────
# Core dedupe primitive — pure, easy to unit test.
# ─────────────────────────────────────────────────────────────────────────────
def dedupe_question_ids(
    benchmarks: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return ``(new_benchmarks, diff_entries)``.

    * ``new_benchmarks`` is a shallow-copied list with the ``id`` /
      ``question_id`` field rewritten where collisions were detected.
    * ``diff_entries`` contains one record per rewritten row.

    The first occurrence of an id wins. Subsequent duplicates get the
    suffix ``:v2``, ``:v3`` …, chosen so the resulting id is also unique
    (including against other rows that already use the suffix format).
    """
    seen: set[str] = set()
    new_rows: list[dict[str, Any]] = []
    diff: list[dict[str, Any]] = []

    for row in benchmarks:
        row_copy = dict(row)
        old_id = str(
            row_copy.get("id") or row_copy.get("question_id") or ""
        ).strip()
        if not old_id:
            # No id to dedupe; leave untouched. Upstream loaders assign ids
            # before writing to UC — an empty id here is a schema bug the
            # dedupe script must not silently paper over.
            new_rows.append(row_copy)
            continue

        if old_id not in seen:
            seen.add(old_id)
            new_rows.append(row_copy)
            continue

        n = 2
        while f"{old_id}:v{n}" in seen:
            n += 1
        new_id = f"{old_id}:v{n}"
        seen.add(new_id)

        if "id" in row_copy:
            row_copy["id"] = new_id
        if "question_id" in row_copy:
            row_copy["question_id"] = new_id

        diff.append(
            {
                "old_id": old_id,
                "new_id": new_id,
                "question_snippet": str(row_copy.get("question", ""))[:120],
            }
        )
        new_rows.append(row_copy)

    return new_rows, diff


def summarize_diff(diff: list[dict[str, Any]], total_rows: int) -> dict[str, Any]:
    """Build the JSON report emitted to stdout / --output."""
    return {
        "total_rows": total_rows,
        "rewrites": len(diff),
        "entries": diff,
    }


# ─────────────────────────────────────────────────────────────────────────────
# I/O adapters
# ─────────────────────────────────────────────────────────────────────────────
def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def _write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False))
            fh.write("\n")


def _read_uc(uc_schema: str, domain: str) -> list[dict[str, Any]]:
    """Read benchmark rows from the canonical UC Delta table.

    Import Spark lazily so the script can run in offline (JSONL) mode
    without a working pyspark install.
    """
    from genie_space_optimizer.optimization.benchmarks import (
        load_benchmarks_from_dataset,
    )

    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:  # pragma: no cover - environment-dependent.
        raise SystemExit(
            "PySpark is not importable — run the script from a Databricks "
            "cluster, or use --jsonl for offline mode."
        ) from exc

    spark = SparkSession.builder.getOrCreate()
    return load_benchmarks_from_dataset(spark, uc_schema, domain)


def _write_uc(uc_schema: str, domain: str, rows: list[dict[str, Any]]) -> None:
    """Write deduped rows back to the UC benchmark table via overwrite."""
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:  # pragma: no cover
        raise SystemExit(
            "PySpark is not importable — cannot --apply against UC."
        ) from exc

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
    p.add_argument("--jsonl", type=Path, help="Path to a JSONL benchmark export for offline mode")
    p.add_argument("--output", type=Path, help="Where to save the JSON diff (stdout if omitted)")
    p.add_argument(
        "--apply",
        action="store_true",
        help="Write changes back to the source (default: dry-run, diff only).",
    )
    p.add_argument(
        "--verbose", "-v", action="count", default=0,
        help="Increase logging verbosity (-v, -vv).",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=max(logging.INFO - 10 * args.verbose, logging.DEBUG),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if args.jsonl:
        src = _read_jsonl(args.jsonl)
        logger.info("Read %d rows from %s", len(src), args.jsonl)
    elif args.uc_schema and args.domain:
        src = _read_uc(args.uc_schema, args.domain)
        logger.info(
            "Read %d rows from %s.genie_benchmarks_%s",
            len(src), args.uc_schema, args.domain,
        )
    else:
        logger.error(
            "Must supply either --jsonl PATH or (--uc-schema AND --domain)."
        )
        return 2

    new_rows, diff = dedupe_question_ids(src)
    report = summarize_diff(diff, total_rows=len(src))

    report_json = json.dumps(report, indent=2, ensure_ascii=False)
    if args.output:
        args.output.write_text(report_json, encoding="utf-8")
        logger.info("Wrote diff (%d rewrites) to %s", report["rewrites"], args.output)
    else:
        sys.stdout.write(report_json + "\n")

    if not args.apply:
        logger.info("Dry-run only (pass --apply to write changes).")
        return 0

    if report["rewrites"] == 0:
        logger.info("No collisions detected — nothing to write.")
        return 0

    if args.jsonl:
        _write_jsonl(args.jsonl, new_rows)
        logger.info("Rewrote %s in place (%d rows).", args.jsonl, len(new_rows))
    else:
        _write_uc(args.uc_schema, args.domain, new_rows)

    return 0


if __name__ == "__main__":  # pragma: no cover - thin CLI wrapper.
    raise SystemExit(main())
