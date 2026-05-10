"""Cycle 14-W hardening — D-5 diagnostic micro-job.

Run this notebook as a one-shot Databricks Jobs task on the same
cluster the lever-loop bundle uses. It calls
``_databricks_ids_from_env()`` once and prints the
``GSO_DATABRICKS_IDS_RESOLVED_V1`` trace marker so corpus
measurement can record which resolution path
(env / dbutils / mixed / sentinel) actually fires in this runtime.

Cost: ~30 seconds of cluster time. NO optimizer loop, NO Genie
Space access, NO MLflow logging, NO Lakebase reads.

Closure protocol (Tier-2, ``closed-runtime``):
  - Run this notebook in the airline anchor's workspace.
  - Run it again in the 7Now anchor's workspace.
  - Read the emitted ``GSO_DATABRICKS_IDS_RESOLVED_V1`` payload.
  - If ``resolution_path in {env, dbutils, mixed}`` and
    ``fields_resolved == fields_total`` -> D-5 closes
    (status -> ``closed``).
  - If ``resolution_path == sentinel`` and
    ``dbutils_attempted=true`` and ``dbutils_succeeded=false``
    -> the dbutils tag names differ in this runtime; register
    D-9 with the specific tag-name evidence in the roadmap
    Defect Registry. Do NOT reopen D-5 — original closure
    stays valid; D-9 is a downstream regression.

Anchor evidence: 7Now run 960148942255012 F8 + airline run
1105451933925748 F8 — both report blank manifest IDs cross-space
despite C14-W T3's resolution-path tracing being shipped. The
question this notebook answers is: which path actually fires?

Deployment: submit as a one-shot Databricks Jobs task. Either via
the Workspace UI (Workflows -> Create Job -> Task type: Notebook
-> select this file) or via the CLI:

  databricks jobs submit --json '{
    "run_name": "c14w-d5-diagnostic-airline",
    "tasks": [{
      "task_key": "diagnostic",
      "notebook_task": {
        "notebook_path": "/Workspace/Users/<you>/cycle_14_w_databricks_ids_diagnostic"
      },
      "existing_cluster_id": "<lever-loop cluster id>"
    }]
  }'

After completion, read the GSO_DATABRICKS_IDS_RESOLVED_V1 marker
from the run's stdout (databricks jobs get-run-output <run-id>).
"""
from __future__ import annotations

import json
import os
import sys


def main() -> int:
    print("=" * 72)
    print("Cycle 14-W hardening — D-5 diagnostic micro-job")
    print("=" * 72)

    print("\n--- Pre-resolver state ---")
    for env_var in (
        "DATABRICKS_JOB_ID",
        "DATABRICKS_RUN_ID",
        "DATABRICKS_JOB_RUN_ID",
        "DATABRICKS_TASK_RUN_ID",
    ):
        val = os.environ.get(env_var) or "(unset)"
        print(f"  env {env_var}: {val}")

    print("\n--- Importing resolver ---")
    try:
        from genie_space_optimizer.optimization.harness import (
            _databricks_ids_from_env,
        )
        print("  ok: imported _databricks_ids_from_env")
    except Exception as exc:
        print(f"  FAILED: {type(exc).__name__}: {exc}")
        return 1

    print("\n--- Calling resolver ---")
    ids = _databricks_ids_from_env()

    print("\n--- Resolved IDs ---")
    print(json.dumps(ids, indent=2, sort_keys=True))

    print("\n--- Closure verdict ---")
    sentinels = [k for k, v in ids.items() if v == "unknown"]
    if not sentinels:
        print("  PASS: every field resolved to a non-sentinel value.")
        print("        D-5 -> closed.")
        return 0
    else:
        print(f"  GAP:  {len(sentinels)}/{len(ids)} field(s) resolved to sentinel:")
        for k in sentinels:
            print(f"          - {k}")
        print("        Read the GSO_DATABRICKS_IDS_RESOLVED_V1 payload above")
        print("        to identify which resolution path fired and which")
        print("        dbutils tag names returned blank. Register D-9 in the")
        print("        roadmap Defect Registry with the tag-name evidence.")
        return 2


if __name__ == "__main__":
    sys.exit(main())
