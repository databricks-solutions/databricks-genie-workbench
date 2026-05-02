# Databricks notebook source
# MAGIC %md
# MAGIC # Cycle 7 Replay Fixture Reconstruction (one-shot operator notebook)
# MAGIC
# MAGIC | Quick Reference | |
# MAGIC |---|---|
# MAGIC | **Task** | One-shot — **Not part of the 6-task DAG** |
# MAGIC | **Entry point** | `genie_space_optimizer.scripts.reconstruct_airline_real_v1_fixture.main` |
# MAGIC | **Run by** | An operator with workspace access where cycle 7 ran |
# MAGIC | **Run ID** | `78557321-4e43-4bc6-9b4c-906771bd2f8d` (from `fixture_id` in raw cycle 7 capture) |
# MAGIC | **Plan** | `docs/2026-05-02-track-a-fixture-reconstruction-and-qid-extractor-fix-plan.md` (Phase 3, Tasks 7–8) |
# MAGIC | **Postmortem** | `docs/2026-05-02-cycle7-reconstruction-postmortem.md` |
# MAGIC
# MAGIC ## Why this exists
# MAGIC
# MAGIC Cycle 7 of the Phase A burn-down captured a structurally complete replay
# MAGIC fixture, but every `eval_rows[*].question_id` is an MLflow trace ID
# MAGIC (`tr-…`) instead of a canonical benchmark qid (`airline_…_gs_NNN`). Trace
# MAGIC IDs are minted fresh on each run, so the captured fixture is useless for
# MAGIC replay until we substitute them.
# MAGIC
# MAGIC The Track D fix in `harness.py:_baseline_row_qid` ensures *future* cycles
# MAGIC will write canonical qids directly. This notebook is a one-shot tool to
# MAGIC repair the cycle 7 capture so we can run validator triage against it
# MAGIC without burning another 2-hour real-Genie cycle.
# MAGIC
# MAGIC ## What it does
# MAGIC
# MAGIC 1. Loads the raw cycle 7 fixture from the synced repo
# MAGIC    (`tests/replay/fixtures/airline_real_v1_cycle7_raw.json`).
# MAGIC 2. For each iteration (1–5), builds a `{trace_id: canonical_qid}` map
# MAGIC    using MLflow `search_traces` (primary path — every predict_fn span
# MAGIC    was tagged with `question_id`).
# MAGIC 3. Falls back to Delta (`<catalog>.<schema>.genie_opt_iterations.rows_json`)
# MAGIC    only if MLflow returns empty for an iteration.
# MAGIC 4. Substitutes trace IDs in `eval_rows[*].question_id` with canonical qids.
# MAGIC 5. Asserts canonical overlap (no `tr-` prefixes; cluster qids ⊂ eval qids).
# MAGIC 6. Writes the corrected fixture to `tests/replay/fixtures/airline_real_v1.json`.
# MAGIC
# MAGIC ## ⚙️ Widget Parameters
# MAGIC
# MAGIC | Parameter | Type | Default | Description |
# MAGIC |---|---|---|---|
# MAGIC | `experiment_id` | text | `""` | MLflow experiment ID where cycle 7 ran (from `MLFLOW_EXPERIMENT_ID`) |
# MAGIC | `optimization_run_id` | text | `78557321-4e43-4bc6-9b4c-906771bd2f8d` | GSO run UUID from the cycle 7 fixture_id |
# MAGIC | `catalog` | text | `""` | UC catalog for `genie_opt_iterations` (Delta fallback only) |
# MAGIC | `schema` | text | `""` | UC schema for `genie_opt_iterations` (Delta fallback only) |
# MAGIC | `repo_root` | text | `""` | Absolute path to the synced repo root in `/Workspace/...` |
# MAGIC
# MAGIC ## How to run
# MAGIC
# MAGIC 1. `databricks sync` (or commit + Repos pull) so the repo is in
# MAGIC    `/Workspace/Users/<you>/databricks-genie-workbench` (or wherever the
# MAGIC    repo lives — pass that as `repo_root`).
# MAGIC 2. Open this notebook in the Databricks workspace.
# MAGIC 3. Attach to a cluster with network access to the workspace MLflow
# MAGIC    tracking server (any standard cluster works).
# MAGIC 4. Set widget parameters and run all cells.
# MAGIC 5. After the final cell prints `DONE`, download the corrected fixture
# MAGIC    from `<repo_root>/packages/genie-space-optimizer/tests/replay/fixtures/airline_real_v1.json`
# MAGIC    back to your local checkout and commit it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Install dependencies
# MAGIC
# MAGIC `mlflow` and `databricks-sdk` are normally pre-installed on Databricks
# MAGIC clusters. We pin recent versions here in case the cluster runtime
# MAGIC ships an older `mlflow` that lacks `search_traces`.

# COMMAND ----------

# MAGIC %pip install --quiet "mlflow>=2.18" "databricks-sdk>=0.30"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📥 Widgets

# COMMAND ----------

# `dbutils` and `spark` are Databricks notebook runtime globals; type-checkers
# running outside Databricks don't see them. Silence basedpyright on this file.
# pyright: reportUndefinedVariable=false

dbutils.widgets.text("experiment_id", "", "MLflow experiment ID")
dbutils.widgets.text(
    "optimization_run_id",
    "78557321-4e43-4bc6-9b4c-906771bd2f8d",
    "GSO run UUID (from cycle 7 fixture_id)",
)
dbutils.widgets.text("catalog", "", "UC catalog (Delta fallback only)")
dbutils.widgets.text("schema", "", "UC schema (Delta fallback only)")
dbutils.widgets.text("repo_root", "", "Absolute path to synced repo root")

experiment_id = dbutils.widgets.get("experiment_id").strip()
optimization_run_id = dbutils.widgets.get("optimization_run_id").strip()
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
repo_root = dbutils.widgets.get("repo_root").strip()

assert experiment_id, "experiment_id widget is required (find it in MLFLOW_EXPERIMENT_ID)"
assert optimization_run_id, "optimization_run_id widget is required"
assert repo_root, "repo_root widget is required (e.g. /Workspace/Users/you/databricks-genie-workbench)"

print(f"experiment_id        = {experiment_id}")
print(f"optimization_run_id  = {optimization_run_id}")
print(f"catalog              = {catalog or '(unset — Delta fallback disabled)'}")
print(f"schema               = {schema or '(unset — Delta fallback disabled)'}")
print(f"repo_root            = {repo_root}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🐍 Make the GSO package importable
# MAGIC
# MAGIC The reconstruction logic lives in
# MAGIC `packages/genie-space-optimizer/src/genie_space_optimizer/scripts/reconstruct_airline_real_v1_fixture.py`.
# MAGIC We add the package's `src/` to `sys.path` so we can import it directly
# MAGIC without installing the wheel on the cluster.

# COMMAND ----------

import os
import sys

gso_src = os.path.join(repo_root, "packages", "genie-space-optimizer", "src")
assert os.path.isdir(gso_src), f"GSO src not found at {gso_src!r} — did you set repo_root correctly?"

if gso_src not in sys.path:
    sys.path.insert(0, gso_src)

from genie_space_optimizer.scripts.reconstruct_airline_real_v1_fixture import (  # noqa: E402
    assert_canonical_overlap,
    load_fixture,
    main,
)

print(f"sys.path[0] = {sys.path[0]}")
print("imported reconstruct_airline_real_v1_fixture.main")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Sanity-check inputs
# MAGIC
# MAGIC Verify the raw fixture is on disk, has the expected run_id, and that
# MAGIC its iterations are populated with `tr-` prefixed qids (i.e. this is
# MAGIC the captured cycle 7 file, not an already-reconstructed one).

# COMMAND ----------

raw_fixture_path = os.path.join(
    repo_root,
    "packages",
    "genie-space-optimizer",
    "tests",
    "replay",
    "fixtures",
    "airline_real_v1_cycle7_raw.json",
)
out_fixture_path = os.path.join(
    repo_root,
    "packages",
    "genie-space-optimizer",
    "tests",
    "replay",
    "fixtures",
    "airline_real_v1.json",
)

assert os.path.isfile(raw_fixture_path), f"raw fixture not found at {raw_fixture_path}"

raw_preview = load_fixture(raw_fixture_path)
print(f"raw fixture_id       = {raw_preview.get('fixture_id')}")
print(f"raw iterations       = {[it.get('iteration') for it in raw_preview.get('iterations') or []]}")
print(f"raw eval_rows counts = {[len(it.get('eval_rows') or []) for it in raw_preview.get('iterations') or []]}")
sample_qid = raw_preview["iterations"][0]["eval_rows"][0].get("question_id")
print(f"sample qid           = {sample_qid!r}")
assert sample_qid.startswith("tr-"), (
    f"raw fixture qids do not start with 'tr-' — got {sample_qid!r}. "
    "This file may already be reconstructed; aborting to avoid double-substitution."
)
expected_run_id = "78557321-4e43-4bc6-9b4c-906771bd2f8d"
assert expected_run_id in str(raw_preview.get("fixture_id", "")), (
    f"raw fixture_id {raw_preview.get('fixture_id')!r} does not contain "
    f"expected cycle 7 run_id {expected_run_id!r}"
)
print("inputs OK — proceeding with reconstruction")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧮 Run the reconstruction
# MAGIC
# MAGIC `main(...)` will:
# MAGIC - For each iteration, call MLflow `search_traces` filtered by
# MAGIC   `genie.optimization_run_id` and `genie.iteration`
# MAGIC - Build `{trace_id: canonical_qid}` from `tags["question_id"]`
# MAGIC - Fall back to Delta for any iteration where MLflow returns empty
# MAGIC - Substitute, assert overlap, and write the corrected fixture
# MAGIC
# MAGIC Hard-fails on missing data — we'd rather see an exception than silently
# MAGIC commit a partially-correct fixture.

# COMMAND ----------

main(
    raw_fixture_path=raw_fixture_path,
    out_fixture_path=out_fixture_path,
    experiment_id=experiment_id,
    optimization_run_id=optimization_run_id,
    catalog=catalog,
    schema=schema,
    spark=spark,  # provided by the Databricks notebook runtime  # noqa: F821
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Post-run verification
# MAGIC
# MAGIC Re-load the freshly written fixture and run the canonical-overlap
# MAGIC assertion one more time as a paranoid sanity check, then print a
# MAGIC small summary so the operator can paste it back into the chat.

# COMMAND ----------

corrected = load_fixture(out_fixture_path)
assert_canonical_overlap(corrected)

print(f"corrected fixture written to: {out_fixture_path}")
print(f"fixture_id = {corrected.get('fixture_id')}")
print(f"iterations = {[it.get('iteration') for it in corrected.get('iterations') or []]}")

for it in corrected.get("iterations") or []:
    eval_qids = [r.get("question_id") for r in (it.get("eval_rows") or [])]
    cluster_qids = sorted({
        q
        for c in (it.get("clusters") or []) + (it.get("soft_clusters") or [])
        for q in (c.get("question_ids") or [])
    })
    print(
        f"  iter {it.get('iteration')}: "
        f"{len(eval_qids)} eval_rows, "
        f"first qid={eval_qids[0] if eval_qids else None!r}, "
        f"cluster qids={cluster_qids}"
    )

print("\nALL CHECKS PASSED — commit packages/genie-space-optimizer/tests/replay/fixtures/airline_real_v1.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📤 Next steps for the operator
# MAGIC
# MAGIC 1. From your local laptop, pull the corrected fixture out of the
# MAGIC    workspace:
# MAGIC    ```
# MAGIC    databricks workspace export-dir \
# MAGIC      <repo_root>/packages/genie-space-optimizer/tests/replay/fixtures \
# MAGIC      packages/genie-space-optimizer/tests/replay/fixtures \
# MAGIC      --overwrite
# MAGIC    ```
# MAGIC    (or just download `airline_real_v1.json` via the Workspace UI).
# MAGIC 2. `git add packages/genie-space-optimizer/tests/replay/fixtures/airline_real_v1.json`
# MAGIC 3. Commit with a message like `Reconstruct cycle 7 airline_real_v1 fixture`.
# MAGIC 4. Resume Phase A burn-down Task 14 (validator triage) against the
# MAGIC    corrected fixture.
