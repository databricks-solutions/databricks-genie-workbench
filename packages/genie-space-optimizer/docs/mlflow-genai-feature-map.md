# MLflow for GenAI: How Every Feature Connects

A progressive, layered guide showing how MLflow's GenAI features build on each other. Each section adds one new concept on top of the previous ones, with running code examples drawn from the [Genie Space Optimizer](../README.md).

By the end, you'll see that MLflow's GenAI surface isn't a grab-bag of unrelated tools -- it's a composable stack where each layer plugs into the ones below it.

---

## The Full Picture (Read This First)

Here's the nesting hierarchy we'll build, from outermost to innermost:

```
Experiment                          ← the project folder
 └─ Run                             ← one evaluation session
     ├─ Params / Metrics / Artifacts ← metadata about the run
     ├─ Tags                         ← searchable labels
     ├─ Prompt Registry              ← versioned prompt loaded during the run
     ├─ LoggedModel                  ← versioned config snapshot
     ├─ genai.evaluate()             ← the evaluation engine
     │   ├─ Evaluation Dataset       ← the benchmark data
     │   ├─ Predict Function         ← what we're evaluating
     │   │   └─ Trace                ← recorded execution of one prediction
     │   │       └─ Spans            ← sub-steps inside the trace
     │   └─ Scorers                  ← judge functions
     │       └─ Feedback             ← structured verdict per question
     ├─ log_feedback()               ← post-eval annotations on traces
     └─ Labeling Session             ← human review of traces
         └─ Label Schemas            ← structured review forms
```

Now let's build it from the ground up.

---

## Layer 1: Experiment -- The Project Folder

Everything in MLflow lives inside an **Experiment**. Think of it as a project folder that groups related work.

```python
import mlflow

mlflow.set_experiment("/Users/alice@company.com/genie-optimization/sales")
```

That's it -- one line. If the experiment doesn't exist, it's created. If it does, it's selected.

But here's what makes it powerful: **tags**. Tags make experiments searchable and self-describing:

```python
mlflow.set_experiment_tags({
    "genie.space_id": "01ef8a...",
    "genie.domain": "sales",
    "genie.pipeline_version": "2.1.0",
    "genie.catalog": "prod_catalog",
    "genie.schema": "sales_schema",
})
```

**Why it matters:** Six months from now, you can search "show me all experiments for the sales domain" and find this instantly. Tags are free metadata -- use them liberally.

**What we have so far:**

```
Experiment  ✅
```

---

## Layer 2: Run -- One Evaluation Session

Inside an experiment, a **Run** represents one unit of work. In GenAI, that's typically one evaluation pass.

```python
mlflow.set_experiment("/Users/alice@company.com/genie-optimization/sales")

with mlflow.start_run(run_name="eval_iter_0_20260303_141500") as run:
    print(f"Run ID: {run.info.run_id}")
    # ... everything else happens inside here ...
```

Runs are context managers. Everything you log inside the `with` block is attached to this run.

You can also tag runs with searchable metadata:

```python
with mlflow.start_run(run_name="eval_iter_0_20260303_141500") as run:
    mlflow.set_tags({
        "genie.space_id": "01ef8a...",
        "genie.iteration": "0",
        "genie.eval_scope": "full",
        "genie.lever": "baseline",
    })
```

**Why it matters:** Each evaluation iteration, each lever application, each repeatability test gets its own run. You can compare runs side-by-side in the MLflow UI.

**What we have so far:**

```
Experiment  ✅
 └─ Run     ✅
```

---

## Layer 3: Params, Metrics, and Artifacts -- The Run's Contents

A run by itself is just an empty container. You fill it with three kinds of data:

### Params -- Configuration (logged once, at the start)

```python
with mlflow.start_run(run_name="eval_iter_0") as run:
    mlflow.log_params({
        "space_id": "01ef8a...",
        "iteration": 0,
        "benchmark_count": 20,
        "num_scorers": 9,
        "eval_scope": "full",
        "domain": "sales",
    })
```

Params answer: "What were the inputs to this run?"

### Metrics -- Measurements (logged during or after the run)

```python
    # Individual metrics
    mlflow.log_metric("thresholds_passed", 1.0)
    mlflow.log_metric("harness_retry_count", 0.0)

    # Batch metrics
    mlflow.log_metrics({
        "eval_syntax_validity": 98.5,
        "eval_schema_accuracy": 96.0,
        "eval_logical_accuracy": 91.2,
        "eval_result_correctness": 87.5,
    })
```

Metrics answer: "What were the outcomes of this run?"

### Artifacts -- Files (JSON reports, configs, anything)

```python
    # Log a dict as a JSON artifact
    mlflow.log_dict(
        {"quarantined": ["q1", "q5"], "reason": "invalid SQL"},
        "evaluation_runtime/benchmark_precheck.json",
    )

    # Log arbitrary text
    mlflow.log_text(
        "SELECT * FROM sales WHERE region = 'EMEA'",
        "judge_prompts/schema_accuracy/template.txt",
    )
```

Artifacts answer: "What are the detailed records from this run?"

**Why it matters:** Params let you filter runs ("show me all runs with eval_scope=full"). Metrics let you compare runs ("which iteration had the best schema_accuracy?"). Artifacts let you debug ("what benchmarks were quarantined?").

**What we have so far:**

```
Experiment                            ✅
 └─ Run                               ✅
     ├─ Params (config inputs)         ✅
     ├─ Metrics (numeric outcomes)     ✅
     └─ Artifacts (JSON/text files)    ✅
```

---

## Layer 4: Tracing -- Recording What Happens Inside a Prediction

Now we leave the "run" level and zoom into what happens when your GenAI app processes a single request. A **Trace** captures the full execution of one prediction.

The simplest way to create a trace is the `@mlflow.trace` decorator:

```python
@mlflow.trace
def genie_predict_fn(question: str, expected_sql: str = "", **kwargs) -> dict:
    # Everything inside this function is recorded as a trace
    result = run_genie_query(w, space_id, question)
    genie_sql = result.get("sql", "")
    return {"response": genie_sql}
```

Every call to `genie_predict_fn` produces one trace. The trace records the inputs, outputs, timing, and any errors.

### Adding business context to traces

Raw traces are useful, but tagged traces are powerful:

```python
@mlflow.trace
def genie_predict_fn(question: str, expected_sql: str = "", **kwargs) -> dict:
    mlflow.update_current_trace(
        tags={
            "question_id": kwargs.get("question_id", ""),
            "space_id": space_id,
            "genie.optimization_run_id": optimization_run_id,
            "genie.iteration": str(iteration),
            "genie.lever": str(lever),
        },
        metadata={
            "mlflow.trace.user": triggered_by,
            "mlflow.trace.session": optimization_run_id,
        },
    )
    # ... rest of prediction logic ...
```

Now you can search traces by question, iteration, or lever in the MLflow UI.

**Why it matters:** When a scorer says "question Q7 failed schema_accuracy," you open the trace for Q7 and see exactly what Genie returned, what SQL it generated, and how it compared to ground truth. Without tracing, you're guessing.

**What we have so far:**

```
Experiment                            ✅
 └─ Run                               ✅
     ├─ Params / Metrics / Artifacts   ✅
     └─ Traces (one per prediction)    ✅
```

---

## Layer 5: Spans -- Sub-steps Inside a Trace

A trace records one top-level function call. But what if that function has multiple interesting steps? **Spans** let you break a trace into a tree of sub-operations.

```python
@mlflow.trace
def genie_predict_fn(question: str, **kwargs) -> dict:
    # Step 1: Call Genie
    with mlflow.start_span(name="call_genie") as genie_span:
        result = run_genie_query(w, space_id, question)
        genie_sql = result.get("sql", "")
        genie_span.set_outputs({"sql": genie_sql[:200]})

    # Step 2: Execute ground truth SQL
    with mlflow.start_span(name="execute_gt_sql") as gt_span:
        gt_result = execute_sql(spark, expected_sql)
        gt_span.set_outputs({"rows": len(gt_result)})

    # Step 3: Compare results
    with mlflow.start_span(name="compare_results") as cmp_span:
        comparison = compare_outputs(genie_result, gt_result)
        cmp_span.set_outputs({"match": comparison["match"]})

    return {"response": genie_sql, "comparison": comparison}
```

The trace UI shows this as a tree:

```
genie_predict_fn (2.3s)
 ├─ call_genie (1.8s)
 ├─ execute_gt_sql (0.3s)
 └─ compare_results (0.2s)
```

Spans also nest, which is powerful for multi-step LLM reasoning:

```python
with mlflow.start_span(name="generate_strategy") as outer:
    outer.set_inputs({"clusters": len(failure_clusters)})

    with mlflow.start_span(name="phase_1_triage") as triage:
        triage_result = call_llm_for_triage(failure_clusters)
        triage.set_outputs({"action_groups": len(triage_result)})

    with mlflow.start_span(name="phase_2_detail") as detail:
        for ag in triage_result:
            with mlflow.start_span(name=f"detail_ag_{ag['id']}") as ag_span:
                plan = call_llm_for_detail(ag)
                ag_span.set_outputs({"patches": len(plan)})
```

**Why it matters:** When a prediction is slow or wrong, spans tell you exactly which step is the bottleneck or source of the error.

**What we have so far:**

```
Experiment                            ✅
 └─ Run                               ✅
     ├─ Params / Metrics / Artifacts   ✅
     └─ Traces                         ✅
         └─ Spans (sub-steps)          ✅
```

---

## Layer 6: Prompt Registry -- Versioned Prompts Linked to Traces

Your GenAI app uses prompts. Those prompts change over time. The **Prompt Registry** versions them like Git versions code.

### Registering a prompt

```python
version = mlflow.genai.register_prompt(
    name="sales__schema_accuracy_judge",
    template=(
        "You are a SQL accuracy judge. Compare the expected SQL against "
        "the generated SQL and determine if they reference the same schema "
        "elements (tables, columns, joins).\n\n"
        "Expected SQL: {{ expected_sql }}\n"
        "Generated SQL: {{ generated_sql }}\n\n"
        "Return a JSON verdict with rationale."
    ),
    commit_message="Initial schema accuracy judge for sales domain",
    tags={"domain": "sales", "type": "judge"},
)
print(f"Registered version: {version.version}")  # e.g., 1
```

### Aliasing a prompt (like git tags)

```python
mlflow.genai.set_prompt_alias(
    name="sales__schema_accuracy_judge",
    alias="production",
    version=version.version,
)
```

### Loading a prompt (and linking it to the current trace)

This is where it connects to tracing. When you load a prompt inside a traced function, MLflow automatically links that prompt version to the trace:

```python
@mlflow.trace
def score_with_llm(question, expected_sql, generated_sql):
    # This load links the prompt to the current trace
    prompt = mlflow.genai.load_prompt("prompts:/sales__schema_accuracy_judge@production")

    # Use the prompt template
    filled = prompt.format(expected_sql=expected_sql, generated_sql=generated_sql)
    response = call_llm(filled)
    return response
```

In the trace UI, you'll see "Linked Prompts: sales__schema_accuracy_judge v3" -- telling you exactly which prompt version produced this result.

### Versioning Genie Space instructions

The same mechanism tracks how your app's configuration evolves:

```python
mlflow.genai.register_prompt(
    name="sales__genie_instructions",
    template=current_instruction_text,
    commit_message=f"After lever 3, iteration 2 (accuracy=0.923)",
    tags={
        "lever": "3",
        "iteration": "2",
        "accuracy": "0.9230",
    },
)
```

**Why it matters:** When you're debugging "why did the app behave differently last week?", the Prompt Registry shows you exactly which prompt version was active. Diffs between versions show what changed.

**What we have so far:**

```
Experiment                            ✅
 └─ Run                               ✅
     ├─ Params / Metrics / Artifacts   ✅
     ├─ Prompt Registry (versioned)    ✅  ← loaded prompts link to traces
     └─ Traces                         ✅
         └─ Spans                      ✅
```

---

## Layer 7: Scorers and Feedback -- Structured Quality Judgments

A **Scorer** is a function that evaluates one prediction and returns a **Feedback** object. MLflow's `@scorer` decorator makes any function a scorer:

### A CODE scorer (deterministic)

```python
from mlflow.entities import Feedback, AssessmentSource
from mlflow.genai.scorers import scorer

CODE_SOURCE = AssessmentSource(source_type="CODE", source_id="genie_optimizer/asset_check")

@scorer
def asset_routing_scorer(inputs: dict, outputs: dict, expectations: dict) -> Feedback:
    expected_type = expectations.get("expected_asset", "TABLE")
    actual_type = detect_asset_type(outputs.get("response", ""))
    correct = actual_type == expected_type

    return Feedback(
        name="asset_routing",
        value="yes" if correct else "no",
        rationale=f"Expected {expected_type}, got {actual_type}",
        source=CODE_SOURCE,
    )
```

### An LLM scorer (non-deterministic)

```python
LLM_SOURCE = AssessmentSource(source_type="LLM_JUDGE", source_id="claude-opus-4-6")

def _make_schema_accuracy_judge(w, catalog, schema):
    @scorer
    def schema_accuracy_judge(inputs: dict, outputs: dict, expectations: dict) -> Feedback:
        # Load the versioned prompt (links to the trace automatically)
        prompt = mlflow.genai.load_prompt("prompts:/sales__schema_accuracy_judge@production")

        # Call LLM to judge
        verdict = call_llm(w, prompt.format(...))

        return Feedback(
            name="schema_accuracy",
            value="yes" if verdict["pass"] else "no",
            rationale=verdict["rationale"],
            source=LLM_SOURCE,
            metadata={
                "failure_type": verdict.get("failure_type", ""),
                "blame_set": verdict.get("blame_set", []),
                "counterfactual_fix": verdict.get("fix", ""),
                "severity": verdict.get("severity", ""),
                "confidence": verdict.get("confidence", 0.5),
            },
        )
    return schema_accuracy_judge
```

### The Feedback object

`Feedback` is richer than a number. It carries:

| Field | Purpose |
|-------|---------|
| `name` | Which judge produced this (e.g., "schema_accuracy") |
| `value` | The verdict ("yes"/"no", or a numeric score) |
| `rationale` | Human-readable explanation |
| `source` | CODE or LLM_JUDGE (provenance) |
| `metadata` | Structured data (failure type, blame set, fix suggestion) |

The metadata is what makes debugging possible. A `value="no"` tells you something failed. A metadata dict with `failure_type="column_mismatch"` and `counterfactual_fix="Add alias 'rev' to column 'revenue'"` tells you exactly what to do about it.

**Why it matters:** Scorers are the quality signal. Feedback is the structured format that makes that signal actionable. Together, they turn "the score dropped" into "here's what broke and here's how to fix it."

**What we have so far:**

```
Experiment                            ✅
 └─ Run                               ✅
     ├─ Params / Metrics / Artifacts   ✅
     ├─ Prompt Registry                ✅
     ├─ Scorers                        ✅  ← produce Feedback objects
     │   └─ Feedback                   ✅  ← structured verdicts
     └─ Traces                         ✅
         └─ Spans                      ✅
```

---

## Layer 8: Evaluation Dataset -- Governed Ground Truth

An **Evaluation Dataset** holds your benchmark questions and expected answers. It's backed by a Unity Catalog table, giving you governance, versioning, and SQL access.

### Creating a dataset

```python
eval_dataset = mlflow.genai.datasets.create_dataset(
    name="prod_catalog.sales_schema.genie_benchmarks_sales",
    experiment_id=["abc123"],  # links to experiment's Datasets tab
)
```

### Populating it

```python
records = []
for b in benchmarks:
    records.append({
        "inputs": {
            "question_id": b["id"],
            "question": b["question"],
            "space_id": "01ef8a...",
            "expected_sql": b["expected_sql"],
        },
        "expectations": {
            "expected_response": b["expected_sql"],
            "expected_asset": "TABLE",
            "category": b["category"],
        },
    })

eval_dataset.merge_records(records)  # upsert -- preserves history
```

### Using it in evaluation

The dataset plugs directly into `mlflow.genai.evaluate()` as the `data` argument (see Layer 9).

### Loading an existing dataset

```python
eval_dataset = mlflow.genai.datasets.get_dataset(
    name="prod_catalog.sales_schema.genie_benchmarks_sales"
)
```

**Why it matters:** Your benchmarks change constantly -- new questions, corrected SQL, human fixes. `merge_records` handles this cleanly. UC backing means access control, lineage, and SQL queryability. The dataset appears in the experiment's Datasets tab in the UI.

**What we have so far:**

```
Experiment                            ✅
 └─ Run                               ✅
     ├─ Params / Metrics / Artifacts   ✅
     ├─ Prompt Registry                ✅
     ├─ Evaluation Dataset             ✅  ← UC-backed benchmarks
     ├─ Scorers → Feedback             ✅
     └─ Traces → Spans                 ✅
```

---

## Layer 9: `mlflow.genai.evaluate()` -- Tying It All Together

This is the function that orchestrates everything. It takes a predict function, data, and scorers -- and produces runs, traces, metrics, and feedback in one call.

### The predict function (traced)

```python
@mlflow.trace
def genie_predict_fn(question: str, expected_sql: str = "", **kwargs) -> dict:
    mlflow.update_current_trace(tags={"question_id": kwargs.get("question_id", "")})
    result = run_genie_query(w, space_id, question)
    return {"response": result.get("sql", "")}
```

### The scorers (from Layer 7)

```python
scorers = [
    _make_syntax_validity_scorer(spark, catalog, schema),
    _make_schema_accuracy_judge(w, catalog, schema),
    _make_logical_accuracy_judge(w, catalog, schema),
    _make_semantic_equivalence_judge(w, catalog, schema),
    _make_completeness_judge(w, catalog, schema),
    _make_response_quality_judge(w, catalog, schema),
    asset_routing_scorer,
    result_correctness_scorer,
    _make_arbiter_scorer(w, catalog, schema),
]
```

### The evaluation call

```python
mlflow.set_experiment(experiment_name)

with mlflow.start_run(run_name="eval_iter_0_20260303") as run:
    # Log what we're doing
    mlflow.set_tags({"genie.iteration": "0", "genie.eval_scope": "full"})
    mlflow.log_params({"space_id": space_id, "benchmark_count": 20, "num_scorers": 9})

    # Load the UC-backed dataset
    eval_dataset = mlflow.genai.datasets.get_dataset(name=uc_table_name)

    # Run evaluation -- this is where everything comes together
    result = mlflow.genai.evaluate(
        predict_fn=genie_predict_fn,    # traced predict function
        data=eval_dataset,               # UC-backed benchmarks
        scorers=scorers,                 # 9 judges returning Feedback
    )

    # result.metrics contains aggregated scores
    # e.g., {"syntax_validity/mean": 0.98, "schema_accuracy/mean": 0.96, ...}
    for name, value in sorted(result.metrics.items()):
        if "/mean" in name:
            print(f"  {name}: {value:.2%}")

    # Log the threshold check
    thresholds_passed = all(
        result.metrics.get(k, 0) >= v
        for k, v in MLFLOW_THRESHOLDS.items()
    )
    mlflow.log_metric("thresholds_passed", 1.0 if thresholds_passed else 0.0)
```

### What `evaluate()` does under the hood

For each row in `data`:

1. Calls `predict_fn(question=..., expected_sql=..., ...)` -- produces a **Trace**
2. Passes `inputs`, `outputs`, and `expectations` to each **Scorer** -- produces **Feedback**
3. Aggregates Feedback across all rows -- produces **Metrics** (e.g., `schema_accuracy/mean`)
4. Stores everything in the current **Run** (traces, assessments, metrics)

One function call. Everything connected.

### The result object

```python
# Aggregated metrics
result.metrics  # {"syntax_validity/mean": 0.98, "schema_accuracy/mean": 0.96, ...}

# Per-row results table
result.tables["eval_results"]  # DataFrame with per-question verdicts
```

**Why it matters:** `evaluate()` is the orchestrator. You don't manually loop over questions, call predict, call scorers, aggregate results. It does all of that, and it connects traces to feedback to metrics automatically.

**What we have so far:**

```
Experiment                            ✅
 └─ Run                               ✅
     ├─ Params / Metrics / Artifacts   ✅
     ├─ Prompt Registry                ✅
     ├─ genai.evaluate()               ✅  ← the orchestrator
     │   ├─ Evaluation Dataset          ✅  ← the data
     │   ├─ Predict Function            ✅  ← what we're evaluating
     │   │   └─ Trace → Spans          ✅  ← recorded execution
     │   └─ Scorers → Feedback          ✅  ← structured judgments
     └─ Metrics (aggregated)            ✅  ← summary scores
```

---

## Layer 10: LoggedModel -- Versioning Your Configuration

In GenAI, the "model" is often a configuration -- not weights. **LoggedModel** versions that configuration with params, tags, artifacts, and aliases.

### Creating a model version

```python
with mlflow.start_run(run_name="model_snapshot_iter_0"):
    # Save the config as an artifact
    mlflow.log_dict(space_config, "model_snapshots/iter_0/space_config.json")
    mlflow.log_dict(metadata_snapshot, "model_snapshots/iter_0/metadata.json")

    # Create a LoggedModel with searchable params
    model = mlflow.create_logged_model(
        name=f"genie_space_{space_id}",
        params={
            "space_id": space_id,
            "iteration": "0",
            "uc_schema": "prod_catalog.sales_schema",
            "patch_count": "0",
        },
        tags={
            "domain": "sales",
            "traceability": "genie_space_optimizer",
        },
    )
    model_id = model.model_id  # e.g., "m-abc123..."
```

### Linking evaluation scores to the model

This connects Layer 9 (evaluation) to Layer 10 (model versioning):

```python
# After evaluation completes
mlflow.log_metrics(
    {f"eval_{judge}": score for judge, score in per_judge_scores.items()},
    model_id=model_id,
)
```

Now the model card in the MLflow UI shows quality metrics alongside the config params.

### Linking a model to an evaluation run

Inside `genai.evaluate()`, you can pass the model_id:

```python
result = mlflow.genai.evaluate(
    predict_fn=predict_fn,
    data=eval_dataset,
    scorers=scorers,
    model_id=model_id,  # links this eval to the model version
)
```

### Promoting the best model

After your optimization loop, promote the winner:

```python
best_model_id = find_best_iteration(iterations_df)
mlflow.set_logged_model_alias(model_id=best_model_id, alias="champion")
```

### Rolling back

Read the config from any model version and re-apply it:

```python
model = mlflow.get_logged_model(model_id=model_id)
config = json.loads(model.params["model_space_config"])
patch_space_config(w, model.params["space_id"], config)  # restore via Genie API
```

**Why it matters:** Every iteration is a versioned snapshot you can inspect, compare, promote, or rollback. The model card becomes a dashboard showing config + quality over time.

**What we have so far:**

```
Experiment                            ✅
 └─ Run                               ✅
     ├─ Params / Metrics / Artifacts   ✅
     ├─ Prompt Registry                ✅
     ├─ LoggedModel                    ✅  ← versioned config
     │   ├─ params (space_id, iter)    ✅
     │   ├─ aliases (champion)         ✅
     │   └─ linked metrics             ✅  ← from evaluation
     ├─ genai.evaluate()               ✅
     │   ├─ Evaluation Dataset          ✅
     │   ├─ Predict → Trace → Spans   ✅
     │   └─ Scorers → Feedback          ✅
     └─ Aggregated Metrics              ✅
```

---

## Layer 11: `log_feedback()` -- Post-Evaluation Annotations

After `evaluate()` runs, you have traces with scorer feedback. But there's more to say -- did the quality gate pass? What was the root cause of failures? **`log_feedback()`** lets you annotate traces *after* evaluation.

### Gate outcomes

```python
for question_id, trace_id in trace_map.items():
    mlflow.log_feedback(
        trace_id=trace_id,
        name="gate_slice",
        value=True,
        rationale="Lever 4 slice gate: PASS. Schema accuracy +3.2%, no regressions.",
        source=AssessmentSource(source_type="CODE", source_id="genie_optimizer/gate"),
        metadata={
            "gate_type": "slice",
            "gate_result": "pass",
            "lever": 4,
            "iteration": 3,
        },
    )
```

### Root-cause analysis

```python
mlflow.log_feedback(
    trace_id=trace_id,
    name="asi_schema_accuracy",
    value=False,
    rationale="Add alias 'rev' to column 'revenue' in sales_table",
    source=AssessmentSource(source_type="CODE", source_id="genie_optimizer/asi"),
    metadata={
        "failure_type": "column_mismatch",
        "severity": "major",
        "blame_set": ["prod_catalog.sales_schema.sales_table.revenue"],
        "counterfactual_fix": "Add alias 'rev' to column description",
    },
)
```

### How it connects to traces

This is the key insight: **`log_feedback()` writes to traces that already exist from `evaluate()`**. The trace now tells a complete story:

1. **From tracing:** What question was asked, what Genie returned (the raw execution)
2. **From scorers:** What each judge thought (the quality signal)
3. **From `log_feedback()`:** What the pipeline concluded (gate decision + root cause)

A reviewer opens one trace and sees everything.

**What we have so far:**

```
Experiment                            ✅
 └─ Run                               ✅
     ├─ Params / Metrics / Artifacts   ✅
     ├─ Prompt Registry                ✅
     ├─ LoggedModel (versioned config) ✅
     ├─ genai.evaluate()               ✅
     │   ├─ Evaluation Dataset          ✅
     │   ├─ Predict → Trace → Spans   ✅
     │   └─ Scorers → Feedback          ✅
     ├─ log_feedback()                  ✅  ← post-eval annotations
     └─ Aggregated Metrics              ✅
```

---

## Layer 12: Labeling Sessions -- Closing the Human Loop

Automated judges are imperfect. Domain experts need to review traces and correct mistakes. **Labeling Sessions** provide structured human review on top of traces.

### Step 1: Define labeling schemas (the review forms)

```python
from mlflow.genai.label_schemas import (
    InputCategorical,
    InputText,
    InputTextList,
    create_label_schema,
)

create_label_schema(
    name="judge_verdict_accuracy",
    type="feedback",
    title="Is the judge's verdict correct for this question?",
    input=InputCategorical(options=[
        "Correct - judge is right",
        "Wrong - Genie answer is actually fine",
        "Wrong - both answers are wrong",
        "Ambiguous - question is unclear",
    ]),
    instruction="Review the question, expected SQL, generated SQL, and judge rationale.",
    enable_comment=True,
    overwrite=True,
)

create_label_schema(
    name="corrected_expected_sql",
    type="expectation",
    title="Provide the correct expected SQL (if benchmark is wrong)",
    input=InputText(),
    instruction="If the benchmark's expected SQL is wrong, provide the corrected version.",
    overwrite=True,
)

create_label_schema(
    name="improvement_suggestions",
    type="expectation",
    title="Suggest improvements for this Genie Space",
    input=InputTextList(max_count=5, max_length_each=500),
    overwrite=True,
)
```

### Step 2: Create a session and populate it with traces

```python
from mlflow.genai.labeling import create_labeling_session

session = create_labeling_session(
    name=f"review_sales_{run_id[:8]}_20260303",
    label_schemas=["judge_verdict_accuracy", "corrected_expected_sql", "improvement_suggestions"],
    assigned_users=["analyst@company.com", "data_lead@company.com"],
)

# Search for traces from the evaluation experiment
exp = mlflow.get_experiment_by_name(experiment_name)
traces_df = mlflow.search_traces(
    experiment_ids=[exp.experiment_id],
    max_results=200,
)

# Prioritize failure traces, then backfill with passing ones
failure_traces = traces_df[traces_df["trace_id"].isin(failure_trace_ids)]
other_traces = traces_df[~traces_df["trace_id"].isin(failure_trace_ids)].head(50)
combined = pd.concat([failure_traces, other_traces])

session.add_traces(combined)
```

Reviewers now see these traces in the MLflow Labeling UI with the structured review forms.

### Step 3: Ingest corrections into the next run

This is where the loop closes. At the start of the *next* optimization run:

```python
# Sync human corrections back to the evaluation dataset
session.sync(dataset_name="prod_catalog.sales_schema.genie_benchmarks_sales")

# Read structured feedback for the pipeline
all_sessions = mlflow.genai.labeling.get_labeling_sessions()
for s in all_sessions:
    if s.name == prior_session_name:
        traces_df = mlflow.search_traces(run_id=s.mlflow_run_id)
        for _, row in traces_df.iterrows():
            for assessment in row.get("assessments", []):
                if assessment.name == "corrected_expected_sql" and assessment.value:
                    # This corrected SQL becomes the new ground truth
                    update_benchmark(assessment.trace_id, assessment.value)
```

### How it connects to everything else

```
Human corrects benchmark SQL in Labeling UI
          ↓
session.sync() pushes to Evaluation Dataset (Layer 8)
          ↓
Next run's genai.evaluate() uses corrected benchmarks (Layer 9)
          ↓
Scorers produce more accurate Feedback (Layer 7)
          ↓
Better metrics flow to LoggedModel (Layer 10)
```

---

## The Complete Picture

```
Experiment                              ← Layer 1: the project folder
 └─ Run                                 ← Layer 2: one evaluation session
     ├─ Params / Metrics / Artifacts     ← Layer 3: run metadata
     ├─ Tags                             ← searchable labels
     ├─ Prompt Registry                  ← Layer 6: versioned prompts
     ├─ LoggedModel                      ← Layer 10: versioned config
     │   ├─ params, tags, aliases
     │   └─ linked metrics
     ├─ genai.evaluate()                 ← Layer 9: the orchestrator
     │   ├─ Evaluation Dataset           ← Layer 8: governed benchmarks
     │   ├─ Predict Function
     │   │   └─ Trace                    ← Layer 4: recorded execution
     │   │       └─ Spans                ← Layer 5: sub-steps
     │   │           └─ Linked Prompts   ← from Prompt Registry
     │   └─ Scorers                      ← Layer 7: judge functions
     │       └─ Feedback                 ← structured verdicts
     ├─ log_feedback()                   ← Layer 11: post-eval annotations
     │   ├─ Gate outcomes
     │   └─ Root-cause analysis
     └─ Labeling Session                 ← Layer 12: human review
         ├─ Label Schemas                ← structured forms
         ├─ Assigned reviewers
         └─ sync() → back to Dataset     ← closes the feedback loop
```

---

## The Feedback Loop

The most powerful thing about this stack is that it's circular. Each run's outputs feed the next run's inputs:

```
┌──────────────────────────────────────────────────────────────┐
│                                                              │
│  Evaluation Dataset ──→ evaluate() ──→ Traces + Feedback     │
│       ↑                                      │               │
│       │                                      ↓               │
│  sync() ←── Labeling Session ←── log_feedback() + Scorers   │
│       ↑                                      │               │
│       │                                      ↓               │
│  Human corrections              LoggedModel (snapshot)       │
│                                      │                       │
│                                      ↓                       │
│                              Promote / Rollback              │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

1. **Benchmarks** feed into **`evaluate()`**
2. **`evaluate()`** produces **Traces** with **Feedback** from **Scorers**
3. **`log_feedback()`** annotates traces with gate decisions and root causes
4. **Labeling Sessions** let humans review and correct
5. **`sync()`** pushes corrections back to the **Evaluation Dataset**
6. **LoggedModel** snapshots the config at each iteration
7. The best model gets the **"champion" alias**; bad iterations get **rolled back**
8. **Next run starts** with improved benchmarks and the cycle repeats

Every MLflow feature is a node in this loop. Remove any one, and the loop breaks.

---

## Quick Reference Card

| Layer | Feature | Key API | Connects To |
|-------|---------|---------|-------------|
| 1 | Experiment | `set_experiment()`, `set_experiment_tags()` | Contains Runs |
| 2 | Run | `start_run()` | Lives in Experiment, contains everything below |
| 3 | Params/Metrics/Artifacts | `log_params()`, `log_metrics()`, `log_dict()` | Stored on Run |
| 4 | Trace | `@mlflow.trace`, `update_current_trace()` | Created by predict_fn inside evaluate() |
| 5 | Span | `start_span()` | Nested inside Trace |
| 6 | Prompt Registry | `genai.register_prompt()`, `genai.load_prompt()` | Loaded inside Trace (auto-linked) |
| 7 | Scorer + Feedback | `@scorer`, `Feedback`, `AssessmentSource` | Called by evaluate(), attached to Trace |
| 8 | Evaluation Dataset | `genai.datasets.create_dataset()`, `merge_records()` | Fed into evaluate() as data |
| 9 | GenAI Evaluate | `genai.evaluate()` | Orchestrates Predict + Scorers + Data inside a Run |
| 10 | LoggedModel | `create_logged_model()`, `set_logged_model_alias()` | Linked to Run, receives metrics from evaluate() |
| 11 | log_feedback() | `log_feedback()` | Annotates Traces post-evaluation |
| 12 | Labeling Session | `genai.labeling.create_labeling_session()` | Reviews Traces, syncs to Dataset |
