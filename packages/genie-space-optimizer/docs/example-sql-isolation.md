# Example-SQL Generation — Isolation Audit + Firewall Invariants

Status: normative spec for the unified example-SQL generator.
Owners: GSO engine team.
Last updated: 2026-04-24 (Phase 0 of `unify-example-sql-onto-benchmark-engine`).

This document is the single source of truth for the Bug #4 leakage-prevention
contract between benchmark generation and example-SQL generation. Every
change to the example-SQL producer, the leakage firewall, or the benchmark
corpus must preserve the invariants described here. The lint rule at
`scripts/lint_example_sql_isolation.py` enforces the machine-checkable
subset of these invariants on every commit.

## Background

The Bug #4 remediation disabled verbatim copying of benchmark `expected_sql`
into `instructions.example_question_sqls`. The replacement — `preflight_synthesis`,
an archetype-templated per-candidate generator — is leak-free but under-yields
(typical ~3 survivors per space versus the benchmark generator's ~25). The
unified generator closes that gap by *reusing* the benchmark generation engine
from a sibling call site, with four independent firewalls that guarantee no
benchmark content reaches the example-SQL producer as input and no generated
example matches a benchmark on output.

## The four firewall invariants

### Invariant 1 — Input firewall (function signature)

The example-SQL producer `generate_example_sqls` MUST NOT accept a parameter
that transports benchmark text. Specifically, no parameter may be named in:

```
{"benchmarks", "benchmark_list", "existing_benchmarks",
 "benchmark_questions", "benchmark_sqls", "expected_sqls",
 "eval_questions", "benchmark_corpus"}
```

Instead, the producer receives a `LeakageOracle` via a **required** keyword
argument. The oracle is an opaque match API — it returns booleans only.
A caller who forgets to pass `leakage_oracle` gets a `TypeError` at call
time rather than silent contamination.

Enforced by:
- Runtime: required-kwarg signature on `generate_example_sqls` +
  `generate_validated_sql_examples` cores (Phase 1.R1 / Phase 3.R3).
- Source: AST-based lint in `scripts/lint_example_sql_isolation.py`
  (Phase 5.R5b) that walks the two target functions and fails CI on any
  forbidden parameter name.

### Invariant 2 — Input firewall (prompt template)

The example-SQL generation and correction prompts
(`EXAMPLE_SQL_GENERATION_PROMPT` and `EXAMPLE_SQL_CORRECTION_PROMPT`,
both in `common/config.py`) MUST NOT reference any template variable whose
name matches the benchmark-derived set listed for Invariant 1.

Enforced by:
- Module-load-time `assert` at the bottom of `common/config.py`
  (Phase 2.R2b) that fails import if a forbidden variable leaks in.

### Invariant 3 — Output firewall (SQL fingerprint)

Every validated candidate passes through `LeakageOracle.contains_sql(sql)`
against the *union* of:

- `BenchmarkCorpus.from_benchmarks(benchmarks)` — the current run's
  benchmark corpus.
- `BenchmarkCorpus.from_benchmarks(existing_example_sqls_as_bm_rows)` —
  every already-installed `instructions.example_question_sqls` entry
  rephrased as a corpus row (prevents duplicate generations).

The oracle delegates to the existing `_check_string_against_corpus` in
`leakage.py`, which combines:

- SHA-256 of `canonicalize_sql` against `sql_fingerprints` (exact match).
- N-gram Jaccard (char trigrams) vs `sql_shingles` with threshold
  `NGRAM_SIMILARITY_THRESHOLD=0.60`.
- Optional embedding cosine vs `sql_embeddings` when available with
  threshold `EMBEDDING_SIMILARITY_THRESHOLD=0.85`.

Matches are rejected and the counter `firewall_drops["fingerprint"]` is
incremented.

### Invariant 4 — Output firewall (question echo)

Every validated candidate additionally passes through
`LeakageOracle.contains_question(question, threshold=0.85)`. This is a
separate firewall from Invariant 3 — it catches the case where the LLM
paraphrases a benchmark question but writes different SQL, which would
still leak training-vs-eval signal via the questions being semantically
identical.

The match uses `_question_token_set_jaccard` over tokens produced by
`_normalize_question_text` (lower + strip + collapse whitespace + drop
punctuation + drop common English stopwords). Threshold tunable via env
var `GSO_EXAMPLE_SQL_QUESTION_ECHO_THRESHOLD`, default `0.85`. Matches
are rejected and the counter `firewall_drops["question_echo"]` is
incremented.

### Last-mile belt-and-suspenders

`_apply_proactive_example_sqls` at `harness.py:1680` already runs
`is_benchmark_leak` on every proposal before the applier persists it.
This path remains active under the unified generator — Phase 6 has an
explicit regression test pinning the line.

## Audit — today's benchmark-ingestion surface

Every function in the pre-flight / enrichment path that accepts a
`benchmarks` parameter as of the unified-generator refactor. The unified
producer must route none of its inputs through any of these except the
last-mile firewall at the applier.

| File | Function | Why it receives benchmarks |
|---|---|---|
| `optimization/evaluation.py` | `generate_benchmarks` | IS the benchmark producer |
| `optimization/evaluation.py` | `_attempt_benchmark_correction` | correction LLM retry loop |
| `optimization/preflight.py` | `preflight_validate_benchmarks` | EXPLAIN+execute validation |
| `optimization/preflight.py` | `_load_or_generate_benchmarks` | load existing or generate |
| `optimization/benchmarks.py` | `validate_benchmarks` | per-row validation |
| `optimization/harness.py` | `_apply_proactive_example_sqls` | trusted last-mile firewall |
| `optimization/applier.py` | `apply_patch_set` | firewall when `benchmarks` passed through |
| `optimization/preflight_synthesis.py` | `run_preflight_example_synthesis` | firewall corpus for archetype path |
| `optimization/cluster_driven_synthesis.py` | `run_cluster_driven_synthesis_for_single_cluster` | firewall corpus for reactive path |
| `optimization/optimizer.py` | Miner functions (`_convert_instructions_to_sql_expressions` etc.) | firewall corpus |

The unified generator `generate_example_sqls` is deliberately absent from
this list. It MUST NEVER be added.

## `_data_profile` provenance

Confirmed in `optimization/preflight.py:_collect_data_profile`: the data
profile is built by sampling the warehouse (`SELECT ... LIMIT N` over
each asset column). It has no benchmark dependency. Safe to thread into
the unified generator's prompt.

## `BenchmarkCorpus` — why the oracle wrapper is justified

`BenchmarkCorpus` is a dataclass declared at `leakage.py:128`. Its public
fields include:

```
questions: list[str]
expected_sqls: list[str]
question_shingles: list[set[str]]
sql_shingles: list[set[str]]
sql_fingerprints: set[str]
question_ids: list[str]
question_embeddings: list[list[float]] | None
sql_embeddings: list[list[float]] | None
```

A caller with a `BenchmarkCorpus` reference can read any of these fields
directly — they are the full benchmark content. For the applier's
last-mile firewall this is fine (it's in the trusted zone). For the
unified example-SQL generator it would be a side-channel. The
`LeakageOracle` wrapper (Phase 1.R1b) exposes only `contains_sql` and
`contains_question` — no `__iter__`, no text getters, no `__repr__`
that leaks content.

## Threat model — what the firewalls catch

| Leak mode | Caught by |
|---|---|
| LLM verbatim-copies a benchmark SQL | Invariant 3 (SQL fingerprint) |
| LLM generates a near-paraphrase of a benchmark SQL (different whitespace, aliases) | Invariant 3 (n-gram Jaccard) |
| LLM generates a semantically-equivalent SQL with different text | Invariant 3 (embedding cosine, when available) |
| LLM paraphrases a benchmark question but writes different SQL | Invariant 4 (question echo) |
| A future refactor adds `benchmarks=` to the producer | Invariant 1 (signature + lint) |
| A future prompt edit interpolates benchmark text | Invariant 2 (module-load assert) |
| A rogue path bypasses the producer and PATCHes directly | Last-mile firewall at applier |

## Out of scope

- Semantic-equivalence firewall (detecting logically-equivalent SQL
  with different text even when embeddings disagree) — deferred.
- Backfill of already-installed example_question_sqls against the new
  firewall — the last-mile firewall catches future leaks; old entries
  are the operator's responsibility to curate.
- Isolation from non-benchmark sources (e.g. the optimizer's own
  iteration history) — these are not evaluation ground truth so leakage
  is not a correctness concern.

## Risk Lanes and Gate Stack (added 2026-04-30)

Enrichment is partitioned into two lanes based on observed regression
risk:

**Safe lane** — patches that almost never regress baseline behaviour.
- Description / synonym / format / entity helpers.
- Execution-proven join specs from baseline ``both_correct`` and
  ``ground_truth_correct`` rows (verdict-side-aware extraction).
- Instruction prose mining and miner-first promotion.

**High-risk lane** — patches where individual self-consistency does
not imply teaching safety.
- Example SQLs (unified or preflight-archetype path).
- Example-derived joins (require corroboration: UC FK or
  ``both_correct`` baseline join pair).

The high-risk lane runs candidates through this gate stack, in order:

1. **Leakage firewall (strict)** — `LeakageOracle.evaluate_example_sql`
   blocks SQL fingerprint or n-gram overlap by default. Toggle with
   ``GSO_EXAMPLE_SQL_FIREWALL_STRICT=false`` to revert to warn-only
   mode (NOT recommended — see plan rationale).
2. **Correctness arbiter (schema-aware)** — `score_example_sql_correctness`
   now plumbs `_asset_semantics` into the prompt so MV-vs-table
   routing errors are caught.
3. **Deterministic teaching-safety gates** — `example_safety.check_teaching_safety`
   runs structural checks (anti-pattern syntax, MV routing, unknown
   asset, unregistered join, extra-filter risk).
4. **Teaching-safety LLM judge** — `score_example_sql_teaching_safety`
   judges canonicality, grain, KPI over-teaching. Toggle with
   ``GSO_EXAMPLE_SQL_TEACHING_SAFETY=false`` to disable.
5. **Pre-promotion smoke test** — `run_pre_promotion_smoke_test`
   stages the surviving batch into a copy of the config and runs a
   capped eval against baseline ``both_correct`` questions. Rejects
   the entire batch when regressions exceed
   ``GSO_EXAMPLE_SQL_SMOKE_REGRESSION_TOLERANCE_PP``.

Synthetic eval rows derived from accepted example SQLs carry the
``synthetic_example`` verdict (NOT ``ground_truth_correct``). The
proven-join extractor skips them; corroborated example-derived joins
are mined separately by `_mine_and_apply_joins_from_example_sqls`,
which requires either a UC FK or a baseline ``both_correct`` join
pair to promote a candidate.

### Operator knobs

| Env var | Default | Effect |
|---|---|---|
| ``GSO_EXAMPLE_SQL_FIREWALL_STRICT`` | ``true`` | Strict-mode leakage firewall blocks SQL pattern overlap; ``false`` warns only. |
| ``GSO_EXAMPLE_SQL_TEACHING_SAFETY`` | ``true`` | Enables the teaching-safety LLM judge. |
| ``GSO_EXAMPLE_SQL_TEACHING_SAFETY_PROMPT`` | ``example_sql_teaching_safety`` | Override the registered prompt name. |
| ``GSO_EXAMPLE_SQL_SMOKE_TEST_ENABLED`` | ``true`` | Enables the pre-promotion smoke test. |
| ``GSO_EXAMPLE_SQL_SMOKE_REGRESSION_TOLERANCE_PP`` | ``0.0`` | Reject batch when regression percentage exceeds this. |
| ``GSO_EXAMPLE_SQL_SMOKE_MAX_QUESTIONS`` | ``20`` | Cap on baseline ``both_correct`` questions to re-run for the smoke test. |

### Funnel observability

`_print_enrichment_risk_lane_banner` emits a per-gate counter banner
(``ENRICHMENT — HIGH-RISK LANE``) so operators can attribute yield
shortfall to a specific gate (firewall vs correctness vs deterministic
safety vs teaching-safety judge vs smoke test) rather than one opaque
``rejected N``.
