# Cycle 11 — Falsification Probe Report

**Hypothesis:** Hand-crafting an L6 SQL-snippet patch that encodes
the cluster's `asi_counterfactual_fixes` will flip the target qid
from `ground_truth_correct/no` to `both_correct/yes`.

**Why this matters:** Cycle 11 closes the loop so we can *verify*
process correctness, but does not synthesize L6 patches. If Genie
ignores SQL snippets for these archetypes, no future synthesis
work in the optimizer can move accuracy on these spaces — the
investment goes to space-config or sample-question layers instead.

## Spaces and target qids

| Space | qid | Cluster | Root cause | Counterfactual |
|---|---|---|---|---|
| <SPACE_ID> (airline) | gs_024 | H004 | missing_filter | "do not add `PAYMENT_CURRENCY_CD = 'USD'`, do not require IS NOT NULL on payment columns" |
| <SPACE_ID> (7NOW) | gs_026 | H002 | plural_top_n_collapse + asset routing | "use `mv_esr_dim_location.zone_vp_name`, return ranked plural results not LIMIT 1" |

## Procedure

1. Open each space in the Genie console.
2. Add an `add_sql_snippet_filter` (airline) / `add_sql_snippet_expression` (7NOW) by hand, encoding the counterfactual.
3. Run a single-question eval on the target qid (use `gso eval --qids gs_024 --space-id ...`).
4. Record verdict, judge reasoning excerpt, and whether the snippet appears in Genie's reasoning trace.

## Result template

| Space | qid | Pre-probe verdict | Post-probe verdict | Snippet visible in trace | Decision |
|---|---|---|---|---|---|
| airline | gs_024 | ground_truth_correct/no | <fill> | <yes/no> | <build WS / redirect / propagation bug> |
| 7NOW | gs_026 | ground_truth_correct/no | <fill> | <yes/no> | <build WS / redirect / propagation bug> |

## Decision rule

- **Either qid flips to pass** → a future cycle (12+) is justified in building deterministic L6 synthesis.
- **Neither flips, snippet visible in Genie trace** → Genie ignores SQL snippets for these archetypes; future cycles redirect to space-config or sample-question layers.
- **Snippet not visible in trace** → upstream propagation bug (the `Patched objects: (none)` warning at airline `text.txt:538`); fix that before any further L6 work.

## Audit

- Probe operator: <name>
- Date: <YYYY-MM-DD>
- Probe duration: <minutes>
- Result: <attached / fill above>
