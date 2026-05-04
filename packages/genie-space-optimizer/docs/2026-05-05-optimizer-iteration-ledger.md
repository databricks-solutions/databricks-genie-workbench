# Optimizer Iteration Ledger — Behavioral Hardening Cycles

> **Status:** Append-only ledger for the post-merge **behavioral** phase of `fix/gso-lossless-contract-replay-gate` and successor branches. Sibling to [`2026-05-02-phase-a-burndown-log.md`](./2026-05-02-phase-a-burndown-log.md): Phase A burned down structural ledger violations on a fixed corpus; this ledger burns down **optimizer behavior gaps** across a growing real-Genie corpus. The structural roadmap [`2026-05-01-burn-down-to-merge-roadmap.md`](./2026-05-01-burn-down-to-merge-roadmap.md) closed Phases 0 → H. This ledger picks up from there.

## Goal

Make "are we still making the optimizer better?" a question answerable from this ledger alone, not from intuition. Each cycle:

1. Names the inspiration run(s) and the corpus those runs sit in.
2. Lists the cross-run postmortem **clusters** (patterns that recur).
3. States one or more **AG hypotheses** — typed fix proposals with explicit RCA citation.
4. Names the **feature flag(s)** the change ships behind.
5. Records the **gate** outcome (byte-stable replay + corpus delta + skill accountability).
6. Appends one row to the [Ledger summary table](#ledger-summary-table-append-only).

The shape mirrors the optimizer's own `RCA Evidence → Cluster → Action Group → Proposal → Gate → Applied Patch → Eval Result → Learning` architecture so a meta-cycle reads like a lever-loop iteration. This is intentional: the same discipline that produced trustworthy structural phases produces trustworthy behavioral cycles.

## Operating principles

These ride on top of the Architecture / RCA-Grounded Decision Invariant / Observability Contract from `2026-05-01-burn-down-to-merge-roadmap.md`. They specialize that contract for behavioral iteration after Phase H landed.

1. **Multi-run corpus.** Cycle 1 starts with one inspiration run by necessity (it establishes the ledger). Every subsequent cycle must run on **≥2** spaces / runs before claiming a forward delta. One-run "wins" stay in the postmortem section; they do not move the corpus baseline.

2. **One cluster ⇒ one AG hypothesis ⇒ one cycle.** A cluster is a postmortem-finding pattern that recurs across runs. The AG hypothesis is the typed fix proposal that addresses that cluster with explicit RCA citation. A cycle MAY ship multiple AGs targeting **the same cluster** when the fix decomposes naturally (e.g., Cycle 1's six AGs across four clusters). A cycle MAY NOT stack independent clusters into one cycle — that confounds attribution and the ledger row becomes uninterpretable.

3. **Feature flags are the ablation discipline.** Every behavioral change ships behind a default-off env-var flag. Replay byte-stability holds with all flags **off** at every commit. Corpus delta is measured with the cycle's flags **on** at the closeout. If a cycle ships ≥2 flags, each flag must have an isolated unit/integration test that exercises only that flag.

4. **Gates per cycle.** A cycle exits with status `SHIPPED` iff:
   - **Byte-stable replay** — `tests/replay/test_lever_loop_replay.py::test_run_replay_airline_real_v1_within_burndown_budget` stays at `BURNDOWN_BUDGET = 0` with all flags off, on every per-task commit.
   - **Corpus delta** — at minimum, the inspiration run's targeted hard-failure resolves on a fresh real-Genie pilot with flags on; **no other corpus run regresses**. From Cycle 2 onward, "corpus" means ≥2 spaces.
   - **Skill accountability** — the `gso-postmortem` / `gso-lever-loop-run-analysis` skill must surface every typed marker the cycle relies on. If a finding required raw-stdout grepping, that's a skill bug; the corresponding skill-improvement plan ships **before** the optimizer cycle closes out.

5. **Stop-iteration signals.** Pause before opening a new cycle if any of these are true:
   - Postmortems begin recommending a fix already tried and rejected in a prior cycle.
   - Recommendations cross a "depth threshold" (e.g., "rewrite the strategist") that belongs in a fresh roadmap section, not a behavioral cycle.
   - `BURNDOWN_BUDGET` regression on a flags-off run.
   - Corpus delta is flat or mixed across ≥3 consecutive cycles — the corpus has saturated and a new corpus run on a fresh space is required.

6. **Ledger entries are append-only.** A cycle row is never edited after `SHIPPED` / `REVERTED`. Mistakes are corrected by a follow-up cycle that names the prior cycle's row and explains the correction.

## Cycle template

Each cycle creates one section under `## Cycles` using this exact shape (literal copy-paste boilerplate at [`## Appendix — Cycle template (boilerplate)`](#appendix--cycle-template-boilerplate)).

### Section 1: Corpus runs

| `opt_run_id` | Genie Space | Domain | Baseline % | Final % | Target QID(s) | Hard-fail QIDs at termination | Postmortem path |
|---|---|---|---|---|---|---|---|

### Section 2: Postmortem clustering — findings that recur

| Cluster ID | Pattern | Run(s) it appears in | Stage / `decision_type` | Failure bucket | Skill that surfaced it |
|---|---|---|---|---|---|

Cluster IDs use the form `C-<cycle>-<letter>` so a finding can be cited unambiguously (e.g., `C-1-A`, `C-2-B`).

### Section 3: AG hypothesis(es)

For each AG, one fenced block:

```text
AG-<cycle>-<letter>: <name>
  Cluster: C-<cycle>-<letter>
  RCA: <one sentence linking observed effect → root cause>
  Causal target: <stage / file / function being changed>
  Expected effect: <which corpus run, which QID(s), which observable>
  Negative-space: <what must NOT regress; specify the flags-off baseline>
  Feature flag: GSO_<NAME>
  Plan ref: <link to the implementation plan doc + task letter>
```

### Section 4: Feature flags introduced this cycle

| Flag | Default | Touches | Isolated test |
|---|---|---|---|

### Section 5: Gate results

| Gate | Status | Evidence |
|---|---|---|

Required rows:
- Byte-stable replay (flags off)
- Corpus delta (flags on)
- Skill accountability — one row per typed marker / skill-update plan the cycle depends on.

### Section 6: Decision

`SHIPPED` (flags flipped on, ledger row finalized) / `HOLD` (code landed but flags stay off pending more corpus) / `REVERTED` (cycle aborted; flag scaffolding stays for Cycle N+1).

### Section 7: Seeds for next cycle

Open postmortem clusters that did NOT get a fix this cycle, with their projected next AG. Format:

```text
C-<cycle>-<letter> (seed): <pattern>
  Run(s) observed: <opt_run_id list>
  Hypothesis for next cycle: <one sentence>
  Blocking on: <"≥2 corpus runs" / "skill update X" / "depth threshold review">
```

## Ledger summary table (append-only)

| Cycle | Date | Inspiration run(s) | Cluster(s) | AG hypothesis(es) | Flag(s) | Plan | Corpus before | Corpus after | Status | Notes |
|---|---|---|---|---|---|---|---|---|---|---|

## Cycles

(Cycle entries appear below in chronological order. Each cycle uses the [Cycle template](#cycle-template).)

## Appendix — Cycle template (boilerplate)

Paste this block to start a new cycle. Replace `<cycle>` with the cycle number, fill in dates and run IDs, and keep the section headings exactly as written so the table-of-contents anchors stay stable.

```markdown
### Cycle <cycle> — YYYY-MM-DD — <one-line summary>

**Inspiration:** [`docs/runid_analysis/<opt_run_id>/postmortem.md`](./runid_analysis/<opt_run_id>/postmortem.md). <one paragraph: what the run produced, where it stalled, what the practical ceiling is, why this cycle exists>.

**Implementation plan:** [`<YYYY-MM-DD-cycle-N-...>.md`](./<YYYY-MM-DD-cycle-N-...>.md).

**Corpus discipline note:** <"Corpus-of-N (N spaces) — meets the ≥2-run rule." OR "Corpus-of-one — only acceptable for Cycle 1, which establishes the ledger.">

#### Section 1: Corpus runs

| `opt_run_id` | Genie Space | Domain | Baseline % | Final % | Target QID(s) | Hard-fail QIDs at termination | Postmortem path |
|---|---|---|---|---|---|---|---|
| ... | ... | ... | ... | ... | ... | ... | `runid_analysis/<id>/postmortem.md` |

#### Section 2: Postmortem clustering

| Cluster ID | Pattern | Run(s) | Stage / `decision_type` | Failure bucket | Skill that surfaced it |
|---|---|---|---|---|---|
| `C-<cycle>-A` | ... | ... | ... | ... | ... |

#### Section 3: AG hypotheses

(One fenced block per AG, using the format from `Section 3: AG hypothesis(es)` in the cycle template.)

#### Section 4: Feature flags introduced this cycle

| Flag | Default | Touches | Isolated test |
|---|---|---|---|
| `GSO_<NAME>` | off | `path/to/file.py:fn_name` | `tests/unit/test_<flag>.py::test_<...>` |

End-to-end (all flags on): `tests/integration/test_<cycle>_flags_end_to_end.py`.

#### Section 5: Gate results

| Gate | Status | Evidence |
|---|---|---|
| Byte-stable replay (flags off) | TBD / PASS / FAIL | `pytest tests/replay/test_run_replay_airline_real_v1_within_burndown_budget` — exit + budget value |
| Corpus delta (flags on) | TBD / PASS / HOLD / REGRESS | per-run before/after table linked from postmortems |
| Skill accountability — `<MARKER_NAME>` | PASS / GAP closed by `<plan-ref>` | `<one-line evidence>` |

#### Section 6: Decision

**Cycle <cycle> status: <IN FLIGHT / SHIPPED / HOLD / REVERTED>.** <one paragraph: what landed, what didn't, what the corpus delta says>.

#### Section 7: Seeds for next cycle

```text
C-<cycle>-<letter> (seed): <pattern>
  Run(s) observed: <opt_run_id list>
  Hypothesis for next cycle: <one sentence>
  Blocking on: <"≥2 corpus runs" / "skill update X" / "depth threshold review">
```
```

End of doc.
