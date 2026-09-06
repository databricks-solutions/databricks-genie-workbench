# Ontology — Wave 2 parallel-build launcher (Claude Code, isolated worktrees)

Wave 1 (Phase-5 apply offline slice, Stage-4.1d Step 2, Phase-3e Step A, §9/§10 drivers) is
**LANDED + integrated** on `ontology`. This is **Wave 2**: three lanes that own **disjoint
subtrees**, so their merges are conflict-free by construction — the only coupling is the frozen
draft-body API contract between Lane 2 (backend) and Lane 3 (frontend).

## The three lanes

| Lane | Subtree it OWNS (disjoint) | Driver | Merge |
|---|---|---|---|
| **1** | `…/genie_space_optimizer/ontology/eval_harness.py` (+ wheel test) | `ontology-eval-harness-driver.md` | any (nominally first) |
| **2** | `backend/ontology/**` (+ backend test) | `ontology-stage4.1d-step34-backend-driver.md` | before Lane 3 |
| **3** | `frontend/src/ontology/**` (+ frontend lockfile) | `ontology-frontend-batch-driver.md` | last |

No file is edited by two lanes. Lane 3 builds its draft UI against the **frozen contract** in the
Lane-2 driver (those endpoints are in Lane 2's worktree, so they won't exist in Lane 3's — they
integrate at merge). The estate-graph tab wires to `GET /api/ontology/graph`, which is **already**
on `ontology` (Phase-3e Step A).

## Preconditions (verify once, before launching)

- On branch `ontology`, working tree clean (`git status`).
- `.claude/settings.local.json` contains `{"worktree": {"baseRef": "head"}}` — so each subagent
  worktree branches from your **local** `ontology` HEAD (carrying all unpushed work), not `origin`.
- `.claude/agents/ontology-lane-builder.md` exists (`isolation: worktree`; offline-only guardrails).
  Each subagent with this frontmatter gets its **own** temporary worktree; Claude Code auto-cleans a
  worktree that ends with no changes and keeps one that has changes for review.

## Launch prompt (paste verbatim into Claude Code on `ontology`)

```text
Run Wave 2 of the ontology parallel build. Launch THREE `ontology-lane-builder` subagents IN
PARALLEL (one message, three Task calls) — each is isolation: worktree, branched from the current
`ontology` HEAD. Give each subagent exactly ONE driver and tell it to read that driver in full and
obey its OWNS / OFF-LIMITS / MERGE-ORDER header literally:

- Lane 1 (wheel):    docs/design/ontology-eval-harness-driver.md
- Lane 2 (backend):  docs/design/ontology-stage4.1d-step34-backend-driver.md
- Lane 3 (frontend): docs/design/ontology-frontend-batch-driver.md

Each lane: build offline per its driver's BUILD sections, touch ONLY the files it OWNS, get its
suite green (Lane 1/2: ./scripts/test.sh + the wheel suite; Lane 3: cd frontend && npm ci && npm run
lint && npx tsc -b && the frontend test runner), commit on its worktree branch, then STOP and
report (worktree branch, `git diff --stat`, test summary). No lane deploys, runs the job, writes
governed tags, adds an unauthorized dependency, or edits docs/design/mv-advisor-playbook.md.

When all three report green, integrate: merge the worktree branches into `ontology` in order —
Lane 1, then Lane 2, then Lane 3. After the final merge, run `./scripts/test.sh` and `cd frontend &&
npm ci && npm run lint && npx tsc -b && the frontend test runner`; report the integrated result.
Do NOT deploy — a human runs the deploy-verify gate.
```

## After integration (human-gated — not for the agents)

1. **Deploy-verify** on `fevm-serverless` (`SKIP_FRONTEND_BUILD=1 ./scripts/deploy.sh --update` for
   wheel/backend-only checks; full `./scripts/deploy.sh --update` once the frontend lane is merged),
   scoped to `serverless_stable_6t92c3_catalog`. Exercise: Draft-with-AI (single + sub-domain), the
   cytoscape Graph tab, the refresh auto-reload; confirm `evidence.body_source ∈ {llm_ondemand,
   llm_bulk}` survives a re-materialize. Run the eval harness for the **baseline** report (P/R/F is
   N/A until §9 lands — expected).
2. **Record** each lane's landing + live evidence in `mv-advisor-playbook.md` (human edit).

## Explicitly OUT of Wave 2 (human gates / blocked — do NOT put in a lane)

- **Phase-5 finish**: resolve the `execute_apply` identity TODOs (metastore/workspace/applied_by),
  build `ApplyPreview.tsx`, and the **live consented `SET TAG` apply** — a human-run gate.
- **§9 industry-reference alignment (MV-D58)**: **blocked** on the Phase-4 Context Pack seam; the
  eval harness (Lane 1) degrades P/R/F to N/A until it lands.
