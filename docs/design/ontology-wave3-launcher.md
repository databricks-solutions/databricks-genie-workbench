# Ontology — Wave 3 parallel-build launcher (Claude Code, isolated worktrees)

**Wave 3 = Ontology Map v2 (MV-D73/74/75).** Three lanes that own **disjoint subtrees** (wheel /
backend / frontend), so their merges are conflict-free by construction. The only coupling is two
frozen contracts: the **blob shape** (Lane 1 → Lane 2) and the **graph/expand API** (Lane 2 → Lane 3).
Spec: `docs/design/ontology-map-v2-build.md`.

## The three lanes

| Lane | Subtree it OWNS (disjoint) | Driver | Merge |
|---|---|---|---|
| **1** | wheel `…/ontology/{layout,materialize}.py` (+ wheel test) | `ontology-map-v2-wheel-driver.md` | **first** |
| **2** | `backend/ontology/**` (+ backend test) | `ontology-map-v2-backend-driver.md` | after 1, before 3 |
| **3** | `frontend/src/ontology/**` | `ontology-map-v2-frontend-driver.md` | **last** |

No file is edited by two lanes. Lane 2 builds against Lane 1's blob keys (`origin`, `subdomains.edges`,
`snippets`); Lane 3 builds against Lane 2's `?origin=` + `/graph/expand` — those live in the sibling
worktrees, so they won't exist in a given lane until merge. That is expected.

## Preconditions (verify once, before launching)

- On branch `ontology`, working tree clean (`git status`).
- `.claude/settings.local.json` contains `{"worktree": {"baseRef": "head"}}` — each subagent worktree
  branches from your **local** `ontology` HEAD (carrying all unpushed work), not `origin`.
- `.claude/agents/ontology-lane-builder.md` exists (`isolation: worktree`; offline-only guardrails).
  Each subagent with this frontmatter gets its **own** temporary worktree; Claude Code auto-cleans a
  worktree that ends with no changes and keeps one that has changes for review.

## Launch prompt (paste verbatim into Claude Code on `ontology`)

```text
Run Wave 3 of the ontology parallel build (Ontology Map v2). Launch THREE `ontology-lane-builder`
subagents IN PARALLEL (one message, three Task calls) — each is isolation: worktree, branched from the
current `ontology` HEAD. Give each subagent exactly ONE driver and tell it to read that driver in full
and obey its OWNS / OFF-LIMITS / MERGE-ORDER header + frozen-contract block literally:

- Lane 1 (wheel):    docs/design/ontology-map-v2-wheel-driver.md
- Lane 2 (backend):  docs/design/ontology-map-v2-backend-driver.md
- Lane 3 (frontend): docs/design/ontology-map-v2-frontend-driver.md

Each lane: build offline per its driver's BUILD sections, touch ONLY the files it OWNS, get its suite
green (Lane 1/2: ./scripts/test.sh + the wheel suite; Lane 3: cd frontend && npm ci && npm run lint &&
npx tsc -b && the frontend test runner), commit on its worktree branch, then STOP and report (worktree
branch, `git diff --stat`, test summary). No lane deploys, runs the job, writes governed tags, adds a
dependency (uv.lock / package-lock.json byte-identical), or edits docs/design/mv-advisor-playbook.md.

When all three report green, integrate: merge the worktree branches into `ontology` in order — Lane 1,
then Lane 2, then Lane 3. After the final merge, run `./scripts/test.sh` and `cd frontend && npm ci &&
npm run lint && npx tsc -b && the frontend test runner`; report the integrated result. Do NOT deploy —
a human runs the deploy-verify gate.
```

## After integration (human-gated — not for the agents)

1. **Deploy-verify** on `fevm-serverless`: full `./scripts/deploy.sh --update` (frontend changed) →
   materialize job scoped to `serverless_stable_6t92c3_catalog`. Assert: snapshot rollups carry
   `origin` + typed asset `kind`s; `subdomains.edges` present; `GET /graph/expand` returns measures for
   a metric_view and Pages for a sub-domain; UI Applied is the default and reads as current-state,
   Proposed shows dashed/"Suggested"; drill Domain→Sub-domain→Assets→(tap) measures/Pages; no
   overlapping-box mush; reload byte-stable.
2. **Record** each lane's landing + live evidence in `mv-advisor-playbook.md` (human edit) —
   flip MV-D73/74/75 to `LANDED + deploy-verified`.

## Explicitly OUT of Wave 3 (human gates / follow-ups — do NOT put in a lane)

- **Cross-refresh position persistence** (anchor unchanged assets run-to-run) — optional follow-up.
- **A separate applied-taxonomy graph build** — v2 uses the `origin` filter over the one snapshot;
  a dedicated applied source is a later decision if the filter proves too coarse (MV-D74 §8).
- **WebGL renderer swap** — only if we lift the 2,000-node cap (out of scope, MV-D45).
