# Refactor: Turn the How-It-Works Page into a Single Guided Interactive Experience

## The Problem

The current `/how-it-works` page reads like a **collection of separate documentation sections** stacked vertically. Each section is fine on its own, but they don't feel connected — there's no sense of narrative flow, no feeling of "I'm being guided through a process." A user lands on this page and sees a wall of expandable panels rather than a story that unfolds.

What we need is closer to an **interactive product tour** — think of how Stripe's documentation walks you through a payment flow, or how Linear's landing page guides you through their product with a single scrolling narrative where each section transitions visually into the next. The user should feel like they're *traveling through the pipeline*, not reading about it.

## The Core Design Shift

**Current state:** Independent sections, each with their own visuals, laid out as a vertical stack. The user scrolls and clicks into things. The sections don't know about each other.

**Target state:** A single, stateful guided experience with ONE primary navigation mechanism — a step-by-step walkthrough where the user advances through the pipeline stages sequentially. Each transition should feel like progression, not like jumping to a different part of a page.

## Concrete Architecture

### 1. Single-State Stepper as the Backbone

The entire page should be controlled by a single piece of state: `currentStage` (an index from 0 to N). Everything on the page reacts to this state.

```
const STAGES = [
  { id: 'overview',       title: 'The Big Picture' },
  { id: 'preflight',      title: 'Preflight' },
  { id: 'preflight-benchmarks', title: 'Generating Benchmarks' },
  { id: 'baseline',       title: 'Baseline Evaluation' },
  { id: 'judges',         title: 'The 9 Judges' },
  { id: 'enrichment',     title: 'Enrichment' },
  { id: 'lever-loop',     title: 'The Optimization Loop' },
  { id: 'failure-analysis', title: 'Failure Analysis' },
  { id: 'levers',         title: 'The 5 Levers' },
  { id: 'three-gates',    title: 'The 3-Gate Pattern' },
  { id: 'convergence',    title: 'Convergence & Safety' },
  { id: 'finalize',       title: 'Finalize & Deploy' },
]
```

Notice these are MORE granular than the 6 pipeline stages — we're breaking the complex stages (especially the Lever Loop) into multiple walkthrough steps so each screen focuses on one concept.

### 2. Layout: Fixed Left Rail + Animated Content Area

```
┌──────────────────────────────────────────────────────┐
│  [Navbar]                                            │
├────────────┬─────────────────────────────────────────┤
│            │                                         │
│  Progress  │     Main Content Area                   │
│  Rail      │     (AnimatePresence — one stage        │
│            │      visible at a time, transitions     │
│  ● Overview│      horizontally like a deck of        │
│  ● Preflight│     slides)                            │
│  ○ Baseline│                                         │
│  ○ ...     │     ┌─────────────────────────────┐     │
│  ○ ...     │     │  Visual + explanation for    │     │
│  ○ Finalize│     │  the current stage           │     │
│            │     └─────────────────────────────┘     │
│            │                                         │
│            │     [← Back]            [Next →]        │
├────────────┴─────────────────────────────────────────┤
│  Keyboard: ← →   Stage 3 of 12                      │
└──────────────────────────────────────────────────────┘
```

**The left rail** is a vertical progress indicator — dots or a vertical line with nodes. Completed stages are filled, the current stage is highlighted and pulsing subtly, future stages are dim. Clicking any completed or current+1 stage navigates there. This gives the user a sense of where they are in the journey at all times.

**The main content area** uses `AnimatePresence` with directional transitions — advancing forward slides content left-to-right, going back slides right-to-left. Only ONE stage is visible at a time. This is the critical difference from the current design: we're NOT showing all sections and letting the user scroll. We're showing one thing, explaining it well, and then moving forward.

**Navigation** is via Next/Back buttons at the bottom, keyboard arrows, and clicking the progress rail. Next button should have a label that previews where you're going: "Next: Baseline Evaluation →" rather than just "Next →".

### 3. Each Stage Screen: Visual First, Text Second

Every stage screen should follow this template:

```
┌─────────────────────────────────────────┐
│                                         │
│   Stage Title                           │
│   One-sentence subtitle                 │
│                                         │
│   ┌─────────────────────────────────┐   │
│   │                                 │   │
│   │   PRIMARY VISUAL                │   │
│   │   (diagram, animation, or       │   │
│   │    interactive element —        │   │
│   │    takes up 50-60% of the       │   │
│   │    vertical space)              │   │
│   │                                 │   │
│   └─────────────────────────────────┘   │
│                                         │
│   2-4 sentences of explanation below    │
│   the visual. Short. Conversational.    │
│                                         │
│   [Optional: expandable "Learn more"    │
│    accordion for deeper detail]         │
│                                         │
└─────────────────────────────────────────┘
```

The visual is the hero of every screen. The text supports it, not the other way around. If someone only looked at the visuals and never read the text, they should still get the gist.

### 4. Specific Visual Direction Per Stage

Here's what each stage screen should feel like — refactor the existing visuals to fit this model:

**Overview:** An animated horizontal pipeline diagram where the 6 stages are connected nodes. As the user watches, a glowing pulse travels from left (Preflight) to right (Deploy), showing the data flow. Below it, 2 sentences: what the tool does and why it matters. This is the "hook" screen.

**Preflight:** An animated checklist where items check off one by one with a satisfying micro-animation (checkmark draws itself). Each item has a small icon. When the last item checks off, a "Ready" badge appears. The visual should feel like a system booting up.

**Generating Benchmarks:** Show a stylized table schema on the left, and benchmark question cards "generating" from it on the right — cards slide in one by one from the schema, each showing a natural language question and its expected SQL. Conveys: "benchmarks are derived from YOUR data."

**Baseline Evaluation:** The benchmark cards from the previous screen now flow through a "judge panel" — visualized as 9 small evaluator icons. Each card gets a score badge. An accuracy gauge fills up at the bottom. This should feel like a testing/grading process.

**The 9 Judges:** A 3×3 grid where each judge card is interactive — hover or click reveals what it checks, its threshold, and whether it's LLM-based or deterministic. Use color coding: green for passing, amber for close, red for failing. This is a reference screen the user can explore freely before moving on.

**Enrichment:** A before/after split-screen. Left side shows a "bare" table config (empty descriptions, no joins). Right side shows the same config enriched (descriptions filled in, joins discovered, instructions seeded). Use a slider or animated transition between the two states.

**The Optimization Loop:** THE key visual. An animated circular loop diagram: Analyze → Cluster → Strategize → Patch → Evaluate → Accept/Rollback → (back to Analyze). The loop should visually "spin" through one iteration when the user arrives on this screen, then pause. Include a small iteration counter: "Iteration 1 of up to 5." The user should viscerally feel "this is a loop that keeps improving."

**Failure Analysis:** A visual showing evaluation results funneling into failure clusters. Show example failures grouping by pattern (wrong_column, missing_join, etc.) and then mapping to levers. This is the "diagnosis" step — make it feel like triage.

**The 5 Levers:** Five vertical cards in a row, each with an icon, name, and the failure types it addresses. Clicking a lever card highlights which parts of the loop diagram it affects. Convey: "each lever is a specialized tool for a specific type of problem."

**The 3-Gate Pattern:** A cascading funnel visual: wide at the top (Slice Gate — fast, cheap), narrowing in the middle (P0 Gate — top 3 questions), narrow at the bottom (Full Gate — all benchmarks). Show a "fast fail" path where a red X at the Slice Gate short-circuits directly to Rollback, skipping the expensive gates. This is about building trust: "we don't waste compute, and we catch regressions early."

**Convergence & Safety:** An interactive state machine diagram with the run statuses. Highlight the safety constants (regression threshold, rollback limit, plateau detection) as "guardrails" — maybe visualize them as bumpers or fences around the optimization path. Convey: "the system knows when to stop."

**Finalize & Deploy:** A "finish line" visual: repeatability test (two checkmarks side by side), model promotion (a trophy or champion badge), optional deployment (an arrow crossing from one workspace to another with an approval gate). This should feel conclusive — the journey is complete.

### 5. Continuity Threads

These are the details that make it feel like ONE experience rather than separate sections:

- **Persistent pipeline mini-map.** In addition to the left rail, show a tiny version of the overview pipeline diagram at the top of every screen, with the current stage highlighted. This anchors the user in the overall flow at all times.

- **Transition animations that connect stages.** When moving from "Baseline Evaluation" to "The 9 Judges," the judge icons from the baseline visual should `layoutId`-animate into their expanded positions on the judges screen. When moving from "Failure Analysis" to "The 5 Levers," the lever labels in the failure mapping should animate into the lever cards. These shared-layout animations create the feeling that you're zooming into detail, not jumping to a new page.

- **Consistent data example.** Use ONE fictional Genie Space throughout the entire walkthrough (e.g., a "Revenue Analytics" space with 3 tables: `fact_sales`, `dim_product`, `dim_date`). Every benchmark example, every failure example, every patch example should reference this same fictional space. This prevents the jarring feeling of context-switching between unrelated examples.

- **Color-coded pipeline stages.** Assign each of the 6 pipeline stages a subtle color accent (e.g., Preflight = blue, Baseline = purple, Enrichment = teal, Lever Loop = amber, Finalize = green, Deploy = slate). Use this color as a background tint or border accent on the corresponding walkthrough screens. The user's peripheral vision registers "I'm still in the Lever Loop section" without reading anything.

### 6. Interaction Polish

- **Keyboard navigation.** Left/right arrows advance/retreat. Number keys 1-9 jump to stages. Escape returns to overview.
- **URL sync.** Each stage should update the URL hash (`/how-it-works#judges`) so the user can share a link to a specific stage.
- **Reduced motion.** Wrap all animations in a `prefers-reduced-motion` check. In reduced-motion mode, transitions are instant fades instead of slides.
- **Auto-advance hint.** On the overview screen, after 3 seconds of inactivity, subtly pulse the "Next" button to hint that this is a guided experience.

## Implementation Approach

1. **Start by creating the stepper shell** — the layout with the progress rail, content area, and navigation. Get stage-to-stage transitions working with placeholder content. This is the structural backbone.

2. **Migrate existing section content into individual stage components**, one at a time. Refactor each to fit the "visual first, text second" template. Don't build new visuals yet — just restructure what exists.

3. **Add the continuity threads** — persistent mini-map, color coding, consistent fictional example data.

4. **Polish the transitions** — add `layoutId` shared-element animations between connected stages, directional slide transitions, and the stage-specific entrance animations.

5. **Add keyboard navigation and URL sync** last — these are interaction layer concerns that don't affect the visual design.

## What to Delete

Remove any "scroll to explore" patterns. Remove any freestanding section headers that aren't part of the stepper flow. Remove any "Learn more" links that navigate away from the walkthrough. Everything should be self-contained within the guided experience. If there's supplementary detail that doesn't fit, put it in a collapsible accordion WITHIN the relevant stage screen — never on a separate page.
