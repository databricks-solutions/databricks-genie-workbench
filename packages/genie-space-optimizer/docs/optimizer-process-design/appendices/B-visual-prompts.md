# Appendix B — Visual Prompts

This appendix contains reusable visual descriptions you can hand to a designer or LLM image tool. Every prompt is concrete about composition, color, labels, and hierarchy so the resulting visual matches the documentation set's voice.

> **Visual style baseline (apply to every prompt below)**
> Dark slate background (`#0f172a`–`#1e293b`). Lakehouse-grid texture, very subtle. Databricks red-orange (`#ff3621`) as the primary accent for *interventions* and *attention*. Cyan (`#06b6d4`) for *evidence* and *traces*. Green (`#10b981`) only for *accepted improvements*. Amber (`#f59e0b`) for *gates* and *caution*. Strong typography. No clipart. Sparse text inside visuals; large readable labels.

## VP-01 — Six-Task Pipeline DAG

> "Create a dark Databricks-themed wide horizontal pipeline diagram. Six rounded cards across, connected by glowing red-orange arrows. The cards are labeled, in order: **1. Preflight — Setup the experiment**, **2. Baseline — Score before changes**, **3. Enrichment — Add safe context**, **4. Lever Loop — Run experiments**, **5. Finalize — Prove it generalizes**, **6. Deploy — Promote the proven config**. Each card has a small monochrome icon (flask, gauge, library, recursive arrow, trophy, rocket). Below each card, a one-line plain-English output (e.g., `space_config.json`, `baseline scoreboard`, `enriched config`, `candidate config`, `champion model`, `target Genie Space`). Slate background with a subtle lakehouse grid. Use cyan to highlight artifact handoffs. Title at top: 'The Genie Space Optimizer Pipeline.'"

## VP-02 — What-Is vs What-Could-Be Split

> "Create a high-contrast split-screen slide. Left panel labeled 'What is — Tune by intuition' on a dim, slightly muted slate background; show scattered sticky notes, question marks, and a small 'looks better?' bubble. Right panel labeled 'What could be — Optimize by evidence' on a clean dark slate; show a vertical stack of evidence cards: `baseline`, `RCA`, `patch`, `re-evaluate`, `accept ✓`, with a green check beside the last card. The two panels are separated by a bright red-orange vertical divider. Use Databricks red-orange for the contrast accent and cyan for evidence cards."

## VP-03 — Scientific Flywheel

> "Create a circular flywheel diagram with five evenly-spaced nodes around the rim, in clockwise order: **Measure**, **Diagnose**, **Intervene**, **Prove**, **Learn**. The center reads 'Evidence, not intuition.' The connecting arc is a glowing cyan dashed circle. Each node has a small monochrome icon (gauge, microscope, wrench, check-circle, repeat) and a one-line subtitle. The diagram should look like a precision lab instrument dial, not a generic cycle chart. Slate background. Red-orange accent only on the active node."

## VP-04 — RCA Process Spine (11 stages)

> "Create a vertical 'process spine' diagram with 11 numbered nodes connected by a glowing cyan vertical line. The 11 nodes are: **1 Evaluation State**, **2 RCA Evidence**, **3 Cluster Formation**, **4 Action Group Selection**, **5 Proposal Generation**, **6 Safety Gates**, **7 Applied Patches**, **8 Post-Patch Evaluation**, **9 Acceptance / Rollback**, **10 Learning / Next Action**, **11 Contract Health**. Each node is a rounded card with the stage number on the left and the title on the right. Group nodes 1–3 with a faint label 'Diagnose' (cyan tint), 4–6 with 'Plan' (amber tint), 7 with 'Act' (red-orange tint), 8–9 with 'Judge' (green tint), 10–11 with 'Learn' (blue tint). Bottom of the spine has a small return arrow labeled 'next iteration.'"

## VP-05 — Patch Survival Funnel

> "Create a top-to-bottom funnel diagram. The wide top is labeled 'Many proposals' and shows ~10 small chip-shaped 'patch idea' tiles. Below, the funnel narrows through seven horizontal gate bands, each labeled with one gate: **Causal grounding**, **Blast radius**, **Patch cap**, **Teaching safety**, **Leakage firewall**, **Structural gates**, **Regression guardrail**. Each gate band has an amber border. Below the gates, a narrow neck labeled 'Surviving patch set' leads into 'Re-evaluation,' which forks into two outcomes: a green 'Accepted' stamp on the left, a red 'Rolled back' stamp on the right. Slate background. Use red-orange for the funnel walls."

## VP-06 — Six-Lever Hexagon

> "Create a hexagon diagram with six lever nodes evenly placed around a central circular card labeled 'Genie Space score.' The six lever nodes, in clockwise order from top: **L1 Tables & Columns**, **L2 Metric Views**, **L3 Table-Valued Functions**, **L4 Join Specifications**, **L5 Genie Space Instructions**, **L6 SQL Expressions**. Each node has a small monochrome icon (table, metric chart, function fx, join venn, instruction page, SQL braces) and a one-line lever description. Color-code the nodes by family: L1 + L2 in cyan (data understanding), L3 + L4 in purple (question routing), L5 + L6 in green (answering). The center card uses red-orange to indicate it is the target of every lever. Slate background, subtle radial gradient toward center."

## VP-07 — Train vs Held-Out Wall

> "Create a horizontal diagram showing a single benchmark dataset splitting into two streams. On the left, label 'Versioned benchmark' as a stacked stack-of-papers icon in cyan. The benchmark splits into two arrows: the upper arrow goes to 'train split — used during lever loop' (cyan), the lower arrow goes to 'held_out split — not seen until finalize' (amber). Between the two streams, draw a tall red-orange vertical wall labeled 'leakage firewall.' Slate background. Use sparse, large labels."

## VP-08 — MLflow Evidence Room

> "Create an investigation-board layout with a dark slate background and faint grid texture. Pin nine connected cards on the board with thin cyan threads between them. The cards: **Experiment**, **Dataset (UC)**, **mlflow.genai.evaluate**, **CODE judges**, **LLM judges**, **Trace + feedback**, **LoggedModel snapshot**, **Phase H bundle**, **operator_transcript.md**. Each card has a tiny icon and a one-line plain-English description. The center of the board has a single label: 'Every score, trace, judge decision, and snapshot is inspectable.' Use cyan for thread connections and red-orange for the center label."

## VP-09 — Deploy Bridge

> "Create a wide horizontal bridge diagram. On the left side, a stylized 'Source workspace' card containing a 'GSO run' icon and a 'Champion LoggedModel' icon. In the middle, a glowing red-orange arch labeled 'patch_space_config' acting as the bridge. On the right, a stylized 'Target workspace' card containing a 'Target Genie Space' icon. Above the bridge, a small amber gate icon labeled 'approval.' Below the bridge, a tiny note: 'Deploy the proven configuration, not the latest edit.' Slate background, lakehouse grid, large readable labels."

## VP-10 — Score Trajectory Sparkline

> "Create a sparkline chart showing optimization progress over six iterations. The X-axis is unlabeled iteration count (1 through 6). The Y-axis is unlabeled accuracy. Plot a stepped line that starts at a low baseline, dips slightly at iteration 2 (rejected), rises at iteration 3 (accepted), stays flat at iteration 4 (rejected), rises at iteration 5 (accepted), and ends at iteration 6 (accepted). At each accepted iteration, place a small green check stamp. At each rejected iteration, place a small red X. Above the chart, a horizontal dashed cyan line labeled 'gain floor.' Below the chart, the label 'Only gains above the floor count.' Slate background; use cyan for the sparkline."

## VP-11 — Action Group One-At-A-Time

> "Create a stylized lab bench scene. On the left, a tray of many small chip-shaped 'failed question' tiles. In the middle, a single isolated cylinder labeled 'one action group' under a magnifying-lens cone of light. On the right, a small cluster of three patch-tiles emerging from the cylinder, labeled 'patches scoped to this action group.' Above the scene, a banner: 'One controlled experiment at a time.' Slate background; red-orange accent only on the magnifying lens beam."

## VP-12 — Acceptance / Rollback Decision

> "Create a clean decision diagram. Top: 'Post-patch scoreboard' card. Below it, a diamond decision shape with the question 'Post-arbiter accuracy ≥ baseline + min_gain_pp?'. Two branches: the left branch labeled 'yes' goes to a green-bordered card 'Accept + LoggedModel snapshot.' The right branch labeled 'no' goes to a red-bordered card 'Rollback via apply_patch_set inverse.' Below both, a single converging arrow into 'Carry baseline forward.' Slate background; use green and red sparingly, only on the outcome cards."

## VP-13 — Repeatability Variance Strip

> "Create a horizontal three-strip visualization. Strip 1 labeled 'Pass 1' shows a row of dots representing per-question scores; mostly green with a few amber. Strip 2 labeled 'Pass 2' shows the same row pattern but with one or two dots flipped between green and amber. Strip 3 labeled 'Pass 3' shows again similar with minor flips. To the right, a small summary card: 'Mean: 86%, Variance: 1.2pp.' Above all strips, the title 'Repeatability proves stability, not just height.' Slate background, sparse labels, cyan for evidence."

## VP-14 — Five-Word Story Banner

> "Create a clean horizontal banner with five large words evenly spaced: **Measure**, **Diagnose**, **Intervene**, **Prove**, **Learn**. Each word in a slightly different accent: Measure in cyan, Diagnose in amber, Intervene in red-orange, Prove in green, Learn in blue. Below the banner, a single small subtitle: 'The optimizer's five-word story.' Slate background, no other decoration."

## VP-15 — Hero / Promise (cinematic)

> "Create a full-bleed dark hero image. Headline on the left: 'Genie Space Optimizer: A Scientific Loop For Better Answers.' Subhead below: 'Measure. Diagnose. Intervene. Prove. Learn.' On the right two-thirds, a stylized SVG instrument dial with a cyan needle resting on a green 'accepted' arc. Around the dial rim, four pill labels: **Measured**, **Auditable**, **Repeatable**, **Promotable**. Slate background with subtle lakehouse grid texture. Use red-orange only for the instrument's center bezel."

## How To Use These Prompts

- Each prompt can be pasted directly into an image-generation tool (Midjourney/DALL·E/Sora) or handed to a designer.
- Pair the prompt with the corresponding section of [08 — Slide Outline](../08-slide-outline.md) to get the slide title, subtitle, and on-slide copy.
- Re-use the visual style baseline at the top of this appendix for any custom visual to keep the deck cohesive.
- For markdown-renderable previews, see the embedded Mermaid blocks in the numbered docs (these prompts intentionally generate higher-fidelity art).
