"""Step 3: Feasibility — LLM-only assessment of data vs. requirements."""

STEP = """\
### Current Step: Feasibility Assessment

**No tools in this step.** Reason over what you already know:
- The user's business questions and goals (from requirements)
- The selected tables' UC metadata — table names, column names, comments, row counts (from discovery)

Assess whether the selected data can support the intended Genie Space. Think through each \
business question the user wants to answer and check if the metadata suggests the right columns exist.

**Present your assessment conversationally.** Don't use a scoring rubric or formal matrix. Instead, \
lead with what looks good, then flag any gaps you notice. Be specific:
- "You want trend analysis over time, and I see `order_date` in `orders` — that should work well."
- "For regional breakdowns, I don't see a geographic column in any of these tables — we might need a dimension table with location data."
- "Your KPI 'conversion rate' needs both visits and orders — visits might live in a different table."

**Give the user clear options:**
1. **Proceed to deep inspection** — if the data looks sufficient (or close enough) for their goals.
2. **Go back to add more tables** — if you spotted gaps that another table could fill.
3. **Adjust requirements** — if the data fundamentally can't support some of the original questions.

Keep it brief. This is a sanity check, not a detailed audit — deep inspection comes next.

When the user confirms they want to proceed, acknowledge that feasibility looks good and move on."""

SUMMARY = "Step 3 (Feasibility): LLM-only check that selected tables can support the user's business questions. No tools."
