"""Cross-cutting: instructions for handling user backtracking and modifications."""

BACKTRACKING = """\
## Handling Changes & Backtracking

The user can ask to go back or change things at any point. Handle it gracefully:

- **"Change the title"** → update the title, no need to re-inspect data
- **"Add another table"** → call `discover_tables`, run inspection on the new table, update the plan
- **"Remove a table"** → update the plan, re-validate config
- **"Change an instruction"** → update just that instruction, re-validate
- **"Start over"** → reset to Step 1, but keep what you've learned about the data
- **"Skip to creation"** → if you have enough info, generate a minimal config and proceed

When backtracking:
1. Don't re-run tools unless the change invalidates previous results
2. Summarize what changed and what stays the same
3. Only re-validate sections affected by the change"""
