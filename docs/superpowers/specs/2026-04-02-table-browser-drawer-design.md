# Table Browser Drawer — Design Spec

**Branch:** `improve-create-agent-sz`
**Parent specs:** `2026-04-01-improve-create-agent-design.md`, `2026-04-02-smart-discovery-design.md`

## Problem

The agent recommends tables conversationally via `search_tables`, but users also need a way to independently browse and select tables alongside the agent conversation. Currently there's no structured UI for exploring the data catalog — users are entirely dependent on the agent's recommendations or must type catalog/schema/table paths manually.

## Solution

A slide-out drawer panel that opens between the left sidebar and the chat. Provides two ways to find tables: hierarchical catalog/schema/table browsing (lazy-loaded tree) and keyword search (calls `search_tables` backend). Always accessible via a "Browse Tables" button in the sidebar. Adding/removing tables auto-sends a message to the chat so the agent stays in sync.

---

## Drawer UI

### Trigger

A "Browse Tables" button in the left sidebar, always visible at every step. Shows selected table count as a badge: "Browse Tables (3)".

### Layout (top to bottom)

1. **Header** — "Table Browser" title + close (X) button
2. **Search bar** — Keyword input that calls the backend `search_tables` API on enter/submit. Results display below, grouped by catalog.schema, replacing the tree temporarily. Clear search to return to tree view.
3. **Catalog/Schema tree** — Lazy-loaded accordion:
   - On drawer open: load catalogs via `GET /api/create/discover/catalogs`
   - Click catalog → load schemas via `GET /api/create/discover/schemas?catalog=X`
   - Click schema → load tables via `GET /api/create/discover/tables?catalog=X&schema=Y`
   - Checkboxes at table level (checked = selected)
   - Loading spinner per node while fetching
4. **Selected tables** (pinned at bottom) — All currently selected tables as removable chips. Always visible so the user sees their running selection.

### Behavior

- Drawer slides out between sidebar and chat, compressing the chat area
- Stays open until user explicitly closes it (X button or clicking outside)
- Available at ALL steps — user can add tables even during inspection or plan review
- Selecting a table auto-sends: "I added `catalog.schema.table` to the selection"
- Deselecting a table auto-sends: "I removed `catalog.schema.table` from the selection"
- The agent sees these messages and can respond naturally (e.g., "Got it, I'll include that in the plan" or "I'll need to inspect that table first")

---

## Backend

### Existing endpoints (no changes needed)

- `GET /api/create/discover/catalogs` — lists catalogs
- `GET /api/create/discover/schemas?catalog=X` — lists schemas
- `GET /api/create/discover/tables?catalog=X&schema=Y` — lists tables

### New endpoint

```
GET /api/create/discover/search?keywords=bank,loan,customer&catalogs=prod,samples
```

Calls `search_tables()` from `uc_client.py`. Returns results grouped by catalog.schema with table names, comments, matching columns, and matched keywords.

**Query parameters:**
- `keywords` (required): comma-separated search terms
- `catalogs` (optional): comma-separated catalog names to scope search

**Response:** Same format as `search_tables()` return value.

---

## Frontend Components

### New: `frontend/src/components/TableBrowserDrawer.tsx`

```typescript
interface TableBrowserDrawerProps {
  open: boolean
  onClose: () => void
  selectedTables: string[]
  onAddTable: (fullName: string) => void
  onRemoveTable: (fullName: string) => void
}
```

Internal state:
- `catalogs: {name, comment}[]` — loaded on open
- `expandedCatalogs: Set<string>` — which catalogs are expanded
- `schemas: Record<string, {name, comment}[]>` — schemas per catalog (lazy)
- `expandedSchemas: Set<string>` — which schemas are expanded
- `tables: Record<string, {full_name, name, comment}[]>` — tables per schema (lazy)
- `searchQuery: string` — current search input
- `searchResults: SearchResult[] | null` — results from backend search (null = show tree)
- `loading: Record<string, boolean>` — loading state per tree node

### Modified: `frontend/src/components/CreateAgentChat.tsx`

- Add `drawerOpen: boolean` state
- Add "Browse Tables" button in left sidebar (always visible)
- Render `<TableBrowserDrawer>` conditionally
- Wire `onAddTable` → update `progress.tables` + call `sendMessage("I added \`{table}\` to the selection")`
- Wire `onRemoveTable` → update `progress.tables` + call `sendMessage("I removed \`{table}\` from the selection")`

### Modified: `frontend/src/lib/api.ts`

Add `searchTables(keywords: string[], catalogs?: string[])` function that calls the new search endpoint.

---

## Files Changed

| File | Change |
|------|--------|
| **New:** `frontend/src/components/TableBrowserDrawer.tsx` | Drawer component |
| `frontend/src/components/CreateAgentChat.tsx` | Drawer state, button, wiring |
| `frontend/src/lib/api.ts` | `searchTables()` API call |
| `backend/routers/create.py` | `GET /api/create/discover/search` endpoint |

**Not changed:** Backend agent, prompts, session state, tools. The drawer is a frontend feature that uses existing + one new REST endpoint.
