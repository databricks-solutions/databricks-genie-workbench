# AGENTS.md — genie-space-optimizer

The `genie-space-optimizer` (GSO) package is the Auto-Optimize engine for Genie Workbench.
It is a self-contained FastAPI + React app that runs as both a Databricks App and a
Databricks Job (the benchmark-driven optimization pipeline).

For top-level project context see the root `AGENTS.md` and `README.md`.

## Build Commands

### Python

```bash
uv sync --frozen                  # Install deps from uv.lock (strict)
uv build                          # Build the distributable wheel
uv run pytest                     # Run tests
```

### Frontend (uses Bun, not npm)

```bash
bun install --frozen-lockfile     # Install from bun.lock (strict)
bun run build                     # Production build
```

## Package Layout

```
src/genie_space_optimizer/
  backend/              # FastAPI app for the GSO service
  optimization/         # Benchmark-driven optimization pipeline (6 stages)
  jobs/                 # Databricks Job notebooks/tasks
  ui/                   # React frontend (Vite + Bun)
  genie_optimizer_skills/ # Excluded from type-checking (see pyproject.toml)
```

## Key Differences from Root Package

- Uses **Bun** (not npm) for the frontend
- Python deps resolved from `https://pypi-proxy.dev.databricks.com/simple/` (internal
  Databricks PyPI proxy), not raw PyPI — this reduces (but does not eliminate) supply
  chain risk
- Dynamic versioning via `uv-dynamic-versioning` (reads from git tags)
- The `_metadata.py` file is generated at build time and gitignored — it is synced
  explicitly by `deploy.sh` because Databricks sync skips gitignored files

## Dependency Security Policy

All dependencies are pinned to exact versions. Lock files must be committed.

**To update a Python dependency:**

```bash
uv lock --upgrade-package <package-name>
git add uv.lock
```

**To update a Bun dependency:**

```bash
bun update <package>@<version>
# update package.json to exact version (no ^)
git add package.json bun.lock
```

## Testing

```bash
uv run pytest           # Unit tests (pytest, configured in pyproject.toml)
```
