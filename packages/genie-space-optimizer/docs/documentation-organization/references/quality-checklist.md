# Documentation Quality Checklist

Comprehensive quality checklist merging organizational enforcement (location, naming, structure) with content quality (depth, usability, maintenance). Run this checklist before finalizing any documentation deliverable.

---

## 1. Organization & Location (Run BEFORE Creating Files)

- [ ] File is NOT in project root (unless README.md, QUICKSTART.md, or CHANGELOG.md)
- [ ] File is placed in correct `docs/[category]/` subdirectory
- [ ] Filename uses `kebab-case.md` format
- [ ] Historical/dated files use `YYYY-MM-DD-description.md` format
- [ ] Root directory has 3 or fewer `.md` files (excluding LICENSE)
- [ ] Misplaced files identified and moved or flagged for cleanup
- [ ] README.md links updated if creating major new documentation

## 2. Naming Conventions (Enforce Always)

- [ ] No `PascalCase.md` filenames
- [ ] No `ALL_CAPS.md` filenames (exception: README.md, QUICKSTART.md, CHANGELOG.md)
- [ ] No `snake_case.md` filenames
- [ ] Framework documentation uses `NN-descriptive-name.md` numbering (e.g., `03-feature-engineering.md`)
- [ ] Appendices use `X-descriptive-name.md` format (e.g., `A-code-examples.md`)

## 3. Structure & Navigation

- [ ] Index page (`00-index.md`) with complete document navigation
- [ ] Logical document ordering (introduction → architecture → components → implementation → operations)
- [ ] Appendices for reference material (code examples, troubleshooting, external references)
- [ ] Consistent header levels across all documents in the set
- [ ] Cross-document links use relative paths
- [ ] Each document set lives under `docs/{framework-name}-design/`

## 4. Content Quality

- [ ] Each document has a clear purpose statement in its first section
- [ ] Architecture diagrams present (Mermaid preferred, ASCII alternative acceptable)
- [ ] Code examples are complete and production-ready (not pseudocode stubs)
- [ ] Tables used for structured information (inventories, matrices, configurations)
- [ ] Validation checklists included where applicable
- [ ] Error-solution matrices for troubleshooting content
- [ ] Best practices include both Do's and Don'ts with rationale

## 5. Usability

- [ ] Quick Start section in index document (3-4 numbered steps)
- [ ] Time estimates for implementation phases
- [ ] Links between related documents (forward and backward references)
- [ ] Search-friendly headings (descriptive, not generic like "Overview" repeated everywhere)
- [ ] Prerequisites clearly listed with status indicators (Required/Optional)
- [ ] Expected results stated after each implementation step

## 6. Maintenance & Longevity

- [ ] Version or date information included (in frontmatter or header)
- [ ] References to official external documentation (URLs, not embedded content that goes stale)
- [ ] Contact information or escalation path for questions
- [ ] Update procedure documented (who updates, when, how)
- [ ] No hardcoded values that change per environment (use `{placeholder}` or parameterization)

## 7. Special Cases

### Historical Records
- [ ] Historical records preserved in `docs/deployment/deployment-history/` or `docs/troubleshooting/`
- [ ] Never delete historical records — they serve as audit trail
- [ ] Date-stamped with `YYYY-MM-DD` prefix

### Temporary Documentation
- [ ] Temporary notes flagged with deletion reminder
- [ ] Prefer issue tracker over temporary `.md` files
- [ ] If temporary file needed: `docs/development/wip-notes.md` with expiry note

### Consolidation
- [ ] No duplicate content across documents (brief summary + link to full doc)
- [ ] README.md has only brief summaries with links to `docs/` for details
- [ ] QUICKSTART.md has only commands, links to guides for explanations

---

## Quick Reference: Category-to-Directory Mapping

| Content Category | Directory | Examples |
|-----------------|-----------|----------|
| Deployment guides/checklists | `docs/deployment/` | `deployment-guide.md`, `pre-deployment-checklist.md` |
| Deployment history | `docs/deployment/deployment-history/` | `2025-01-15-bronze-deployment.md` |
| Issues/troubleshooting | `docs/troubleshooting/` | `issue-2025-01-15-parameter-fix.md`, `common-issues.md` |
| Architecture/design | `docs/architecture/` | `architecture-overview.md` |
| Operations/runbooks | `docs/operations/` | `monitoring.md`, `runbooks/` |
| Development/roadmap | `docs/development/` | `roadmap.md`, `setup.md` |
| Reference/config | `docs/reference/` | `configuration.md`, `glossary.md` |
| Framework documentation | `docs/{framework-name}-design/` | `00-index.md`, `01-introduction.md`, ... |
