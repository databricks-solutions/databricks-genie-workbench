---
name: documentation-organization
description: >
  Enforces documentation organization standards and provides comprehensive framework documentation
  authoring for Databricks projects. Operates in two modes: (1) Organizational Enforcement — validates
  file placement (root directory rules, docs/ hierarchy), naming conventions (kebab-case, date-prefixed),
  and proactively suggests cleanup for misorganized files. (2) Framework Documentation — orchestrates
  creation of complete technical documentation sets using fill-in-the-blank templates (index, introduction,
  architecture, component deep dives, implementation guides, operations guides, troubleshooting guides)
  with requirements gathering, quality checklists, and worked examples. Use when creating any .md file,
  organizing existing documentation, maintaining project structure, or creating comprehensive framework
  documentation sets.
metadata:
  author: prashanth subrahmanyam
  version: "2.0.0"
  domain: admin
  role: utility
  standalone: true
  triggers:
    - "document"
    - "checklist"
    - "summary"
    - "guide"
    - "issue"
    - "steps"
    - "deployment"
    - "troubleshooting"
    - "framework documentation"
    - "documentation set"
    - "document this system"
    - "create docs"
    - "architecture docs"
    - "operations guide"
  last_verified: "2026-02-07"
  volatility: low
  upstream_sources: []  # Internal convention
---

# Documentation Organization & Framework Authoring

This skill serves two complementary purposes:

1. **Organizational Enforcement** — Ensures every documentation file lands in the right place with the right name, every time.
2. **Framework Documentation Authoring** — Orchestrates creation of complete, professional documentation sets for technical frameworks and systems using structured templates.

---

## Decision Tree: Which Mode Do I Need?

| User Intent | Mode | What Happens |
|------------|------|-------------|
| "Create a deployment checklist" | **Organizational** | Route to `docs/deployment/deployment-checklist.md`, suggest cleanup |
| "Document this issue" | **Organizational** | Route to `docs/troubleshooting/issue-YYYY-MM-DD-description.md` |
| "Write next steps" | **Organizational** | Route to `docs/development/roadmap.md` or suggest issue tracker |
| "Document the ML framework" | **Framework** | Gather requirements, generate full doc set from templates |
| "Create architecture docs for the alerting system" | **Framework** | Generate `docs/alerting-framework-design/` with all templates |
| "Help me create comprehensive docs for this project" | **Framework** | Run full requirements + template workflow |
| File is `.md` and being created | **Organizational** | Auto-check location before creation |
| Root has >3 `.md` files | **Organizational** | Proactively suggest cleanup |

---

## Mode 1: Organizational Enforcement

### Auto-Trigger Conditions

I will **automatically** enforce organization when:
- Creating any `.md` file (check location first)
- User mentions: "document", "checklist", "summary", "guide", "issue", "steps"
- Root directory has >3 `.md` files (excluding README/QUICKSTART/CHANGELOG)
- See files matching patterns: `*DEPLOYMENT*`, `*CHECKLIST*`, `ISSUE*`, `*STEPS*`, `*SUMMARY*`

### Root Directory Rules (Enforce Always)

#### ALLOWED in Root (Only These)

```
README.md              # Project hub with links
QUICKSTART.md          # Commands-only quick start  
CHANGELOG.md           # Version history (optional)
LICENSE                # License file (optional)
```

#### NEVER Allowed in Root

```
*DEPLOYMENT*.md        → docs/deployment/deployment-history/YYYY-MM-DD-name.md
*CHECKLIST*.md         → docs/deployment/checklist-name.md
ISSUE*.md              → docs/troubleshooting/issue-YYYY-MM-DD-description.md
*STEPS*.md             → docs/development/roadmap.md (or delete if temporary)
*SUMMARY*.md           → docs/reference/ or merge into README.md
*GUIDE*.md             → docs/deployment/ or docs/operations/
.hidden-*.md           → docs/[appropriate-category]/
```

### Standard Project Structure (Always Use)

```
project_root/
├── README.md                          
├── QUICKSTART.md                      
├── docs/
│   ├── deployment/
│   │   ├── deployment-guide.md
│   │   ├── pre-deployment-checklist.md
│   │   ├── deployment-checklist.md
│   │   └── deployment-history/
│   │       └── YYYY-MM-DD-description.md
│   ├── troubleshooting/
│   │   ├── common-issues.md
│   │   └── issue-YYYY-MM-DD-description.md
│   ├── architecture/
│   │   └── architecture-overview.md
│   ├── operations/
│   │   ├── monitoring.md
│   │   └── runbooks/
│   ├── development/
│   │   ├── roadmap.md
│   │   └── setup.md
│   ├── reference/
│   │   ├── configuration.md
│   │   └── glossary.md
│   └── {framework-name}-design/        # Framework documentation sets
│       ├── 00-index.md
│       ├── 01-introduction.md
│       └── ...
└── context/
    └── prompts/
```

### Naming Rules (Always Enforce)

| Format | Use For | Example |
|--------|---------|---------|
| `kebab-case.md` | All docs | `deployment-guide.md` |
| `YYYY-MM-DD-description.md` | Historical/dated | `2025-01-15-bronze-deployment.md` |
| `NN-descriptive-name.md` | Framework docs (numbered) | `03-feature-engineering.md` |
| NEVER `PascalCase.md` | -- | `DeploymentGuide.md` |
| NEVER `ALL_CAPS.md` | -- | `DEPLOYMENT_GUIDE.md` |
| NEVER `snake_case.md` | -- | `deployment_guide.md` |

### Automatic Response Pattern

**Before creating any `.md` file, I will always check:**

```python
# Location routing logic (I run this mentally)
if filename in ["README.md", "QUICKSTART.md", "CHANGELOG.md"]:
    location = "root"  # OK
elif "deployment" in content or "checklist" in filename:
    location = "docs/deployment/"
elif "issue" in filename or "troubleshoot" in content:
    location = "docs/troubleshooting/"
elif "architecture" in content or "design" in content:
    location = "docs/architecture/"
elif user_intent == "framework documentation":
    location = "docs/{framework-name}-design/"  # → Switch to Mode 2
elif user_intent == "temporary notes":
    suggest_alternative = "Use issue tracker or delete after completion"
```

**My response format:**

```
I'll create that documentation in the correct location:
  docs/[category]/[kebab-case-name].md

I also notice these files should be organized:
  DEPLOYMENT_COMPLETE.md → docs/deployment/deployment-history/2025-01-15-summary.md
  .deployment-checklist.md → docs/deployment/deployment-checklist.md
  ISSUE_RESOLUTION.md → docs/troubleshooting/issue-2025-01-15-parameter-fix.md

Would you like me to reorganize these?
```

### Proactive Cleanup Suggestions

#### Trigger: User Creates Documentation

| User Says | I Suggest |
|-----------|-----------|
| "Create a deployment summary" | `docs/deployment/deployment-history/YYYY-MM-DD-summary.md` |
| "Document this issue" | `docs/troubleshooting/issue-YYYY-MM-DD-brief-description.md` |
| "Make a checklist" | `docs/deployment/[type]-checklist.md` |
| "Write next steps" | `docs/development/roadmap.md` or "Use issue tracker instead?" |

#### Trigger: Multiple Files in Root

When I see >3 `.md` files in root:

```
Root directory has [N] documentation files. I recommend organizing them:

Current structure:
  7 files in root

Suggested structure:
  3 files in root (README, QUICKSTART, CHANGELOG)
  4 files in docs/deployment/
  
Would you like me to create a cleanup plan?
```

See `scripts/organize_docs.sh` for the automated cleanup script.

### Special Cases

#### Temporary Files
If user wants "quick notes" or "temporary file":
- Suggest: Use your IDE scratch file or issue tracker
- Alternative: `docs/development/wip-notes.md` (but add reminder to delete)

#### Historical Records
Always preserve, never delete:
- `docs/deployment/deployment-history/YYYY-MM-DD-description.md`
- `docs/troubleshooting/issue-YYYY-MM-DD-description.md`

#### Consolidation Opportunity
If I see duplicate content:
```
I notice:
- README.md has deployment info (100 lines)
- docs/deployment/deployment-guide.md has same content
- QUICKSTART.md duplicates some steps

Suggest:
- Keep brief summary in README with link
- Full guide in docs/deployment/deployment-guide.md
- QUICKSTART only has commands, links to guide for details
```

---

## Mode 2: Framework Documentation Authoring

Use this mode when creating comprehensive documentation for a technical framework, system, or multi-component project. This is an orchestrated workflow that produces a complete, professional documentation set.

### When to Activate Framework Mode

- User asks to "document the [framework/system]"
- User asks for "architecture docs", "comprehensive docs", or "documentation set"
- Project has 5+ components that need structured documentation
- User is creating documentation for training or onboarding purposes

### Step 1: Requirements Gathering (5 min)

**MANDATORY: Before generating any templates, gather requirements from the user.**

Present the requirements table (see `references/framework-examples.md` for the full template):

| Field | Your Input |
|-------|------------|
| **Framework/System Name** | _________________ |
| **Primary Audience** | _________________ |
| **Secondary Audience** | _________________ |
| **Documentation Purpose** | [ ] Project Documentation  [ ] Training Material  [ ] Both |
| **Technology Stack** | _________________ |
| **Number of Components** | _________________ |

Then determine documentation depth:

| Level | Description | Needed? |
|-------|-------------|---------|
| **Executive Summary** | 1-page overview for leadership | [ ] |
| **Architecture Guide** | System design, data flows, component interactions | [ ] |
| **Implementation Guide** | Step-by-step build instructions | [ ] |
| **Operations Guide** | Deployment, monitoring, maintenance | [ ] |
| **Reference Manual** | API docs, configurations, schemas | [ ] |
| **Troubleshooting Guide** | Common errors and solutions | [ ] |
| **Best Practices** | Patterns and anti-patterns | [ ] |

### Step 2: Generate File Structure (5 min)

Based on requirements, create the numbered file structure under `docs/{framework-name}-design/`:

```
docs/{framework-name}-design/
├── 00-index.md                    # Always included
├── 01-introduction.md             # Always included
├── 02-architecture-overview.md    # If Architecture Guide selected
├── 03-{component-type-1}.md       # Per major component
├── 04-{component-type-2}.md       # Per major component
├── ...
├── {n}-implementation-guide.md    # If Implementation Guide selected
├── {n+1}-operations-guide.md      # If Operations Guide selected
└── appendices/
    ├── A-code-examples.md         # If Reference Manual selected
    ├── B-troubleshooting.md       # If Troubleshooting Guide selected
    └── C-references.md            # Always included
```

### Step 3: Fill Templates (Varies — 30 min to 4 hours)

**Load `references/document-templates.md` for complete fill-in-the-blank templates.**

Templates available (load only the ones needed):
1. **00-index.md** — Document index, architecture summary, quick start, key statistics
2. **01-introduction.md** — Purpose, scope, prerequisites, timeline, success criteria
3. **02-architecture-overview.md** — Mermaid/ASCII diagrams, data flows, component inventory, design principles
4. **Component Deep Dive** — Key concepts, configuration, code patterns, best practices, common issues
5. **Implementation Guide** — Phased steps with validation, deployment commands, rollback procedures
6. **Operations Guide** — Health checks, alerting, escalation matrix, incident response, backup/recovery
7. **Troubleshooting Guide** — Error-solution matrix, diagnostic procedures, FAQ

### Step 4: Quality Validation

**Load `references/quality-checklist.md` for the comprehensive quality checklist** covering:
1. Organization & Location (7 checks)
2. Naming Conventions (5 checks)
3. Structure & Navigation (6 checks)
4. Content Quality (7 checks)
5. Usability (6 checks)
6. Maintenance & Longevity (5 checks)
7. Special Cases (7 checks)

### Worked Examples

**Load `references/framework-examples.md` for complete worked examples** including:
- ML Framework (25 models, 5 domains → 17 docs + 3 appendices)
- Alerting Framework (56 alerts, 5 domains → 10 docs + 3 appendices)
- Semantic Layer (focused scope → 7 docs + 3 appendices)
- Scaling guidelines (small/medium/large projects)

---

## Enforcement Checklist (I Run Mentally Before Every Response)

Before responding to any documentation request:

- [ ] **Location**: Is this README/QUICKSTART/CHANGELOG? (only valid root files)
- [ ] **Routing**: Propose correct `docs/[category]/` location
- [ ] **Naming**: Use `kebab-case`, add date if historical, add number if framework doc
- [ ] **Root audit**: Count root `.md` files — suggest cleanup if >3
- [ ] **Misplaced files**: Offer to move existing misplaced files
- [ ] **README links**: Update README.md links if creating major new documentation
- [ ] **Mode detection**: Is this organizational (quick routing) or framework (full template workflow)?

---

## Examples

### Example 1: Organizational Mode (Quick Routing)

```
User: "Create a deployment checklist"

My response:
I'll create: docs/deployment/deployment-checklist.md

I also see these in root that should be moved:
- DEPLOYMENT_COMPLETE.md → docs/deployment/deployment-history/2025-01-15-summary.md
- PRE_DEPLOYMENT_CHECKLIST.md → docs/deployment/pre-deployment-checklist.md

Want me to reorganize?
```

### Example 2: Organizational Mode (Bad — I Never Do This)

```
User: "Create a deployment checklist"

My response:
I'll create: DEPLOYMENT_CHECKLIST.md   [WRONG — wrong location, wrong naming]
[Creates file without checking structure]  [WRONG — no proactive cleanup]
```

### Example 3: Framework Mode (Full Workflow)

```
User: "Document the ML framework"

My response:
I'll create comprehensive framework documentation. First, let me gather requirements:

1. Framework Name: ML Framework
2. Primary Audience: ML Engineers
3. Components: 25 models across 5 domains
4. Depth needed: Architecture + Implementation + Operations + Troubleshooting

Generating docs/ml-framework-design/ with:
  00-index.md, 01-introduction.md, 02-architecture-overview.md,
  03-feature-engineering.md, ... 17-operations-guide.md
  + appendices/A-code-examples.md, B-troubleshooting.md, C-references.md

[Loads references/document-templates.md for fill-in templates]
[Generates each document from templates]
[Runs references/quality-checklist.md for validation]
```

---

## Integration with Project Structure

### Clear Separation of Concerns

| Location | Purpose | Maintained By |
|----------|---------|---------------|
| `data_product_accelerator/skills/` | Agent skills (AI behavioral patterns) | This skill system |
| `context/prompts/` | Domain context for AI | Project team |
| `docs/` | Human-readable documentation | This skill (organizational + framework) |

### How This Skill Relates to Others

| Skill | Relationship |
|-------|-------------|
| `common/databricks-expert-agent` | Extract-don't-generate principle applies to doc content too |
| `admin/self-improvement` | Documents learnings and patterns — this skill enforces WHERE |
| `planning/project-plan-methodology` | Plans produce docs — this skill enforces structure |
| All domain skills | Technical content goes in `docs/` using this skill's structure |

---

## Summary: What I Will Always Do

1. Check location before creating any `.md` file
2. Suggest correct `docs/[category]/` location
3. Use `kebab-case.md` naming (numbered for framework docs)
4. Add dates for historical docs (`YYYY-MM-DD-`)
5. Proactively suggest cleanup when root has >3 files
6. Offer cleanup script when I see misorganized files
7. Never create status/summary/checklist files in root
8. Always preserve historical records in `deployment-history/`
9. Detect framework documentation requests and switch to full template workflow
10. Gather requirements before generating framework documentation
11. Use standardized templates from `references/document-templates.md`
12. Validate all documentation against `references/quality-checklist.md`

---

## Reference Files

- **[Document Templates](references/document-templates.md)** — All 7 fill-in-the-blank templates (index, introduction, architecture, component deep dive, implementation guide, operations guide, troubleshooting guide)
- **[Quality Checklist](references/quality-checklist.md)** — Comprehensive merged checklist (43 items across 7 categories covering organization, naming, structure, content, usability, maintenance, and special cases)
- **[Framework Examples](references/framework-examples.md)** — Requirements gathering template, depth selector, 3 worked examples (ML Framework, Alerting Framework, Semantic Layer), and scaling guidelines
- **[Cleanup Script](scripts/organize_docs.sh)** — Automated script to reorganize misplaced documentation files

## External References

- [Diataxis Documentation Framework](https://diataxis.fr/) — Tutorials, How-to Guides, Reference, Explanation
- [Good Docs Project](https://thegooddocsproject.dev/) — Templates and best practices for documentation
- [Databricks Documentation Style Guide](https://docs.databricks.com/) — Databricks-specific conventions
