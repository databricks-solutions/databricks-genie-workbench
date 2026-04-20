# Contributing to Genie Workbench

Genie Workbench is maintained by Databricks Field Engineering. The repository is public so anyone can read, fork, and learn from it, but **pull requests are currently accepted only from Databricks employees**. If you're a Databricks FE and want to pitch in, we'd love the help — start here.

We plan work on the [Genie Workbench Roadmap](https://github.com/orgs/databricks-solutions/projects/10/views/3). Contributions flow from there: **pick an issue, claim it, ship it.**

## Picking Up an Issue

1. Browse the [Roadmap board](https://github.com/orgs/databricks-solutions/projects/10/views/3).
2. Pick an open issue that isn't already assigned.
3. **Assign yourself** via the Assignees sidebar. This signals you're working on it so no one duplicates effort.
4. If you want to sanity-check scope or approach first, comment on the issue or drop a note in **#genie-workbench** on Slack.

If you start on something and realize you can't finish, unassign yourself so someone else can pick it up.

## Branches and PRs

- **Branch name:** `feature/<issue-#>-short-desc` — e.g., `feature/141-contributing-rewrite`.
- **PR title:** one descriptive sentence — no required prefix. e.g., `Rewrite CONTRIBUTING.md for board-first flow`.
- **PR description:** link the issue (`Closes #141`), summarize the change, and note what you tested. There is no local dev server — test against a deployed workspace via `./scripts/deploy.sh`.
- **Target branch:** `main`.

## Suggesting Something New

Not on the board yet? Two options:

- **Small ideas or questions:** drop them in **#genie-workbench** on Slack.
- **Concrete feature requests:** open a [feature request](../../issues/new?template=feature_request.md). Core contributors triage these ad hoc. Outcomes:
  - **Added to the Roadmap** — we plan to work on it; the issue joins the board.
  - **Accepted, unscheduled** — reasonable ask, no plan yet. PRs welcome; otherwise we'll get to it when there's bandwidth.
  - **Declined** — closed with a comment explaining why.

## Reporting a Bug

Use [GitHub Issues](../../issues/new?template=bug_report.md) to report bugs.

> **Do not include sensitive information in issues.** This is a public repository. Never include customer names, workspace URLs, access tokens, or any customer-identifiable data.

If your bug involves a customer environment or contains details that can't be shared publicly, report it in **#genie-workbench** on Slack instead.

Please include:
- A clear description of the problem
- Steps to reproduce
- Expected vs. actual behavior
- Browser and workspace region (not the URL)

## Development Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/databricks-solutions/databricks-genie-workbench.git
   cd databricks-genie-workbench
   ```

2. Install dependencies:
   ```bash
   uv sync --frozen
   cd frontend && npm ci
   ```

3. First-time setup (creates `.env.deploy` with your workspace config):
   ```bash
   ./scripts/install.sh
   ```

See the [README](README.md) for full architecture and environment variable reference.

## Pull Request Process

1. Create a `feature/<issue-#>-short-desc` branch from `main`.
2. Make changes with clear, descriptive commits.
3. Run the test suite:
   ```bash
   ./scripts/test.sh
   ```
4. Deploy and test against a real Databricks workspace:
   ```bash
   ./scripts/deploy.sh
   ```
5. Open a PR linking the issue you claimed, describing the change, and noting what you tested.
6. Address review feedback.

## Community

Join **#genie-workbench** on Slack for questions, discussion, sensitive bug reports, and general feedback.

## Security

To report a security vulnerability, see [SECURITY.md](SECURITY.md).

## License

By submitting a contribution, you agree that your contributions will be licensed under the same terms as the project. See [LICENSE.md](LICENSE.md).
