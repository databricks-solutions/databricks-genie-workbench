# Contributing to Genie Workbench

This repository is maintained by Databricks and intended for contributions from Databricks Field Engineers. While the repository is public and meant to help anyone working with Genie Spaces, external contributions are not currently accepted. Feel free to open an issue with bug reports or feature suggestions.

## Reporting a Bug

Use [GitHub Issues](../../issues/new?template=bug_report.md) to report bugs.

> **Do not include sensitive information in issues.** This is a public repository. Never include customer names, workspace URLs, access tokens, or any customer-identifiable data in bug reports.

If your bug involves a customer environment or contains details that cannot be shared publicly, report it in **#genie-workbench** on Slack instead.

When filing a bug report, please include:
- A clear description of the problem
- Steps to reproduce the issue
- Expected vs. actual behavior
- Browser and workspace region (not the URL)

## Requesting a Feature

Use [GitHub Issues](../../issues/new?template=feature_request.md) to suggest new features or improvements. Include the problem you're trying to solve and any alternatives you've considered.

## Community

Join **#genie-workbench** on Slack for:
- Questions and discussion
- Reporting issues that involve sensitive or customer-specific details
- Sharing feedback and ideas

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

See the [README](README.md) for full architecture details and environment variable reference.

## Pull Request Process

1. Create a feature branch from `main`
2. Make your changes with clear, descriptive commits
3. Run the test suite:
   ```bash
   ./scripts/test.sh
   ```
4. Deploy and test against a real Databricks workspace (there is no local dev server):
   ```bash
   ./scripts/deploy.sh
   ```
5. Open a PR with:
   - Brief description of the change
   - Motivation or linked issue
   - Testing performed
6. Address review feedback

## Security

To report a security vulnerability, see [SECURITY.md](SECURITY.md).

## License

By submitting a contribution, you agree that your contributions will be licensed under the same terms as the project. See [LICENSE.md](LICENSE.md).
