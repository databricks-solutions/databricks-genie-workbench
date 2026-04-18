"""Factory for creating telemetry-tagged WorkspaceClient instances."""

from databricks.sdk import WorkspaceClient

from genie_space_optimizer._telemetry import PRODUCT_NAME, PRODUCT_VERSION


def make_workspace_client(**kwargs) -> WorkspaceClient:
    """Create a WorkspaceClient tagged with GSO product telemetry."""
    return WorkspaceClient(
        product=PRODUCT_NAME,
        product_version=PRODUCT_VERSION,
        **kwargs,
    )
