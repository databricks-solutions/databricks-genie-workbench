"""Databricks SDK telemetry constants for the genie-workbench product."""

from backend.app_version import read_version

PRODUCT_NAME = "genie-workbench"
PRODUCT_VERSION = read_version()  # From the repo-root VERSION file
