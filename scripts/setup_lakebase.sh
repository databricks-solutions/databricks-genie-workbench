#!/bin/bash
# Setup Lakebase PostgreSQL instance for Genie Workbench
# Usage: ./scripts/setup_lakebase.sh [instance-name] [catalog] [schema]
#
# Prerequisites:
#   - Databricks CLI installed and configured (databricks auth login)
#   - psql client available for running the SQL schema

set -e

INSTANCE_NAME="${1:-genie-workbench-db}"
CATALOG="${2:-main}"
SCHEMA="${3:-genie_workbench}"

echo "Setting up Lakebase for Genie Workbench..."
echo "  Instance: $INSTANCE_NAME"
echo "  Catalog:  $CATALOG"
echo "  Schema:   $SCHEMA"
echo ""

# Create Lakebase instance (Databricks CLI)
echo "Creating Lakebase instance..."
databricks lakebase create \
  --name "$INSTANCE_NAME" \
  --catalog "$CATALOG" \
  --schema "$SCHEMA"

echo ""
echo "Lakebase instance created: $INSTANCE_NAME"
echo ""
echo "Next steps:"
echo "  1. Get connection details: databricks lakebase get --name $INSTANCE_NAME"
echo "  2. Run SQL schema: psql -h <host> -U <user> -d <database> -f sql/setup_lakebase.sql"
echo "  3. Update app.yaml with your Lakebase connection details"
echo "  4. Deploy: databricks apps deploy --source-code-path . --app-name genie-workbench"
