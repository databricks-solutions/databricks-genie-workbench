#!/bin/bash
# Build script for Genie Workbench
# Compiles the React frontend before deployment.
# Usage: ./scripts/build.sh

set -e

echo "Building Genie Workbench..."

# Build frontend
echo "Building frontend..."
cd frontend
npm ci
npm run build
cd ..

echo "Frontend built successfully at frontend/dist/"
echo ""
echo "To deploy:"
echo "  databricks apps deploy --source-code-path . --app-name genie-workbench"
