#!/bin/bash
# Auto-organize documentation files into proper directory structure
# Ensures project root stays clean and documentation is properly categorized
# NOTE: Run this script from the data_product_accelerator/ directory (project root)

# Verify we're in the right directory
if [ ! -d "skills" ] || [ ! -d "docs" ]; then
    echo "ERROR: Run this script from the data_product_accelerator/ directory." >&2
    exit 1
fi

# Create structure
mkdir -p docs/{deployment/deployment-history,troubleshooting,architecture,operations,development,reference}

# Move deployment docs (timestamp-based)
for file in *DEPLOYMENT*.md DEPLOY*.md; do
    [ -f "$file" ] && mv "$file" "docs/deployment/deployment-history/$(date +%Y-%m-%d)-${file,,}"
done

# Move checklists
mv *CHECKLIST*.md *checklist*.md docs/deployment/ 2>/dev/null

# Move issues/troubleshooting
mv ISSUE*.md issue*.md docs/troubleshooting/ 2>/dev/null

# Move summaries
mv *SUMMARY*.md docs/reference/ 2>/dev/null

# Convert to kebab-case
cd docs
find . -name "*.md" | while read f; do
    new=$(echo "$f" | sed 's/\([A-Z]\)/-\L\1/g' | sed 's/^-//' | tr '_' '-' | tr -s '-')
    [ "$f" != "$new" ] && mv "$f" "$new" 2>/dev/null
done

echo "✅ Documentation organized into docs/ subdirectories"
