#!/usr/bin/env bash
# Release guard: the published tag (minus any leading "v") must equal VERSION.
# Usage: bash scripts/check_release_version.sh <tag>
set -euo pipefail
cd "$(dirname "$0")/.."

tag="${1:?usage: check_release_version.sh <tag>}"
want="${tag#v}"
have=""
if [ -f VERSION ]; then
    have="$(tr -d '[:space:]' < VERSION)"
fi

if [ "$have" != "$want" ]; then
    echo "::error::VERSION file says '${have:-<missing>}' but the release tag is '$tag'. Bump VERSION to '$want' in a PR, merge it, and re-create the release from that commit." >&2
    exit 1
fi
echo "VERSION ($have) matches release tag ($tag)."
