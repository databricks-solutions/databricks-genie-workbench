#!/usr/bin/env bash
# Release guard: the published tag must equal VERSION exactly. The app links
# "Release notes" to /releases/tag/<VERSION>, so a "v"-prefixed tag would 404.
# Usage: bash scripts/check_release_version.sh <tag>
set -euo pipefail
cd "$(dirname "$0")/.."

tag="${1:?usage: check_release_version.sh <tag>}"
have=""
if [ -f VERSION ]; then
    have="$(tr -d '[:space:]' < VERSION)"
fi

if [ -z "$have" ]; then
    echo "::error::VERSION file is <missing> or empty, but the release tag is '$tag'. Add VERSION with '${tag#v}' in a PR, merge it, and re-create the release from that commit." >&2
    exit 1
fi

if [ "$tag" != "$have" ]; then
    if [ "${tag#v}" = "$have" ]; then
        echo "::error::Release tag '$tag' must be exactly '$have' (no 'v' prefix), because the app links Release notes to /releases/tag/$have. Re-create the release with tag '$have'." >&2
    else
        echo "::error::VERSION file says '$have' but the release tag is '$tag'. Bump VERSION to '${tag#v}' in a PR, merge it, and re-create the release from that commit." >&2
    fi
    exit 1
fi
echo "VERSION ($have) matches release tag ($tag)."
