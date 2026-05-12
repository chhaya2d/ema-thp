#!/usr/bin/env sh
# From repo root: sh scripts/setup-git-hooks.sh
cd "$(dirname "$0")/.." || exit 1
git config core.hooksPath .githooks
echo "Set core.hooksPath=.githooks (pre-push runs scripts/generate-pr-description.sh)"
