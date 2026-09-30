#!/bin/sh
# bumpver pre_commit_hook (see [tool.bumpver] in pyproject.toml). bumpver has just
# rewritten the project version; re-lock so uv.lock records it, and stage the result
# so it lands in the "Bump version" commit and tag.
set -eu
uv lock --quiet
git add uv.lock
