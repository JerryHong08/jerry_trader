#!/usr/bin/env bash
set -euo pipefail

# Deploy GridTrader build to GitHub Pages (gh-pages branch on origin)
# Usage: ./deploy.sh              (build + deploy)
#        ./deploy.sh --skip-build (deploy existing build/)

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

BRANCH="gh-pages"
BUILD_DIR="build"

# Build unless --skip-build
if [[ "${1:-}" != "--skip-build" ]]; then
  echo "Building with .env.ghpages (no secrets)..."
  # Vite loads .env.local > .env.[mode] > .env
  # We copy .env.ghpages → .env.production so it overrides .env during build
  cp "$SCRIPT_DIR/.env.ghpages" "$SCRIPT_DIR/.env.production"
  pnpm run build
  rm -f "$SCRIPT_DIR/.env.production"
fi

if [ ! -d "$BUILD_DIR" ]; then
  echo "No build/ directory found."
  exit 1
fi

REPO_ROOT="$(git rev-parse --show-toplevel)"

# Create commit from build/ without touching the working tree.
# Uses a throwaway index file so the real index stays untouched.
export GIT_DIR="$REPO_ROOT/.git"
export GIT_INDEX_FILE="$(mktemp)"
rm -f "$GIT_INDEX_FILE"  # git needs a non-existent path, not an empty file
export GIT_WORK_TREE="$SCRIPT_DIR/$BUILD_DIR"
trap "rm -f $GIT_INDEX_FILE" EXIT

git add -A
TREE=$(git write-tree)
COMMIT=$(git commit-tree "$TREE" -m "Deploy GridTrader $(date '+%Y-%m-%d %H:%M:%S')")

unset GIT_WORK_TREE GIT_INDEX_FILE
git push origin "$COMMIT":refs/heads/"$BRANCH" --force

echo "Deployed to https://JerryHong08.github.io/jerryib_trader/"
