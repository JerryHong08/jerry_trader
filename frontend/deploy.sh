#!/usr/bin/env bash
set -euo pipefail

# Deploy JerryTrader build to GitHub Pages (gh-pages branch on origin)
# Usage: ./deploy.sh                              (build + deploy)
#        ./deploy.sh --skip-build                 (deploy existing build/)
#        ./deploy.sh --template-id 07             (build + deploy with template)
#        DEPLOY_TEMPLATE_ID=07 ./deploy.sh        (same as --template-id)

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

BRANCH="gh-pages"
BUILD_DIR="build"

SKIP_BUILD=false
TEMPLATE_ID="${DEPLOY_TEMPLATE_ID:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-build)
      SKIP_BUILD=true
      shift
      ;;
    --template-id)
      if [[ -z "${2:-}" ]]; then
        echo "Missing value for --template-id"
        exit 1
      fi
      TEMPLATE_ID="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1"
      exit 1
      ;;
  esac
done

# Build unless --skip-build
if [[ "$SKIP_BUILD" != true ]]; then
  echo "Building with .env.ghpages (no secrets)..."
  # Vite loads .env.local > .env.[mode] > .env
  # We copy .env.ghpages → .env.production so it overrides .env during build
  cp "$SCRIPT_DIR/.env.ghpages" "$SCRIPT_DIR/.env.production"

  if [[ -n "$TEMPLATE_ID" ]]; then
    echo "VITE_DEFAULT_TEMPLATE_ID=$TEMPLATE_ID" >> "$SCRIPT_DIR/.env.production"
    echo "Using VITE_DEFAULT_TEMPLATE_ID=$TEMPLATE_ID"
  fi

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
COMMIT=$(git commit-tree "$TREE" -m "Deploy JerryTrader $(date '+%Y-%m-%d %H:%M:%S')")

unset GIT_WORK_TREE GIT_INDEX_FILE
git push origin "$COMMIT":refs/heads/"$BRANCH" --force

echo "Deployed to https://JerryHong08.github.io/jerry_trader/"
