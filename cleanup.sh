#!/usr/bin/env bash
set -euo pipefail

# Load configuration
if [ ! -f .env ]; then
    echo "Error: .env file not found. Copy .env.example to .env and fill in your values."
    exit 1
fi
source .env

if [ -z "${VERSION:-}" ]; then
    echo "Error: VERSION is not set in .env"
    exit 1
fi

DATA_DIR="data"

echo "This will delete all local data files under ${DATA_DIR}/ for version ${VERSION}."
echo ""
echo "  ${DATA_DIR}/raw/${VERSION}/"
echo "  ${DATA_DIR}/converted/${VERSION}/"
echo ""
read -p "Are you sure? (y/N) " confirm
if [ "$confirm" != "y" ] && [ "$confirm" != "Y" ]; then
    echo "Aborted."
    exit 0
fi

for subdir in "raw/${VERSION}" "converted/${VERSION}"; do
    dir="${DATA_DIR}/${subdir}"
    if [ -d "$dir" ]; then
        echo "Removing ${dir} ..."
        rm -rf "$dir"
    else
        echo "Skipping ${dir} (not found)"
    fi
done

echo "Done."
