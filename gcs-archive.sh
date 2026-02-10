#!/usr/bin/env bash
set -euo pipefail

# Load configuration
if [ ! -f .env ]; then
    echo "Error: .env file not found. Copy .env.example to .env and fill in your values."
    exit 1
fi
source .env

for var in VERSION GCS_BUCKET; do
    if [ -z "${!var:-}" ]; then
        echo "Error: $var is not set in .env"
        exit 1
    fi
done

GCS_PATH="gs://${GCS_BUCKET}/${VERSION}/"

echo "This will change the storage class of all objects under"
echo "  ${GCS_PATH}"
echo "to ARCHIVE."
echo ""
read -p "Are you sure? (y/N) " confirm
if [ "$confirm" != "y" ] && [ "$confirm" != "Y" ]; then
    echo "Aborted."
    exit 0
fi

echo "Rewriting to ARCHIVE storage class ..."
gsutil -m rewrite -s ARCHIVE "${GCS_PATH}**"

echo "Done."
