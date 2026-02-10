#!/usr/bin/env bash
set -euo pipefail

# Load configuration
if [ ! -f .env ]; then
    echo "Error: .env file not found. Copy .env.example to .env and fill in your values."
    exit 1
fi
source .env

DATASETS=(authors awards concepts domains fields funders institutions publishers sources subfields topics works)

usage() {
    echo "Usage: bash upload.sh <dataset> [dataset ...]"
    echo ""
    echo "Uploads data to GCS at gs://<GCS_BUCKET>/<VERSION>/<dataset>/."
    echo ""
    echo "Available datasets: ${DATASETS[*]}"
    echo "Use 'all' to upload everything."
    echo ""
    echo "Examples:"
    echo "  bash upload.sh works"
    echo "  bash upload.sh works concepts"
    echo "  bash upload.sh all"
    exit 1
}

if [ $# -eq 0 ]; then
    usage
fi

for var in VERSION GCS_BUCKET; do
    if [ -z "${!var:-}" ]; then
        echo "Error: $var is not set in .env"
        exit 1
    fi
done

# Resolve dataset list
if [ "$1" = "all" ]; then
    selected=("${DATASETS[@]}")
else
    selected=("$@")
    for ds in "${selected[@]}"; do
        valid=false
        for known in "${DATASETS[@]}"; do
            if [ "$ds" = "$known" ]; then
                valid=true
                break
            fi
        done
        if [ "$valid" = false ]; then
            echo "Error: unknown dataset '$ds'"
            echo "Available datasets: ${DATASETS[*]}"
            exit 1
        fi
    done
fi

for ds in "${selected[@]}"; do
    GCS_PATH="gs://${GCS_BUCKET}/${VERSION}/${ds}/"

    # Only works/concepts/institutions have conversion scripts; rest upload from raw
    CONVERTED_DIR="data/converted/${VERSION}/${ds}"
    if [ -d "$CONVERTED_DIR" ]; then
        SRC_DIR="$CONVERTED_DIR"
    else
        SRC_DIR="data/raw/${VERSION}/${ds}"
    fi

    echo "Uploading ${SRC_DIR} to ${GCS_PATH} ..."
    gsutil -m cp -r -x 'manifest$' "${SRC_DIR}/"* "${GCS_PATH}"
done

echo "Done."
