#!/usr/bin/env bash
set -euo pipefail

# Load configuration
if [ ! -f .env ]; then
    echo "Error: .env file not found. Copy .env.example to .env and fill in your values."
    exit 1
fi
source .env

DATASETS=(works concepts institutions authors venues)

usage() {
    echo "Usage: bash upload.sh <dataset> [dataset ...]"
    echo ""
    echo "Uploads converted data to GCS and loads into BigQuery."
    echo "Table names are versioned as <dataset>_${VERSION} (from .env)."
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

# Validate .env variables
for var in VERSION PROJECT_ID GCS_BUCKET BQ_DATASET; do
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
    TABLE="${ds}_${VERSION}"
    GCS_PATH="gs://${GCS_BUCKET}/${VERSION}/${ds}/"

    echo "=== ${ds} ==="

    # Authors and venues don't need conversion — upload from raw
    if [ "$ds" = "authors" ] || [ "$ds" = "venues" ]; then
        SRC_DIR="data/raw/${VERSION}/${ds}"
    else
        SRC_DIR="data/converted/${VERSION}/${ds}"
    fi

    echo "Uploading ${SRC_DIR} to ${GCS_PATH} ..."
    gsutil -m cp -r "${SRC_DIR}/"* "${GCS_PATH}"

    # Find schema file (prefer oa_2023, fall back to oa_2022)
    SCHEMA=""
    for schema_dir in schemas/oa_2023 schemas/oa_2022; do
        for f in "${schema_dir}/${ds}.schema.json" "${schema_dir}/${ds}_schema.json"; do
            if [ -f "$f" ]; then
                SCHEMA="$f"
                break 2
            fi
        done
    done

    if [ -z "$SCHEMA" ]; then
        echo "Warning: no schema found for ${ds}, skipping BQ load"
        continue
    fi

    echo "Loading into ${BQ_DATASET}.${TABLE} (schema: ${SCHEMA}) ..."
    bq load \
        --source_format=NEWLINE_DELIMITED_JSON \
        --project_id="${PROJECT_ID}" \
        --replace=true \
        "${BQ_DATASET}.${TABLE}" \
        "${GCS_PATH}*" \
        "${SCHEMA}"

    echo "${ds} done."
    echo ""
done

echo "All uploads complete."
