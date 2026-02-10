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
    echo "Usage: bash bq_load.sh <dataset> [dataset ...]"
    echo ""
    echo "Creates BigQuery tables from GCS data."
    echo "Tables are named <dataset>_<VERSION> (e.g. works_${VERSION})."
    echo ""
    echo "Available datasets: ${DATASETS[*]}"
    echo "Use 'all' to load everything."
    echo ""
    echo "Examples:"
    echo "  bash bq_load.sh works"
    echo "  bash bq_load.sh works concepts"
    echo "  bash bq_load.sh all"
    exit 1
}

if [ $# -eq 0 ]; then
    usage
fi

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
        echo "Warning: no schema found for ${ds}, skipping"
        continue
    fi

    echo "Loading ${BQ_DATASET}.${TABLE} from ${GCS_PATH} (schema: ${SCHEMA}) ..."
    bq load \
        --source_format=NEWLINE_DELIMITED_JSON \
        --project_id="${PROJECT_ID}" \
        --replace=true \
        "${BQ_DATASET}.${TABLE}" \
        "${GCS_PATH}*" \
        "${SCHEMA}"

    echo "${ds} done."
done

echo "Done."
