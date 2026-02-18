#!/usr/bin/env bash
set -euo pipefail

# Load configuration
if [ ! -f .env ]; then
    echo "Error: .env file not found. Copy .env.example to .env and fill in your values."
    exit 1
fi
source .env

DATASETS=(authors awards concepts domains fields funders institutions publishers sources subfields topics works)
ROLE="roles/bigquery.dataViewer"
MEMBER="allUsers"

usage() {
    echo "Usage: bash public.sh <dataset> [dataset ...]"
    echo ""
    echo "Grants public read access (allUsers dataViewer) to BigQuery tables."
    echo "Targets tables named <dataset>_<VERSION> (e.g. works_${VERSION})."
    echo ""
    echo "Available datasets: ${DATASETS[*]}"
    echo "Use 'all' to make all tables public."
    echo ""
    echo "Examples:"
    echo "  bash public.sh works"
    echo "  bash public.sh all"
    exit 1
}

if [ $# -eq 0 ]; then
    usage
fi

for var in VERSION PROJECT_ID BQ_DATASET; do
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

echo "Granting ${ROLE} to ${MEMBER} on ${#selected[@]} table(s) ..."
echo ""

for ds in "${selected[@]}"; do
    TABLE="${BQ_DATASET}.${ds}_${VERSION}"
    FULL="${PROJECT_ID}:${TABLE}"

    echo "  ${FULL} ..."
    bq add-iam-policy-binding \
        --member="${MEMBER}" \
        --role="${ROLE}" \
        "${FULL}"
    echo "  ✓ ${FULL}"
done

echo ""
echo "Done: ${#selected[@]} table(s) made public."
