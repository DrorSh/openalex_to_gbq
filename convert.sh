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

DATASETS=(works concepts institutions)

usage() {
    echo "Usage: bash convert.sh <dataset> [dataset ...]"
    echo ""
    echo "Available datasets: ${DATASETS[*]}"
    echo "Use 'all' to convert everything."
    echo ""
    echo "Note: authors and venues do not require conversion."
    echo ""
    echo "Examples:"
    echo "  bash convert.sh works"
    echo "  bash convert.sh works concepts"
    echo "  bash convert.sh all"
    exit 1
}

if [ $# -eq 0 ]; then
    usage
fi

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
    echo "Converting $ds ..."
    DATA_VERSION="$VERSION" node "scripts/run_${ds}.js"
done

echo "Done."
