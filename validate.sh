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

DATASETS=(authors awards concepts domains fields funders institutions publishers sources subfields topics works)

usage() {
    echo "Usage: bash validate.sh <dataset> [dataset ...]"
    echo ""
    echo "Validates record counts against the manifest for each dataset."
    echo ""
    echo "Available datasets: ${DATASETS[*]}"
    echo "Use 'all' to validate everything."
    echo ""
    echo "Examples:"
    echo "  bash validate.sh works"
    echo "  bash validate.sh works concepts"
    echo "  bash validate.sh all"
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

exit_code=0

for ds in "${selected[@]}"; do
    MANIFEST="data/raw/${VERSION}/${ds}/manifest"

    if [ ! -f "$MANIFEST" ]; then
        echo "Error: manifest not found at $MANIFEST"
        exit_code=1
        continue
    fi

    echo "Validating ${ds} ..."

    actual_total=0
    expected_total=$(jq '.meta.record_count' "$MANIFEST")
    all_ok=true

    # Iterate over each entry in the manifest
    while IFS=$'\t' read -r url expected_count; do
        # Strip s3://openalex/data/ prefix to get relative path
        rel_path="${url#s3://openalex/data/}"

        # Validate converted files only
        converted_file="data/converted/${VERSION}/${rel_path}"

        if [ -f "$converted_file" ]; then
            file="$converted_file"
        else
            echo "  ${rel_path}: FILE NOT FOUND"
            all_ok=false
            continue
        fi

        actual_count=$(zcat "$file" | wc -l)
        actual_total=$((actual_total + actual_count))

        if [ "$actual_count" -eq "$expected_count" ]; then
            echo "  ${rel_path}: ${actual_count}/${expected_count} OK"
        else
            echo "  ${rel_path}: ${actual_count}/${expected_count} MISMATCH"
            all_ok=false
        fi
    done < <(jq -r '.entries[] | [.url, .meta.record_count] | @tsv' "$MANIFEST")

    if [ "$actual_total" -eq "$expected_total" ]; then
        echo "  TOTAL: ${actual_total}/${expected_total} OK"
    else
        echo "  TOTAL: ${actual_total}/${expected_total} MISMATCH"
        all_ok=false
    fi

    echo ""
    if [ "$all_ok" = true ]; then
        echo "${ds}: PASS"
    else
        diff=$((expected_total - actual_total))
        echo "${ds}: FAIL (${diff} records missing)"
        exit_code=1
    fi
    echo ""
done

exit $exit_code
