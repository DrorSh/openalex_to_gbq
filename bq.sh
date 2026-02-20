#!/usr/bin/env bash
set -euo pipefail

# Load configuration
if [ ! -f .env ]; then
    echo "Error: .env file not found. Copy .env.example to .env and fill in your values."
    exit 1
fi
source .env

DATASETS=(authors awards concepts domains fields funders institutions publishers sources subfields topics works)
MAX_BAD_RECORDS=0

usage() {
    echo "Usage: bash bq.sh [-b N] <dataset> [dataset ...]"
    echo ""
    echo "Creates BigQuery tables from GCS data."
    echo "Tables are named <dataset>_<VERSION> (e.g. works_${VERSION})."
    echo ""
    echo "Options:"
    echo "  -b N    Max bad records to skip per file (default: 0)"
    echo ""
    echo "Available datasets: ${DATASETS[*]}"
    echo "Use 'all' to load everything."
    echo ""
    echo "Examples:"
    echo "  bash bq.sh works"
    echo "  bash bq.sh -b 100 works"
    echo "  bash bq.sh all"
    exit 1
}

while getopts ":b:" opt; do
    case $opt in
        b) MAX_BAD_RECORDS="$OPTARG" ;;
        *) usage ;;
    esac
done
shift $((OPTIND - 1))

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

    # Find schema file: try exact version first, then most recent available
    SCHEMA=""
    # Build list: exact version first, then all schema dirs sorted descending
    SCHEMA_DIRS=("schemas/${VERSION}")
    for d in $(ls -d schemas/*/ 2>/dev/null | sort -r); do
        dir="${d%/}"
        if [ "$dir" != "schemas/${VERSION}" ]; then
            SCHEMA_DIRS+=("$dir")
        fi
    done
    for schema_dir in "${SCHEMA_DIRS[@]}"; do
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

    ERROR_DIR="data/bq_errors/${VERSION}"
    mkdir -p "$ERROR_DIR"
    ERROR_FILE="${ERROR_DIR}/${ds}.txt"

    echo "Loading ${BQ_DATASET}.${TABLE} from ${GCS_PATH} (schema: ${SCHEMA}) ..."
    JOB_OUTPUT=$(bq load \
        --source_format=NEWLINE_DELIMITED_JSON \
        --project_id="${PROJECT_ID}" \
        --replace=true \
        --max_bad_records="${MAX_BAD_RECORDS}" \
        "${BQ_DATASET}.${TABLE}" \
        "${GCS_PATH}*.gz" \
        "${SCHEMA}" 2>&1) || true

    echo "$JOB_OUTPUT"

    # Extract job ID and save errors if any
    JOB_ID=$(echo "$JOB_OUTPUT" | grep -oP 'job_\S+' | head -1 || true)
    if [ -n "$JOB_ID" ]; then
        ERRORS=$(bq show --format=json -j "$JOB_ID" 2>/dev/null \
            | python3 -c "
import sys, json
job = json.load(sys.stdin)
status = job.get('status', {})
errors = status.get('errors', [])
if errors:
    for e in errors:
        print(f\"- {e.get('location','')}: {e.get('message','')}\")
" 2>/dev/null || true)
        if [ -n "$ERRORS" ]; then
            echo "$ERRORS" > "$ERROR_FILE"
            NUM_ERRORS=$(echo "$ERRORS" | wc -l)
            echo "  ${NUM_ERRORS} errors saved to ${ERROR_FILE}"
        else
            rm -f "$ERROR_FILE"
            echo "  No errors."
        fi
    fi

    echo "${ds} done."
done

echo "Done."
