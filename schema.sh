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
SCHEMA_DIR="schemas/${VERSION}"

usage() {
    echo "Usage: bash schema.sh <dataset> [dataset ...]"
    echo ""
    echo "Generates BigQuery schema from downloaded data using bigquery-schema-generator."
    echo "Schemas are saved to ${SCHEMA_DIR}/."
    echo ""
    echo "Available datasets: ${DATASETS[*]}"
    echo "Use 'all' to generate all schemas."
    echo ""
    echo "Examples:"
    echo "  bash schema.sh authors"
    echo "  bash schema.sh works authors"
    echo "  bash schema.sh all"
    exit 1
}

if [ $# -eq 0 ]; then
    usage
fi

if ! command -v generate-schema &> /dev/null; then
    echo "Error: generate-schema not found. Run 'pixi run setup' first."
    exit 1
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

mkdir -p "$SCHEMA_DIR"

for ds in "${selected[@]}"; do
    # Use converted data if available, otherwise raw
    CONVERTED_DIR="data/converted/${VERSION}/${ds}"
    RAW_DIR="data/raw/${VERSION}/${ds}"
    if [ -d "$CONVERTED_DIR" ]; then
        DATA_DIR="$CONVERTED_DIR"
    elif [ -d "$RAW_DIR" ]; then
        DATA_DIR="$RAW_DIR"
    else
        echo "Warning: no data found for ${ds}, skipping"
        continue
    fi
    SCHEMA_FILE="${SCHEMA_DIR}/${ds}.schema.json"

    echo "Generating schema for ${ds} ..."

    # Sample 1000 records from every .gz file to capture all field variations
    TMPFILE=$(mktemp)
    python3 -c "
import gzip, glob, sys, os

raw_dir = sys.argv[1]
files = sorted(glob.glob(os.path.join(raw_dir, '**', '*.gz'), recursive=True))
if not files:
    print(f'Warning: no .gz files found in {raw_dir}', file=sys.stderr)
    sys.exit(1)

print(f'  Sampling 1000 records from each of {len(files)} files ...', file=sys.stderr)
for path in files:
    with gzip.open(path, 'rt') as f:
        for i, line in enumerate(f):
            if i >= 1000: break
            sys.stdout.write(line)
" "$DATA_DIR" > "$TMPFILE"

    LINES=$(wc -l < "$TMPFILE")
    echo "  Sampled ${LINES} records total"

    SCHEMA_FLAGS="--keep_nulls --ignore_invalid_lines"
    if [ -f "$SCHEMA_FILE" ]; then
        echo "  Updating existing schema incrementally"
        SCHEMA_FLAGS="$SCHEMA_FLAGS --existing_schema_path $SCHEMA_FILE"
    fi
    generate-schema $SCHEMA_FLAGS < "$TMPFILE" > "${SCHEMA_FILE}.tmp"
    mv "${SCHEMA_FILE}.tmp" "$SCHEMA_FILE"
    rm -f "$TMPFILE"

    echo "  Saved to ${SCHEMA_FILE}"
done

echo "Done."
