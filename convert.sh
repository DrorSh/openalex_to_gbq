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

# Only datasets that have conversion scripts in scripts/
DATASETS=(works concepts institutions sources)

usage() {
    echo "Usage: bash convert.sh [--parallel[=N]] <dataset> [dataset ...] [START-END]"
    echo ""
    echo "Available datasets: ${DATASETS[*]}"
    echo "Use 'all' to convert everything."
    echo ""
    echo "Options:"
    echo "  --parallel     Process folders concurrently (default: number of CPUs)"
    echo "  --parallel=N   Process with N concurrent workers"
    echo ""
    echo "Batch mode (for large datasets like works):"
    echo "  Append a range like 1-50 to process only folders 1 through 50."
    echo "  Folders are sorted alphabetically. Use 'count' to see total folder count."
    echo ""
    echo "Examples:"
    echo "  bash convert.sh works"
    echo "  bash convert.sh --parallel works 1-50"
    echo "  bash convert.sh --parallel=4 works 51-100"
    echo "  bash convert.sh works count"
    echo "  bash convert.sh all"
    exit 1
}

if [ $# -eq 0 ]; then
    usage
fi

# Check for --parallel flag
PARALLEL=""
CONCURRENCY=""
if [[ "${1:-}" == --parallel* ]]; then
    if [[ "$1" == --parallel=* ]]; then
        CONCURRENCY="${1#--parallel=}"
    fi
    PARALLEL=1
    shift
fi

if [ $# -eq 0 ]; then
    usage
fi

# Check for batch range or count as last argument
BATCH_RANGE=""
last_arg="${!#}"
if [[ "$last_arg" =~ ^[0-9]+-[0-9]+$ ]]; then
    BATCH_RANGE="$last_arg"
    # Remove last argument from positional params
    set -- "${@:1:$#-1}"
elif [ "$last_arg" = "count" ]; then
    # Remove 'count' from args
    set -- "${@:1:$#-1}"
    for ds in "$@"; do
        RAW_DIR="./data/raw/${VERSION}/${ds}/"
        if [ ! -d "$RAW_DIR" ]; then
            echo "${ds}: raw data directory not found at ${RAW_DIR}"
            continue
        fi
        num_folders=$(find "$RAW_DIR" -mindepth 1 -maxdepth 1 -type d | wc -l)
        echo "${ds}: ${num_folders} folders"
    done
    exit 0
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
    if [ -n "$BATCH_RANGE" ]; then
        echo "Converting $ds (batch ${BATCH_RANGE}) ..."
    else
        echo "Converting $ds ..."
    fi
    if [ -f "scripts/run_${ds}.js" ]; then
        DATA_VERSION="$VERSION" BATCH_RANGE="$BATCH_RANGE" CONCURRENCY="${CONCURRENCY}" node "scripts/run_${ds}.js"
    elif [ -f "scripts/run_${ds}.py" ]; then
        DATA_VERSION="$VERSION" BATCH_RANGE="$BATCH_RANGE" python3 "scripts/run_${ds}.py"
    else
        echo "Error: no conversion script found for ${ds}"
        exit 1
    fi
done

echo "Done."
