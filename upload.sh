#!/usr/bin/env bash
set -euo pipefail

# Load configuration
if [ ! -f .env ]; then
    echo "Error: .env file not found. Copy .env.example to .env and fill in your values."
    exit 1
fi
source .env

DATASETS=(authors awards concepts domains fields funders institutions publishers sources subfields topics works)
MAX_FILES=0  # 0 means no limit

usage() {
    echo "Usage: bash upload.sh [-m max_files] <dataset> [dataset ...]"
    echo ""
    echo "Uploads data to GCS at gs://<GCS_BUCKET>/<VERSION>/<dataset>/."
    echo ""
    echo "Options:"
    echo "  -m N    Upload at most N files per dataset (default: all files)"
    echo ""
    echo "Available datasets: ${DATASETS[*]}"
    echo "Use 'all' to upload everything."
    echo ""
    echo "Examples:"
    echo "  bash upload.sh works"
    echo "  bash upload.sh -m 5 works"
    echo "  bash upload.sh -m 10 all"
    exit 1
}

while getopts ":m:" opt; do
    case $opt in
        m) MAX_FILES="$OPTARG" ;;
        *) usage ;;
    esac
done
shift $((OPTIND - 1))

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

UPLOAD_DIR="data/upload/${VERSION}"
mkdir -p "$UPLOAD_DIR"

for ds in "${selected[@]}"; do
    GCS_PATH="gs://${GCS_BUCKET}/${VERSION}/${ds}/"

    # Only works/concepts/institutions have conversion scripts; rest upload from raw
    CONVERTED_DIR="data/converted/${VERSION}/${ds}"
    if [ -d "$CONVERTED_DIR" ]; then
        SRC_DIR="$CONVERTED_DIR"
    else
        SRC_DIR="data/raw/${VERSION}/${ds}"
    fi

    PROGRESS_FILE="${UPLOAD_DIR}/${ds}.tsv"

    # Load previously uploaded files from progress file
    declare -A UPLOADED=()
    if [ -f "$PROGRESS_FILE" ]; then
        while IFS=$'\t' read -r prev_file prev_dest prev_status; do
            [ "$prev_file" = "file" ] && continue  # skip header
            [ "$prev_status" = "OK" ] || continue   # retry failed uploads
            UPLOADED["$prev_file"]=1
        done < "$PROGRESS_FILE"
    else
        printf 'file\tdestination\tstatus\n' > "$PROGRESS_FILE"
    fi

    # Collect local files (excluding manifest), filter out already-uploaded ones
    mapfile -t ALL_LOCAL < <(find "$SRC_DIR" -type f ! -name 'manifest' | sort)
    PENDING=()
    for f in "${ALL_LOCAL[@]}"; do
        fname=$(basename "$f")
        if [ -z "${UPLOADED[$fname]+x}" ]; then
            PENDING+=("$f")
        fi
    done

    SKIPPED=$(( ${#ALL_LOCAL[@]} - ${#PENDING[@]} ))
    if [ "$SKIPPED" -gt 0 ]; then
        echo "  Skipping ${SKIPPED} files already uploaded"
    fi

    if [ ${#PENDING[@]} -eq 0 ]; then
        echo "  All files already uploaded for ${ds}, nothing to do"
        unset UPLOADED
        continue
    fi

    if [ "$MAX_FILES" -gt 0 ]; then
        UPLOAD=("${PENDING[@]:0:$MAX_FILES}")
        echo "Uploading ${#UPLOAD[@]} of ${#PENDING[@]} pending files from ${SRC_DIR} to ${GCS_PATH} (limited by -m ${MAX_FILES}) ..."
    else
        UPLOAD=("${PENDING[@]}")
        echo "Uploading ${#UPLOAD[@]} files from ${SRC_DIR} to ${GCS_PATH} ..."
    fi

    # Upload one file at a time and record progress
    TOTAL=${#UPLOAD[@]}
    COUNT=0
    for f in "${UPLOAD[@]}"; do
        fname=$(basename "$f")
        gsutil cp "$f" "${GCS_PATH}" && {
            printf '%s\t%s\t%s\n' "$fname" "${GCS_PATH}${fname}" "OK" >> "$PROGRESS_FILE"
            COUNT=$((COUNT + 1))
            echo "  [${COUNT}/${TOTAL}] ${fname} uploaded"
        } || {
            printf '%s\t%s\t%s\n' "$fname" "${GCS_PATH}${fname}" "FAILED" >> "$PROGRESS_FILE"
            COUNT=$((COUNT + 1))
            echo "  [${COUNT}/${TOTAL}] ${fname} FAILED"
        }
    done

    unset UPLOADED
done

echo "Done."
