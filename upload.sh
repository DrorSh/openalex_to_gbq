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
    echo "  -m N    Upload at most N new files per dataset (default: all)"
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

    # Find pending files (not yet uploaded)
    mapfile -t ALL_LOCAL < <(find "$SRC_DIR" -type f ! -name 'manifest' ! -name '*.tmp' | sort)
    PENDING_FILES=()
    SKIPPED=0

    for f in "${ALL_LOCAL[@]}"; do
        rel_path="${f#$SRC_DIR/}"
        if [ -n "${UPLOADED[$rel_path]+x}" ]; then
            SKIPPED=$((SKIPPED + 1))
            continue
        fi
        PENDING_FILES+=("$f")
    done

    if [ "$SKIPPED" -gt 0 ]; then
        echo "  Skipping ${SKIPPED} files already uploaded"
    fi

    if [ ${#PENDING_FILES[@]} -eq 0 ]; then
        echo "  All files already uploaded for ${ds}, nothing to do"
        unset PENDING_FILES
        continue
    fi

    TOTAL_PENDING=${#PENDING_FILES[@]}
    if [ "$MAX_FILES" -gt 0 ] && [ "$TOTAL_PENDING" -gt "$MAX_FILES" ]; then
        PENDING_FILES=("${PENDING_FILES[@]:0:$MAX_FILES}")
        echo "Uploading ${#PENDING_FILES[@]} of ${TOTAL_PENDING} pending files from ${SRC_DIR} to ${GCS_PATH} (limited by -m ${MAX_FILES}) ..."
    else
        echo "Uploading ${TOTAL_PENDING} files from ${SRC_DIR} to ${GCS_PATH} ..."
    fi

    # Upload each pending file individually to its correct GCS path
    UPLOAD_OK=0
    UPLOAD_FAIL=0
    for f in "${PENDING_FILES[@]}"; do
        rel_path="${f#$SRC_DIR/}"
        dest="${GCS_PATH}${rel_path}"
        if gsutil cp "$f" "$dest" 2>/dev/null; then
            printf '%s\t%s\t%s\n' "$rel_path" "$dest" "OK" >> "$PROGRESS_FILE"
            UPLOAD_OK=$((UPLOAD_OK + 1))
        else
            printf '%s\t%s\t%s\n' "$rel_path" "$dest" "FAIL" >> "$PROGRESS_FILE"
            UPLOAD_FAIL=$((UPLOAD_FAIL + 1))
        fi
        echo "  [${UPLOAD_OK}/${#PENDING_FILES[@]}] ${rel_path}"
    done

    echo "  Uploaded ${UPLOAD_OK} files (${UPLOAD_FAIL} failed)"
    unset PENDING_FILES

    unset UPLOADED
done

echo "Done."
