#!/usr/bin/env bash
set -euo pipefail

# Load configuration
if [ ! -f .env ]; then
    echo "Error: .env file not found. Copy .env.example to .env and fill in your values."
    exit 1
fi
source .env

DATASETS=(authors awards concepts domains fields funders institutions publishers sources subfields topics works)
MAX_DIRS=0  # 0 means no limit

usage() {
    echo "Usage: bash upload.sh [-m max_dirs] <dataset> [dataset ...]"
    echo ""
    echo "Uploads data to GCS at gs://<GCS_BUCKET>/<VERSION>/<dataset>/."
    echo ""
    echo "Options:"
    echo "  -m N    Upload at most N subdirectories per dataset (default: all)"
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
        m) MAX_DIRS="$OPTARG" ;;
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

    # Find pending subdirectories (those with any un-uploaded files)
    mapfile -t ALL_LOCAL < <(find "$SRC_DIR" -type f ! -name 'manifest' | sort)
    declare -A DONE_SUBDIRS=()
    declare -A PENDING_SUBDIRS=()
    declare -a SUBDIR_ORDER=()
    SKIPPED=0

    for f in "${ALL_LOCAL[@]}"; do
        rel_path="${f#$SRC_DIR/}"
        subdir=$(dirname "$rel_path")
        if [ -n "${UPLOADED[$rel_path]+x}" ]; then
            SKIPPED=$((SKIPPED + 1))
            DONE_SUBDIRS["$subdir"]=1
            continue
        fi
        if [ -z "${PENDING_SUBDIRS[$subdir]+x}" ]; then
            SUBDIR_ORDER+=("$subdir")
            PENDING_SUBDIRS["$subdir"]=1
        fi
    done

    if [ "$SKIPPED" -gt 0 ]; then
        echo "  Skipping ${#DONE_SUBDIRS[@]} subdirectories already uploaded (${SKIPPED} files)"
    fi

    if [ ${#SUBDIR_ORDER[@]} -eq 0 ]; then
        echo "  All files already uploaded for ${ds}, nothing to do"
        unset UPLOADED DONE_SUBDIRS PENDING_SUBDIRS SUBDIR_ORDER
        continue
    fi

    TOTAL_DIRS=${#SUBDIR_ORDER[@]}
    if [ "$MAX_DIRS" -gt 0 ] && [ "$TOTAL_DIRS" -gt "$MAX_DIRS" ]; then
        SUBDIR_ORDER=("${SUBDIR_ORDER[@]:0:$MAX_DIRS}")
        echo "Uploading ${#SUBDIR_ORDER[@]} of ${TOTAL_DIRS} pending subdirectories from ${SRC_DIR} to ${GCS_PATH} (limited by -m ${MAX_DIRS}) ..."
    else
        echo "Uploading ${TOTAL_DIRS} subdirectories from ${SRC_DIR} to ${GCS_PATH} ..."
    fi

    # Build list of source subdirectory paths for a single gsutil -m cp -r call
    SRC_PATHS=()
    for subdir in "${SUBDIR_ORDER[@]}"; do
        SRC_PATHS+=("${SRC_DIR}/${subdir}")
    done

    gsutil -m cp -r "${SRC_PATHS[@]}" "${GCS_PATH}" && {
        # Record all files from uploaded subdirectories
        for f in "${ALL_LOCAL[@]}"; do
            rel_path="${f#$SRC_DIR/}"
            subdir=$(dirname "$rel_path")
            if [ -n "${PENDING_SUBDIRS[$subdir]+x}" ]; then
                # Only record subdirs we just uploaded
                found=false
                for s in "${SUBDIR_ORDER[@]}"; do
                    if [ "$s" = "$subdir" ]; then found=true; break; fi
                done
                if [ "$found" = true ]; then
                    printf '%s\t%s\t%s\n' "$rel_path" "${GCS_PATH}${rel_path}" "OK" >> "$PROGRESS_FILE"
                fi
            fi
        done
        echo "  ${#SUBDIR_ORDER[@]} subdirectories uploaded successfully"
    } || {
        echo "  Upload failed"
    }
    unset DONE_SUBDIRS PENDING_SUBDIRS SUBDIR_ORDER

    unset UPLOADED
done

echo "Done."
