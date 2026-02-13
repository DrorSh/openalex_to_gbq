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

# Use pigz for faster decompression if available, otherwise fall back to zcat
if command -v pigz &> /dev/null; then
    DECOMPRESS="pigz -dc"
else
    DECOMPRESS="zcat"
fi

NPROC=$(( ($(nproc 2>/dev/null || echo 4) + 1) / 2 ))

VALIDATION_DIR="data/validation/${VERSION}"
mkdir -p "$VALIDATION_DIR"

exit_code=0

for ds in "${selected[@]}"; do
    MANIFEST="data/raw/${VERSION}/${ds}/manifest"

    if [ ! -f "$MANIFEST" ]; then
        echo "Error: manifest not found at $MANIFEST"
        exit_code=1
        continue
    fi

    echo "Validating ${ds} ..."

    expected_total=$(jq '.meta.record_count' "$MANIFEST")
    PROGRESS_FILE="${VALIDATION_DIR}/${ds}.tsv"

    # Load previously validated files (format: file\tactual\texpected\tstatus)
    declare -A VALIDATED=()
    if [ -f "$PROGRESS_FILE" ]; then
        while IFS=$'\t' read -r prev_rel prev_actual prev_expected prev_status; do
            [ "$prev_rel" = "file" ] && continue  # skip header
            VALIDATED["$prev_rel"]="$prev_actual"
        done < "$PROGRESS_FILE"
    else
        printf 'file\tactual\texpected\tstatus\n' > "$PROGRESS_FILE"
    fi

    # Build list of files from manifest
    TMPDIR_VALIDATE=$(mktemp -d)
    MISSING=()
    PENDING_COUNT=0
    CACHED_COUNT=0

    while IFS=$'\t' read -r url expected_count; do
        rel_path="${url#s3://openalex/data/}"
        converted_file="data/converted/${VERSION}/${rel_path}"

        if [ -f "$converted_file" ]; then
            if [ -n "${VALIDATED[$rel_path]+x}" ]; then
                CACHED_COUNT=$((CACHED_COUNT + 1))
            else
                printf '%s\t%s\t%s\n' "$converted_file" "$expected_count" "$rel_path" >> "${TMPDIR_VALIDATE}/files.tsv"
                PENDING_COUNT=$((PENDING_COUNT + 1))
            fi
        else
            MISSING+=("$rel_path")
        fi
    done < <(jq -r '.entries[] | [.url, .meta.record_count] | @tsv' "$MANIFEST")

    for m in "${MISSING[@]+"${MISSING[@]}"}"; do
        echo "  ${m}: FILE NOT FOUND"
    done

    all_ok=true
    if [ ${#MISSING[@]} -gt 0 ]; then
        all_ok=false
    fi

    if [ "$CACHED_COUNT" -gt 0 ]; then
        echo "  ${CACHED_COUNT} files already validated (cached)"
    fi

    # Count lines in parallel for pending files, saving each result immediately
    if [ "$PENDING_COUNT" -gt 0 ]; then
        FILE_COUNT=$(wc -l < "${TMPDIR_VALIDATE}/files.tsv")
        echo "  Counting lines in ${FILE_COUNT} files (${NPROC} parallel) ..."

        # Each worker: count lines, compare, append result to progress file
        while IFS=$'\t' read -r file expected_count rel_path; do
            printf '%s\t%s\t%s\n' "$file" "$rel_path" "$expected_count"
        done < "${TMPDIR_VALIDATE}/files.tsv" | \
            xargs -P "$NPROC" -d '\n' -I{} bash -c "
                file=\$(printf '%s' \"{}\" | cut -f1)
                rel_path=\$(printf '%s' \"{}\" | cut -f2)
                expected=\$(printf '%s' \"{}\" | cut -f3)
                actual=\$($DECOMPRESS \"\$file\" | wc -l | tr -d ' ')
                if [ \"\$actual\" -eq \"\$expected\" ]; then status=OK; else status=MISMATCH; fi
                flock \"${PROGRESS_FILE}.lock\" bash -c \"printf '%s\t%s\t%s\t%s\n' '\$rel_path' '\$actual' '\$expected' '\$status' >> '${PROGRESS_FILE}'\"
                done=\$(grep -c -v '^file' \"${PROGRESS_FILE}\")
                printf '\r  Progress: %s/%s files counted' \"\$done\" \"$(( FILE_COUNT + CACHED_COUNT ))\" >&2
            "
        rm -f "${PROGRESS_FILE}.lock"
        echo ""

        # Reload progress file into VALIDATED
        while IFS=$'\t' read -r prev_rel prev_actual prev_expected prev_status; do
            [ "$prev_rel" = "file" ] && continue
            VALIDATED["$prev_rel"]="$prev_actual"
        done < "$PROGRESS_FILE"
    fi

    rm -rf "$TMPDIR_VALIDATE"

    # Report results using cached + new counts
    actual_total=0
    while IFS=$'\t' read -r url expected_count; do
        rel_path="${url#s3://openalex/data/}"

        if [ -z "${VALIDATED[$rel_path]+x}" ]; then
            continue
        fi

        actual_count="${VALIDATED[$rel_path]}"
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

    unset VALIDATED
done

exit $exit_code
