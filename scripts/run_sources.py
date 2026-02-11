"""Convert sources data: fix null arrays so BQ schema generation works cleanly."""

import gzip
import json
import glob
import os
import sys

VERSION = os.environ.get("DATA_VERSION")
if not VERSION:
    print("Error: DATA_VERSION env var not set", file=sys.stderr)
    sys.exit(1)

BASE_PATH = f"./data/raw/{VERSION}/sources/"
CONVERTED_PATH = f"./data/converted/{VERSION}/sources/"

# Top-level fields that should always be arrays, never null
ARRAY_FIELDS = {"apc_prices", "alternate_titles", "issn", "societies", "counts_by_year",
                "topics", "topic_share", "host_organization_lineage",
                "host_organization_lineage_names"}

def fix_nulls(obj, top_level=False):
    """Recursively fix null values: null elements stripped from arrays, known array fields forced to []."""
    if isinstance(obj, dict):
        result = {}
        for k, v in obj.items():
            if v is None and (top_level and k in ARRAY_FIELDS):
                result[k] = []
            else:
                result[k] = fix_nulls(v)
        return result
    elif isinstance(obj, list):
        return [fix_nulls(v) for v in obj if v is not None]
    return obj

# Get all subdirectories, sorted
all_folders = sorted([
    d for d in os.listdir(BASE_PATH)
    if os.path.isdir(os.path.join(BASE_PATH, d))
])

# Apply batch range if specified (e.g. BATCH_RANGE="1-50")
batch_range = os.environ.get("BATCH_RANGE", "")
if batch_range:
    start_idx, end_idx = batch_range.split("-")
    start_idx = int(start_idx) - 1  # convert to 0-based
    end_idx = int(end_idx)
    print(f"Batch mode: processing folders {start_idx+1}-{end_idx} of {len(all_folders)}")
    all_folders = all_folders[start_idx:end_idx]

# Collect .gz files from selected folders
files = []
for folder in all_folders:
    folder_path = os.path.join(BASE_PATH, folder)
    files.extend(sorted(glob.glob(os.path.join(folder_path, "*.gz"))))

if not files:
    print(f"No .gz files found in {BASE_PATH}")
    sys.exit(1)

print(f"Converting {len(files)} files ...")

for filepath in files:
    rel = os.path.relpath(filepath, BASE_PATH)
    outpath = os.path.join(CONVERTED_PATH, rel)
    os.makedirs(os.path.dirname(outpath), exist_ok=True)

    with gzip.open(filepath, "rt") as fin, gzip.open(outpath, "wt") as fout:
        for i, line in enumerate(fin):
            rec = fix_nulls(json.loads(line), top_level=True)
            fout.write(json.dumps(rec, ensure_ascii=False) + "\n")

        if (i + 1) % 10000 == 0:
            print(f"  {filepath}: {i + 1} records")

    print(f"  {rel} done")

print("Done.")
