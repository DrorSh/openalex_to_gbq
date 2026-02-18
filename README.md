# OpenAlex data to Google Bigquery

OpenAlex data require some modifications before it can be uploaded as columnar data to BQ. Namely, all hyphens in tag names need to be removed and missing arrays should be added. 

The source data is hosted on AWS at https://openalex.s3.amazonaws.com/.

This repo provides node.js scripts and instructions to convert and upload the data. It uses [pixi](https://pixi.sh) for reproducible environment management.

## Available datasets

- authors
- awards
- concepts
- domains
- fields
- funders
- institutions
- publishers
- sources
- subfields
- topics
- works

## Instructions

1. Configure settings

Copy `.env.example` to `.env` and fill in your values:

```
VERSION="20260210"          # Table version suffix (yyyymmdd)
PROJECT_ID="your-project-id"
GCS_BUCKET="your-bucket-name"
BQ_DATASET="openalex"
```

2. Set up the environment with pixi

```
pixi install
pixi run setup
```

3. Download data from AWS

Download one or more datasets using the `download` task (see list above).

```
pixi run download works
pixi run download works authors
pixi run download all        # download everything
```

4. Convert files

Only `works`, `concepts`, `institutions`, and `sources` require conversion (fixing null arrays, stringifying inverted index). All other datasets can be uploaded as-is. Already-converted files are skipped on re-run, so it's safe to interrupt and resume.

For large datasets like `works` (~600GB), you can process in batches by appending a range. Folders are sorted alphabetically; use `count` to see the total. Use `--parallel` to process multiple folders concurrently.

```
pixi run convert works count       # show number of folders
pixi run convert works 1-50        # convert folders 1-50
pixi run convert works 51-100      # convert folders 51-100
pixi run convert --parallel works   # parallel, all folders
pixi run convert --parallel=4 works 1-50  # 4 workers, folders 1-50
pixi run convert works             # sequential, all folders
pixi run convert all               # convert works, concepts, institutions, sources
```

5. Generate schemas

Uses [bigquery-schema-generator](https://github.com/bxparks/bigquery-schema-generator) to generate BQ schemas from the data. Prefers converted data when available, otherwise falls back to raw. If a schema file already exists, it is updated incrementally (new fields are merged in).

Use `-m N` to limit the number of files sampled per dataset (useful for quick schema drafts).

```
pixi run schema authors
pixi run schema all              # generate all schemas
pixi run schema -m 5 works       # sample at most 5 files
```

6. Validate record counts

Checks that converted files match the record counts in the OpenAlex manifest. Uses parallel decompression (cores/2) and `pigz` when available for faster validation.

Results are saved to `data/validation/<VERSION>/<dataset>.tsv` as each file completes, so interrupted runs resume where they left off. Delete the `.tsv` file to re-validate from scratch.

```
pixi run validate works
pixi run validate works concepts
pixi run validate all            # validate everything
```

7. Upload to GCS

Uploads files to GCS, tracking progress in `data/upload/<VERSION>/<dataset>.tsv`. Already-uploaded files (status OK) are skipped on re-run; failed uploads are retried automatically.

Use `-m N` to upload in batches of N files per dataset.

```
pixi run upload works
pixi run upload -m 5 works       # upload at most 5 new files
pixi run upload all              # upload everything
```

8. Load into BigQuery

Creates tables with versioned names (e.g. `works_20260210`).

```
pixi run bq works
pixi run bq works concepts
pixi run bq all              # load everything
```

9. Cleanup

Remove local data files for the current version:

```
pixi run cleanup
```

Move GCS files to archive storage class:

```
pixi run gcs-archive
```

## Notes
- Inverted Abstracts are converted to strings.
- Concepts.international is dropped because it was a headache to deal with.


Enjoy!
