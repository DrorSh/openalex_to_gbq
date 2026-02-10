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

4. Generate schemas

Uses [bigquery-schema-generator](https://github.com/bxparks/bigquery-schema-generator) to generate BQ schemas from the downloaded data.

```
pixi run schema authors
pixi run schema all          # generate all schemas
```

5. Convert files

Note: `venues` and `authors` do not require conversion and can be uploaded as is.

```
pixi run convert works
pixi run convert works concepts
pixi run convert all         # convert everything
```

6. Upload to GCS

```
pixi run upload works
pixi run upload works concepts
pixi run upload all          # upload everything
```

7. Load into BigQuery

Creates tables with versioned names (e.g. `works_20260210`).

```
pixi run bq works
pixi run bq works concepts
pixi run bq all              # load everything
```

8. Cleanup

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
