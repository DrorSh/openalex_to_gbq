# OpenAlex data to Google Bigquery

OpenAlex data require some modifications before it can be uploaded as columnar data to BQ. Namely, all hyphens in tag names need to be removed and missing arrays should be added. 

This repo provides dockerized node.js scripts and instructions to convert and upload the data.

## Instructions

1. Download the data from `AWS`. Follow isntructions on https://docs.openalex.org/download-snapshot/download-to-your-machine

```
aws s3 sync 's3://openalex' 'openalex-snapshot' --no-sign-request
```

2. Build Docker image and start a container

```
docker build -t node .
winpty docker run --name node -v /$PWD/proj:/proj --rm -ti node sh
```

3. Convert files

In each of the scripts provided, edit the `FOLDER` to correspond to the desired source and run the script from the container

Note: `venues` and `authors` do not require convertion and can be uploaded as is. 

```
node run_works
```

For the larger parts of the data this process takes several hours. (TODO: cycle through folders)

4. Copy files to google cloud storage

```
gsutil -m cp -r . gs://my-bucket/dest
```

5. Create the bigquery table

Use provided schema files (no need to upload to GCS)

```
bq load --source_format=NEWLINE_DELIMITED_JSON -project_id=<PROJID> --replace=true openalex.<TABLE> gs://my-bucket/dest* <LOCAL SCHEMA>
```

## Notes
- Inverted Abstracts are converted to strings.
- Concepts.international is dropped because it was a headache to deal with.


Enjoy!
