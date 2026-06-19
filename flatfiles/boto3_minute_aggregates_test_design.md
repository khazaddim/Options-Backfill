# Massive Flat Files Minute Aggregates Test Script Design

## Purpose

This document describes how to add a small Python test script for downloading a few Massive options minute-aggregate flat files with `boto3`.

This work is intentionally separate from the existing REST helper in this repository.

- `massive_options_helper.py` is built around Massive REST endpoints, JSON payloads, pagination, and API token authentication.
- Massive flat files use S3-compatible object storage, CSV or CSV.GZ files, and a separate access key plus secret key credential pair.

Because the transport, authentication model, and data delivery format are different, the first flat-file experiment should live in its own folder and remain isolated from the existing helper until the workflow is proven.

## Recommended Repo Layout

```text
Options-Backfill/
  flatfiles/
    boto3_minute_aggregates_test_design.md
    test_download_minute_aggregates.py
```

For this first pass, `flatfiles/` should be treated as an investigation area for S3-based bulk downloads. If the workflow proves useful, the code can later be moved into a packaged module.

## Scope

The initial script should do only four things:

1. Create an S3 client pointed at Massive's endpoint.
2. List a small set of objects under the options prefix.
3. Select one or a few minute-aggregate files.
4. Download them into a local test directory without touching PostgreSQL or the REST helper.

This first script should not try to ingest data into the existing storage layer. It is a connectivity and file-shape test only.

## Massive Flat File Model

Massive documents these S3 details:

- Endpoint: `https://files.massive.com`
- Bucket: `flatfiles`
- Options prefix: `us_options_opra`

The documented options flat-file datasets include:

- day aggregates
- minute aggregates
- trades
- quotes

For this experiment, the target dataset is minute aggregates.

## Credential Model

Flat files do not use the existing Massive REST API token.

The script should use a dedicated Access Key ID and Secret Access Key supplied by Massive for flat-file access. These credentials should not be committed into the repository.

### Recommended local credential source

Use environment variables for the first script:

- `MASSIVE_S3_ACCESS_KEY_ID`
- `MASSIVE_S3_SECRET_ACCESS_KEY`

Optional future extension:

- allow a JSON file path passed by argument
- allow a JSON key name for parity with the REST helper style

For the initial experiment, environment variables are simpler and safer.

## Dependency

The current project dependencies do not include `boto3`, so this script should assume an extra local install step:

```powershell
.\.venv\Scripts\python.exe -m pip install boto3
```

If the flat-file workflow becomes a permanent part of the repo, `boto3` can later be added deliberately to `pyproject.toml`.

## Design Goals For The Test Script

- keep the script standalone and easy to run from the repo root
- validate credentials and endpoint access early
- avoid broad recursive downloads
- download only a small sample at first
- preserve the original `.csv.gz` files without transforming them
- print enough metadata to confirm file naming and size

## Non-Goals

- no integration with PostgreSQL
- no merge with `massive_options_helper.py`
- no retry orchestration yet
- no async downloader yet
- no attempt to backfill all history
- no production-grade CLI surface

## Expected S3 Client Configuration

The script should create a boto3 session and then an S3 client with Massive's custom endpoint.

```python
import boto3
from botocore.config import Config

session = boto3.Session(
    aws_access_key_id=access_key_id,
    aws_secret_access_key=secret_access_key,
)

s3 = session.client(
    "s3",
    endpoint_url="https://files.massive.com",
    config=Config(signature_version="s3v4"),
)
```

Key points:

- the client type is still `s3`
- the endpoint is Massive's host, not the default AWS endpoint
- the signature version should be set to `s3v4`

## Prefix Discovery Strategy

The public docs confirm the top-level options prefix `us_options_opra`, but they do not fully prove the exact minute-aggregate object-key convention from the docs alone.

Because of that, the first script should list keys before trying to hardcode downloads.

### Recommended listing sequence

1. List `us_options_opra/`
2. Inspect the first-level dataset folders
3. Find the minute-aggregate folder name exactly as exposed to the account
4. List one year folder
5. List one month folder
6. Download one specific day file

This avoids guessing wrong on names such as:

- `minute_aggs_v1`
- `minute_aggregates_v1`
- `minute-aggregates_v1`

The script should treat the exact object key format as discovered data, not as an assumption.

## Proposed Test Script Behavior

### Inputs

The script should accept a few simple constants or command-line arguments:

- `prefix_root`: default `us_options_opra`
- `dataset_hint`: default `minute`
- `max_keys_to_show`: default `50`
- `download_count`: default `1`
- `output_dir`: default `flatfiles/downloads/minute_aggregates`

For a first version, constants at the top of the script are acceptable. A later version can switch to `argparse`.

### Output

The script should print:

- whether credentials were found
- the endpoint and bucket being used
- the discovered dataset-like prefixes
- the chosen object keys
- each file's size
- the final local download path

### Safe execution behavior

The script should stop early when:

- required environment variables are missing
- listing returns `403`, which likely means plan or prefix access is missing
- the minute-aggregate dataset prefix cannot be identified
- no files are found under the chosen date path

Failing fast is better than downloading a large unexpected tree.

## Proposed Script Structure

The script can stay as one file but should still have small functions.

```python
def read_credentials() -> tuple[str, str]:
    ...

def build_s3_client(access_key_id: str, secret_access_key: str):
    ...

def list_prefixes(s3, bucket: str, prefix: str) -> list[str]:
    ...

def find_minute_dataset_prefix(prefixes: list[str]) -> str:
    ...

def list_objects_under_prefix(s3, bucket: str, prefix: str, max_keys: int) -> list[dict]:
    ...

def choose_sample_files(objects: list[dict], download_count: int) -> list[str]:
    ...

def download_objects(s3, bucket: str, object_keys: list[str], output_dir: Path) -> None:
    ...

def main() -> int:
    ...
```

## Listing Implementation Notes

The script should use `list_objects_v2` pagination.

For folder discovery, use:

- `Prefix=...`
- `Delimiter="/"`

This makes S3 return `CommonPrefixes`, which is the closest thing to folders.

For example, listing the immediate children of `us_options_opra/` should reveal dataset directories without pulling every object in the tree.

## Download Implementation Notes

Downloaded files should keep their original filenames.

If an object key is:

```text
us_options_opra/.../2025-11-05.csv.gz
```

the local file should be saved as:

```text
flatfiles/downloads/minute_aggregates/2025-11-05.csv.gz
```

The script should create the output directory if it does not exist.

It should not automatically decompress files. Keeping the raw `.csv.gz` is better for inspection and reproducibility.

## Suggested Minimal Workflow

### Step 1: install boto3 locally

```powershell
.\.venv\Scripts\python.exe -m pip install boto3
```

### Step 2: export credentials into the shell

```powershell
$env:MASSIVE_S3_ACCESS_KEY_ID = "YOUR_ACCESS_KEY_ID"
$env:MASSIVE_S3_SECRET_ACCESS_KEY = "YOUR_SECRET_ACCESS_KEY"
```

### Step 3: run the test script

```powershell
.\.venv\Scripts\python.exe .\flatfiles\test_download_minute_aggregates.py
```

## Suggested First Script Logic

The first execution path should be conservative:

1. Read credentials from environment variables.
2. Build the boto3 S3 client.
3. List immediate child prefixes under `us_options_opra/`.
4. Pick the prefix that clearly matches minute aggregates.
5. List a small number of objects below that prefix.
6. Choose the first one or first few `.csv.gz` files.
7. Download those files to `flatfiles/downloads/minute_aggregates/`.
8. Print filenames and byte sizes.

This is enough to validate:

- the credentials work
- the account has access to options minute aggregates
- the actual key naming convention
- the approximate file sizes
- whether the files are shaped as expected for later ingestion

## Error Handling Expectations

The first script only needs basic error handling.

### Handle these cases explicitly

- missing environment variables
- `ClientError` with `403 Forbidden`
- `ClientError` with `404 Not Found`
- empty `Contents` or empty `CommonPrefixes`
- filesystem write errors

### Do not over-engineer yet

The script does not need:

- retries
- exponential backoff
- concurrency
- resumable downloads
- checksum tracking

Those concerns belong in a later production downloader.

## Verification Checklist

The script should be considered successful if it can prove all of the following:

- the Massive S3 credentials authenticate successfully
- the account can list under `us_options_opra/`
- the minute-aggregate dataset prefix can be identified
- at least one `.csv.gz` file downloads locally
- the resulting file can be inspected manually with pandas or gzip tools later

## Follow-On Work After The Test Script

If the test succeeds, the next likely steps are:

1. add `argparse` for selecting prefix, date, and output directory
2. add a second script to inspect a downloaded `.csv.gz` with pandas
3. confirm the exact schema for options minute aggregates on real files
4. decide whether flat-file ingestion should feed the existing PostgreSQL storage layer directly
5. decide whether a packaged module should be created under `options_backfill/flatfiles/`

## Recommendation

Do not blend flat-file logic into `massive_options_helper.py` yet.

The first practical milestone should be a disposable but clean script in `flatfiles/test_download_minute_aggregates.py` that proves:

- the credential flow
- the real object-key layout
- the size and shape of a small sample of options minute-aggregate files

Once that is confirmed, the repo can make a better architectural decision about whether flat files become a production ingestion path.