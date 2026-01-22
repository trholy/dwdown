# Download module
## OSDownloader

### Overview

The `OSDownloader` class handles downloading files from an Object Storage service (e.g., S3-compatible). It supports secure connections, verifying file integrity using hashes, parallel downloading, and resuming processing by checking for existing files.

### Constructor

```python
OSDownloader(
    endpoint: str,
    access_key: str,
    secret_key: str,
    files_path: str,
    bucket_name: str,
    secure: bool = True,
    log_files_path: str | None = None,
    delay: int | float = 1,
    n_jobs: int = 1,
    retry: int = 0
)
```

#### Parameters

- `endpoint` : `str`
  - The endpoint of the storage service.
- `access_key` : `str`
  - The access key for authentication.
- `secret_key` : `str`
  - The secret key for authentication.
- `files_path` : `str`
  - The local path to the files to be downloaded.
- `bucket_name` : `str`
  - The name of the bucket to download files from.
- `secure` : `bool`, default=`True`
  - Whether to use secure connection (HTTPS).
- `log_files_path` : `str | None`, default=`None`
  - The path to store log files.
- `delay` : `int | float`, default=`1`
  - Optional delay between downloads (in seconds).
- `n_jobs` : `int`, default=`1`
  - Number of parallel jobs for downloading.
- `retry` : `int`, default=`0`
  - Number of retries for failed downloads.

### Methods

#### `_build_download_list`

```python
_build_download_list(filtered_remote_files: dict[str, str]) -> list[tuple[str, str, str]]
```

Builds a list of files to download, skipping those that already exist and match the remote hash.

#### Parameters

- `filtered_remote_files` : `dict[str, str]`
  - Dictionary of remote file paths and their ETags.

#### Returns

- `list[tuple[str, str, str]]`
  - A list of tuples (local_file_path, remote_path, remote_hash).

#### `_download_file`

```python
_download_file(local_file_path: str, remote_path: str, check_for_existence: bool, remote_hash: str) -> bool
```

Downloads a single file with immediate logging and integrity check.

#### Parameters

- `local_file_path` : `str`
  - The path to the local file.
- `remote_path` : `str`
  - The path to the remote file.
- `check_for_existence` : `bool`
  - Whether to check for existing files.
- `remote_hash` : `str`
  - The ETag of the remote file for integrity verification.

#### Returns

- `bool`
  - True if the file was downloaded successfully, False otherwise.

#### `_log_summary`

```python
_log_summary() -> None
```

Logs the summary of the download process, including successful and corrupted files.

#### `download`

```python
download(
    check_for_existence: bool = False,
    remote_prefix: str | None = None,
    suffix: str | None = None,
    min_timestep: str | int | None = None,
    max_timestep: str | int | None = None,
    include_pattern: list[str] | None = None,
    exclude_pattern: list[str] | None = None,
    additional_patterns: dict | None = None,
    skip_time_step_filtering_variables: list[str] | None = None,
    variables: list[str] | None = None
) -> None
```

Downloads files from a specified bucket based on given criteria.

#### Parameters

- `check_for_existence` : `bool`, default=`False`
  - If True, checks if the file already exists in the download directory and skips the download if it does.
- `remote_prefix` : `str | None`, default=`None`
  - Prefix for the folder in the bucket.
- `suffix` : `str | None`, default=`None`
  - The file extension to filter by.
- `min_timestep` : `str | int | None`, default=`None`
  - Minimum timestep to include.
- `max_timestep` : `str | int | None`, default=`None`
  - Maximum timestep to include.
- `include_pattern` : `list[str] | None`, default=`None`
  - List of patterns to include.
- `exclude_pattern` : `list[str] | None`, default=`None`
  - List of patterns to exclude.
- `additional_patterns` : `dict | None`, default=`None`
  - Additional patterns for filtering.
- `skip_time_step_filtering_variables` : `list[str] | None`, default=`None`
  - Variables to skip timestep filtering.
- `variables` : `list[str] | None`, default=`None`
  - List of variables to filter by.

#### `_count_existing_files`

```python
_count_existing_files(
    bucket_name: str,
    remote_prefix: str | None = None
) -> int
```

Counts existing objects in the specified bucket under a given prefix.

#### Parameters

- `bucket_name` : `str`
  - The name of the bucket.
-`remote_prefix` : `str | None`, default=`None`
  - The prefix to filter objects by. If `None`, all objects in the bucket are counted.

#### Returns

- `int`
  -  The number of objects found under the specified prefix.

#### Raises

- `S3Error`
  - If there is an error while fetching or listing objects from the bucket.

#### `delete`

```python
delete() -> None
```

Deletes local files after successful download.
