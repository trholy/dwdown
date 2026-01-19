# Upload module
## OSUploader

### Overview

The `OSUploader` class handles uploading files to an Object Storage service. It supports verifying file integrity, checking for existing files, parallel uploading, and managing log files.

### Constructor

```python
OSUploader(
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
  - The local path to the files to be uploaded.
- `bucket_name` : `str`
  - The name of the bucket to upload files to.
- `secure` : `bool`, default=`True`
  - Whether to use secure connection.
- `log_files_path` : `str | None`, default=`None`
  - The path to store log files.
- `delay` : `int | float`, default=`1`
  - Optional delay between downloads (in seconds).
- `n_jobs` : `int`, default=`1`
  - Number of parallel jobs for uploading.
- `retry` : `int`, default=`0`
  - Number of retries for failed uploads.

### Methods

#### `_build_upload_list`

```python
_build_upload_list(
    local_files_with_hashes: dict[str, str],
    remote_prefix: str,
    existing_remote_files_with_hashes: dict[str, str] | None = None
)
```

Builds a list of files to upload.

#### Parameters

- `local_files_with_hashes` : `dict[str, str]`
  - Dictionary of local filenames and their hashes.
- `remote_prefix` : `str`
  - Prefix for the remote path.
- `existing_remote_files_with_hashes` : `dict[str, str] | None`, default=`None`
  - Dictionary of remote filenames and their hashes.

#### Returns

- List of tuples containing local and remote file paths.

#### `_upload_file`

```python
_upload_file(local_file_path: str, remote_path: str, local_md5: str) -> bool
```

Uploads a single file and checks for integrity.

#### Parameters

- `local_file_path` : `str`
  - The path to the local file.
- `remote_path` : `str`
  - The path to the remote file.
- `local_md5` : `str`
  - The MD5 hash of the local file.

#### Returns

- `bool`
  - True if the file was uploaded successfully, False otherwise.

#### `upload`

```python
upload(
    check_for_existence: bool = False,
    prefix: str | None = None,
    suffix: str | None = None,
    include_pattern: list[str] | None = None,
    exclude_pattern: list[str] | None = None,
    additional_patterns: dict | None = None,
    skip_time_step_filtering_variables: list[str] | None = None,
    variables: list[str] | None = None,
    min_timestep: str | int | None = None,
    max_timestep: str | int | None = None,
    remote_prefix: str = ""
) -> None
```

Uploads files from the local path to the specified bucket.

#### Parameters

- `check_for_existence` : `bool`, default=`False`
  - If True, checks if the file already exists in the download directory and skips the download if it does.
- `prefix` : `str | None`, default=`None`
  - Prefix for the folder in the bucket.
- `suffix` : `str | None`, default=`None`
  - The file extension to filter by.
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
- `min_timestep` : `str | int | None`, default=`None`
  - Minimum timestep to include.
- `max_timestep` : `str | int | None`, default=`None`
  - Maximum timestep to include.
- `remote_prefix` : `str`, default=`""`
  - Prefix for the folder in the bucket.

#### `delete`

```python
delete() -> None
```

Deletes local files after successful upload.
