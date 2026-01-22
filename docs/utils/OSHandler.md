# Utils module
## OSHandler

### Overview

The `OSHandler` class handles operations related to object storage, such as bucket management, file fetching, filtering, and integrity verification.

### Constructor

```python
OSHandler(
    log_handler: LogHandler,
    client: Minio,
    filehandler: FileHandler
)
```

#### Parameters

- `log_handler` : `LogHandler`
  - LogHandler instance.
- `client` : `Minio`
  - Minio client instance.
- `filehandler` : `FileHandler`
  - FileHandler instance.

### Methods

#### `_ensure_bucket`

```python
_ensure_bucket(bucket_name: str, create_if_not_exists: bool = False) -> None
```

Ensures that the specified bucket exists, creating it if necessary.

#### Parameters

- `bucket_name` : `str`
  - The name of the bucket to ensure.
- `create_if_not_exists` : `bool`, default=`False`
  - Whether to create the bucket if it does not exist.

#### `_fetch_existing_files`

```python
_fetch_existing_files(
    bucket_name: str,
    remote_prefix: str,
    return_basename: bool = False
) -> dict[str, str]
```

Fetches existing files from the specified bucket and prefix.

#### Parameters

- `bucket_name` : `str`
  - The name of the bucket.
- `remote_prefix` : `str`
  - The prefix to filter objects by.
- `return_basename` : `bool`, default=`False`
  - Whether to return only the basename of the file.

#### Returns

- `dict[str, str]`
  - A dictionary of object names and their ETags.

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

#### `_verify_file_integrity`

```python
_verify_file_integrity(
    local_file_path: str | None = None,
    remote_path: str | None = None,
    bucket_name: str | None = None,
    remote_hash: str | None = None,
    local_md5: str | None = None
) -> bool
```

Verifies the integrity of a local file against a remote file using MD5 hash.

#### Parameters

- `local_file_path` : `str | None`, default=`None`
  - The path to the local file.
- `remote_path` : `str | None`, default=`None`
  - The path to the remote file.
- `bucket_name` : `str | None`, default=`None`
  - The name of the bucket.
- `remote_hash` : `str | None`, default=`None`
  - The ETag of the remote file.
- `local_md5` : `str | None`, default=`None`
  - The MD5 hash of the local file.

#### Returns

- `bool`
  - True if the files match, False otherwise.
