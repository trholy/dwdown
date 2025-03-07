# Upload module
## MinioUploader

### Overview

The `MinioUploader` class is designed to facilitate the uploading of files to a MinIO server. It supports parallel uploading, integrity checks using MD5 hashes, and logging of upload activities. The class ensures that the specified bucket exists and provides methods to upload files recursively from a specified directory.

### Constructor

```python
MinioUploader(endpoint: str, access_key: str, secret_key: str, files_path: str, bucket_name: str = 'my-bucket', secure: bool = False, log_uploads: bool = True, log_files_path: str = "log_files_MinioUploader", workers: int = 1)
```

#### Parameters

- `endpoint` : `str`
  - The MinIO server endpoint.
- `access_key` : `str`
  - Access key for authentication.
- `secret_key` : `str`
  - Secret key for authentication.
- `files_path` : `str`
  - Directory containing files to upload.
- `bucket_name` : `str`, default=`'my-bucket'`
  - Name of the bucket to upload files to.
- `secure` : `bool`, default=`False`
  - If `True`, use HTTPS to connect to the MinIO server.
- `log_uploads` : `bool`, default=`True`
  - If `True`, log upload activities.
- `log_files_path` : `str`, default=`"log_files_MinioUploader"`
  - Directory to save log files.
- `workers` : `int`, default=`1`
  - Number of worker threads for parallel uploading.

### Methods

#### `_ensure_directory_exists`

```python
@staticmethod
_ensure_directory_exists(path: str) -> None
```

Helper function to ensure a directory exists, creates if not.

#### `_ensure_bucket`

```python
_ensure_bucket() -> None
```

Ensure the bucket exists, or create it if necessary.

#### `calculate_md5`

```python
@staticmethod
calculate_md5(file_path: str) -> str
```

Computes the MD5 hash of a file.

#### Parameters

- `file_path` : `str`
  - Path to the file for which to compute the MD5 hash.

#### Returns

- `str`
  - MD5 hash of the file.

#### `_get_current_date`

```python
@staticmethod
_get_current_date() -> str
```

Get the current system date, formatted as "DD-MMM-YYYY-HH-MM".

#### Returns

- `str`
  - Current date in formatted datetime format.

#### `upload_directory`

```python
upload_directory(remote_prefix: str = "", check_for_existence: bool = False) -> None
```

Recursively uploads a directory to MinIO with real-time logging.

#### Parameters

- `remote_prefix` : `str`, default=`""`
  - Prefix to use for remote paths in the bucket.
- `check_for_existence` : `bool`, default=`False`
  - If `True`, skip uploading files that already exist in the bucket with the same MD5 hash.

#### `_fetch_existing_files`

```python
_fetch_existing_files(remote_prefix: str) -> dict
```

Fetches existing files in the bucket with their ETags. Returns a dictionary: {remote_path: etag}

#### Parameters

- `remote_prefix` : `str`
  - Prefix to use for remote paths in the bucket.

#### Returns

- `dict`
  - Dictionary containing remote paths and their ETags.

#### `_upload_file`

```python
_upload_file(local_file_path: str, remote_path: str, check_for_existence: bool, existing_files: dict) -> bool
```

Uploads a single file with immediate logging.

#### Parameters

- `local_file_path` : `str`
  - Local path of the file to upload.
- `remote_path` : `str`
  - Remote path to save the file in the bucket.
- `check_for_existence` : `bool`
  - If `True`, skip uploading if the file already exists in the bucket with the same MD5 hash.
- `existing_files` : `dict`
  - Dictionary containing existing files and their ETags.

#### Returns

- `bool`
  - `True` if the file was successfully uploaded and verified, else `False`.

#### `delete_local_files`

```python
delete_local_files() -> None
```

Deletes local files after successful upload verification.

#### `_write_log_file`

```python
_write_log_file(filename: str, data: list) -> None
```

Writes a list of file links to a log file, ensuring each entry is on a new line.

#### Parameters

- `filename` : `str`
  - Path to the log file.
- `data` : `list`
  - List of file links to write to the log file.

---
