# Download module
## MinioDownloader

### Overview

The `MinioDownloader` class is designed to facilitate the downloading of files from a MinIO server. It supports parallel downloading, integrity checks using MD5 hashes, and logging of download activities. The class ensures that the specified bucket exists and provides methods to download files recursively from a specified bucket or folder.

### Constructor

```python
MinioDownloader(endpoint: str, access_key: str, secret_key: str, files_path: str, secure: bool = False, log_downloads: bool = True, log_files_path: str = "log_files_MinioDownloader", workers: int = 1)
```

#### Parameters

- `endpoint` : `str`
  - The MinIO server endpoint.
- `access_key` : `str`
  - Access key for authentication.
- `secret_key` : `str`
  - Secret key for authentication.
- `files_path` : `str`
  - Directory to save downloaded files.
- `secure` : `bool`, default=`False`
  - If `True`, use HTTPS to connect to the MinIO server.
- `log_downloads` : `bool`, default=`True`
  - If `True`, log download activities.
- `log_files_path` : `str`, default=`"log_files_MinioDownloader"`
  - Directory to save log files.
- `workers` : `int`, default=`1`
  - Number of worker threads for parallel downloading.

### Methods

#### `_ensure_directory_exists`

```python
@staticmethod
_ensure_directory_exists(path: str) -> None
```

Helper function to ensure a directory exists, creates if not.

#### `_ensure_bucket`

```python
_ensure_bucket(bucket_name: str) -> None
```

Ensure the bucket exists.

#### Parameters

- `bucket_name` : `str`
  - Name of the bucket to check.

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

#### `_get_remote_files`

```python
_get_remote_files(bucket_name: str, folder_prefix: str) -> list
```

Retrieve a list of remote files from MinIO.

#### Parameters

- `bucket_name` : `str`
  - Name of the bucket to retrieve files from.
- `folder_prefix` : `str`
  - Prefix of the folder to retrieve files from.

#### Returns

- `list`
  - List of remote files.

#### `download_bucket`

```python
download_bucket(bucket_name: str, folder_prefix: str | None = None) -> None
```

Recursively downloads a bucket or folder from MinIO.

#### Parameters

- `bucket_name` : `str`
  - Name of the bucket to download from.
- `folder_prefix` : `str | None`, default=`None`
  - Prefix of the folder to download from. If `None`, downloads the entire bucket.

#### `_download_file`

```python
_download_file(bucket_name: str, local_file_path: str, remote_path: str) -> bool
```

Downloads a single file with immediate logging and integrity check.

#### Parameters

- `bucket_name` : `str`
  - Name of the bucket to download from.
- `local_file_path` : `str`
  - Local path to save the downloaded file.
- `remote_path` : `str`
  - Remote path of the file to download.

#### Returns

- `bool`
  - `True` if the file was successfully downloaded and verified, else `False`.

#### `_verify_file_integrity`

```python
_verify_file_integrity(bucket_name: str, local_file_path: str, remote_path: str) -> bool
```

Verifies if the local file matches the remote file's checksum.

#### Parameters

- `bucket_name` : `str`
  - Name of the bucket.
- `local_file_path` : `str`
  - Local path of the file.
- `remote_path` : `str`
  - Remote path of the file.

#### Returns

- `bool`
  - `True` if the local file matches the remote file's checksum, else `False`.

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
