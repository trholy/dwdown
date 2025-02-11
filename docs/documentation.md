# dwdown Documentation

## DWDDownloader

### Overview

The `DWDDownloader` class is designed to facilitate the downloading of files from a specified URL. It supports parallel downloading, retry mechanisms for failed downloads, and logging of download activities. The class uses XPath expressions to parse filenames and dates from the HTML content of the URL.

### Constructor

```python
DWDDownloader(url: str, restart_failed_downloads: bool = False, log_downloads: bool = True, delay: int | float | None = None, workers: int = 1, download_path: str = "downloaded_files", log_files_path: str = "log_files_DWDDownloader", xpath_files: str = "/html/body/pre//a/@href", xpath_dates: str = "//pre/text()")
```

#### Parameters

- `url` : `str`
  - Base URL to fetch data from.
- `restart_failed_downloads` : `bool`, default=`False`
  - If `True`, retry failed downloads sequentially.
- `log_downloads` : `bool`, default=`True`
  - If `True`, log download activities.
- `delay` : `int | float | None`, default=`None`
  - Optional delay between downloads (in seconds).
- `workers` : `int`, default=`1`
  - Number of worker threads for parallel downloading.
- `download_path` : `str`, default=`"downloaded_files"`
  - Directory to save downloaded files.
- `log_files_path` : `str`, default=`"log_files_DWDDownloader"`
  - Directory to save log files.
- `xpath_files` : `str`, default=`"/html/body/pre//a/@href"`
  - XPath expression to extract filenames from the HTML.
- `xpath_dates` : `str`, default=`"//pre/text()"`
  - XPath expression to extract date strings from the HTML.

### Methods

#### `_ensure_directory_exists`

```python
@staticmethod
_ensure_directory_exists(path: str) -> None
```

Helper function to ensure a directory exists, creates if not.

#### `_fix_date_format`

```python
@staticmethod
_fix_date_format(dates: list[str]) -> list[str]
```

Cleans and formats date strings by:
1. Adding space between a number and a letter (e.g., "2025Jan" → "2025 Jan").
2. Replacing space with "-" between two numbers (e.g., "2025 10:20" → "2025-10:20").
3. Removing trailing numbers if preceded by two or more spaces.

#### `_parse_dates`

```python
@staticmethod
_parse_dates(date_strings: list[str]) -> list[datetime]
```

Converts a list of date strings into datetime objects. Expected format: '21-Jan-2025-10:20' → datetime(2025, 1, 21, 10, 20).

#### `get_data_dates`

```python
get_data_dates(url: str | None = None) -> tuple[datetime, datetime]
```

Fetches and processes date strings from a given URL.

#### Parameters

- `url` : `str | None`, default=`None`
  - URL to fetch date strings from. If `None`, uses the URL provided during initialization.

#### Returns

- `tuple[datetime, datetime]`
  - Minimum and maximum date from the URL.

#### `get_current_date`

```python
@staticmethod
get_current_date(utc: bool = True, time_of_day: bool = False) -> datetime
```

Get the current system date, formatted as "DD-MMM-YYYY-HH:MM".

#### Parameters

- `utc` : `bool`, default=`True`
  - If `True`, return date with UTC time; otherwise, return system time.
- `time_of_day` : `bool`, default=`False`
  - If `True`, return date with time; otherwise, return only the date.

#### Returns

- `datetime`
  - Current date (with or without time) in formatted datetime format.

#### `_get_filenames_from_url`

```python
_get_filenames_from_url() -> list[str]
```

Fetches the list of filenames from the given URL by parsing the HTML.

#### Returns

- `list[str]`
  - A list of filenames (URLs).

#### `_filter_file_names`

```python
@staticmethod
_filter_file_names(filenames: list[str], name_startswith: str = "icon-d2_germany", name_endswith: str = ".bz2", include_pattern: list[str] | None = None, exclude_pattern: list[str] | None = None) -> list[str]
```

Filters the list of filenames based on the given start and end patterns and inclusion/exclusion patterns.

#### Parameters

- `filenames` : `list[str]`
  - List of filenames to filter.
- `name_startswith` : `str`, default=`"icon-d2_germany"`
  - String that filenames must start with.
- `name_endswith` : `str`, default=`".bz2"`
  - String that filenames must end with.
- `include_pattern` : `list[str] | None`, default=`None`
  - List of substrings; at least one must be in the filename.
- `exclude_pattern` : `list[str] | None`, default=`None`
  - List of substrings; filenames with any of these are excluded.

#### Returns

- `list[str]`
  - Filtered list of filenames.

#### `_generate_links`

```python
_generate_links(filtered_filenames: list[str]) -> list[str]
```

Generates full URLs from the list of filtered filenames.

#### Parameters

- `filtered_filenames` : `list[str]`
  - List of filtered filenames.

#### Returns

- `list[str]`
  - List of full URLs.

#### `_process_timestamps`

```python
@staticmethod
_process_timestamps(min_timestamp: str | int | None = None, max_timestamp: str | int | None = None, include_pattern: list | None = None) -> list[str]
```

Generates a list of formatted timestamp patterns within a given range.

#### Parameters

- `min_timestamp` : `str | int | None`, default=`None`
  - The minimum timestamp value (default is 0 if `None`).
- `max_timestamp` : `str | int | None`, default=`None`
  - The maximum timestamp value (default is 48 if `None`).
- `include_pattern` : `list | None`, default=`None`
  - A list to which the generated timestamp patterns will be added. If `None`, a new list is created.

#### Returns

- `list[str]`
  - A list of formatted timestamp patterns (e.g., "_000_", "_001_", ..., "_048_").

#### `get_links`

```python
get_links(name_startswith: str = "icon-d2_germany", name_endswith: str = ".bz2", include_pattern: list[str] | None = None, exclude_pattern: list[str] | None = None, min_timestamp: str | int | None = None, max_timestamp: str | int | None = None) -> list[str]
```

Main method to get all the download links after filtering filenames.

#### Parameters

- `name_startswith` : `str`, default=`"icon-d2_germany"`
  - String that filenames must start with.
- `name_endswith` : `str`, default=`".bz2"`
  - String that filenames must end with.
- `include_pattern` : `list[str] | None`, default=`None`
  - List of substrings; at least one must be in the filename.
- `exclude_pattern` : `list[str] | None`, default=`None`
  - List of substrings; filenames with any of these are excluded.
- `min_timestamp` : `str | int | None`, default=`None`
  - The minimum timestamp value.
- `max_timestamp` : `str | int | None`, default=`None`
  - The maximum timestamp value.

#### Returns

- `list[str]`
  - List of full download URLs.

#### `_download_file`

```python
_download_file(link: str, check_for_existence: bool) -> bool
```

Downloads a single file from the provided URL if it does not already exist.

#### Parameters

- `link` : `str`
  - The URL to download the file from.
- `check_for_existence` : `bool`
  - If `True`, skips download if the file already exists.

#### Returns

- `bool`
  - `True` if the file was successfully downloaded or already exists, else `False`.

#### `download_files`

```python
download_files(check_for_existence: bool = False) -> None
```

Downloads all files from the generated links using concurrency for faster processing. If downloads fail and `self.restart_failed_downloads` is enabled, retry them sequentially. Finally failed downloads are stored in `self.finally_failed_files`.

#### Parameters

- `check_for_existence` : `bool`, default=`False`
  - If `True`, skips download if the file already exists.

#### `_log_name_formatting`

```python
_log_name_formatting(link: str) -> tuple[str, str]
```

#### `_get_variable_from_link`

```python
@staticmethod
_get_variable_from_link(link: str) -> str
```

#### `get_formatted_time_stamp`

```python
@staticmethod
get_formatted_time_stamp(time_stamp: datetime) -> str
```

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

## DataProcessor

### Overview

The `DataProcessor` class is designed to handle the processing of GRIB files. It includes functionalities to decompress `.bz2` files, convert GRIB files to CSV format, and apply geographic filtering if required. The class ensures that necessary directories exist and provides methods to search for files, decompress them, and convert them to CSV.

### Constructor

```python
DataProcessor(search_path: str, extraction_path: str, converted_files_path: str)
```

#### Parameters

- `search_path` : `str`
  - Directory to search for GRIB files.
- `extraction_path` : `str`
  - Directory to save decompressed files.
- `converted_files_path` : `str`
  - Directory to save converted CSV files.

### Methods

#### `_ensure_directory_exists`

```python
@staticmethod
_ensure_directory_exists(directory: str) -> None
```

Ensures that the directory exists. If not, it creates the directory.

#### Parameters

- `directory` : `str`
  - Path to the directory to ensure.

#### `_decompress_files`

```python
_decompress_files(file_to_decompress: str) -> str
```

Decompresses a `.bz2` file and returns the path to the decompressed file.

#### Parameters

- `file_to_decompress` : `str`
  - Path to the `.bz2` file to decompress.

#### Returns

- `str`
  - Path to the decompressed file.

#### `_get_decompressed_file_path`

```python
_get_decompressed_file_path(file_to_decompress: str) -> str
```

Generates the path for the decompressed file.

#### Parameters

- `file_to_decompress` : `str`
  - Path to the `.bz2` file to decompress.

#### Returns

- `str`
  - Path to the decompressed file.

#### `_read_grib_to_dataframe`

```python
_read_grib_to_dataframe(decompressed_file_path: str, apply_geo_filtering: bool, start_lat: float | None, end_lat: float | None, start_lon: float | None, end_lon: float | None) -> None
```

Converts the decompressed GRIB file to a DataFrame and saves it as a CSV file.

#### Parameters

- `decompressed_file_path` : `str`
  - Path to the decompressed GRIB file.
- `apply_geo_filtering` : `bool`
  - Whether to apply geographic filtering.
- `start_lat` : `float | None`
  - Minimum latitude for filtering.
- `end_lat` : `float | None`
  - Maximum latitude for filtering.
- `start_lon` : `float | None`
  - Minimum longitude for filtering.
- `end_lon` : `float | None`
  - Maximum longitude for filtering.

#### `_filter_by_coordinates`

```python
@staticmethod
_filter_by_coordinates(df: pd.DataFrame, start_lat: float, end_lat: float, start_lon: float, end_lon: float) -> pd.DataFrame
```

Filters the DataFrame to include only rows within the given latitude and longitude range.

#### Parameters

- `df` : `pd.DataFrame`
  - Input DataFrame.
- `start_lat` : `float`
  - Minimum latitude.
- `end_lat` : `float`
  - Maximum latitude.
- `start_lon` : `float`
  - Minimum longitude.
- `end_lon` : `float`
  - Maximum longitude.

#### Returns

- `pd.DataFrame`
  - Filtered DataFrame.

#### `_save_as_csv`

```python
@staticmethod
_save_as_csv(df: pd.DataFrame, csv_file_path: str) -> None
```

Saves a DataFrame as a CSV file.

#### Parameters

- `df` : `pd.DataFrame`
  - DataFrame to save.
- `csv_file_path` : `str`
  - Path to save the CSV file.

#### `_get_converted_file_path`

```python
_get_converted_file_path(decompressed_file_path: str) -> str
```

Generates the path for the CSV file based on the decompressed file path.

#### Parameters

- `decompressed_file_path` : `str`
  - Path to the decompressed GRIB file.

#### Returns

- `str`
  - Path to the CSV file.

#### `flatten_list`

```python
flatten_list(nested_list: list | str) -> list[str]
```

Recursively flattens a nested list of filenames.

#### Parameters

- `nested_list` : `list | str`
  - Nested list or string to flatten.

#### Returns

- `list[str]`
  - Flattened list of filenames.

#### `_search_directory`

```python
_search_directory(directory: str, include_pattern: list[str], exclude_pattern: list[str], name_startswith: str, name_endswith: str) -> list[str]
```

Recursively searches for matching files in the given directory.

#### Parameters

- `directory` : `str`
  - Directory to search.
- `include_pattern` : `list[str]`
  - List of substrings; at least one must be in the filename.
- `exclude_pattern` : `list[str]`
  - List of substrings; filenames with any of these are excluded.
- `name_startswith` : `str`
  - String that filenames must start with.
- `name_endswith` : `str`
  - String that filenames must end with.

#### Returns

- `list[str]`
  - List of matching filenames.

#### `get_filenames`

```python
get_filenames(name_startswith: str = "", name_endswith: str = "", include_pattern: list[str] | None = None, exclude_pattern: list[str] | None = None) -> list
```

Searches for files in the search path and includes subdirectory search if needed.

#### Parameters

- `name_startswith` : `str`, default=`""`
  - String that filenames must start with.
- `name_endswith` : `str`, default=`""`
  - String that filenames must end with.
- `include_pattern` : `list[str] | None`, default=`None`
  - List of substrings; at least one must be in the filename.
- `exclude_pattern` : `list[str] | None`, default=`None`
  - List of substrings; filenames with any of these are excluded.

#### Returns

- `list`
  - Sorted, filtered list of filenames.

#### `get_csv`

```python
get_csv(file_names: list[str], apply_geo_filtering: bool = False, start_lat: float | None = None, end_lat: float | None = None, start_lon: float | None = None, end_lon: float | None = None) -> None
```

Processes GRIB files: decompresses and converts them to CSV.

#### Parameters

- `file_names` : `list[str]`
  - List of file names to process.
- `apply_geo_filtering` : `bool`, default=`False`
  - Whether to filter data based on geographic coordinates.
- `start_lat` : `float | None`, default=`None`
  - Minimum latitude for filtering.
- `end_lat` : `float | None`, default=`None`
  - Maximum latitude for filtering.
- `start_lon` : `float | None`, default=`None`
  - Minimum longitude for filtering.
- `end_lon` : `float | None`, default=`None`
  - Maximum longitude for filtering.

#### Returns

- `None`

---

## DataEditor

### Overview

The `DataEditor` class is designed to handle the processing and merging of CSV files. It includes functionalities to validate columns, filter DataFrames, parse datetime columns, merge multiple DataFrames, and handle variable mappings. The class ensures that necessary columns exist and provides methods to read, filter, and merge CSV files based on specified criteria.

### Constructor

```python
DataEditor(files_path: str, required_columns: set[str] | None = None, join_method: str = 'inner', sep: str = ',', index_col: str | None = None, mapping_dictionary: dict[str, str] | None = None, additional_pattern_selection: dict[str, list[int] | int] | None = None)
```

#### Parameters

- `files_path` : `str`
  - Root path of the CSV files.
- `required_columns` : `set[str] | None`, default=`None`
  - Required columns in DataFrame. Defaults to `{'latitude', 'longitude', 'valid_time'}`.
- `join_method` : `str`, default=`'inner'`
  - Method to use for merging DataFrames.
- `sep` : `str`, default=`,``
  - CSV separator.
- `index_col` : `str | None`, default=`None`
  - Column to be used as index.
- `mapping_dictionary` : `dict[str, str] | None`, default=`None`
  - Dictionary mapping variable names to actual CSV column names.
- `additional_pattern_selection` : `dict[str, list[int] | int] | None`, default=`None`
  - Dictionary specifying additional patterns for known variables.

### Methods

#### `_validate_columns_exist`

```python
_validate_columns_exist(df: pd.DataFrame, required_columns: set[str], variable: str) -> bool
```

Validates that the required columns exist in the DataFrame.

#### Parameters

- `df` : `pd.DataFrame`
  - DataFrame to validate.
- `required_columns` : `set[str]`
  - Required columns in DataFrame.
- `variable` : `str`
  - Variable name being processed.

#### Returns

- `bool`
  - `True` if all required columns exist, else `False`.

#### `_filter_dataframe`

```python
@staticmethod
_filter_dataframe(df: pd.DataFrame, required_columns: set, variable: str) -> pd.DataFrame
```

Filters the DataFrame to keep only required columns.

#### Parameters

- `df` : `pd.DataFrame`
  - DataFrame to filter.
- `required_columns` : `set`
  - Required columns to keep.
- `variable` : `str`
  - Variable name being processed.

#### Returns

- `pd.DataFrame`
  - Filtered DataFrame.

#### `_parse_datetime`

```python
@staticmethod
_parse_datetime(df: pd.DataFrame) -> pd.DataFrame
```

Attempts to parse the 'valid_time' column to datetime.

#### Parameters

- `df` : `pd.DataFrame`
  - DataFrame containing the 'valid_time' column.

#### Returns

- `pd.DataFrame`
  - DataFrame with the parsed 'valid_time' column.

#### `_merge_dataframes`

```python
_merge_dataframes(df1: pd.DataFrame, df2: pd.DataFrame, merge_on: set[str]) -> pd.DataFrame
```

Merges two DataFrames based on specified columns.

#### Parameters

- `df1` : `pd.DataFrame`
  - First DataFrame to merge.
- `df2` : `pd.DataFrame`
  - Second DataFrame to merge.
- `merge_on` : `set[str]`
  - Columns to merge on.

#### Returns

- `pd.DataFrame`
  - Merged DataFrame.

#### `_get_csv_file`

```python
_get_csv_file(variable: str) -> list[str]
```

Function to get CSV file path for a variable.

#### Parameters

- `variable` : `str`
  - Variable name to search for.

#### Returns

- `list[str]`
  - List of CSV file paths for the variable.

#### `_filter_file_names`

```python
_filter_file_names(filenames: list[str], name_startswith: str = "icon-d2_germany", name_endswith: str = ".csv", include_pattern: str | list[str] | None = None, exclude_pattern: list[str] | None = None, variable: str | None = None) -> list[str] | None
```

Filters filenames based on start, end, inclusion, exclusion patterns, and additional pattern selection.

#### Parameters

- `filenames` : `list[str]`
  - List of filenames to filter.
- `name_startswith` : `str`, default=`"icon-d2_germany"`
  - String that filenames must start with.
- `name_endswith` : `str`, default=`".csv"`
  - String that filenames must end with.
- `include_pattern` : `str | list[str] | None`, default=`None`
  - A substring or a list of substrings; at least one must be in the filename.
- `exclude_pattern` : `list[str] | None`, default=`None`
  - List of substrings; filenames with any of these are excluded.
- `variable` : `str | None`, default=`None`
  - Variable name being processed.

#### Returns

- `list[str] | None`
  - The selected filename(s) or `None` if no match is found.

#### `_variable_mapping`

```python
_variable_mapping(variables: list[str]) -> list[str]
```

Maps manual variable names to actual CSV column names. Maintains the original order of variables.

#### Parameters

- `variables` : `list[str]`
  - List of variable names to map.

#### Returns

- `list[str]`
  - List of mapped variable names.

#### `_read_dataframe_from_csv`

```python
_read_dataframe_from_csv(csv_file: str) -> pd.DataFrame | None
```

Reads a CSV file into a DataFrame with error handling.

#### Parameters

- `csv_file` : `str`
  - Path to the CSV file.

#### Returns

- `pd.DataFrame | None`
  - DataFrame if successful, else `None`.

#### `merge_dfs`

```python
merge_dfs(time_step: str | int, variables: list[str], required_columns: set[str] | None = None) -> pd.DataFrame | None
```

Merges multiple CSV files into a DataFrame based on shared columns.

#### Parameters

- `time_step` : `str | int`
  - Time step identifier.
- `variables` : `list[str]`
  - List of variable names to merge.
- `required_columns` : `set[str] | None`, default=`None`
  - Required columns in DataFrame.

#### Returns

- `pd.DataFrame | None`
  - Merged DataFrame if successful, else `None`.

#### `_extract_additional_pattern`

```python
@staticmethod
_extract_additional_pattern(filename: str) -> int | None
```

Extracts the additional pattern from a filename. Returns an integer if a valid pattern exists; otherwise, `None`.

#### Parameters

- `filename` : `str`
  - Filename to extract the pattern from.

#### Returns

- `int | None`
  - Extracted pattern as an integer or `None` if no pattern is found.

---

### Helper Functions

#### `get_formatted_time_stamp`

```python
get_formatted_time_stamp(date: datetime) -> str
```

Converts a `datetime` object to a formatted string with underscores replacing hyphens, colons, and spaces.

#### Parameters

- `date` : `datetime`
  - The `datetime` object to format.

#### Returns

- `str`
  - Formatted string with underscores replacing hyphens, colons, and spaces.

#### Example

```python
from datetime import datetime

date = datetime(2023, 10, 5, 14, 30)
formatted_date = get_formatted_time_stamp(date)
print(formatted_date)  # Output: "2023_10_05_14_30"
```

#### `get_current_date`

```python
get_current_date(utc: bool = True, time_of_day: bool = False) -> datetime
```

Get the current system date, formatted as "DD-MMM-YYYY-HH:MM".

#### Parameters

- `utc` : `bool`, default=`True`
  - If `True`, return date with UTC time; otherwise, return system time.
- `time_of_day` : `bool`, default=`False`
  - If `True`, return date with time; otherwise, return only the date.

#### Returns

- `datetime`
  - Current date (with or without time) in formatted datetime format.

#### Example

```python
from datetime import datetime

# Get current UTC date with time
current_date_utc = get_current_date(utc=True, time_of_day=True)
print(current_date_utc)  # Output: "05-Oct-2023-14:30"

# Get current system date without time
current_date_system = get_current_date(utc=False, time_of_day=False)
print(current_date_system)  # Output: "05-Oct-2023-00:00"
```

---
