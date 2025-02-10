# Documentation

# `DWDDownloader` Class

## Overview
The `DWDDownloader` class is designed for:
- **Downloading files** from a specified URL, particularly from the **Deutscher Wetterdienst (DWD)**.
- **Filtering filenames** based on patterns.
- **Extracting dates** from web pages.
- **Logging downloads** for tracking progress and errors.
- **Handling concurrent downloads** with retry mechanisms.

---

## Constructor

```python
DWDDownloader(
    url: str,
    restart_failed_downloads: bool = False,
    log_downloads: bool = True,
    delay: Union[int, float] = None,
    workers: int = 1,
    download_path: str = "downloaded_files",
    log_files_path: str = "log_files_DWDDownloader",
    xpath_files: str = "/html/body/pre//a/@href",
    xpath_dates: str = "//pre/text()"
)
```

### Parameters
- **`url`** (`str`):  
  **Base URL** from which files will be downloaded.
- **`restart_failed_downloads`** (`bool`, default=`False`):  
  If `True`, **failed downloads** are retried sequentially.
- **`log_downloads`** (`bool`, default=`True`):  
  If `True`, logs **download details**.
- **`delay`** (`int | float`, default=`None`):  
  **Delay (seconds)** between downloads to avoid excessive requests.
- **`workers`** (`int`, default=`1`):  
  Number of **concurrent downloads** using threading.
- **`download_path`** (`str`, default=`"downloaded_files"`):  
  Directory where downloaded files are stored.
- **`log_files_path`** (`str`, default=`"log_files_DWDDownloader"`):  
  Directory for storing **log files**.
- **`xpath_files`** (`str`, default=`"/html/body/pre//a/@href"`):  
  XPath query to **extract file names** from the webpage.
- **`xpath_dates`** (`str`, default=`"//pre/text()"`):  
  XPath query to **extract dates** from the webpage.

### Usage
```python
downloader = DWDDownloader(
    url="https://example.com/data/",
    workers=4,
    restart_failed_downloads=True
)
```

---

## Methods

### `get_data_dates`
```python
get_data_dates(url: str = None) -> tuple[datetime, datetime]
```
Extracts and processes **date strings** from the webpage.

### Parameters
- **`url`** (`str`, optional):  
  If provided, fetches dates from this **specific URL** instead of `self.url`.

### Returns
- **`tuple[datetime, datetime]`**:  
  A **tuple of the earliest and latest dates** extracted.

### Usage
```python
min_date, max_date = downloader.get_data_dates()
```

---

### `get_current_date`
```python
get_current_date(utc: bool = True, time_of_day: bool = False) -> datetime
```
Returns the **current date** in a formatted string.

### Parameters
- **`utc`** (`bool`, default=`True`):  
  If `True`, returns **UTC time**; otherwise, returns local time.
- **`time_of_day`** (`bool`, default=`False`):  
  If `True`, includes the **time of day**.

### Returns
- **`datetime`**:  
  The formatted **current date and time**.

### Usage
```python
current_date = downloader.get_current_date()
```

---

### `get_links`
```python
get_links(
    name_startswith: str,
    name_endswith: str,
    include_pattern: List[str],
    exclude_pattern: List[str]
) -> List[str]
```
Filters and **generates a list of file download links**.

### Parameters
- **`name_startswith`** (`str`):  
  **Filters files** that start with this prefix.
- **`name_endswith`** (`str`):  
  **Filters files** that end with this suffix.
- **`include_pattern`** (`List[str]`):  
  List of substrings that **must be present** in the filename.
- **`exclude_pattern`** (`List[str]`):  
  List of substrings that **must not be present** in the filename.

### Returns
- **`List[str]`**:  
  A **list of URLs** that match the filter criteria.

### Usage
```python
links = downloader.get_links(name_startswith="data_", name_endswith=".csv", include_pattern=["2024"], exclude_pattern=["old"])
```

---

### `download_files`
```python
download_files(check_for_existence: bool = False) -> None
```
Downloads **all files** from `self.full_links` using **multi-threading**.  
Retries failed downloads if `restart_failed_downloads=True`.

### Parameters
- **`check_for_existence`** (`bool`, default=`False`):  
  If `True`, skips downloading **files that already exist**.

### Returns
- **`None`**

### Usage
```python
downloader.download_files(check_for_existence=True)
```

---

## Other Methods

### `_ensure_directory_exists`
```python
_ensure_directory_exists(path: str) -> None
```
Ensures that the **specified directory exists**, creating it if necessary.

---

### `_fix_date_format`
```python
_fix_date_format(dates: List[str]) -> List[str]
```
Converts raw **date strings** into a **standardized format**.

---

### `_parse_dates`
```python
_parse_dates(date_strings: List[str]) -> List[datetime]
```
Converts **formatted date strings** into `datetime` objects.

---

### `_get_filenames_from_url`
```python
_get_filenames_from_url() -> List[str]
```
Extracts **file names** from the given **URL**.

---

### `_filter_file_names`
```python
_filter_file_names(
    filenames: List[str],
    name_startswith: str,
    name_endswith: str,
    include_pattern: List[str],
    exclude_pattern: List[str]
) -> List[str]
```
Filters **file names** based on given criteria.

---

### `_generate_links`
```python
_generate_links(filtered_filenames: List[str]) -> List[str]
```
Creates **full download links** from **filtered filenames**.

---

### `_download_file`
```python
_download_file(link: str, check_for_existence: bool) -> bool
```
Downloads a **single file** and logs its progress.

---

### `_log_name_formatting`
```python
_log_name_formatting(link: str) -> tuple[str, str]
```
Formats **log file names** with timestamps.

---

### `_get_variable_from_link`
```python
_get_variable_from_link(link: str) -> str
```
Extracts **variable name** from the URL.

---

### `get_formatted_time_stamp`
```python
get_formatted_time_stamp(time_stamp: datetime) -> str
```
Formats a **datetime object** as a timestamp for logging.

---

### `_write_log_file`
```python
_write_log_file(filename: str, data: list) -> None
```
Writes a list of **entries to a log file**.

---

### `_process_timestamps`
```python
_process_timestamps(
    min_timestamp: Union[str, int] = None,
    max_timestamp: Union[str, int] = None,
    include_pattern: list = None
) -> List[str]
```
Generates a **list of timestamp patterns** within a given range.

---

# `MinioDownloader` Class

## Overview
The `MinioDownloader` class provides functionality for:
- **Downloading files** from a MinIO bucket.
- **Verifying file integrity** using checksums.
- **Logging** download progress and errors.
- **Parallel downloads** using multiple worker threads.

---

## Constructor

```python
MinioDownloader(
    endpoint: str,
    access_key: str,
    secret_key: str,
    files_path: str,
    secure: bool = False,
    log_downloads: bool = True,
    log_files_path: str = "log_files_MinioDownloader",
    workers: int = 1
)
```

### Parameters
- **`endpoint`** (`str`):  
  The **MinIO server endpoint**.
- **`access_key`** (`str`):  
  Authentication access key.
- **`secret_key`** (`str`):  
  Authentication secret key.
- **`files_path`** (`str`):  
  **Local directory** where downloaded files are stored.
- **`secure`** (`bool`, default=`False`):  
  If `True`, enables **secure (HTTPS) connections**.
- **`log_downloads`** (`bool`, default=`True`):  
  If `True`, enables **logging of download details**.
- **`log_files_path`** (`str`, default=`"log_files_MinioDownloader"`):  
  Directory where **log files** are stored.
- **`workers`** (`int`, default=`1`):  
  Number of **parallel worker threads** for downloading.

### Usage
```python
downloader = MinioDownloader(
    endpoint="play.min.io",
    access_key="your-access-key",
    secret_key="your-secret-key",
    files_path="./downloads",
    secure=True,
    workers=4
)
```

---

## Methods

### `download_bucket`
```python
download_bucket(bucket_name: str, folder_prefix: str = None) -> None
```
Downloads **all files** from a specified MinIO bucket.  
Supports **optional folder prefix filtering**.

### Parameters
- **`bucket_name`** (`str`):  
  The **name of the MinIO bucket**.
- **`folder_prefix`** (`str`, optional):  
  If provided, **only downloads files from this folder**.

### Returns
- **`None`**

### Usage
```python
downloader.download_bucket("my-bucket", "subfolder/")
```

---

### `calculate_md5`
```python
calculate_md5(file_path: str) -> str
```
Computes the **MD5 hash** of a given file.

### Parameters
- **`file_path`** (`str`):  
  Path to the file.

### Returns
- **`str`**:  
  The **MD5 hash** of the file.

### Usage
```python
hash_value = MinioDownloader.calculate_md5("file.txt")
```

---

## Other Methods

### `_ensure_bucket`
```python
_ensure_bucket(bucket_name: str) -> None
```
Checks if the **specified bucket exists** and logs the result.

### Parameters
- **`bucket_name`** (`str`):  
  Name of the MinIO bucket.

### Returns
- **`None`**

---

### `_get_remote_files`
```python
_get_remote_files(bucket_name: str, folder_prefix: str) -> List[str]
```
Retrieves a **list of available files** in the MinIO bucket.

### Parameters
- **`bucket_name`** (`str`):  
  Name of the MinIO bucket.
- **`folder_prefix`** (`str`):  
  If provided, **filters files by folder prefix**.

### Returns
- **`List[str]`**:  
  A **list of available files** in the bucket.

---

### `_download_file`
```python
_download_file(
    bucket_name: str,
    local_file_path: str,
    remote_path: str
) -> bool
```
Downloads a **single file** from MinIO and **verifies its integrity**.

### Parameters
- **`bucket_name`** (`str`):  
  Name of the MinIO bucket.
- **`local_file_path`** (`str`):  
  Path where the downloaded file will be saved.
- **`remote_path`** (`str`):  
  Remote file path inside the bucket.

### Returns
- **`bool`**:  
  `True` if **download is successful**, `False` otherwise.

---

### `_verify_file_integrity`
```python
_verify_file_integrity(
    bucket_name: str,
    local_file_path: str,
    remote_path: str
) -> bool
```
Verifies whether the **downloaded file matches the original checksum**.

### Parameters
- **`bucket_name`** (`str`):  
  Name of the MinIO bucket.
- **`local_file_path`** (`str`):  
  Path of the **downloaded file**.
- **`remote_path`** (`str`):  
  Remote file path inside the bucket.

### Returns
- **`bool`**:  
  `True` if the **file is intact**, `False` otherwise.

---

### `_write_log_file`
```python
_write_log_file(filename: str, data: List[str]) -> None
```
Writes **downloaded file names** to a log file.

### Parameters
- **`filename`** (`str`):  
  The name of the **log file**.
- **`data`** (`List[str]`):  
  List of downloaded file names.

### Returns
- **`None`**

---

### `_get_current_date`
```python
_get_current_date() -> str
```
Returns the **current date** formatted as `"DD-MMM-YYYY-HH-MM"`.

### Returns
- **`str`**:  
  The formatted **current date**.

---

# `DataProcessor` Class

## Overview
The `DataProcessor` class provides functionality to:
- **Search, decompress, and process** GRIB files.
- Convert GRIB files into **CSV format**.
- Filter files based on **naming patterns** and **geographic coordinates**.
- Log all activities, including **errors and warnings**.

---

## Constructor

```python
DataProcessor(
    search_path: str,
    extraction_path: str,
    converted_files_path: str
)
```

### Parameters
- **`search_path`** (`str`):  
  The directory where GRIB files are searched.
- **`extraction_path`** (`str`):  
  The directory where decompressed files are stored.
- **`converted_files_path`** (`str`):  
  The directory where converted CSV files are stored.

### Usage
- Creates necessary directories.
- Initializes structured logging.

---

## Methods

### `get_filenames`
```python
get_filenames(
    name_startswith: str = "",
    name_endswith: str = "",
    include_pattern: List[str] = None,
    exclude_pattern: List[str] = None
) -> List[str]
```
Searches for files in the specified directory and **filters them based on naming criteria**.

### Parameters
- **`name_startswith`** (`str`, optional):  
  Filters files by **starting characters**.
- **`name_endswith`** (`str`, optional):  
  Filters files by **ending characters**.
- **`include_pattern`** (`List[str]`, optional):  
  Ensures filenames contain **specific substrings**.
- **`exclude_pattern`** (`List[str]`, optional):  
  Excludes filenames containing **specific substrings**.

### Returns
- **`List[str]`**: A list of **filtered file paths**.

---

### `get_csv`
```python
get_csv(
    file_names: List[str],
    apply_geo_filtering: bool = False,
    start_lat: Optional[float] = None,
    end_lat: Optional[float] = None,
    start_lon: Optional[float] = None,
    end_lon: Optional[float] = None
) -> None
```
Processes **GRIB files** by:
1. **Decompressing** them.
2. **Converting** them into CSV format.
3. **Applying geographic filtering** (optional).

### Parameters
- **`file_names`** (`List[str]`):  
  List of GRIB files to process.
- **`apply_geo_filtering`** (`bool`, default=`False`):  
  If `True`, filters data based on **geographic coordinates**.
- **`start_lat`, `end_lat`** (`float`, optional):  
  Latitude range for filtering.
- **`start_lon`, `end_lon`** (`float`, optional):  
  Longitude range for filtering.

### Returns
- **`None`**

---

## Other Methods

### `_ensure_directory_exists`
```python
_ensure_directory_exists(directory: str) -> None
```
Ensures that the given **directory exists**, creating it if necessary.

### Parameters
- **`directory`** (`str`):  
  The directory path to check or create.

### Returns
- **`None`**

---

### `_decompress_files`
```python
_decompress_files(file_to_decompress: str) -> str
```
Decompresses a `.bz2` file.

### Parameters
- **`file_to_decompress`** (`str`):  
  The compressed `.bz2` file path.

### Returns
- **`str`**:  
  The **path of the decompressed file**.

---

### `_get_decompressed_file_path`
```python
_get_decompressed_file_path(file_to_decompress: str) -> str
```
Generates a **decompressed file path**, ensuring directory structure is maintained.

### Parameters
- **`file_to_decompress`** (`str`):  
  The compressed `.bz2` file path.

### Returns
- **`str`**:  
  The **expected decompressed file path**.

---

### `_read_grib_to_dataframe`
```python
_read_grib_to_dataframe(
    decompressed_file_path: str,
    apply_geo_filtering: bool,
    start_lat: Optional[float],
    end_lat: Optional[float],
    start_lon: Optional[float],
    end_lon: Optional[float]
) -> pd.DataFrame
```
Reads a **GRIB file**, converts it into a **Pandas DataFrame**, and optionally applies **geographic filtering**.

### Parameters
- **`decompressed_file_path`** (`str`):  
  Path of the **decompressed** GRIB file.
- **`apply_geo_filtering`** (`bool`):  
  Whether to **filter data** based on coordinates.
- **`start_lat`, `end_lat`** (`float`, optional):  
  Latitude range for filtering.
- **`start_lon`, `end_lon`** (`float`, optional):  
  Longitude range for filtering.

### Returns
- **`pd.DataFrame`**:  
  A **filtered** or **unfiltered** DataFrame.

---

### `_filter_by_coordinates`
```python
_filter_by_coordinates(
    df: pd.DataFrame,
    start_lat: float,
    end_lat: float,
    start_lon: float,
    end_lon: float
) -> pd.DataFrame
```
Filters a **Pandas DataFrame** by latitude and longitude.

### Parameters
- **`df`** (`pd.DataFrame`):  
  The input DataFrame.
- **`start_lat`, `end_lat`** (`float`):  
  Latitude range for filtering.
- **`start_lon`, `end_lon`** (`float`):  
  Longitude range for filtering.

### Returns
- **`pd.DataFrame`**:  
  The **filtered** DataFrame.

---

### `_save_as_csv`
```python
_save_as_csv(df: pd.DataFrame, csv_file_path: str) -> None
```
Saves a **DataFrame as a CSV file**.

### Parameters
- **`df`** (`pd.DataFrame`):  
  The DataFrame to save.
- **`csv_file_path`** (`str`):  
  The path where the CSV file will be saved.

### Returns
- **`None`**

---

### `_get_converted_file_path`
```python
_get_converted_file_path(decompressed_file_path: str) -> str
```
Generates the **CSV file path** based on the decompressed file path.

### Parameters
- **`decompressed_file_path`** (`str`):  
  The path of the decompressed GRIB file.

### Returns
- **`str`**:  
  The **expected CSV file path**.

---

### `flatten_list`
```python
flatten_list(nested_list: Union[List, str]) -> List[str]
```
Recursively flattens a **nested list** of filenames.

### Parameters
- **`nested_list`** (`Union[List, str]`):  
  A **nested list** of filenames.

### Returns
- **`List[str]`**:  
  A **flattened list** of filenames.

---

### `_search_directory`
```python
_search_directory(
    directory: str,
    include_pattern: List[str],
    exclude_pattern: List[str],
    name_startswith: str,
    name_endswith: str
) -> List[str]
```
Searches a **directory recursively**, filtering files based on patterns.

### Parameters
- **`directory`** (`str`):  
  The directory to search.
- **`include_pattern`** (`List[str]`):  
  Substrings that must be included.
- **`exclude_pattern`** (`List[str]`):  
  Substrings that should be excluded.
- **`name_startswith`** (`str`):  
  Prefix filter.
- **`name_endswith`** (`str`):  
  Suffix filter.

### Returns
- **`List[str]`**:  
  A list of **matching file paths**.

---

# `MinioUploader` Class

## Overview
The `MinioUploader` class provides functionality for uploading files to a **MinIO object storage server**.  
It supports:
- Uploading files from a local directory.
- Ensuring data integrity via **MD5 hash verification**.
- **Parallel uploads** using threading.
- Logging upload results and handling errors.
- Creating buckets automatically if they do not exist.
- **Deleting local files** after a successful upload.

---

## Constructor

```python
MinioUploader(
    endpoint: str,
    access_key: str,
    secret_key: str,
    files_path: str,
    bucket_name: str = 'my-bucket',
    secure: bool = False,
    log_uploads: bool = True,
    log_files_path: str = "log_files_MinioUploader",
    workers: int = 1,
)
```

### Parameters
- **`endpoint`** (`str`):  
  The endpoint (URL) of the MinIO server.
- **`access_key`** (`str`):  
  The access key for MinIO authentication.
- **`secret_key`** (`str`):  
  The secret key for MinIO authentication.
- **`files_path`** (`str`):  
  The local directory where the files to be uploaded are located.
- **`bucket_name`** (`str`, default=`'my-bucket'`):  
  The name of the MinIO bucket for uploads.
- **`secure`** (`bool`, default=`False`):  
  If `True`, enables **SSL** (secure connection).
- **`log_uploads`** (`bool`, default=`True`):  
  If `True`, logs the upload results.
- **`log_files_path`** (`str`, default=`'log_files_MinioUploader'`):  
  Directory to store log files.
- **`workers`** (`int`, default=`1`):  
  The number of concurrent threads for uploading.

---

## Methods

### `upload_directory`
```python
upload_directory(remote_prefix: str = "") -> None
```
Recursively uploads all files from the local directory to the **MinIO bucket**, supporting parallel uploads.

### Parameters
- **`remote_prefix`** (`str`, default=`""`):  
  A prefix to prepend to the remote file paths.

### Returns
- **`None`**  

---

### `delete_local_files`
```python
delete_local_files() -> None
```
Deletes the **successfully uploaded** local files and removes any **empty directories**.

### Parameters
- **None**  

### Returns
- **`None`**  

---

## Other Methods

### `_upload_file`
```python
_upload_file(local_file_path: str, remote_path: str) -> bool
```
Uploads a **single file** to the MinIO bucket, ensuring integrity by **comparing MD5 hashes**.

### Parameters
- **`local_file_path`** (`str`):  
  The local file path to be uploaded.
- **`remote_path`** (`str`):  
  The destination path in the MinIO bucket.

### Returns
- **`bool`**:  
  `True` if the file is **successfully uploaded** and integrity is **verified**, otherwise `False`.

---

### `_ensure_directory_exists`
```python
_ensure_directory_exists(path: str) -> None
```
Ensures that the given **directory exists**, creating it if necessary.

### Parameters
- **`path`** (`str`):  
  The directory path to check or create.

### Returns
- **`None`**  

---

### `_ensure_bucket`
```python
_ensure_bucket() -> None
```
Ensures that the **MinIO bucket exists**. If it does not, the method **creates** it.

### Parameters
- **None**  

### Returns
- **`None`**  

---

### `_fetch_existing_files`
```python
_fetch_existing_files(remote_prefix: str) -> dict
```
Fetches **existing files** in the bucket along with their **ETags**.

### Parameters
- **`remote_prefix`** (`str`):  
  Object name prefix to filter files.

### Returns
- **`dict`**:  
  A dictionary containing `{remote_path: etag}`.

---

### `_write_log_file`
```python
_write_log_file(filename: str, data: list) -> None
```
Writes a **list of uploaded or corrupted files** to a log file.

### Parameters
- **`filename`** (`str`):  
  The name of the log file.
- **`data`** (`list`):  
  A list of file paths to log.

### Returns
- **`None`**  

---

# `DataEditor` Class

## Overview

The `DataEditor` class provides functionality for handling and processing CSV data. It enables merging, filtering, and validating data from multiple CSV files based on various parameters.

## Initialization

```python
DataEditor(
    files_path: str,
    required_columns: Optional[Set[str]] = None,
    join_method: str = 'inner',
    sep: str = ',',
    index_col: Optional[str] = None,
    mapping_dictionary: Optional[Dict[str, str]] = None,
    additional_pattern_selection: Optional[Dict[str, int]] = None
)
```

### Parameters
- **`files_path`** (`str`):  
  Path to the directory containing CSV files.
- **`required_columns`** (`Optional[Set[str]]`, default=`{'latitude', 'longitude', 'valid_time'}`):  
  Set of columns that must be present in the DataFrame.
- **`join_method`** (`str`, default=`'inner'`):  
  Method for merging DataFrames (`'inner'`, `'outer'`, etc.).
- **`sep`** (`str`, default=`','`):  
  Delimiter used in the CSV files.
- **`index_col`** (`Optional[str]`, default=`None`):  
  Column to be used as the DataFrame index.
- **`mapping_dictionary`** (`Optional[Dict[str, str]]`, default=`None`):  
  Dictionary mapping original variable names to CSV column names.
- **`additional_pattern_selection`** (`Optional[Dict[str, int]]`, default=`None`):  
  Dictionary defining additional filtering patterns.

---

## Methods

### `merge_dfs`
```python
merge_dfs(
    matching_id: Union[str, List[str]],
    variables: List[str],
    required_columns: Optional[Set[str]] = None
) -> Optional[pd.DataFrame]
```
Merges multiple CSV files into a single DataFrame based on shared columns.

#### Parameters
- **`matching_id`** (`Union[str, List[str]]`):  
  ID or pattern used to identify files for merging.
- **`variables`** (`List[str]`):  
  List of variables to include in the merge.
- **`required_columns`** (`Optional[Set[str]]`, default=`None`):  
  Required columns that must be present in the DataFrame.

#### Returns
- **`Optional[pd.DataFrame]`**:  
  Merged DataFrame or `None` if no valid data is found.

---

### `_validate_columns_exist`
```python
_validate_columns_exist(
    df: pd.DataFrame,
    required_columns: Set[str],
    variable: str
) -> bool
```
Ensures that all required columns exist in the DataFrame.

#### Parameters
- **`df`** (`pd.DataFrame`):  
  DataFrame to validate.
- **`required_columns`** (`Set[str]`):  
  Set of required column names.
- **`variable`** (`str`):  
  Variable being processed.

#### Returns
- **`bool`**:  
  `True` if all required columns exist, otherwise `False`.

---

### `_filter_dataframe`
```python
_filter_dataframe(
    df: pd.DataFrame,
    required_columns: Set[str],
    variable: str
) -> pd.DataFrame
```
Filters the DataFrame to retain only required columns.

#### Parameters
- **`df`** (`pd.DataFrame`):  
  DataFrame to filter.
- **`required_columns`** (`Set[str]`):  
  Required columns to retain.
- **`variable`** (`str`):  
  Variable being processed.

#### Returns
- **`pd.DataFrame`**:  
  Filtered DataFrame.

---

### `_parse_datetime`
```python
_parse_datetime(df: pd.DataFrame) -> pd.DataFrame
```
Attempts to parse the `'valid_time'` column as datetime.

#### Parameters
- **`df`** (`pd.DataFrame`):  
  DataFrame containing the `'valid_time'` column.

#### Returns
- **`pd.DataFrame`**:  
  DataFrame with parsed datetime values.

---

### `_merge_dataframes`
```python
_merge_dataframes(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    merge_on: Set[str]
) -> pd.DataFrame
```
Merges two DataFrames on specified columns.

#### Parameters
- **`df1`** (`pd.DataFrame`):  
  First DataFrame.
- **`df2`** (`pd.DataFrame`):  
  Second DataFrame.
- **`merge_on`** (`Set[str]`):  
  Columns to merge on.

#### Returns
- **`pd.DataFrame`**:  
  Merged DataFrame.

---

### `_get_csv_file`
```python
_get_csv_file(variable: str) -> List[str]
```
Retrieves CSV file paths for a given variable.

#### Parameters
- **`variable`** (`str`):  
  Name of the variable.

#### Returns
- **`List[str]`**:  
  List of file paths.

---

### `_filter_file_names`
```python
_filter_file_names(
    filenames: List[str],
    name_startswith: str = "icon-d2_germany",
    name_endswith: str = ".csv",
    include_pattern: Union[str, List[str]] = None,
    exclude_pattern: List[str] = None,
    variable: Optional[str] = None
) -> Optional[List[str]]
```
Filters filenames based on inclusion/exclusion patterns.

#### Parameters
- **`filenames`** (`List[str]`):  
  List of filenames to filter.
- **`name_startswith`** (`str`, default=`"icon-d2_germany"`):  
  Prefix that filenames must start with.
- **`name_endswith`** (`str`, default=`".csv"`):  
  Suffix that filenames must end with.
- **`include_pattern`** (`Union[str, List[str]]`, optional, default=`None`):  
  Substring(s) that must be present.
- **`exclude_pattern`** (`List[str]`, optional, default=`[]`):  
  Substrings to exclude.
- **`variable`** (`Optional[str]`, optional, default=`None`):  
  Variable name being processed.

#### Returns
- **`Optional[List[str]]`**:  
  Filtered filenames or `None` if no matches are found.

---

### `_variable_mapping`
```python
_variable_mapping(variables: List[str]) -> List[str]
```
Maps variable names to actual CSV column names.

#### Parameters
- **`variables`** (`List[str]`):  
  List of variables to map.

#### Returns
- **`List[str]`**:  
  Mapped column names.

---

### `_read_dataframe_from_csv`
```python
_read_dataframe_from_csv(csv_file: str) -> Optional[pd.DataFrame]
```
Reads a CSV file into a DataFrame with error handling.

#### Parameters
- **`csv_file`** (`str`):  
  Path to the CSV file.

#### Returns
- **`Optional[pd.DataFrame]`**:  
  Loaded DataFrame or `None` if an error occurs.

---

### `_extract_additional_pattern`
```python
_extract_additional_pattern(filename: str) -> Optional[int]
```
Extracts an additional numeric pattern from a filename.

#### Parameters
- **`filename`** (`str`):  
  Filename to process.

#### Returns
- **`Optional[int]`**:  
  Extracted pattern or `None` if not found.

---

# Utility Functions

## `get_formatted_time_stamp`
```python
get_formatted_time_stamp(date: datetime) -> str
```
Formats a given `datetime` object into a string with underscores as separators.

### Parameters
- **`date`** (`datetime`):  
  Datetime object to be formatted.

### Returns
- **`str`**:  
  Formatted string in the `YYYY_MM_DD_HH_MM` format.

---

## `get_current_date`
```python
get_current_date(
    utc: bool = True,
    time_of_day: bool = False
) -> datetime
```
Retrieves the current system date and time, formatted as `"DD-MMM-YYYY-HH:MM"`, and returns a `datetime` object.

### Parameters
- **`utc`** (`bool`, default=`True`):  
  If `True`, returns the current UTC date and time.  
  If `False`, returns the local system date and time.
- **`time_of_day`** (`bool`, default=`False`):  
  If `True`, includes the exact time in the returned `datetime` object.  
  If `False`, the time is set to midnight (`00:00`).

### Returns
- **`datetime`**:  
  `datetime` object representing the current date, optionally including time.
