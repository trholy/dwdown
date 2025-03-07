# Download module
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
download_files(check_for_existence: bool = False, max_retries: int = 3) -> None
```

Downloads all files from the generated links using concurrency for faster processing. If downloads fail and `self.restart_failed_downloads` is enabled, retry them sequentially. Finally failed downloads are stored in `self.finally_failed_files`.

#### Parameters

- `check_for_existence` : `bool`, default=`False`
  - If `True`, skips download if the file already exists.
- `max_retries` : `int`, default=3
  - Number of retry attempts before marking a file as failed.

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
