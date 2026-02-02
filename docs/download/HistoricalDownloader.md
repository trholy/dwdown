# Download module
## HistoricalDownloader

### Overview

The `HistoricalDownloader` class is responsible for downloading historical climate data from the DWD (Deutscher Wetterdienst) Open Data server. It supports filtering by station IDs and filename patterns, and includes features for downloading station descriptions, unpacking zip files, and reading data into pandas DataFrames.

### Constructor

```python
HistoricalDownloader(
    base_url: str | None = None,
    files_path: str | None = None,
    extracted_files_path: str | None = None,
    log_files_path: str | None = None,
    encoding: str | None = None,
    station_description_file_name: str | None = None,
    delay: int | float = 1,
    retry: int = 0,
    timeout: int = 30
)
```

#### Parameters

- `base_url` : `str | None`, default=`None`
  - Base URL to fetch data from. If None, uses the default DWD historical data URL.
- `files_path` : `str | None`, default=`None`
  - Directory to save downloaded files. Defaults to "download_files".
- `extracted_files_path` : `str | None`, default=`None`
  - Directory to save extracted files. Defaults to "extracted_files".
- `log_files_path` : `str | None`, default=`None`
  - Directory to save log files. Defaults to "log_files".
- `encoding` : `str | None`, default=`None`
  - Encoding used for station description files. Defaults to "windows-1252".
- `station_description_file_name` : `str | None`, default=`None`
  - Name of the station description file. Defaults to "KL_Tageswerte_Beschreibung_Stationen".
- `delay` : `int | float`, default=`1`
  - Optional delay between downloads (in seconds).
- `retry` : `int`, default=`0`
  - If > 0, retry failed downloads sequentially this many times.
- `timeout` : `int`, default=`30`
  - Timeout for both the connect and the read timeouts.

### Methods

#### `_get_filenames_from_url`

```python
_get_filenames_from_url() -> list[str]
```

Fetches filenames from the URL using an XPath expression.

#### Returns

- `list[str]`
  - A list of filenames extracted from the URL. Returns an empty list if the request fails or no filenames are found.

#### `get_links`

```python
get_links(
    station_ids: list[str],
    prefix: str | None = None,
    suffix: str | None = None,
    include_pattern: list[str] | None = None,
    exclude_pattern: list[str] | None = None
) -> list[str]
```

Generates download links based on the provided filters.

#### Parameters

- `station_ids` : `list[str] | None`
  - List of station IDs to filter for. The IDs will be zero-padded to 5 digits (e.g., '1' becomes '00001').
- `prefix` : `str | None`, default=`None`
  - The prefix that filenames should start with.
- `suffix` : `str | None`, default=`None`
  - The suffix that filenames should end with.
- `include_pattern` : `list[str] | None`, default=`None`
  - A list of regex patterns that filenames should match. If None, no inclusion filtering is applied.
- `exclude_pattern` : `list[str] | None`, default=`None`
  - A list of regex patterns that filenames should not match. If None, no exclusion filtering is applied.

#### Returns

- `list[str]`
  - A list of download links that match the specified filters.

#### `_download_file`

```python
_download_file(link: str, check_for_existence: bool) -> bool
```

Downloads a file from the given link.

#### Parameters

- `link` : `str`
  - The URL of the file to download.
- `check_for_existence` : `bool`
  - If True, checks if the file already exists in the download directory and skips the download if it does.

#### Returns

- `bool`
  - True if the file was successfully downloaded or already exists, False if the download failed.

#### `download`

```python
download(check_for_existence: bool = False) -> None
```

Downloads files from the generated links sequentially.

#### Parameters

- `check_for_existence` : `bool`, default=`False`
  - If True, checks if the file already exists in the download directory and skips the download if it does.

#### `download_station_description`

```python
download_station_description() -> None
```

Downloads the station description file.

#### `extract`

```python
extract(
    zip_files: list[str] | str | None = None,
    unpack_hist_data_only: bool = False,
    check_for_existence: bool = False
) -> None
```

Unpacks downloaded zip files.

#### Parameters

- `zip_files` : `list[str] | str | None`, default=`None`
  - A single filename, a list of filenames, or None. If None, processes all files in `self.download_links`.
- `unpack_hist_data_only` : `bool`, default=`False`
  - If True, only extracts the product file (containing data).
- `check_for_existence` : `bool`, default=`False`
  - If True, skips unpacking if the target folder already exists.

#### `read_station_description`

```python
read_station_description() -> pd.DataFrame
```

Reads the station description file into a DataFrame. Dynamically determines column widths using the separator line.

#### Returns

- `pd.DataFrame`
  - A pandas DataFrame containing the station description data.

#### `read_data`

```python
read_data(
    zip_files: list[str] | str | None = None,
    save_as_csv: bool = False
) -> dict[str, pd.DataFrame]
```

Reads the unpacked station data into pandas DataFrames.

#### Parameters

- `zip_files` : `list[str] | str | None`, default=`None`
  - List of zip files (or single file) to process. If None, checks folders for all files encountered in `self.download_links` or scans `extracted_files_path` if `download_links` is empty.
- `save_as_csv` : `bool`, default=`False`
  - If True, saves each DataFrame to a CSV file (same name as source, replaced extension).

#### Returns

- `dict[str, pd.DataFrame]`
  - A dictionary mapping filenames to pandas DataFrames.
