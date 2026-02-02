# Download module
## MOSMIX_Downloader

### Overview

The `MOSMIX_Downloader` class is responsible for downloading and extracting MOSMIX forecast data (MOSMIX_L, MOSMIX_S, MOSMIX-SNOW_S) from the DWD (Deutscher Wetterdienst) Open Data server.

### Constructor

```python
MOSMIX_Downloader(
    mosmix_type: Literal["MOSMIX_L", "MOSMIX_S", "MOSMIX-SNOW_S"],
    base_url: str | None = None,
    files_path: str | None = None,
    extracted_files_path: str | None = None,
    log_files_path: str | None = None,
    delay: int | float = 1,
    retry: int = 0,
    timeout: int = 30
)
```

#### Parameters

- `mosmix_type` : `Literal["MOSMIX_L", "MOSMIX_S", "MOSMIX-SNOW_S"]`
  - Type of MOSMIX forecast to download.
- `base_url` : `str | None`, default=`None`
  - Base URL to fetch data from. If `None`, it is constructed based on `mosmix_type`.
- `files_path` : `str | None`, default=`None`
  - Directory to save downloaded files. Defaults to "download_files".
- `extracted_files_path` : `str | None`, default=`None`
  - Directory to save extracted files. Defaults to "extracted_files".
- `log_files_path` : `str | None`, default=`None`
  - Directory to save log files. Defaults to "log_files".
- `delay` : `int | float`, default=`1`
  - Optional delay between downloads (in seconds).
- `retry` : `int`, default=`0`
  - If > 0, retry failed downloads sequentially this many times.
- `timeout` : `int`, default=`30`
  - Timeout for both the connect and the read timeouts.

### Methods

#### `get_links`

```python
get_links(
    station_ids: list[str] | None = None,
    prefix: str | None = None,
    suffix: str | None = None,
    include_pattern: list[str] | None = None,
    exclude_pattern: list[str] | None = None
) -> list[str]
```

Generates download links based on the provided filters and MOSMIX type.

#### Parameters

- `station_ids` : `list[str] | None`, default=`None`
  - List of station IDs to filter for. Only relevant for MOSMIX_L if fetching single stations.
- `prefix` : `str | None`, default=`None`
  - The prefix that filenames should start with.
- `suffix` : `str | None`, default=`None`
  - The suffix that filenames should end with.
- `include_pattern` : `list[str] | None`, default=`None`
  - A list of regex patterns that filenames should match.
- `exclude_pattern` : `list[str] | None`, default=`None`
  - A list of regex patterns that filenames should not match.

#### Returns

- `list[str]`
  - A list of download links.

#### `download`

```python
download(check_for_existence: bool = False) -> None
```

Downloads files from the generated links sequentially.

#### Parameters

- `check_for_existence` : `bool`, default=`False`
  - If `True`, checks if the file already exists locally before downloading.

#### `extract`

```python
extract(
    zip_files: list[str] | str | None = None,
    check_for_existence: bool = False
) -> None
```

Unpacks downloaded `.kmz` files to `.kml`.

#### Parameters

- `zip_files` : `list[str] | str | None`, default=`None`
  - List of kmz files or a single file to extract. If `None`, tries to use previously generated download links.
- `check_for_existence` : `bool`, default=`False`
  - Skip extracting if target directory exists.

#### `read_data`

```python
read_data(
    zip_files: list[str] | str | None = None,
    save_as_csv: bool = False
) -> dict[str, pd.DataFrame]
```

Reads unpacked KML data into Pandas DataFrames.

#### Parameters

- `zip_files` : `list[str] | str | None`, default=`None`
  - Used to identify which folders to look into.
- `save_as_csv` : `bool`, default=`False`
  - If `True`, save processed DataFrame to CSV.

#### Returns

- `dict[str, pd.DataFrame]`
  - A dictionary where keys are filenames and values are corresponding DataFrames.
