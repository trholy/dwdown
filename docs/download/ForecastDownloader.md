# Download module
## ForecastDownloader

### Overview

The `ForecastDownloader` class is responsible for downloading forecast data from the DWD (Deutscher Wetterdienst) Open Data server. It supports filtering by model, grid, forecast run, and variable, and includes features for parallel downloading, retries, and logging.

### Constructor

```python
ForecastDownloader(
    model: str | None = None,
    forecast_run: str | None = None,
    variable: str | None = None,
    grid: str | None = None,
    files_path: str | None = None,
    log_files_path: str | None = None,
    delay: int | float = 1,
    n_jobs: int = 1,
    retry: int = 0,
    timeout: int = 30,
    url: str | None = None,
    base_url: str | None = None,
    xpath_files: str | None = None,
    xpath_meta_data: str | None = None
)
```

#### Parameters

- `url` : `str | None`, default=`None`
  - Full URL to fetch data from (following parameters like model, grid, etc. are NOT needed and will be overwritten).
- `base_url` : `str | None`, default=`None`
  - Base URL to fetch data from. Defaults to "https://opendata.dwd.de/weather/nwp".
- `model` : `str | None`, default=`None`
  - The NWP model name, e.g., icon-d2, icon-eu.
- `grid` : `str | None`, default=`None`
  - The model grid [regular-lat-lon | icosahedral].
- `forecast_run` : `str | None`, default=`None`
  - The forecast run in the 3-hourly assimilation cycle, e.g., 00, 03, 06, ..., 21.
- `variable` : `str | None`, default=`None`
  - The single-level model fields that should be downloaded, e.g., t_2m, tmax_2m, clch, pmsl.
- `retry` : `int`, default=`0`
  - If > 0, retry failed downloads sequentially this many times.
- `timeout` : `int`, default=`30`
  - Timeout for both the connect and the read timeouts.
- `delay` : `int | float`, default=`1`
  - Optional delay between downloads (in seconds).
- `n_jobs` : `int`, default=`1`
  - Number of worker threads for parallel downloading.
- `files_path` : `str | None`, default=`None`
  - Directory to save downloaded files. Defaults to "download_files".
- `log_files_path` : `str | None`, default=`None`
  - Directory to save log files. Defaults to "log_files".
- `xpath_files` : `str | None`, default=`None`
  - XPath expression to extract filenames from the HTML.
- `xpath_meta_data` : `str | None`, default=`None`
  - XPath expression to extract meta data strings from the HTML.

### Methods

#### `_get_filenames_from_url`

```python
_get_filenames_from_url() -> list[str]
```

Fetches filenames from the URL using an XPath expression.

#### Returns

- `list[str]`
  - A list of filenames extracted from the URL. Returns an empty list if the request fails or no filenames are found.

#### `_get_variable_from_link`

```python
@staticmethod
_get_variable_from_link(link: str) -> str
```

Extracts the variable name from the link using URL parsing.

#### Parameters

- `link` : `str`
  - The URL from which to extract the variable name.

#### Returns

- `str`
  - The extracted variable name. Returns an empty string if the variable name cannot be determined.

#### `get_data_dates`

```python
get_data_dates(url: str | None = None, date_pattern: str | None = None) -> tuple[datetime, datetime]
```

Fetches and parses dates from the URL.

#### Parameters

- `url` : `str | None`, default=`None`
  - The URL from which to fetch the dates. If None, uses the default URL of the instance.
- `date_pattern` : `str | None`, default=`None`
  - A regex pattern to use for parsing dates. If None, uses a default pattern.

#### Returns

- `tuple[datetime, datetime]`
  - A tuple containing the minimum and maximum dates parsed from the URL.

#### `_set_grid_filter`

```python
@staticmethod
_set_grid_filter(grid: str | None) -> list
```

Sets the grid filter based on the provided grid type.

#### Parameters

- `grid` : `str | None`
  - The type of grid, either 'icosahedral' or 'regular'.

#### Returns

- `list`
  - A list containing the grid type if valid, otherwise an empty list.

#### `get_links`

```python
get_links(
    prefix: str | None = None,
    suffix: str | None = None,
    include_pattern: list[str] | None = None,
    exclude_pattern: list[str] | None = None,
    additional_patterns: dict | None = None,
    skip_time_step_filtering_variables: list[str] | None = None,
    min_timestep: str | int | None = None,
    max_timestep: str | int | None = None
) -> list[str]
```

Generates download links based on the provided filters.

#### Parameters

- `prefix` : `str | None`, default=`None`
  - The prefix that filenames should start with. Defaults to the model name.
- `suffix` : `str | None`, default=`None`
  - The suffix that filenames should end with. Defaults to ".grib2.bz2".
- `include_pattern` : `list[str] | None`, default=`None`
  - A list of regex patterns that filenames should match.
- `exclude_pattern` : `list[str] | None`, default=`None`
  - A list of regex patterns that filenames should not match.
- `additional_patterns` : `dict | None`, default=`None`
  - Additional patterns for filtering.
- `skip_time_step_filtering_variables` : `list[str] | None`, default=`None`
  - Variables to skip timestep filtering.
- `min_timestep` : `str | int | None`, default=`None`
  - The minimum timestep to include in the filenames.
- `max_timestep` : `str | int | None`, default=`None`
  - The maximum timestep to include in the filenames.

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

Downloads files from the generated links.

#### Parameters

- `check_for_existence` : `bool`, default=`False`
  - If True, checks if the file already exists in the download directory and skips the download if it does.

#### `delete`

```python
delete() -> None
```

Deletes local files after successful download.
