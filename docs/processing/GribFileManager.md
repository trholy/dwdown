# Processing module
## GribFileManager

### Overview

The `GribFileManager` class handles the decompression of BZ2 files and the conversion of GRIB2 files to CSV format. It supports geographic filtering during conversion and manages file paths for extraction and conversion.

### Constructor

```python
GribFileManager(
    files_path: str,
    extracted_files_path: str | None = None,
    converted_files_path: str | None = None,
    log_files_path: str | None = None
)
```

#### Parameters

- `files_path` : `str`
  - Path to the directory containing the files to process.
- `extracted_files_path` : `str | None`, default=`None`
  - Path to the directory for decompressed files. Defaults to "extracted_files".
- `converted_files_path` : `str | None`, default=`None`
  - Path to the directory for converted files. Defaults to "converted_files".
- `log_files_path` : `str | None`, default=`None`
  - Path to the directory for log files. Defaults to "log_files".

### Methods

#### `_get_decompression_path`

```python
_get_decompression_path(file_path: str) -> str
```

Generates the path for the decompressed file.

#### Parameters

- `file_path` : `str`
  - Path to the file to decompress.

#### Returns

- `str`
  - Path to the decompressed file.

#### `_get_conversion_path`

```python
_get_conversion_path(file_path: str) -> str
```

Generates the path for the converted file.

#### Parameters

- `file_path` : `str`
  - Path to the decompressed file.

#### Returns

- `str`
  - Path to the converted file.

#### `_decompress_files`

```python
_decompress_files(file_path: str) -> str
```

Decompresses a .bz2 file.

#### Parameters

- `file_path` : `str`
  - Path to the file to decompress.

#### Returns

- `str`
  - Path to the decompressed file.

#### `_grib_to_df`

```python
_grib_to_df(
    file_path: str,
    apply_geo_filtering: bool,
    start_lat: float | None,
    end_lat: float | None,
    start_lon: float | None,
    end_lon: float | None
) -> None
```

Reads a GRIB file into a DataFrame and optionally applies geographic filtering.

#### Parameters

- `file_path` : `str`
  - Path to the decompressed GRIB file.
- `apply_geo_filtering` : `bool`
  - Whether to apply geographic filtering.
- `start_lat` : `float | None`
  - Starting latitude for filtering.
- `end_lat` : `float | None`
  - Ending latitude for filtering.
- `start_lon` : `float | None`
  - Starting longitude for filtering.
- `end_lon` : `float | None`
  - Ending longitude for filtering.

#### `get_filenames`

```python
get_filenames(
    prefix: str | None = None,
    suffix: str | None = None,
    min_timestep: str | int | None = None,
    max_timestep: str | int | None = None,
    include_pattern: list[str] | None = None,
    exclude_pattern: list[str] | None = None,
    additional_patterns: dict | None = None,
    skip_time_step_filtering_variables: list[str] | None = None,
    variables: list[str] | None = None
) -> list
```

Retrieves filenames based on specified criteria.

#### Parameters

- `prefix` : `str | None`, default=`None`
  - Files must start with this string.
- `suffix` : `str | None`, default=`None`
  - Files must end with this string.
- `min_timestep` : `str | int | None`, default=`None`
  - Minimum timestep for filtering.
- `max_timestep` : `str | int | None`, default=`None`
  - Maximum timestep for filtering.
- `include_pattern` : `list[str] | None`, default=`None`
  - Patterns to include in filenames.
- `exclude_pattern` : `list[str] | None`, default=`None`
  - Patterns to exclude from filenames.
- `additional_patterns` : `dict | None`, default=`None`
  - Additional patterns for filtering.
- `skip_time_step_filtering_variables` : `list[str] | None`, default=`None`
  - Variables to skip timestep filtering for.
- `variables` : `list[str] | None`, default=`None`
  - Variables to filter by.

#### Returns

- `list`
  - List of filenames.

#### `get_csv`

```python
get_csv(
    file_names: list[str],
    apply_geo_filtering: bool = False,
    start_lat: float | None = None,
    end_lat: float | None = None,
    start_lon: float | None = None,
    end_lon: float | None = None
) -> None
```

Processes files to convert them to CSV.

#### Parameters

- `file_names` : `list[str]`
  - List of file names to process.
- `apply_geo_filtering` : `bool`, default=`False`
  - Whether to apply geographic filtering.
- `start_lat` : `float | None`, default=`None`
  - Starting latitude for filtering.
- `end_lat` : `float | None`, default=`None`
  - Ending latitude for filtering.
- `start_lon` : `float | None`, default=`None`
  - Starting longitude for filtering.
- `end_lon` : `float | None`, default=`None`
  - Ending longitude for filtering.

#### `delete`

```python
delete(
    delete_downloaded: bool = False,
    delete_decompressed: bool = True,
    converted_files: bool = False
) -> None
```

Deletes local files after successful processing.

#### Parameters

- `delete_downloaded` : `bool`, default=`False`
  - Whether to delete downloaded files.
- `delete_decompressed` : `bool`, default=`True`
  - Whether to delete decompressed files.
- `converted_files` : `bool`, default=`False`
  - Whether to delete converted files.
