# Processing module
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
