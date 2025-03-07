# Processing module
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
