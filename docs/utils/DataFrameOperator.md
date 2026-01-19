# Utils module
## DataFrameOperator

### Overview

The `DataFrameOperator` class provides utility methods for operating on pandas DataFrames, including validation, filtering, parsing, and merging.

### Constructor

```python
DataFrameOperator(log_handler: LogHandler)
```

#### Parameters

- `log_handler` : `LogHandler`
  - LogHandler instance.

### Methods

#### `_validate_columns_exist`

```python
_validate_columns_exist(
    df: pd.DataFrame,
    required_columns: set[str],
    variable: str,
    mapping_dictionary: dict[str, str]
) -> bool
```

Validates that the required columns exist in the DataFrame after applying the mapping dictionary.

#### Parameters

- `df` : `pd.DataFrame`
  - DataFrame to validate.
- `required_columns` : `set[str]`
  - Set of required column names.
- `variable` : `str`
  - Variable column name to check.
- `mapping_dictionary` : `dict[str, str]`
  - Dictionary to map column names.

#### Returns

- `bool`
  - True if all required columns exist, False otherwise.

#### `_filter_dataframe`

```python
@staticmethod
_filter_dataframe(
    df: pd.DataFrame,
    required_columns: set[str],
    variable: str
) -> pd.DataFrame
```

Filters the DataFrame to include only the required columns and the variable column.

#### Parameters

- `df` : `pd.DataFrame`
  - DataFrame to filter.
- `required_columns` : `set[str]`
  - Set of required column names.
- `variable` : `str`
  - Variable column name to include.

#### Returns

- `pd.DataFrame`
  - Filtered DataFrame.

#### `_parse_datetime`

```python
_parse_datetime(series: pd.Series, column: str) -> pd.Series
```

Parses a Series to datetime format, coercing errors to NaT.

#### Parameters

- `series` : `pd.Series`
  - Series to parse.
- `column` : `str`
  - Column name for logging purposes.

#### Returns

- `pd.Series`
  - Parsed Series with datetime format.

#### `_filter_by_coordinates`

```python
_filter_by_coordinates(
    df: pd.DataFrame,
    start_lat: float | None,
    end_lat: float | None,
    start_lon: float | None,
    end_lon: float | None
) -> pd.DataFrame
```

Filters a DataFrame by geographic coordinates.

#### Parameters

- `df` : `pd.DataFrame`
  - DataFrame to filter.
- `start_lat` : `float | None`
  - Starting latitude.
- `end_lat` : `float | None`
  - Ending latitude.
- `start_lon` : `float | None`
  - Starting longitude.
- `end_lon` : `float | None`
  - Ending longitude.

#### Returns

- `pd.DataFrame`
  - Filtered DataFrame.

#### `_merge_dataframes`

```python
_merge_dataframes(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    merge_on: set[str],
    join_method: str
) -> pd.DataFrame
```

Merges two DataFrames on specified columns.

#### Parameters

- `df1` : `pd.DataFrame`
  - First DataFrame.
- `df2` : `pd.DataFrame`
  - Second DataFrame.
- `merge_on` : `set[str]`
  - Set of columns to merge on.
- `join_method` : `str`
  - Method to use for merging (e.g., 'inner', 'outer', 'left', 'right').

#### Returns

- `pd.DataFrame`
  - Merged DataFrame.

#### `_arrange_df`

```python
@staticmethod
_arrange_df(df: pd.DataFrame) -> pd.DataFrame
```

Sorts and arranges the DataFrame columns.

#### Parameters

- `df` : `pd.DataFrame`
  - DataFrame to arrange.

#### Returns

- `pd.DataFrame`
  - Arranged DataFrame.

#### `_read_df_from_csv`

```python
_read_df_from_csv(
    csv_file: str,
    index_col: str | None = None,
    sep: str = ','
) -> pd.DataFrame | None
```

Reads a CSV file into a DataFrame.

#### Parameters

- `csv_file` : `str`
  - Path to the CSV file.
- `index_col` : `str | None`, default=`None`
  - Column to set as the index.
- `sep` : `str`, default=`,``
  - Separator used in the CSV file.

#### Returns

- `pd.DataFrame | None`
  - DataFrame if successful, None otherwise.

#### `_save_as_csv`

```python
_save_as_csv(
    df: pd.DataFrame,
    file_path: str,
    index: bool = False
) -> None
```

Saves a DataFrame as a CSV file.

#### Parameters

- `df` : `pd.DataFrame`
  - DataFrame to save.
- `file_path` : `str`
  - Path to save the CSV file.
- `index` : `bool`, default=`False`
  - Whether to include the index in the CSV file.
