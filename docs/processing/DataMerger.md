# Processing module
## DataMerger

### Overview

The `DataMerger` class is responsible for processing and merging CSV files. It handles column validation, datetime parsing, filtering, and merging of DataFrames based on specified criteria.

### Constructor

```python
DataMerger(
    files_path: str,
    join_method: str | None = None,
    additional_patterns: dict[str, list[int] | int] | None = None,
    mapping_dictionary: dict[str, str] | None = None,
    required_columns: set[str] | None = None,
    log_files_path: str | None = None,
    sep: str = ',',
    index_col: str | None = None
)
```

#### Parameters

- `files_path` : `str`
  - Path to the directory containing input files.
- `join_method` : `str | None`, default=`None`
  - Method to use for merging dataframes (default is 'outer').
- `additional_patterns` : `dict[str, list[int] | int] | None`, default=`None`
  - Additional patterns for file filtering (optional).
- `mapping_dictionary` : `dict[str, str] | None`, default=`None`
  - Dictionary for mapping variable names (optional).
- `required_columns` : `set[str] | None`, default=`None`
  - Set of required columns in the dataframes (optional).
- `log_files_path` : `str | None`, default=`None`
  - Path to the directory for log files (optional).
- `sep` : `str`, default=`,``
  - Separator used in CSV files.
- `index_col` : `str | None`, default=`None`
  - Column to use as the index in dataframes (optional).

### Methods

#### `_process_dataframe`

```python
_process_dataframe(
    df: pd.DataFrame,
    variable_mapped: str,
    additional_pattern: str | None = None,
    skip_variable_validation: bool = False
) -> pd.DataFrame | None
```

Processes a DataFrame by validating columns, parsing datetime, renaming columns, and filtering the DataFrame.

#### Parameters

- `df` : `pd.DataFrame`
  - DataFrame to process.
- `variable_mapped` : `str`
  - Mapped variable name.
- `additional_pattern` : `str | None`, default=`None`
  - Additional pattern to append to the variable name.
- `skip_variable_validation` : `bool`, default=`False`
  - If True, skip column validation.

#### Returns

- `pd.DataFrame | None`
  - Processed DataFrame or None if validation fails and skipping is not allowed.

#### `merge`

```python
merge(
    time_step: str | int,
    variables: list[str],
    prefix: str | None = None,
    suffix: str | None = None,
    include_pattern: list[str] | None = None,
    exclude_pattern: list[str] | None = None,
    skip_time_step_filtering_variables: list[str] | None = None,
    skip_variable_validation: bool = False
) -> pd.DataFrame | None
```

Merges dataframes based on the provided parameters.

#### Parameters

- `time_step` : `str | int`
  - Time step for filtering files.
- `variables` : `list[str]`
  - List of variables to merge.
- `prefix` : `str | None`, default=`None`
  - Prefix for filtering filenames.
- `suffix` : `str | None`, default=`None`
  - Suffix for filtering filenames.
- `include_pattern` : `list[str] | None`, default=`None`
  - Patterns to include in filenames.
- `exclude_pattern` : `list[str] | None`, default=`None`
  - Patterns to exclude from filenames.
- `skip_time_step_filtering_variables` : `list[str] | None`, default=`None`
  - Variables to skip time step filtering.
- `skip_variable_validation` : `bool`, default=`False`
  - If True, use the last column of the DataFrame as the variable column.

#### Returns

- `pd.DataFrame | None`
  - Merged dataframe or None if no valid dataframes are found.

#### `_match_filenames_by_patterns`

```python
_match_filenames_by_patterns(filenames: list[str], variable: str) -> list[str] | None
```

Filters filenames based on additional patterns.

#### Parameters

- `filenames` : `list[str]`
  - List of filenames to filter.
- `variable` : `str`
  - Variable for which to filter filenames.

#### Returns

- `list[str] | None`
  - List of filtered filenames or None if no valid files are found.

#### `delete`

```python
delete() -> None
```

Deletes local files after successful processing.
