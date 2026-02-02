# Utils module
## FileHandler

### Overview

The `FileHandler` class handles file operations such as directory creation, file filtering, MD5 calculation, file deletion, and directory cleanup.

### Constructor

```python
FileHandler(log_handler: LogHandler, utilities: Utilities = Utilities())
```

#### Parameters

- `log_handler` : `LogHandler`
  - LogHandler instance.
- `utilities` : `Utilities`, default=`Utilities()`
  - Utilities instance.

### Methods

#### `_ensure_directory_exists`

```python
@staticmethod
_ensure_directory_exists(path: str) -> None
```

Ensures that the specified directory exists, creating it if necessary.

#### Parameters

- `path` : `str`
  - The directory path to ensure.

#### `_ensure_directories_exist`

```python
_ensure_directories_exist(paths: list[str]) -> None
```

Ensures that all specified directories exist.

#### Parameters

- `paths` : `list[str]`
  - A list of paths to the directories to ensure existence.

#### `_search_directory`

```python
_search_directory(directory: str, suffix: str = "") -> list[str]
```

Searches a directory for files recursively.

#### Parameters

- `directory` : `str`
  - Directory to search.
- `suffix` : `str`, default=`""`
  - Suffix to filter files by (e.g., ".csv").

#### Returns

- `list[str]`
  - List of file paths.

#### `_simple_filename_filter`

```python
_simple_filename_filter(
    filenames: list[str],
    prefix: str = "",
    suffix: str = "",
    include_pattern: list[str] | None = None,
    exclude_pattern: list[str] | None = None,
    skip_time_step_filtering_variables: list[str] | None = None,
    timesteps: list[str] | None = None,
    norm_path: bool = True,
    use_all_for_include: bool = True,
    mock_time_steps: bool = False
) -> list[str]
```

Filters filenames based on specified criteria.

#### Parameters

- `filenames` : `list[str]`
  - List of filenames to filter.
- `prefix` : `str`, default=`""`
  - Prefix to filter filenames by.
- `suffix` : `str`, default=`""`
  - Suffix to filter filenames by.
- `include_pattern` : `list[str] | None`, default=`None`
  - List of patterns to include in filenames.
- `exclude_pattern` : `list[str] | None`, default=`None`
  - List of patterns to exclude from filenames.
- `skip_time_step_filtering_variables` : `list[str] | None`, default=`None`
  - List of variables to skip time step filtering.
- `timesteps` : `list[str] | None`, default=`None`
  - List of timesteps to include.
- `norm_path` : `bool`, default=`True`
  - Whether to normalize paths.
- `use_all_for_include` : `bool`, default=`True`
  - Check if filename matches patterns using either 'all' or 'any' logic.
- `mock_time_steps` : `bool`, default=`False`
  - If `True`, returns `True` regardless of timesteps.

#### Returns

- `list[str]`
  - List of filtered filenames.

#### `_switchable_pattern_check`

```python
@staticmethod
_switchable_pattern_check(
    filename: str,
    patterns: list[str] | None,
    use_all: bool = True
) -> bool
```

Check if filename matches patterns using either 'all' or 'any' logic.

#### Parameters

- `filename` : `str`
  - The filename to check against patterns.
- `patterns` : `list[str] | None`
  - List of pattern strings to search for in filename.
- `use_all` : `bool`, default=`True`
  - If `True`, all patterns must be present in filename. If `False`, at least one pattern must be present.

#### Returns

- `bool`
  - `True` if the filename matches the pattern requirements, `False` otherwise.

#### `_mock_time_steps`

```python
@staticmethod
_mock_time_steps(
    filename: str,
    timesteps: list[str] | None,
    mock_time_steps: bool = False
) -> bool
```

Checks if filename matches any of the provided timesteps or if mocking is enabled.

#### Parameters

- `filename` : `str`
  - The filename to check.
- `timesteps` : `list[str] | None`
  - List of timesteps to check against.
- `mock_time_steps` : `bool`, default=`False`
  - If `True`, returns `True` regardless of timesteps.

#### Returns

- `bool`
  - `True` if matches or mocked, `False` otherwise.

#### `_advanced_filename_filter`

```python
@staticmethod
_advanced_filename_filter(
    filenames: list[str],
    variables: set[str] | list[str] | None = None,
    patterns: dict[str, set[int] | list[int]] | None = None
) -> list[str]
```

Filters remote files based on specified variables and patterns.

#### Parameters

- `filenames` : `list[str]`
  - List of filenames to filter.
- `variables` : `set[str] | list[str] | None`, default=`None`
  - List of variables to filter by.
- `patterns` : `dict[str, set[int] | list[int]] | None`, default=`None`
  - Dictionary of patterns to filter by.

#### Returns

- `list[str]`
  - List of filtered filenames.

#### `_calculate_md5`

```python
@staticmethod
_calculate_md5(file_path: str) -> str
```

Calculates the MD5 hash of a file.

#### Parameters

- `file_path` : `str`
  - The path to the file.

#### Returns

- `str`
  - The MD5 hash of the file.

#### `_delete_files_safely`

```python
_delete_files_safely(files: list[str], label: str = "file") -> None
```

Deletes files safely, handling potential errors.

#### Parameters

- `files` : `list[str]`
  - List of file paths to delete.
- `label` : `str`, default=`"file"`
  - Label for the type of file being deleted (for logging).

#### `_cleanup_empty_dirs`

```python
_cleanup_empty_dirs(base_path: str) -> None
```

Cleans up empty directories within the specified base path.

#### Parameters

- `base_path` : `str`
  - The base directory path to clean up.
