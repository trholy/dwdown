# Utils module
## LogHandler

### Overview

The `LogHandler` class handles logging configurations, including console and file logging setup.

### Constructor

```python
LogHandler(
    timehandler: TimeHandler | None,
    log_file_path: str | None = None,
    logger_name: str | None = None,
    log_to_console: bool = True,
    log_to_file: bool = True
)
```

#### Parameters

- `timehandler` : `TimeHandler | None`
  - TimeHandler instance.
- `log_file_path` : `str | None`, default=`None`
  - The directory path where log files will be stored. Defaults to "log_files".
- `logger_name` : `str | None`, default=`None`
  - The name of the logger (usually the class name).
- `log_to_console` : `bool`, default=`True`
  - Whether to log to the console.
- `log_to_file` : `bool`, default=`True`
  - Whether to log to a file.

### Methods

#### `_setup_logger`

```python
_setup_logger() -> logging.Logger
```

Sets up and returns a logger with console and/or file handlers based on the configuration.

#### Returns

- `logging.Logger`
  - Configured logger.

#### `get_logger`

```python
get_logger() -> logging.Logger
```

Returns the configured logger.

#### Returns

- `logging.Logger`
  - Configured logger.

#### `_write_log_file`

```python
_write_log_file(
    files: list[str],
    file_category: str,
    variable_name: str = ""
) -> None
```

Logs a list of files to a separate log file with a timestamp.

#### Parameters

- `files` : `list[str]`
  - List of file paths to log.
- `file_category` : `str`
  - Type of files being logged (e.g., "uploaded_files", "corrupted_files").
- `variable_name` : `str`, default=`""`
  - Optional variable name to include in the filename.
