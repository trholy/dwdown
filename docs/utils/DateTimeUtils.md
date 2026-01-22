# Utils module
## DateHandler

### Overview

The `DateHandler` class provides methods for handling date-related operations, particularly parsing date strings and processing timesteps.

### Constructor

```python
DateHandler(log_handler: LogHandler | None = None)
```

#### Parameters

- `log_handler` : `LogHandler | None`, default=`None`
  - Optional LogHandler instance for logging.

### Methods

#### `_fix_date_format`

```python
@staticmethod
_fix_date_format(dates: list[str]) -> list[str]
```

Fixes the formatting of date strings.

#### Parameters

- `dates` : `list[str]`
  - List of date strings to be cleaned.

#### Returns

- `list[str]`
  - List of cleaned date strings.

#### `_parse_dates`

```python
_parse_dates(date_strings: list[str], date_pattern: str | None = None) -> list[datetime]
```

Parses date strings into datetime objects.

#### Parameters

- `date_strings` : `list[str]`
  - List of date strings to be parsed.
- `date_pattern` : `str | None`, default=`None`
  - The format pattern for parsing dates. Defaults to "%d-%b-%Y-%H:%M:%S".

#### Returns

- `list[datetime]`
  - List of parsed datetime objects.

#### `_process_timesteps`

```python
@staticmethod
_process_timesteps(
    min_timestep: str | int | None = None,
    max_timestep: str | int | None = None
) -> list[str]
```

Processes timesteps and returns a list of formatted timesteps.

#### Parameters

- `min_timestep` : `str | int | None`, default=`None`
  - Minimum timestep value. Defaults to 0.
- `max_timestep` : `str | int | None`, default=`None`
  - Maximum timestep value. Defaults to 48.

#### Returns

- `list[str]`
  - List of formatted timesteps (e.g., "_000_", "_001_").


## TimeHandler

### Overview

The `TimeHandler` class handles time-related operations, such as getting the current date and time in various formats.

### Constructor

```python
TimeHandler(date_format: str | None = None)
```

#### Parameters

- `date_format` : `str | None`, default=`None`
  - The default date format to use. Defaults to "%Y-%m-%d-%H:%M".

### Methods

#### `get_current_date`

```python
get_current_date(
    utc: bool = False,
    time_of_day: bool = False,
    date_format: str | None = None,
    convert_to_str: bool = True
) -> str | datetime
```

Gets the current date in a formatted string or datetime object.

#### Parameters

- `utc` : `bool`, default=`False`
  - Whether to use UTC time.
- `time_of_day` : `bool`, default=`False`
  - Whether to include the time of day.
- `date_format` : `str | None`, default=`None`
  - The date format to use. Defaults to the instance's date format.
- `convert_to_str` : `bool`, default=`True`
  - Whether to return the result as a string.

#### Returns

- `str | datetime`
  - Formatted date string or datetime object.

#### `_get_current_datetime`

```python
@staticmethod
_get_current_datetime(utc: bool) -> datetime
```

Gets the current datetime.

#### Parameters

- `utc` : `bool`
  - Whether to use UTC time.

#### Returns

- `datetime`
  - Current datetime.

#### `_format_datetime`

```python
@staticmethod
_format_datetime(dt: datetime, date_format: str) -> str
```

Formats a datetime object to a string.

#### Parameters

- `dt` : `datetime`
  - The datetime object to format.
- `date_format` : `str`
  - The date format to use.

#### Returns

- `str`
  - Formatted date string.
