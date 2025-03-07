# Tools module
## Helper Functions

### `get_formatted_time_stamp`

```python
get_formatted_time_stamp(date: datetime) -> str
```

Converts a `datetime` object to a formatted string with underscores replacing hyphens, colons, and spaces.

### Parameters

- `date` : `datetime`
  - The `datetime` object to format.

### Returns

- `str`
  - Formatted string with underscores replacing hyphens, colons, and spaces.

### Example

```python
from datetime import datetime

date = datetime(2023, 10, 5, 14, 30)
formatted_date = get_formatted_time_stamp(date)
print(formatted_date)  # Output: "2023_10_05_14_30"
```

#### `get_current_date`

```python
get_current_date(utc: bool = True, time_of_day: bool = False) -> datetime
```

Get the current system date, formatted as "DD-MMM-YYYY-HH:MM".

#### Parameters

- `utc` : `bool`, default=`True`
  - If `True`, return date with UTC time; otherwise, return system time.
- `time_of_day` : `bool`, default=`False`
  - If `True`, return date with time; otherwise, return only the date.

#### Returns

- `datetime`
  - Current date (with or without time) in formatted datetime format.

#### Example

```python
from datetime import datetime

# Get current UTC date with time
current_date_utc = get_current_date(utc=True, time_of_day=True)
print(current_date_utc)  # Output: "05-Oct-2023-14:30"

# Get current system date without time
current_date_system = get_current_date(utc=False, time_of_day=False)
print(current_date_system)  # Output: "05-Oct-2023-00:00"
```

---
