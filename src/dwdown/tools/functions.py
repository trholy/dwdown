import re
import sys
from datetime import datetime
from typing import Optional

if sys.version_info >= (3, 11):
    from datetime import UTC
else:
    from datetime import timezone
    UTC = timezone.utc


def get_formatted_time_stamp(
        date: datetime
) -> str:

    if not isinstance(date, str):
        date = date.strftime("%Y-%m-%d %H:%M")

    return re.sub(r"[-:\s]", "_", date)


def get_current_date(
        utc: bool = True,
        time_of_day: bool = False,
        overwrite: bool = False,
        date_string: Optional[str] = None
) -> datetime:
    """
    Get the current system date and time.

    Parameters:
        utc (bool): Whether to use UTC time. Defaults to True.
        time_of_day (bool): Whether to retain the current time of day.
         Defaults to False.
        overwrite (bool): Whether to use a provided date string instead
         of the current date.
        date_string (Optional[str]): The date string to use if overwrite
         is True. Must be in 'YYYY-MM-DD HH:MM:SS' format.

    Returns:
        datetime: The resulting datetime object.
    """

    if overwrite:
        if date_string is None:
            raise ValueError(
                "date_string must be provided when overwrite is True.")
        dt = datetime.strptime(date_string, "%Y-%m-%d %H:%M")
    else:
        dt = datetime.now(UTC) if utc else datetime.now()
        dt = dt.replace(tzinfo=None) if utc else dt

    return dt if time_of_day else dt.replace(
        hour=0, minute=0, second=0, microsecond=0
    )
