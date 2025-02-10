import re
from datetime import datetime


def get_formatted_time_stamp(
        date: datetime
) -> str:

    if not isinstance(date, str):
        date = date.strftime("%Y-%m-%d %H:%M")

    return re.sub(r"[-:\s]", "_", date)


def get_current_date(
        utc: bool = True,
        time_of_day: bool = False
):
    """Get current system date."""
    if utc:
        now = datetime.utcnow()
    else:
        now = datetime.now()

    # Format as string and parse back to enforce "DD-MMM-YYYY-HH:MM" format
    formatted_str = now.strftime("%d-%b-%Y-%H:%M")
    formatted_datetime = datetime.strptime(formatted_str, "%d-%b-%Y-%H:%M")

    # If time_of_day is False, reset time to midnight
    return formatted_datetime if time_of_day\
        else formatted_datetime.replace(hour=0, minute=0)
