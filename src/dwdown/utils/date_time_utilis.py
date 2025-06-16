import re
import sys
from datetime import datetime

if sys.version_info >= (3, 11):
    from datetime import UTC
else:
    from datetime import timezone
    UTC = timezone.utc


class DateHandler:
    """
    A class to handle time-related operations, esp. date parsing.

    """

    def __init__(self):
        """
        Initializes the DateHandler.

        """

    @staticmethod
    def _fix_date_format(dates: list[str]) -> list[str]:
        """
        Fixes the formatting of date strings.

        :param dates: List of date strings to be cleaned.
        :return: List of cleaned date strings.
        """
        processed_dates = []

        for date in dates:
            date = date.strip()  # Remove leading/trailing whitespace

            # Fix formatting
            date = re.sub(r'(\d)([A-Za-z])', r'\1 \2', date)  # Space between number and letter
            date = re.sub(r'(\d) (\d)', r'\1-\2', date)  # Replace space with "-" between two numbers
            date = re.sub(r'\s{2,}\d+$', '', date)  # Remove trailing numbers if preceded by two spaces

            if date:  # Avoid empty entries
                processed_dates.append(date)

        return processed_dates

    def _parse_dates(
            self,
            date_strings: list[str],
            date_pattern: str | None = None
    ) -> list[datetime]:
        """
        Parses date strings into datetime objects.

        :param date_strings: List of date strings to be parsed.
        :param date_pattern: The format pattern for parsing dates.
        :return: List of parsed datetime objects.
        """
        date_pattern = date_pattern or "%d-%b-%Y-%H:%M:%S"
        parsed_dates = []

        for date in date_strings:
            try:
                parsed_dates.append(datetime.strptime(date, date_pattern))
            except ValueError as e:
                self._logger.info(f"Skipping invalid date format: {date} ({e})")

        return parsed_dates

    @staticmethod
    def _process_timesteps(
        min_timestep: str | int | None = None,
        max_timestep: str | int | None = None,
        include_pattern: list[str] | None = None
    ) -> list[str]:
        """
        Processes timesteps and returns a list of formatted timesteps.

        :param min_timestep: Minimum timestep value.
        :param max_timestep: Maximum timestep value.
        :param include_pattern: List of existing timesteps to extend.
        :return: List of formatted timesteps.
        """
        # Assign default values using `or`
        min_timestep = int(min_timestep) if min_timestep is not None else 0
        max_timestep = int(max_timestep) if max_timestep is not None else 48

        # Ensure they are integers
        if isinstance(min_timestep, int) and isinstance(max_timestep, int):
            pattern = [
                f"_{str(t).zfill(3)}_"
                for t in range(min_timestep, max_timestep + 1)]

            if isinstance(include_pattern, list):
                include_pattern.extend(pattern)
            else:
                include_pattern = pattern

        return include_pattern


class TimeHandler:
    """
    A class to handle time-related operations.

    """

    def __init__(self, date_format: str | None = None):
        """
        Initializes the TimeHandler.

        """
        self._date_format = date_format or "%d-%m-%Y-%H:%M"

    def _get_current_date(
            self,
            utc: bool = True,
            time_of_day: bool = False,
            date_format: str | None = None
    ) -> str:
        """
        Gets the current date in a formatted string.

        :param utc: Whether to use UTC time.
        :param time_of_day: Whether to include the time of day.
        :return: Formatted date string.
        """
        date_format = date_format or self._date_format

        # Get current datetime
        if utc:
            now = datetime.now(timezone.utc)
        else:
            now = datetime.now()

        # Format as string and parse back to enforce "DD-MMM-YYYY-HH:MM" format
        formatted_str = now.strftime(date_format)
        formatted_datetime = datetime.strptime(formatted_str, date_format)

        # If time_of_day is False, reset time to midnight
        if not time_of_day:
            formatted_datetime = formatted_datetime.replace(hour=0, minute=0)

        # Convert datetime object to string using strftime()
        formatted_date_str = formatted_datetime.strftime(date_format)

        return formatted_date_str
