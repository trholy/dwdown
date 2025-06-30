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
        max_timestep: str | int | None = None
    ) -> list[str]:
        """
        Processes timesteps and returns a list of formatted timesteps.

        :param min_timestep: Minimum timestep value.
        :param max_timestep: Maximum timestep value.
        :return: List of formatted timesteps.
        """
        # Assign default values using `or`
        min_timestep = int(min_timestep) if min_timestep is not None else 0
        max_timestep = int(max_timestep) if max_timestep is not None else 48

        # Ensure they are integers
        if (not isinstance(min_timestep, int)
                or not isinstance(max_timestep, int)):
            raise ValueError(
                "min_timestep and max_timestep must be convertible to integers.")

        # Generate the list of formatted timesteps
        return [
            f"_{str(t).zfill(3)}_"
            for t in range(min_timestep, max_timestep + 1)
        ]


class TimeHandler:
    """
    A class to handle time-related operations.

    """

    def __init__(self, date_format: str | None = None):
        """
        Initializes the TimeHandler.

        :param date_format: The default date format to use.
        """
        self._date_format = date_format or "%d-%m-%Y-%H:%M"

    def get_current_date(
            self,
            utc: bool = True,
            time_of_day: bool = False,
            date_format: str | None = None,
            convert_to_str: bool = True
    ) -> str | datetime:
        """
        Gets the current date in a formatted string or datetime object.

        :param utc: Whether to use UTC time.
        :param time_of_day: Whether to include the time of day.
        :param date_format: The date format to use. Defaults to the instance's date format.
        :param convert_to_str: Whether to return the result as a string.
        :return: Formatted date string or datetime object.
        """
        date_format = date_format or self._date_format
        now = self._get_current_datetime(utc)

        if not time_of_day:
            now = now.replace(hour=0, minute=0, second=0, microsecond=0)

        if convert_to_str:
            return self._format_datetime(now, date_format)

        return now

    @staticmethod
    def _get_current_datetime(utc: bool) -> datetime:
        """
        Gets the current datetime.

        :param utc: Whether to use UTC time.
        :return: Current datetime.
        """
        return datetime.now(UTC) if utc else datetime.now()

    @staticmethod
    def _format_datetime(dt: datetime, date_format: str) -> str:
        """
        Formats a datetime object to a string.

        :param dt: The datetime object to format.
        :param date_format: The date format to use.
        :return: Formatted date string.
        """
        try:
            return dt.strftime(date_format)
        except ValueError as e:
            raise ValueError(f"Invalid date format: {date_format}") from e
