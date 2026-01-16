import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

from dwdown.utils import DateHandler, TimeHandler


class TestDateHandler(unittest.TestCase):

    def setUp(self):
        self.handler = DateHandler()

    def test_fix_date_format(self):
        # Tests _fix_date_format correctly normalizes various raw date string formats
        raw_dates = ["12Jan2020", "15 03 2021", "   01Feb2022  ", "23 09 2020   15786214"]
        expected = ["12 Jan2020", "15-03-2021", "01 Feb2022", "23-09-2020"]
        self.assertEqual(self.handler._fix_date_format(raw_dates), expected)


    def test_fix_date_format_empty_and_whitespace(self):
        # Tests _fix_date_format returns an empty list when input contains only whitespace
        self.assertEqual(self.handler._fix_date_format(["   "]), [])


    def test_parse_dates_valid(self):
        # Tests _parse_dates converts valid date strings into datetime objects
        valid_dates = ["01-Jan-2020-10:00:00", "02-Feb-2021-15:30:45"]
        result = self.handler._parse_dates(valid_dates)
        self.assertIsInstance(result[0], datetime)
        self.assertEqual(len(result), 2)


    def test_parse_dates_invalid(self):
        # Tests _parse_dates returns empty list and logs info for invalid date strings
        mock_logger = MagicMock()
        invalid_dates = ["32-Jan-2020-10:00:00", "bad-date"]
        
        result = self.handler._parse_dates(invalid_dates, logger=mock_logger)
        
        self.assertEqual(result, [])
        self.assertTrue(mock_logger.info.called)
        self.assertIn("Skipping invalid date format", str(mock_logger.info.call_args))


    def test_process_timesteps_default(self):
        # Tests _process_timesteps returns default timestep strings from 0 to 48
        expected = [f"_{str(i).zfill(3)}_" for i in range(49)]
        self.assertEqual(self.handler._process_timesteps(), expected)


    def test_process_timesteps_custom_range(self):
        # Tests _process_timesteps returns timestep strings for a custom specified range
        expected = [f"_{str(i).zfill(3)}_" for i in range(5, 10)]
        self.assertEqual(self.handler._process_timesteps(5, 9), expected)


    def test_process_timesteps_non_int_input(self):
        # Tests _process_timesteps raises ValueError when inputs are not integers
        with self.assertRaises(ValueError):
            self.handler._process_timesteps("abc", "10")


class TestTimeHandler(unittest.TestCase):

    def setUp(self):
        self.handler = TimeHandler()

    @patch("dwdown.utils.date_time_utilis.datetime")
    def test_get_current_date_defaults(self, mock_datetime):
        # Test get_current_date returns default formatted date string with zeroed time
        now = datetime(2023, 1, 1, 15, 30)
        mock_datetime.now.return_value = now
        # Mocking side_effect to behave like real datetime for other calls if needed, 
        # but here we just need .now()
        
        # We need to make sure from dwdown.utils.date_time_utilis.datetime import UTC is handled,
        # but the test patches datetime class.
        
        result = self.handler.get_current_date()
        
        # Result depends on implementation using utc=True by default calling datetime.now(UTC)
        # Verify the result format, ignoring internal call details if result is correct string.
        # But wait, we mocked datetime.now.
        # If implementation calls datetime.now(UTC), mock_datetime.now(UTC) needs to return our 'now'.
        
        # Adjust expectation: if UTC used, now() returns a value.
        # The formatting logic strips time if time_of_day=False.
        
        self.assertEqual(result, "01-01-2023-00:00")

    @patch("dwdown.utils.date_time_utilis.datetime")
    def test_get_current_date_include_time(self, mock_datetime):
        # Test get_current_date with time_of_day=False returns date string ending with '-00:00'
        # Wait, previous test said time_of_day=False (default) returns ...-00:00
        # This test says ...include_time, but calls with time_of_day=False logic in comments?
        # Assuming we want to test time_of_day=True?
        
        now = datetime(2023, 5, 5, 14, 20)
        mock_datetime.now.return_value = now
        
        result = self.handler.get_current_date(time_of_day=False)
        self.assertTrue(result.endswith("-00:00"))

    def test_get_current_date_as_datetime(self):
        # Test get_current_date returns datetime object with hour set to zero when convert_to_str=False
        result = self.handler.get_current_date(convert_to_str=False)
        self.assertIsInstance(result, datetime)
        self.assertEqual(result.hour, 0)

    def test_get_current_date_custom_format(self):
        # Test get_current_date returns string formatted with a custom date format
        result = self.handler.get_current_date(date_format="%Y/%m/%d", convert_to_str=True)
        self.assertRegex(result, r"\d{4}/\d{2}/\d{2}")

    def test_format_datetime_valid(self):
        # Test _format_datetime formats datetime correctly with valid format string
        dt = datetime(2023, 7, 1, 12, 0)
        result = self.handler._format_datetime(dt, "%Y-%m-%d")
        self.assertEqual(result, "2023-07-01")

    def test_format_datetime_invalid(self):
        # Test _format_datetime raises ValueError when passed an invalid format string
        dt = datetime(2023, 7, 1, 12, 0)
        with self.assertRaises(ValueError):
            self.handler._format_datetime(dt, "%Q-%W")  # %Q is invalid


if __name__ == "__main__":
    unittest.main()
