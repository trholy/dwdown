import os
import unittest
from unittest.mock import MagicMock, mock_open, patch

from dwdown.utils import LogHandler


class TestLogHandler(unittest.TestCase):
    def setUp(self):
        self.mock_timehandler = MagicMock()
        self.test_log_dir = "test_logs"
        self.fixed_time = "2024-01-01 12:00:00"

    @patch('dwdown.utils.log_handling.logging.FileHandler')
    @patch('dwdown.utils.log_handling.os.path.join', return_value="mock_path.log")
    @patch('dwdown.utils.log_handling.logging.getLogger')
    def test_init_with_defaults(self, mock_get_logger, mock_path_join, mock_file_handler):
        # Tests that LogHandler initializes correctly with default parameters
        logger_mock = MagicMock()
        mock_get_logger.return_value = logger_mock

        handler = LogHandler(timehandler=self.mock_timehandler)

        self.assertEqual(handler._log_file_path, "log_files")
        self.assertTrue(handler._log_to_console)
        self.assertTrue(handler._log_to_file)
        self.assertEqual(handler._logger, logger_mock)
        self.assertEqual(handler._timehandler, self.mock_timehandler)

    @patch('dwdown.utils.log_handling.logging.getLogger')
    def test_init_with_custom_params(self, mock_get_logger):
        # Tests that LogHandler initializes correctly with custom parameters
        logger_mock = MagicMock()
        mock_get_logger.return_value = logger_mock

        handler = LogHandler(
            timehandler=self.mock_timehandler,
            log_file_path="custom_logs",
            log_to_console=False,
            log_to_file=False
        )

        self.assertEqual(handler._log_file_path, "custom_logs")
        self.assertFalse(handler._log_to_console)
        self.assertFalse(handler._log_to_file)
        self.assertEqual(handler._logger, logger_mock)

    @patch('dwdown.utils.log_handling.logging.FileHandler')
    @patch('dwdown.utils.log_handling.os.path.join', return_value="mock_path.log")
    @patch('dwdown.utils.log_handling.logging.getLogger')
    def test_setup_logger_with_file_and_console(self, mock_get_logger, mock_path_join, mock_file_handler):
        # Tests that logger is configured with both file and console handlers when enabled
        logger_instance = MagicMock()
        mock_get_logger.return_value = logger_instance

        # Trigger the logger setup logic via instantiation
        LogHandler(
            timehandler=self.mock_timehandler,
            log_file_path=self.test_log_dir,
            logger_name="TestLogger",
            log_to_console=True,
            log_to_file=True
        )

        self.assertTrue(logger_instance.addHandler.called)
        self.assertGreaterEqual(logger_instance.addHandler.call_count, 2)  # Console and file handlers

    @patch('dwdown.utils.log_handling.logging.FileHandler', side_effect=OSError("Permission denied"))
    @patch('dwdown.utils.log_handling.os.path.join',  return_value="fail_path.log")
    @patch('dwdown.utils.log_handling.logging.getLogger')
    def test_setup_logger_file_error_raises(self, mock_get_logger, mock_path_join, mock_file_handler):
        # Tests that LogHandler raises RuntimeError when file handler creation fails
        with self.assertRaises(RuntimeError) as context:
            LogHandler(
                timehandler=self.mock_timehandler,
                log_file_path="logs",
                log_to_console=False,
                log_to_file=True
            )
        self.assertIn("Failed to create log file", str(context.exception))

    @patch('dwdown.utils.log_handling.logging.FileHandler')
    @patch('dwdown.utils.log_handling.os.path.join', return_value="mock_path.log")
    @patch('dwdown.utils.log_handling.logging.getLogger')
    def test_get_logger_returns_logger(self, mock_get_logger, mock_path_join, mock_file_handler):
        # Tests that get_logger() method returns the internal logger instance
        logger_instance = MagicMock()
        mock_get_logger.return_value = logger_instance
        handler = LogHandler(timehandler=self.mock_timehandler, logger_name="TestLogger")
        self.assertIs(handler.get_logger(), logger_instance)
        # Verify that getLogger was called with the correct name
        mock_get_logger.assert_called_with("TestLogger")

    @patch('dwdown.utils.log_handling.open', new_callable=mock_open)
    @patch('dwdown.utils.log_handling.logging.getLogger')
    def test_write_log_file_success(self, mock_get_logger, mock_open_file):
        # Tests that _write_log_file writes correct content to the expected log file successfully
        logger_instance = MagicMock()
        mock_get_logger.return_value = logger_instance
        
        # Configure mock timehandler
        self.mock_timehandler.get_current_date.return_value = self.fixed_time

        handler = LogHandler(
            timehandler=self.mock_timehandler,
            log_to_file=False,
            log_file_path=self.test_log_dir,
            logger_name="TestLogger"
        )  # to avoid file handler creation logic affecting this test

        test_files = ["file1.txt", "file2.txt"]
        handler._write_log_file(test_files, "test_category", "var")

        # The LogHandler replaces chars in timestamp for filename
        # 2024-01-01 12:00:00 -> 2024_01_01_12_00_00
        expected_filename = os.path.normpath(os.path.join(
            self.test_log_dir,
            f"TestLogger_var_test_category_{self.fixed_time.replace('-', '_').replace(' ', '_').replace(':', '_')}.log"))

        mock_open_file.assert_called_once_with(expected_filename, "w", encoding="utf-8")
        handle = mock_open_file()
        handle.write.assert_called_once_with("file1.txt\nfile2.txt\n")

        logger_instance.info.assert_called_once()

    @patch('dwdown.utils.log_handling.open', side_effect=OSError("disk full"))
    @patch('dwdown.utils.log_handling.logging.getLogger')
    def test_write_log_file_write_error(self, mock_get_logger, mock_open_file):
        # Tests that _write_log_file logs an error when file writing fails due to IOError
        logger_instance = MagicMock()
        mock_get_logger.return_value = logger_instance
        
        self.mock_timehandler.get_current_date.return_value = "2024-01-01 12:00:00"
        
        handler = LogHandler(
            timehandler=self.mock_timehandler,
            log_to_file=False
        )

        test_files = ["file1.txt"]
        handler._write_log_file(test_files, "errors", "var")

        logger_instance.error.assert_called_once()


if __name__ == '__main__':
    unittest.main()
