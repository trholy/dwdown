import logging
import os
import re
from datetime import datetime


class LogHandler:
    """
    A class to handle logging for other classes.

    Attributes:
        _log_file_path (str): The directory path where log files will be stored.
        _log_to_console (bool): Whether to log to the console.
        _log_to_file (bool): Whether to log to a file.
    """

    def __init__(
        self,
        log_file_path: str | None = None,
        log_to_console: bool = True,
        log_to_file: bool = True
    ):
        """
        Initializes the LogHandler with specified logging options.

        :param log_file_path: The directory path where log files will be stored.
        :param log_to_console: Whether to log to the console.
        :param log_to_file: Whether to log to a file.
        """

        self._log_file_path = log_file_path or "log_files"
        self._log_to_console = log_to_console
        self._log_to_file = log_to_file

        self._logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """
        Sets up and returns a logger with console and/or file handlers based on the configuration.

        :return: Configured logger.
        """
        # Create formatters: simpler for console, detailed for file
        console_formatter = logging.Formatter('%(levelname)s: %(message)s')
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # Create a logger and set its level to INFO
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.INFO)

        # Set up console handler if enabled
        if self._log_to_console:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(console_formatter)
            logger.addHandler(console_handler)

        # Set up file handler if enabled
        time_stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        if self._log_to_file:
            log_file_name = f"{self.__class__.__name__}_{time_stamp}.log"
            log_file_path = os.path.join(self._log_file_path, log_file_name)

            try:
                file_handler = logging.FileHandler(log_file_path)
                file_handler.setFormatter(file_formatter)
                logger.addHandler(file_handler)
            except Exception as e:
                raise RuntimeError(f"Failed to create log file {log_file_path}: {e}")

        return logger

    def get_logger(self) -> logging.Logger:
        """
        Returns the configured logger.

        :return: Configured logger.
        """
        return self._logger

    def _write_log_file(
            self,
            files: list[str],
            file_category: str,
            variable_name: str = ""
    ) -> None:
        """
        Logs a list of files to a log file with a timestamp.

        :param files: List of file paths to log.
        :param file_category: Type of files being logged (e.g., "uploaded_files", "corrupted_files").
        """
        if isinstance(variable_name, str):
            variable_name = variable_name + '_'

        time_stamp = self.get_current_date(time_of_day=True, convert_to_str=True)
        formatted_time_stamp = re.sub(r"[-:\s]", "_", time_stamp)
        log_file_name = (f"{self._log_file_path}/{self.__class__.__name__}_"
                         f"{variable_name}{file_category}_{formatted_time_stamp}.log")

        try:
            with open(log_file_name, "w", encoding="utf-8") as file:
                file.write("\n".join(files) + "\n")
            self._logger.info(f"Saved log: {log_file_name} ({len(files)} entries)")
        except Exception as e:
            self._logger.error(f"Error writing log file {log_file_name}: {e}")
