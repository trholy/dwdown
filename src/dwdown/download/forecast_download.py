import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from urllib.parse import urljoin, urlparse

import requests
from lxml import html

from ..utils import (
    DateHandler,
    FileHandler,
    LogHandler,
    SessionHandler,
    TimeHandler,
    Utilities
)


class ForecastDownloader(
    Utilities, LogHandler, FileHandler, TimeHandler, DateHandler, SessionHandler
):
    def __init__(
            self,
            model: str | None = None,
            forecast_run: str| None = None,
            variable: str| None = None,
            grid: str| None = None,
            files_path: str | None = None,
            log_files_path: str | None = None,
            delay: int | float = 1,
            n_jobs: int = 1,
            retry: int = 0,
            timeout: int = 30,
            url: str | None = None,
            base_url: str | None = None,
            xpath_files: str | None = None,
            xpath_meta_data: str | None = None
    ):
        """
        Initializes the ForecastDownloader with the URL and an optional delay.

        :param url: Full URL to fetch data from (following five parameters are
         NOT needed and will be overwritten).
        :param base_url: Base URL to fetch data from (following four parameters
         are needed to build full URL).
        :param model: The NWP model name, e.g. icon-d2, icon-eu, ...
        :param grid: The model grid [regular-lat-lon | icosahedral].
        :param forecast_run: The forecast run in the 3-hourly assimilation
         cycle, e.g. 00, 03, 06, ..., 21.
        :param variable: The single-level model fields that should be
         downloaded, e.g. t_2m, tmax_2m, clch, pmsl, ...
        :param retry: If True, retry failed downloads sequentially.
        :param timeout: Timeout for both the connect and the read timeouts.
        :param delay: Optional delay between downloads (in seconds).
        :param n_jobs: Number of worker threads for parallel downloading.
        :param files_path: Directory to save downloaded files.
        :param log_files_path: Directory to save log files.
        :param xpath_files: XPath expression to extract filenames from the HTML.
        :param xpath_meta_data: XPath expression to extract meta data strings
         from the HTML.
        """
        self.model = model
        self.forecast_run = forecast_run
        self.variable = variable
        self.grid = grid

        self.files_path = os.path.normpath(files_path or "download_files")
        self.log_files_path = os.path.normpath(log_files_path or "log_files")

        self._delay = delay
        self._n_jobs = n_jobs
        self._retry = retry
        self._timeout = timeout

        self._xpath_files = xpath_files or "/html/body/pre//a/@href"
        self._xpath_meta_data = xpath_meta_data or "//pre/text()"

        FileHandler.__init__(self)
        self._ensure_directories_exist([self.files_path, self.log_files_path])

        LogHandler.__init__(self, self.log_files_path, True, True)
        self._logger = self.get_logger()

        TimeHandler.__init__(self)
        DateHandler.__init__(self)
        Utilities.__init__(self)

        SessionHandler.__init__(
            self,
            num_retries=5,
            backoff_factor=2,
            status_forcelist=(429, 500, 502, 503, 504)
        )
        self._session = self.get_session()

        self._base_url = base_url or "https://opendata.dwd.de/weather/nwp"
        if url:
            self.url = url
        elif all((self._base_url, self.model, self.forecast_run, self.variable)):
            self.url = f"{self._base_url}/{self.model}/grib/{self.forecast_run}/{self.variable}/"
        else:
            raise ValueError(
                "Either a full URL (:param 'url') or all components"
                " (:param 'base_url', 'model', 'forecast_run', 'variable')"
                " must be provided.")

        self.downloaded_files = []
        self._downloaded_files_paths = []
        self.failed_files = []

        self.download_links = []
        self._raw_filenames = []
        self.filtered_filenames = []

    def _get_filenames_from_url(self) -> list[str]:
        """
        Fetches filenames from the URL using an XPath expression.

        :return: A list of filenames extracted from the URL.
         Returns an empty list if the request fails or no filenames are found.
        """
        try:
            response = self._session.get(self.url, timeout=self._timeout)
            response.raise_for_status()
            tree = html.fromstring(response.content)
            filenames = tree.xpath(self._xpath_files)
            return filenames
        except requests.exceptions.RequestException as e:
            self._logger.error(f"Failed to fetch filenames from {self.url}: {e}")
            return []

    @staticmethod
    def _get_variable_from_link(link: str) -> str:
        """
        Extracts the variable name from the link using URL parsing.

        :param link: The URL from which to extract the variable name.
        :return: The extracted variable name. Returns an empty string if the
         variable name cannot be determined.
        """
        parsed_url = urlparse(link)
        path_components = parsed_url.path.strip('/').split('/')
        # Assuming the variable name is the second last component in the path
        return path_components[-2] if len(path_components) > 1 else ''

    def get_data_dates(
            self,
            url: str | None = None,
            date_pattern: str | None = None
    ) -> tuple[datetime, datetime]:
        """
        Fetches and parses dates from the URL.

        :param url: The URL from which to fetch the dates. If None, uses the
         default URL of the instance.
        :param date_pattern: A regex pattern to use for parsing dates.
         If None, uses a default pattern.
        :return: A tuple containing the minimum and maximum dates parsed
         from the URL.
        """
        response = self._session.get(url or self.url, timeout=self._timeout)
        response.raise_for_status()

        tree = html.fromstring(response.content)
        raw_meta_data = tree.xpath(self._xpath_meta_data)

        fixed_dates = self._fix_date_format(raw_meta_data)
        cleaned_dates = [
            re.sub(r'\s+', '', date.replace(' -', ''))
            for date in fixed_dates]

        parsed_dates = self._parse_dates(cleaned_dates, date_pattern)

        return min(parsed_dates), max(parsed_dates)

    @staticmethod
    def _set_grid_filter(grid: str | None) -> list:
        """
        Sets the grid filter based on the provided grid type.

        :param grid: The type of grid, either 'icosahedral' or 'regular'.
        :return: A list containing the grid type if valid, otherwise an empty list.
        :raises ValueError: If the grid type is not 'icosahedral' or 'regular'.
        """
        if grid is None:
            return []
        elif grid in ['icosahedral', 'regular']:
            return [grid]
        else:
            raise ValueError(
                f"Parameter 'grid' must be either 'icosahedral' or 'regular'."
                f" Got {grid}")

    def get_links(
            self,
            prefix: str | None = None,
            suffix: str | None = None,
            include_pattern: list[str] | None = None,
            exclude_pattern: list[str] | None = None,
            additional_patterns: dict | None = None,
            skip_time_step_filtering_variables: list[str] | None = None,
            min_timestep: str | int | None = None,
            max_timestep: str | int | None = None
    ) -> list[str]:
        """
        Generates download links based on the provided filters.

        :param prefix: The prefix that filenames should start with.
         Defaults to "icon-d2_germany".
        :param suffix: The suffix that filenames should end with.
         Defaults to ".bz2".
        :param include_pattern: A list of regex patterns that filenames should
         match. If None, no inclusion filtering is applied.
        :param exclude_pattern: A list of regex patterns that filenames should
         not match. If None, no exclusion filtering is applied.
        :param additional_patterns: Additional patterns for filtering.
        :param skip_time_step_filtering_variables: Variables to skip timestep filtering.
        :param min_timestep: The minimum timestep to include in the filenames.
         If None, no minimum timestep filtering is applied.
        :param max_timestep: The maximum timestep to include in the filenames.
         If None, no maximum timestep filtering is applied.
        :return: A list of download links that match the specified filters.
        """
        prefix = prefix or self.model
        suffix = suffix or ".grib2.bz2"

        include_pattern = self._string_to_list(include_pattern)
        exclude_pattern = self._string_to_list(exclude_pattern)

        include_pattern += self._set_grid_filter(self.grid)

        filenames = self._get_filenames_from_url()
        if not filenames:
            return []

        timesteps = self._process_timesteps(
            min_timestep=min_timestep,
            max_timestep=max_timestep
        )

        filtered_filenames = self._simple_filename_filter(
            filenames=filenames,
            prefix=prefix,
            suffix=suffix,
            include_pattern=include_pattern,
            exclude_pattern=exclude_pattern,
            skip_time_step_filtering_variables=skip_time_step_filtering_variables,
            timesteps=timesteps
        )

        filtered_filenames = [
            urljoin(self.url, file) for file in filtered_filenames
        ]

        filtered_filenames = self._advanced_filename_filter(
            filenames=filtered_filenames,
            patterns=additional_patterns,
            variables=self.variable if self.variable is None
            else self._string_to_list(self.variable)
        )

        self.download_links = filtered_filenames
        self._raw_filenames = filenames
        self.filtered_filenames = filtered_filenames

        return self.download_links

    def _download_file(
            self,
            link: str,
            check_for_existence: bool
    ) -> bool:
        """
        Downloads a file from the given link.

        :param link: The URL of the file to download.
        :param check_for_existence: If True, checks if the file already exists
         in the download directory and skips the download if it does.
        :return: True if the file was successfully downloaded or already exists,
         False if the download failed.
        """
        try:
            filename = os.path.basename(link)

            if all((self.forecast_run, self.variable)):
                files_path = os.path.join(
                    self.files_path, self.forecast_run, self.variable
                )
                downloaded_file_path = os.path.join(files_path, filename)
                self._ensure_directory_exists(files_path)
            else:
                downloaded_file_path = os.path.join(self.files_path, filename)
            downloaded_file_path = os.path.normpath(downloaded_file_path)
            self._downloaded_files_paths.append(downloaded_file_path)

            # Check if file already exists
            if check_for_existence and os.path.exists(downloaded_file_path):
                self._logger.info(f"Skipping {filename}, file already exists.")
                return True  # Treat as successful since the file is already present

            # Respect the delay between downloads
            if self._delay > 0:
                time.sleep(self._delay)

            response = self._session.get(
                link, stream=True, timeout=self._timeout
            )
            response.raise_for_status()  # Raise an error for bad responses

            # Download the file in chunks
            with open(downloaded_file_path, "wb") as file:
                for chunk in response.iter_content(1024):
                    file.write(chunk)

            self._logger.info(f"Downloaded: {filename}")
            return True

        except requests.exceptions.RequestException as e:
            self._logger.error(f"Failed to download {filename}: {e}")
            return False

    def download(self, check_for_existence: bool = False) -> None:
        """
        Downloads files from the generated links.

        :param check_for_existence: If True, checks if the file already exists
        in the download directory and skips the download if it does.
        """
        if not self.download_links:
            self._logger.warning(
                "No files to download. Please fetch links first.")
            return

        # Step 1: Parallel Download Attempt
        with ThreadPoolExecutor(max_workers=self._n_jobs) as executor:
            futures = {executor.submit(
                self._download_file, link, check_for_existence
            ): link for link in self.download_links}

            for future in as_completed(futures):
                link = futures[future]
                try:
                    if future.result():
                        self.downloaded_files.append(link)
                    else:
                        self.failed_files.append(link)
                except Exception as e:
                    self._logger.error(
                        f"Unexpected error downloading {link}: {e}")
                    self.failed_files.append(link)

        self._logger.info(
            f"Downloaded {len(self.downloaded_files)} files successfully."
        )
        if self.failed_files:
            self._logger.warning(
                f"{len(self.failed_files)} downloads failed initially."
            )

        # Step 2: Retry Failed Downloads
        if self._retry > 0 and self.failed_files:
            self._logger.warning(
                f"Retrying {len(self.failed_files)}"
                f" failed downloads up to {self._retry} times...")

            for link in self.failed_files[:]:  # Iterate over a copy to allow modification
                retry_count = 0
                while retry_count < self._retry:
                    try:
                        if self._download_file(link, check_for_existence):
                            self.downloaded_files.append(link)
                            self.failed_files.remove(link)  # Remove from failed list
                            break  # Exit retry loop if download succeeds
                        retry_count += 1
                    except Exception as e:
                        self._logger.error(
                            f"Retry {retry_count} failed for {link}: {e}")
                        retry_count += 1

        # Step 3: Write log files
        if self.failed_files:
            self._logger.error(
                f"Failed to download {len(self.failed_files)}"
                f" files after {self._retry} retries.")
        else:
            if self._retry > 0:
                self._logger.info(
                    f"All downloads completed successfully"
                    f" after {self._retry} retries.")
            else:
                self._logger.info("All downloads completed successfully.")

        variable = self._get_variable_from_link(self.url)

        self._write_log_file(
            self.downloaded_files, "downloaded_files", variable
        )
        self._write_log_file(
            self.failed_files, "failed_files", variable
        )

    def delete(self) -> None:
        """
        Deletes local files after successful download.

        :return: None
        """
        self._delete_files_safely(
            self._downloaded_files_paths, "downloaded file"
        )
        self._cleanup_empty_dirs(self.files_path)
