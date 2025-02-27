import hashlib
import logging
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

if sys.version_info >= (3, 11):
    from datetime import UTC
else:
    from datetime import timezone
    UTC = timezone.utc

import requests
from lxml import html
from minio import Minio
from minio.error import S3Error

# Configure logging to remove the default prefix
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
)


class DWDDownloader:

    def __init__(
            self,
            url: str,
            restart_failed_downloads: bool = False,
            log_downloads: bool = True,
            delay: int | float | None = None,
            workers: int = 1,
            download_path: str = "downloaded_files",
            log_files_path: str = "log_files_DWDDownloader",
            xpath_files: str = "/html/body/pre//a/@href",
            xpath_dates: str = "//pre/text()"
    ):
        """
        Initializes the DWDDownloader with the URL and an optional delay.

        :param url: Base URL to fetch data from
        :param delay: Optional delay between downloads (in seconds)
        """
        self.url = url
        self.delay = delay
        self.workers = workers
        self.xpath_files = xpath_files
        self.xpath_dates = xpath_dates
        self.log_downloads = log_downloads
        self.restart_failed_downloads = restart_failed_downloads

        self.download_path = download_path
        self.log_files_path = log_files_path

        self.failed_files = []
        self.downloaded_files = []
        self.finally_failed_files = []

        self.full_links = None
        self.raw_filenames = None
        self.filtered_filenames = None

        # Create directories if they don't exist
        self._ensure_directory_exists(self.download_path)
        self._ensure_directory_exists(self.log_files_path)

        self.logger = logging.getLogger(__name__)

    @staticmethod
    def _ensure_directory_exists(
            path: str
    ) -> None:
        """Helper function to ensure a directory exists, creates if not."""
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)

    @staticmethod
    def _fix_date_format(
            dates: list[str]
    ) -> list[str]:
        """
        Cleans and formats date strings by:
        1. Adding space between a number and a letter
        (e.g., "2025Jan" → "2025 Jan").
        2. Replacing space with "-" between two numbers
        (e.g., "2025 10:20" → "2025-10:20").
        3. Removing trailing numbers if preceded by two or more spaces.
        """
        cleaned_dates = []

        for date in dates:
            date = date.strip()  # Remove leading/trailing whitespace

            # Fix formatting
            date = re.sub(
                r'(\d)([A-Za-z])', r'\1 \2', date
            )  # Space between number and letter
            date = re.sub(
                r'(\d) (\d)', r'\1-\2', date
            )  # Replace space with "-" between two numbers
            date = re.sub(
                r'\s{2,}\d+$', '', date
            )  # Remove trailing numbers if preceded by two spaces

            if date:  # Avoid empty entries
                cleaned_dates.append(date)

        return cleaned_dates

    @staticmethod
    def _parse_dates(
            date_strings: list[str]
    ) -> list[datetime]:
        """
        Converts a list of date strings into datetime objects.
        Expected format: '21-Jan-2025-10:20' → datetime(2025, 1, 21, 10, 20).
        """
        parsed_dates = []

        for date in date_strings:
            try:
                parsed_dates.append(datetime.strptime(date, "%d-%b-%Y-%H:%M"))
            except ValueError as e:
                logging.info(
                    f"Skipping invalid date format: {date} ({e})"
                )  # Debugging invalid formats

        return parsed_dates

    def get_data_dates(
            self,
            url: str | None = None
    ) -> tuple[datetime, datetime]:
        """
        Fetches and processes date strings from a given URL.

        :return: min and max date from url.
        """
        url = url if url else self.url

        response = requests.get(url)
        response.raise_for_status()

        tree = html.fromstring(response.content)
        raw_dates = tree.xpath(self.xpath_dates)

        fixed_dates = self._fix_date_format(raw_dates)
        cleaned_dates = [
            re.sub(r'\s+', '', date.replace(' -', ''))
            for date in fixed_dates
        ]

        return (min(self._parse_dates(cleaned_dates)),
                max(self._parse_dates(cleaned_dates)))

    @staticmethod
    def get_current_date(
            utc: bool = True,
            time_of_day: bool = False
    ) -> datetime:
        """
        Get the current system date, formatted as "DD-MMM-YYYY-HH:MM".

        :param time_of_day: If True, return date with time; otherwise,
         return only the date.
        :param utc: If True, return date with UTC time; otherwise,
         return system time.
        :return: Current date (with or without time)
         in formatted datetime format.
        """
        # Get current datetime
        if utc:
            now = datetime.now(UTC)
        else:
            now = datetime.now()

        # Format as string and parse back to enforce "DD-MMM-YYYY-HH:MM" format
        formatted_str = now.strftime("%d-%b-%Y-%H:%M")
        formatted_datetime = datetime.strptime(formatted_str, "%d-%b-%Y-%H:%M")

        # If time_of_day is False, reset time to midnight
        return formatted_datetime if time_of_day\
            else formatted_datetime.replace(hour=0, minute=0)

    def _get_filenames_from_url(
            self
    ) -> list[str]:
        """
        Fetches the list of filenames from the given URL by parsing the HTML.

        :return: A list of filenames (URLs)
        """
        try:
            response = requests.get(self.url)
            # Raises exception for 4xx/5xx status codes
            response.raise_for_status()
            tree = html.fromstring(response.content)
            filenames = tree.xpath(self.xpath_files)
            return filenames
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to fetch filenames from {self.url}: {e}")
            return []

    @staticmethod
    def _filter_file_names(
            filenames: list[str],
            name_startswith: str = "icon-d2_germany",
            name_endswith: str = ".bz2",
            include_pattern: list[str] | None = None,
            exclude_pattern: list[str] | None = None
    ) -> list[str]:
        """
        Filters the list of filenames based on the given start and end patterns
        and inclusion/exclusion patterns.

        :param filenames: List of filenames to filter
        :param name_startswith: String that filenames must start with
        :param name_endswith: String that filenames must end with
        :param include_pattern: List of substrings;
         at least one must be in the filename
        :param exclude_pattern: List of substrings;
         filenames with any of these are excluded
        :return: Filtered list of filenames
        """
        include_pattern = include_pattern or [""]
        exclude_pattern = exclude_pattern or []

        filtered_filenames = [
            filename for filename in filenames
            if filename.startswith(name_startswith)
            and filename.endswith(name_endswith)
            and any(pattern in filename for pattern in include_pattern)
            and not any(pattern in filename for pattern in exclude_pattern)
        ]
        return filtered_filenames

    def _generate_links(
            self,
            filtered_filenames: list[str]
    ) -> list[str]:
        """
        Generates full URLs from the list of filtered filenames.

        :param filtered_filenames: List of filtered filenames
        :return: List of full URLs
        """
        return [self.url + name for name in filtered_filenames]

    @staticmethod
    def _process_timesteps(
            min_timestep: str | int | None = None,
            max_timestep: str | int | None = None,
            include_pattern: list | None = None
    ) -> list[str]:
        """
        Generates a list of formatted timestep patterns within a given range.

        Parameters:
        -----------
        min_timestep : Union[str, int, None], optional
            The minimum timestep value (default is 0 if None).
        max_timestep : Union[str, int, None], optional
            The maximum timestep value (default is 48 if None).
        include_pattern : Optional[List[str]], optional
            A list to which the generated timestep patterns will be added.
            If None, a new list is created.

        Returns:
        --------
        List[str]
            A list of formatted timestep patterns (e.g., "_000_",
             "_001_", ..., "_048_").
        """
        # Assign default values using `or`
        min_timestep = int(min_timestep) if min_timestep is not None else 0
        max_timestep = int(max_timestep) if max_timestep is not None else 48

        # Ensure they are integers
        if isinstance(min_timestep, int) and isinstance(max_timestep, int):
            temp = [f"_{str(t).zfill(3)}_" for t in
                    range(min_timestep, max_timestep + 1)]

            # Use list concatenation instead of += (avoids nested lists)
            if isinstance(include_pattern, list):
                include_pattern.extend(temp)
            else:
                include_pattern = temp

        return include_pattern

    def get_links(
            self,
            name_startswith: str = "icon-d2_germany",
            name_endswith: str = ".bz2",
            include_pattern: list[str] | None = None,
            exclude_pattern: list[str] | None = None,
            min_timestep: str | int | None = None,
            max_timestep: str | int | None = None,
    ) -> list[str]:
        """
        Main method to get all the download links after filtering filenames.

        :return: List of full download URLs
        """
        filenames = self._get_filenames_from_url()
        if not filenames:
            return []

        include_pattern = self._process_timesteps(
            min_timestep=min_timestep,
            max_timestep=max_timestep,
            include_pattern=include_pattern
        )

        filtered_filenames = self._filter_file_names(
            filenames=filenames,
            name_startswith=name_startswith,
            name_endswith=name_endswith,
            include_pattern=include_pattern,
            exclude_pattern=exclude_pattern
        )

        full_links = self._generate_links(filtered_filenames)

        # Store the state for potential future use or debugging
        self.raw_filenames = filenames
        self.full_links = full_links
        self.filtered_filenames = filtered_filenames

        return full_links

    def _download_file(
            self,
            link: str,
            check_for_existence: bool
    ) -> bool:
        """
        Downloads a single file from the provided URL if it does not
         already exist.

        :param link: The URL to download the file from
        :param check_for_existence: If True, skips download if
         the file already exists
        :return: True if the file was successfully downloaded
         or already exists, else False
        """
        filename = link.split("/")[-1]
        downloaded_file_path = os.path.join(self.download_path, filename)
        self.downloaded_files_path = downloaded_file_path

        # Check if file already exists
        if check_for_existence and os.path.exists(downloaded_file_path):
            self.logger.info(f"Skipping {filename}, file already exists.")
            return True  # Treat as successful since the file is already present

        try:
            # Respect the delay between downloads
            if isinstance(self.delay, int) and self.delay > 0:
                time.sleep(self.delay)

            response = requests.get(link, stream=True)
            response.raise_for_status()  # Raise an error for bad responses

            # Download the file in chunks
            with open(downloaded_file_path, "wb") as file:
                for chunk in response.iter_content(1024):
                    file.write(chunk)

            self.logger.info(f"Downloaded: {filename}")
            return True

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to download {filename}: {e}")
            return False

    def download_files(
            self,
            check_for_existence: bool = False,
            max_retries: int = 3
    ) -> None:
        """
        Downloads all files from the generated links using concurrency for
        faster processing. If downloads fail, retries up to `max_retries` times
        before considering them as final failures.

        :param check_for_existence: If True, skips download if the file already exists
        :param max_retries: Number of retry attempts before marking a file as failed
        """
        if not self.full_links:
            self.logger.warning("No files to download."
                                " Please fetch links first.")
            return

        # Step 1: Parallel Download Attempt
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            futures = {executor.submit(
                self._download_file, link, check_for_existence
            ): link for link in self.full_links}

            for future in as_completed(futures):
                link = futures[future]
                try:
                    if future.result():
                        self.downloaded_files.append(link)
                    else:
                        self.failed_files.append(link)
                except Exception as e:
                    self.logger.error(
                        f"Unexpected error downloading {link}: {e}"
                    )
                    self.failed_files.append(link)

        self.logger.info(
            f"Downloaded {len(self.downloaded_files)} files successfully.")
        self.logger.warning(
            f"{len(self.failed_files)} downloads failed initially.")\
            if self.failed_files else None

        # Step 2: Retry Failed Downloads with max_retries
        if self.restart_failed_downloads and self.failed_files:
            self.logger.warning(
                f"Retrying {len(self.failed_files)}"
                f" failed downloads up to {max_retries} times...")

            remaining_failed_files = []  # Temporary storage for files that still fail after retries

            for link in self.failed_files[
                        :]:  # Iterate over a copy to allow modification
                retry_count = 0
                while retry_count < max_retries:
                    try:
                        if self._download_file(link, check_for_existence):
                            self.downloaded_files.append(link)
                            self.failed_files.remove(link)  # Remove from failed list
                            break  # Exit retry loop if download succeeds
                        retry_count += 1
                    except Exception as e:
                        self.logger.error(f"Retry {retry_count} failed for {link}: {e}")
                        retry_count += 1

                if retry_count == max_retries:
                    remaining_failed_files.append(link)

            self.finally_failed_files.extend(remaining_failed_files)

        # Log final results
        if self.finally_failed_files:
            self.logger.error(
                f"Failed to download {len(self.finally_failed_files)}"
                f" files after {max_retries} retries.")
        else:
            self.logger.info("All downloads completed successfully"
                             " after retries.")

        # Step 3: Write log files if enabled
        if self.log_downloads:
            variable, time_stamp = self._log_name_formatting(link)

            self._write_log_file(
                f"{self.log_files_path}/"
                f"DWDDownloader_downloaded_files_"
                f"{variable}_{time_stamp}.log",
                self.downloaded_files
            )
            self._write_log_file(
                f"{self.log_files_path}/"
                f"DWDDownloader_failed_files_"
                f"{variable}_{time_stamp}.log",
                self.failed_files
            )
            self._write_log_file(
                f"{self.log_files_path}/"
                f"DWDDownloader_finally_failed_files_"
                f"{variable}_{time_stamp}.log",
                self.finally_failed_files
            )

    def _log_name_formatting(
            self,
            link: str
    ) -> tuple[str, str]:

        variable_from_link = self._get_variable_from_link(link)

        time_stamp = self.get_current_date(time_of_day=True)
        formatted_time_stamp = self.get_formatted_time_stamp(time_stamp)

        return variable_from_link, formatted_time_stamp

    @staticmethod
    def _get_variable_from_link(
            link: str
    ) -> str:
        search_pattern = re.search(r'/\d{2}/([^/]+)/', link)
        variable_from_link = search_pattern.group(
            1) if search_pattern else ''

        return variable_from_link

    @staticmethod
    def get_formatted_time_stamp(
            time_stamp: datetime,
    ) -> str:

        if not isinstance(time_stamp, str):
            time_stamp = time_stamp.strftime("%Y-%m-%d %H:%M")

        return re.sub(r"[-:\s]", "_", time_stamp)

    def _write_log_file(
            self,
            filename: str,
            data: list
    ) -> None:
        """
        Writes a list of file links to a log file,
         ensuring each entry is on a new line.
        """
        try:
            with open(filename, "w", encoding="utf-8") as file:
                file.write("\n".join(data) + "\n")
            self.logger.info(f"Saved log: {filename} ({len(data)} entries)")
        except Exception as e:
            self.logger.error(f"Error writing log file {filename}: {e}")


class MinioDownloader:
    def __init__(
            self,
            endpoint: str,
            access_key: str,
            secret_key: str,
            files_path: str,
            secure: bool = False,
            log_downloads: bool = True,
            log_files_path: str = "log_files_MinioDownloader",
            workers: int = 1,
    ):
        """
        Initializes the MinioUploader.
        """
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )
        self.workers = workers
        self.files_path = files_path
        self.log_downloads = log_downloads
        self.log_files_path = log_files_path

        self.remote_files = []
        self.downloaded_files = []
        self.corrupted_files = []

        # Create directories if they don't exist
        self._ensure_directory_exists(self.log_files_path)

        self.logger = logging.getLogger(__name__)

    @staticmethod
    def _ensure_directory_exists(
            path: str
    ) -> None:
        """Helper function to ensure a directory exists, creates if not."""
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)

    def _ensure_bucket(
            self,
            bucket_name
    ) -> None:
        """Ensure the bucket exists."""
        if self.client.bucket_exists(bucket_name):
            self.logger.info(f"Bucket {bucket_name} exists")
        else:
            self.logger.error(f"Bucket {bucket_name} does not exists")

    @staticmethod
    def calculate_md5(
            file_path: str
    ) -> str:
        """Computes the MD5 hash of a file."""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    @staticmethod
    def _get_current_date() -> str:
        """
        Get the current system date, formatted as "DD-MMM-YYYY-HH-MM".

        :return: Current date in formatted datetime format.
        """
        # Get current datetime
        now = datetime.now()

        # Format as string and parse back to enforce "DD-MMM-YYYY-HH:MM" format
        formatted_str = now.strftime("%d-%b-%Y-%H-%M")

        return formatted_str

    def _get_remote_files(
            self,
            bucket_name: str,
            folder_prefix: str,
    ) -> list:
        """Retrieve a list of remote files from MinIO."""
        remote_files = list(self.client.list_objects(
            bucket_name,
            prefix=folder_prefix,
            recursive=True
        ))

        if not remote_files:
            self.logger.warning(
                f"No files found in bucket '{bucket_name}'"
                f" with prefix '{folder_prefix}'.")

        return remote_files

    def download_bucket(
            self,
            bucket_name: str,
            folder_prefix: str | None = None
    ) -> None:
        """Recursively downloads a bucket or folder from MinIO."""
        self._ensure_bucket(bucket_name)
        remote_files = self._get_remote_files(bucket_name, folder_prefix)

        if not remote_files:
            self.logger.info(
                f"No files to download from bucket '{bucket_name}'"
                f" with prefix '{folder_prefix}'.")
            return  # Exit early if no files

        files_to_download = []
        for obj in remote_files:
            remote_path = obj.object_name
            self.remote_files.append(remote_path)
            local_file_path = os.path.join(self.files_path, remote_path)

            # Ensure the directory exists
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

            # Skip if the file already exists and matches hash
            if os.path.exists(local_file_path) and self._verify_file_integrity(
                    bucket_name, local_file_path, remote_path):
                self.logger.info(
                    f"Skipping already downloaded file: {remote_path}")
                continue

            files_to_download.append((local_file_path, remote_path))

        if not files_to_download:
            self.logger.info("All files are already downloaded and verified.")
            return  # Exit early

        self.logger.info(
            f"Starting download of {len(files_to_download)} files...")

        # Parallel Download with Real-time Logging
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            futures = {
                executor.submit(self._download_file, bucket_name, local,
                                remote): (local, remote)
                for local, remote in files_to_download
            }

            for future in as_completed(futures):
                local_file_path, remote_path = futures[future]
                try:
                    if future.result():
                        self.downloaded_files.append(remote_path)
                        self.logger.info(
                            f"Successfully downloaded: {remote_path}")
                    else:
                        self.corrupted_files.append(remote_path)
                        self.logger.warning(
                            f"Hash mismatch: {remote_path}"
                            f"(Possible corruption)")

                except Exception as e:
                    self.logger.error(f"Error downloading {remote_path}: {e}")

        # Final summary logs
        self.logger.info(
            f"Downloaded {len(self.downloaded_files)} files successfully.")
        if self.corrupted_files:
            self.logger.warning(
                f"{len(self.corrupted_files)} files may be corrupted.")

        # Log downloads if enabled
        if self.log_downloads:
            time_stamp = self._get_current_date()
            formatted_time_stamp = re.sub(r"[-:\s]", "_", time_stamp)

            self._write_log_file(
                f"{self.log_files_path}/"
                f"MinioDownloader_downloaded_files_{formatted_time_stamp}.log",
                self.downloaded_files)
            self._write_log_file(
                f"{self.log_files_path}/"
                f"MinioDownloader_corrupted_files_{formatted_time_stamp}.log",
                self.corrupted_files)

    def _download_file(
            self,
            bucket_name: str,
            local_file_path: str,
            remote_path: str
    ) -> bool:
        """
        Downloads a single file with immediate logging and integrity check.
        """
        try:
            self.client.fget_object(bucket_name, remote_path, local_file_path)

            # Verify integrity after download
            return self._verify_file_integrity(bucket_name, local_file_path,
                                               remote_path)

        except S3Error as e:
            self.logger.error(
                f"Failed to download {remote_path} due to S3 error: {e}")
        except Exception as e:
            self.logger.error(
                f"Unexpected error downloading {remote_path}: {e}")

        return False

    def _verify_file_integrity(self, bucket_name: str, local_file_path: str,
                               remote_path: str) -> bool:
        """Verifies if the local file matches the remote file's checksum."""
        try:
            local_md5 = self.calculate_md5(local_file_path)
            obj_stat = self.client.stat_object(bucket_name, remote_path)
            return obj_stat.etag == local_md5

        except Exception as e:
            self.logger.error(
                f"Error verifying file integrity for {remote_path}: {e}")
            return False

    def _write_log_file(
            self,
            filename: str,
            data: list
    ) -> None:
        """
        Writes a list of file links to a log file,
         ensuring each entry is on a new line.
        """
        try:
            with open(filename, "w", encoding="utf-8") as file:
                file.write("\n".join(data) + "\n")
            self.logger.info(f"Saved log: {filename} ({len(data)} entries)")
        except Exception as e:
            self.logger.error(f"Error writing log file {filename}: {e}")
