import os
import re
import zipfile
import time
from urllib.parse import urljoin

import pandas as pd
import requests
from lxml import html

from dwdown.utils.date_time_utilis import TimeHandler
from dwdown.utils.file_handling import FileHandler
from dwdown.utils.general_utilis import Utilities
from dwdown.utils.log_handling import LogHandler
from dwdown.utils.network_handling import SessionHandler


class HistoricalDownloader:
    def __init__(
            self,
            base_url: str | None = None,
            files_path: str | None = None,
            extracted_files_path: str | None = None,
            log_files_path: str | None = None,
            encoding: str | None = None,
            station_description_file_name: str | None = None,
            delay: int | float = 1,
            retry: int = 0,
            timeout: int = 30,
    ):
        """
        Initializes the HistoricalDownloader.

        :param base_url: Base URL to fetch data from. If None, uses the default
         DWD historical data URL.
        :param files_path: Directory to save downloaded files. Defaults to
         "download_files".
        :param extracted_files_path: Directory to save extracted files.
         Defaults to "extracted_files".
        :param log_files_path: Directory to save log files. Defaults to
         "log_files".
        :param encoding: Encoding used for station description files.
         Defaults to "windows-1252".
        :param station_description_file_name: Name of the station description
         file. Defaults to "KL_Tageswerte_Beschreibung_Stationen".
        :param delay: Optional delay between downloads (in seconds).
        :param retry: If > 0, retry failed downloads sequentially this many
         times.
        :param timeout: Timeout for both the connect and the read timeouts.
        :param n_jobs: Number of worker threads (unused in sequential mode,
         kept for compatibility).
        """
        self._encoding = encoding or "windows-1252"
        self._delay = delay
        self._retry = retry
        self._timeout = timeout

        self.files_path = os.path.normpath(files_path or "download_files")
        self.extracted_files_path = os.path.normpath(
            extracted_files_path or "extracted_files"
        )
        self.log_files_path = os.path.normpath(log_files_path or "log_files")

        self._utilities = Utilities()
        self._timehandler = TimeHandler()

        # Initialize Logger
        self._loghandler = LogHandler(
            timehandler=self._timehandler,
            log_file_path=self.log_files_path,
            logger_name=self.__class__.__name__,
            log_to_console=True,
            log_to_file=True
        )
        self._logger = self._loghandler.get_logger()

        # Initialize FileHandler
        self._filehandler = FileHandler(
            log_handler=self._loghandler,
            utilities=self._utilities
        )
        self._filehandler._ensure_directories_exist(
            [self.files_path, self.extracted_files_path, self.log_files_path]
        )

        # Initialize Session
        self._sessionhandler = SessionHandler(
            num_retries=5,
            backoff_factor=2.0,
            status_forcelist=(429, 500, 502, 503, 504)
        )
        self._session = self._sessionhandler.get_session()

        self._base_url = (
                base_url or
                "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl/historical/")
        self._station_description_file_name = (
                station_description_file_name
                or "KL_Tageswerte_Beschreibung_Stationen")

        self.downloaded_files = []
        self._downloaded_files_paths = []
        self.failed_files = []
        self.download_links = []

    def _get_filenames_from_url(self) -> list[str]:
        """
        Fetches filenames from the URL using an XPath expression.

        :return: A list of filenames extracted from the URL.
        """
        try:
            response = self._session.get(self._base_url, timeout=self._timeout)
            response.raise_for_status()
            tree = html.fromstring(response.content)
            filenames = tree.xpath("/html/body/pre//a/@href")
            return filenames
        except requests.exceptions.RequestException as e:
            self._logger.error(f"Failed to fetch filenames from {self._base_url}: {e}")
            return []

    def get_links(
            self,
            station_ids: list[str],
            prefix: str | None = None,
            suffix: str | None = None,
            include_pattern: list[str] | None = None,
            exclude_pattern: list[str] | None = None
    ) -> list[str]:
        """
        Generates download links based on the provided filters.

        :param station_ids: List of station IDs to filter for. The IDs will be
         zero-padded to 5 digits (e.g., '1' becomes '00001').
        :param prefix: The prefix that filenames should start with.
        :param suffix: The suffix that filenames should end with.
        :param include_pattern: A list of regex patterns that filenames should
         match. If None, no inclusion filtering is applied.
        :param exclude_pattern: A list of regex patterns that filenames should
         not match. If None, no exclusion filtering is applied.
        :return: A list of download links that match the specified filters.
        """
        filenames = self._get_filenames_from_url()
        if not filenames:
            return []

        # If station_ids are provided, we construct a specific pattern
        effective_include_pattern = self._utilities._string_to_list(include_pattern)

        if station_ids:
            # Pad station IDs to 5 digits with leading zeros (e.g., '1' -> '00001')
            station_patterns = [f"_{str(sid).zfill(5)}_" for sid in station_ids]
            effective_include_pattern.extend(station_patterns)

        # Use filehandler to filter
        filtered_filenames = self._filehandler._simple_filename_filter(
            filenames=filenames,
            prefix=prefix,
            suffix=suffix,
            include_pattern=effective_include_pattern,
            exclude_pattern=self._utilities._string_to_list(exclude_pattern),
            use_all_for_include=False,
            mock_time_steps=True
        )
        
        full_urls = [urljoin(self._base_url, file) for file in filtered_filenames]

        self.download_links = full_urls
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
        :return: True if the file was successfully downloaded or already
         exists, False if the download failed.
        """
        try:
            filename = os.path.basename(link)
            downloaded_file_path = os.path.join(self.files_path, filename)
            downloaded_file_path = os.path.normpath(downloaded_file_path)
            
            # Check if file already exists
            if check_for_existence and os.path.exists(downloaded_file_path):
                self._logger.info(f"Skipping {filename}, file already exists.")
                self._downloaded_files_paths.append(downloaded_file_path)
                return True

            # Respect the delay between downloads
            if self._delay > 0:
                time.sleep(self._delay)

            response = self._session.get(
                link, stream=True, timeout=self._timeout
            )
            response.raise_for_status()

            with open(downloaded_file_path, "wb") as file:
                for chunk in response.iter_content(1024):
                    file.write(chunk)

            self._logger.info(f"Downloaded: {filename}")
            self._downloaded_files_paths.append(downloaded_file_path)
            return True

        except requests.exceptions.RequestException as e:
            self._logger.error(f"Failed to download {filename}: {e}")
            return False

    def download(self, check_for_existence: bool = False) -> None:
        """
        Downloads files from the generated links sequentially.

        :param check_for_existence: If True, checks if the file already exists
         in the download directory and skips the download if it does.
        :return: None
        """
        if not self.download_links:
            self._logger.warning(
                "No files to download. Please fetch links first via get_links().")
            return

        self._logger.info(f"Starting download of {len(self.download_links)} files...")

        for link in self.download_links:
            try:
                success = False
                # First attempt
                if self._download_file(link, check_for_existence):
                    self.downloaded_files.append(link)
                    success = True
                else:
                    self.failed_files.append(link)
                
                # Retry logic if failed
                if not success and self._retry > 0:
                    retry_count = 0
                    while retry_count < self._retry:
                         self._logger.info(f"Retrying {link} (Attempt {retry_count + 1}/{self._retry})...")
                         if self._download_file(link, check_for_existence):
                             self.downloaded_files.append(link)
                             self.failed_files.remove(link)
                             success = True
                             break
                         retry_count += 1
                    
                    if not success:
                         self._logger.error(
                             f"Permanently failed to download {link} after retries."
                         )

            except Exception as e:
                 self._logger.error(f"Unexpected error processing {link}: {e}")
                 if link not in self.failed_files:
                     self.failed_files.append(link)

        self._logger.info(
            f"Downloaded {len(self.downloaded_files)} files successfully."
        )
        if self.failed_files:
             self._logger.warning(f"{len(self.failed_files)} files failed to download.")

    def download_station_description(self) -> None:
        """
        Downloads the station description file.
        """
        url = f"{self._base_url}{self._station_description_file_name}.txt"
        if self._download_file(url, check_for_existence=False):
            self._logger.info("Station description file downloaded successfully.")
        else:
            self._logger.error("Failed to download station description file.")

    def extract(
            self,
            zip_files: list[str] | str | None = None,
            unpack_hist_data_only: bool = False,
            check_for_existence: bool = False
    ) -> None:
        """
        Unpacks downloaded zip files.

        :param zip_files: A single filename, a list of filenames, or None.
         If None, processes all files in self.download_links.
        :param unpack_hist_data_only: If True, only extracts the product file
         (containing data).
        :param check_for_existence: If True, skips unpacking if the target
         folder already exists.
        :return: None
        """
        # Determine which files to unpack
        if zip_files is None:
             # Use generic download_links if available and extract filenames
             if self.download_links:
                 files_to_unpack_list = [os.path.basename(link) for link in self.download_links]
             else:
                 self._logger.warning("No zip files provided and no download links found.")
                 return
        elif isinstance(zip_files, str):
            files_to_unpack_list = [zip_files]
        else:
            files_to_unpack_list = zip_files

        for zip_file in files_to_unpack_list:
            try:
                folder_name = os.path.splitext(zip_file)[0]
                zip_file_path = os.path.join(self.files_path, zip_file)
                
                # Check for existence of content
                target_root_dir = os.path.join(self.extracted_files_path, folder_name)
                if check_for_existence and os.path.exists(target_root_dir):
                    self._logger.info(
                        f"Skipping unpacking of {zip_file},"
                        f" target directory {target_root_dir} already exists."
                    )
                    continue

                if not os.path.exists(zip_file_path):
                     self._logger.warning(f"Zip file not found: {zip_file_path}")
                     continue

                with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                    # Get list of all files in the zip
                    start_zip_content = zip_ref.namelist()
                    
                    files_to_extract = start_zip_content

                    if unpack_hist_data_only:
                        # Try to extract station_id from zip filename
                        # Pattern: tageswerte_KL_{station_id}_{date}_{date}_hist.zip
                        # Matches digits between 'KL_' and '_'
                        match = re.search(r'tageswerte_KL_(\d+)_', zip_file)
                        if match:
                            station_id = match.group(1)
                            # Filter files that match the pattern for this station_id
                            pattern = f"^produkt_klima_tag_.*_{station_id}\\.txt$"
                            files_to_extract = [
                                f for f in start_zip_content if re.match(pattern, f)]
                        else:
                            self._logger.warning(
                                f"Could not extract station_id from {zip_file}"
                                f" for filtering. Unpacking all files."
                            )

                    # Unpack matching files
                    for file in files_to_extract:
                        target_dir = os.path.join(self.extracted_files_path, folder_name)
                        zip_ref.extract(file, target_dir)
                        self._logger.info(f"{file} unpacked to {target_dir}")

                self._logger.info(f"{zip_file} processed.")
            except Exception as e:
                self._logger.error(f"Error unpacking zip file {zip_file}: {e}")
                # Continue with next file
                continue

    def read_station_description(self) -> pd.DataFrame:
        """
        Reads the station description file into a DataFrame.
        Dynamically determines column widths using the separator line.

        :return: A pandas DataFrame containing the station description data.
        :raises Exception: If reading the file fails.
        """
        file_path = os.path.join(
            self.files_path,
            f"{self._station_description_file_name}.txt"
        )
        
        try:
            # Read first few lines to find separator
            with open(file_path, 'r', encoding=self._encoding) as f:
                lines = [f.readline() for _ in range(5)]
            
            # Find separator line (usually line 2, index 1)
            separator_line_idx = -1
            colspecs = 'infer'
            skiprows = [1]
            
            for idx, line in enumerate(lines):
                 # Look for a line that is mostly dashes
                 if line.strip() and set(line.strip().replace(' ', '')) == {'-'}:
                     separator_line_idx = idx
                     # Calculate colspecs using regex to find ranges of dashes
                     colspecs = [(m.start(), m.end()) for m in re.finditer(r'-+', line)]
                     skiprows = [separator_line_idx]
                     break
            
            if separator_line_idx == -1:
                 self._logger.warning("Could not detect separator line. Defaulting to 'infer'.")

            station_description = pd.read_fwf(
                filepath_or_buffer=file_path,
                colspecs=colspecs, 
                skiprows=skiprows,
                header=0,
                index_col="Stations_id",
                encoding=self._encoding,
                dtype={"Stations_id": str}
            )
            return station_description
        except Exception as e:
            self._logger.error(f"Error reading station description file: {e}")
            raise

    def read_data(
            self,
            zip_files: list[str] | str | None = None,
            save_as_csv: bool = False
    ) -> dict[str, pd.DataFrame]:
        """
        Reads the unpacked station data into pandas DataFrames.

        :param zip_files: List of zip files (or single file) to process.
         If None, checks folders for all files encountered in
         self.download_links or scans extracted_files_path if
         download_links is empty.
        :param save_as_csv: If True, saves each DataFrame to a CSV file
         (same name as source, replaced extension).
        :return: A dictionary mapping filenames to pandas DataFrames.
        """
        # Determine folders to check
        if zip_files is None:
             if self.download_links:
                 # Derive folder names from download links
                 folders_to_check = [
                     os.path.splitext(os.path.basename(link))[0]
                     for link in self.download_links
                 ]
             else:
                 # Fallback: check all directories in extracted_files_path
                 self._logger.warning(
                     "No zip files specified or known."
                     " Processing all subdirectories in extracted files path."
                 )
                 folders_to_check = [
                     d for d in os.listdir(self.extracted_files_path)
                     if os.path.isdir(os.path.join(self.extracted_files_path, d))
                 ]
        elif isinstance(zip_files, str):
            folders_to_check = [os.path.splitext(zip_files)[0]]
        else:
             folders_to_check = [os.path.splitext(f)[0] for f in zip_files]

        results = {}
        for folder in folders_to_check:
            folder_path = os.path.join(self.extracted_files_path, folder)
            if not os.path.exists(folder_path):
                 self._logger.warning(f"Directory not found: {folder_path}")
                 continue

            # Find the data file
            found = False
            for file in os.listdir(folder_path):
                # Pattern for historical data files
                if re.match(r"^produkt_klima_tag_.*\.txt$", file):
                    file_path = os.path.join(folder_path, file)
                    try:
                        # Read csv
                        df = pd.read_csv(
                            file_path,
                            sep=";",
                            na_values="-999",
                            skipinitialspace=True
                        )
                        
                        # Clean up
                        if 'eor' in df.columns:
                            df = df.drop(columns=['eor'])
                        # Strip whitespace from column names
                        df.columns = df.columns.str.strip()
                        
                        results[file] = df
                        found = True
                        self._logger.info(f"Read data from {file}")

                        if save_as_csv:
                             csv_filename = file.replace('.txt', '.csv')
                             # Save in the same folder as the source text file
                             output_path = os.path.join(folder_path, csv_filename)
                             try:
                                 df.to_csv(output_path, index=True, sep=";")
                                 self._logger.info(f"Data saved to {output_path}")
                             except Exception as e:
                                 self._logger.error(f"Failed to save CSV to {output_path}: {e}")
                        break
                    except Exception as e:
                        self._logger.error(f"Failed to read {file_path}: {e}")

            if not found:
                 self._logger.warning(
                     f"No historical data file found in {folder_path}"
                 )

        if not results:
            self._logger.warning("No data read.")

        return results
