import os
import zipfile
import time
from urllib.parse import urljoin
from typing import Literal

import pandas as pd
import requests
from lxml import html, etree

from dwdown.utils.date_time_utilis import TimeHandler
from dwdown.utils.file_handling import FileHandler
from dwdown.utils.general_utilis import Utilities
from dwdown.utils.log_handling import LogHandler
from dwdown.utils.network_handling import SessionHandler


class MOSMIX_Downloader:
    def __init__(
            self,
            mosmix_type: Literal["MOSMIX_L", "MOSMIX_S", "MOSMIX-SNOW_S"],
            base_url: str | None = None,
            files_path: str | None = None,
            extracted_files_path: str | None = None,
            log_files_path: str | None = None,
            delay: int | float = 1,
            retry: int = 0,
            timeout: int = 30,
    ):
        """
        Initializes the MOSMIX_Downloader.

        :param mosmix_type: Type of MOSMIX forecast to download.
         Defaults to "MOSMIX_L".
        :param base_url: Base URL to fetch data from. If None, constructed based on mosmix_type.
        :param files_path: Directory to save downloaded files. Defaults to
         "download_files".
        :param extracted_files_path: Directory to save extracted files.
         Defaults to "extracted_files".
        :param log_files_path: Directory to save log files. Defaults to
         "log_files".
         file. (Currently not used for MOSMIX but kept for interface consistency).
        :param delay: Optional delay between downloads (in seconds).
        :param retry: If > 0, retry failed downloads sequentially this many
         times.
        :param timeout: Timeout for both the connect and the read timeouts.
        """
        self.mosmix_type = mosmix_type
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

        if base_url:
            self._base_url = base_url
        else:
            # Default base URL construction logic
            self._base_url = f"https://opendata.dwd.de/weather/local_forecasts/mos/{self.mosmix_type}/"

        self.downloaded_files = []
        self._downloaded_files_paths = []
        self.failed_files = []
        self.download_links = []

    def _get_filenames_from_url(self, url: str) -> list[str]:
        """
        Fetches filenames from the URL using an XPath expression.

        :param url: The URL to scrape filenames from.
        :return: A list of filenames extracted from the URL.
        """
        try:
            response = self._session.get(url, timeout=self._timeout)
            response.raise_for_status()
            tree = html.fromstring(response.content)
            filenames = tree.xpath("/html/body/pre//a/@href")
            # Filter out parent directory link and directories
            filenames = [
                f for f in filenames
                if not f.startswith("../") and not f.endswith("/")
            ]
            return filenames
        except requests.exceptions.RequestException as e:
            self._logger.error(f"Failed to fetch filenames from {url}: {e}")
            return []

    def get_links(
            self,
            station_ids: list[str] | None = None,
            prefix: str | None = None,
            suffix: str | None = None,
            include_pattern: list[str] | None = None,
            exclude_pattern: list[str] | None = None
    ) -> list[str]:
        """
        Generates download links based on the provided filters and MOSMIX type.

        :param station_ids: List of station IDs to filter for.
         Only relevant for MOSMIX_L if fetching single stations.
        :param prefix: The prefix that filenames should start with.
        :param suffix: The suffix that filenames should end with.
        :param include_pattern: A list of regex patterns that filenames should
         match.
        :param exclude_pattern: A list of regex patterns that filenames should
         not match.
        :return: A list of download links.
        """
        
        target_urls = []
        
        # Logic for URL construction
        if self.mosmix_type == "MOSMIX_L" and station_ids:
             # For MOSMIX_L with station IDs, we look into single_stations/{id}/kml/
             
             padded_ids = [str(sid).zfill(5) for sid in station_ids]
             for pid in padded_ids:
                 # Construct URL for this station
                 station_url = urljoin(self._base_url, f"single_stations/{pid}/kml/")
                 
                 # Fetch files for this station
                 filenames = self._get_filenames_from_url(station_url)
                 
                 if filenames:
                     # Add full URLs
                     full_urls = [urljoin(station_url, f) for f in filenames]
                     target_urls.extend(full_urls)
                 else:
                     self._logger.warning(
                         f"No files found for station {pid} at {station_url}"
                     )

        else:
             # For MOSMIX_S, MOSMIX-SNOW_S, or MOSMIX_L without specific stations,
             # we default to 'all_stations/kml/'
             all_stations_url = urljoin(self._base_url, "all_stations/kml/")
             
             filenames = self._get_filenames_from_url(all_stations_url)
             if filenames:
                 full_urls = [urljoin(all_stations_url, f) for f in filenames]
                 target_urls.extend(full_urls)
             else:
                  self._logger.warning(f"No files found at {all_stations_url}")

        if not target_urls:
            return []
            
        # Separate base names to apply filters
        filename_url_map = {os.path.basename(url): url for url in target_urls}
        filenames = list(filename_url_map.keys())
        
        effective_include_pattern = self._utilities._string_to_list(include_pattern)

        filtered_filenames = self._filehandler._simple_filename_filter(
            filenames=filenames,
            prefix=prefix,
            suffix=suffix,
            include_pattern=effective_include_pattern,
            exclude_pattern=self._utilities._string_to_list(exclude_pattern),
            mock_time_steps=True
        )
        
        self.download_links = [filename_url_map[f] for f in filtered_filenames]
        return self.download_links

    def _download_file(
            self,
            link: str,
            check_for_existence: bool
    ) -> bool:
        """
        Downloads a file from the given link. (Reusing logic from HistoricalDownloader)
        """
        try:
            filename = os.path.basename(link)
            downloaded_file_path = os.path.join(self.files_path, filename)
            downloaded_file_path = os.path.normpath(downloaded_file_path)
            
            if check_for_existence and os.path.exists(downloaded_file_path):
                self._logger.info(f"Skipping {filename}, file already exists.")
                self._downloaded_files_paths.append(downloaded_file_path)
                return True

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
        """
        if not self.download_links:
            self._logger.warning(
                "No files to download. Please fetch links first via get_links()."
            )
            return

        self._logger.info(
            f"Starting download of {len(self.download_links)} files..."
        )

        for link in self.download_links:
            try:
                success = False
                if self._download_file(link, check_for_existence):
                    self.downloaded_files.append(link)
                    success = True
                else:
                    self.failed_files.append(link)
                
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

    def extract(
            self,
            zip_files: list[str] | str | None = None,
            check_for_existence: bool = False
    ) -> None:
        """
        Unpacks downloaded .kmz files to .kml.
        
        :param zip_files: List of kmz files or single file.
        :param check_for_existence: Skip if target exists.
        """
        # KMZ is just a zip with a KML inside (usually named doc.kml or similar)
        
        if zip_files is None:
             if self.download_links:
                 files_to_unpack_list = [
                     os.path.basename(link) for link in self.download_links
                 ]
             else:
                 self._logger.warning(
                     "No zip files provided and no download links found."
                 )
                 return
        elif isinstance(zip_files, str):
            files_to_unpack_list = [zip_files]
        else:
            files_to_unpack_list = zip_files
            
        for zip_file in files_to_unpack_list:
             if not zip_file.lower().endswith('.kmz') and not zip_file.lower().endswith('.zip'):
                 self._logger.warning(
                     f"File {zip_file} does not appear to be a KMZ/Zip archive."
                 )
                 continue

             try:
                 # Clean filename for folder (remove extension)
                 folder_name = os.path.splitext(zip_file)[0]
                 zip_file_path = os.path.join(self.files_path, zip_file)
                 
                 target_root_dir = os.path.join(self.extracted_files_path, folder_name)
                 
                 if check_for_existence and os.path.exists(target_root_dir):
                      self._logger.info(
                          f"Skipping unpacking {zip_file}, target directory exists."
                      )
                      continue
                 
                 if not os.path.exists(zip_file_path):
                      self._logger.warning(f"Zip file not found: {zip_file_path}")
                      continue
                      
                 with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                      zip_ref.extractall(target_root_dir)
                      self._logger.info(f"Unpacked {zip_file} to {target_root_dir}")
                      
             except Exception as e:
                 self._logger.error(f"Error unpacking {zip_file}: {e}")

    def read_data(
            self,
            zip_files: list[str] | str | None = None,
            save_as_csv: bool = False
    ) -> dict[str, pd.DataFrame]:
        """
        Reads unpacked KML data into Pandas DataFrames.
        
        :param zip_files: Used to identify which folders to look into.
        :param save_as_csv: If True, save processed DataFrame to CSV.
        """
        
        # Determine folders
        if zip_files is None:
             if self.download_links:
                 folders_to_check = [
                     os.path.splitext(os.path.basename(link))[0] for link in self.download_links
                 ]
             else:
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
                 
             # Look for .kml files
             found = False
             for file in os.listdir(folder_path):
                 if file.lower().endswith('.kml'):
                     file_path = os.path.join(folder_path, file)
                     try:
                         # Parse KML
                         df = self._parse_kml(file_path)
                         if df is not None:
                             results[file] = df
                             found = True
                             self._logger.info(f"Read data from {file}")
                             
                             if save_as_csv:
                                 csv_filename = file.replace('.kml', '.csv')
                                 output_path = os.path.join(folder_path, csv_filename)
                                 df.to_csv(output_path, sep=";")
                                 self._logger.info(f"Saved CSV to {output_path}")
                                 
                     except Exception as e:
                         self._logger.error(f"Failed to read KML {file_path}: {e}")
            
             if not found:
                 self._logger.warning(f"No KML file found in {folder_path}")
                 
        return results

    def _parse_kml(self, file_path: str) -> pd.DataFrame | None:
        """
        Parses a DWD MOSMIX KML file.
        """
        try:
            tree = etree.parse(file_path)
            root = tree.getroot()

            ns = root.nsmap
            # Ensure 'kml' prefix is initialized
            if None in ns:
                ns['kml'] = ns.pop(None)
            
            # Ensure 'dwd' prefix is available
            if 'dwd' not in ns:
                ns['dwd'] = "https://opendata.dwd.de/weather/lib/pointforecast_dwd_extension_V1_0.xsd"
            
            dwd_ns = ns['dwd']

            # Get Timestamps
            time_steps = []
            times = root.xpath(".//dwd:TimeStep", namespaces=ns)
            for t in times:
                time_steps.append(t.text)
                
            if not time_steps:
                self._logger.warning("No TimeSteps found in KML.")
                return None

            placemarks = root.xpath(".//kml:Placemark", namespaces=ns)
            
            data_list = []
            
            for pm in placemarks:
                name = pm.find("kml:name", namespaces=ns).text
                
                forecasts = pm.xpath(".//dwd:Forecast", namespaces=ns)
                
                for forecast in forecasts:
                    element_name = forecast.get(f"{{{dwd_ns}}}elementName")
                    value_str = forecast.find(f"dwd:value", namespaces=ns).text

                    values = value_str.strip().split()
                    # Check length match
                    if len(values) != len(time_steps):
                        # Sometimes values might be compressed or fewer?
                        self._logger.warning(
                            f"Value count mismatch for {element_name}: {len(values)} vs {len(time_steps)} timestamps."
                        )
                    
                    for i, val in enumerate(values):
                        if i < len(time_steps):
                            # Clean value
                            try:
                                v_float = float(val)
                            except ValueError:
                                v_float = float('nan') # Handle '-' or other
                                
                            data_list.append({
                                'Station': name,
                                'Time': time_steps[i],
                                'Parameter': element_name,
                                'Value': v_float
                            })
                            
            if not data_list:
                return None
                
            # Convert to DataFrame
            long_df = pd.DataFrame(data_list)
            
            # Index: Station, Time. Columns: Parameter
            df = long_df.pivot_table(index=['Station', 'Time'], columns='Parameter', values='Value')
            
            return df

        except Exception as e:
            self._logger.error(f"Error parsing KML: {e}")
            raise
