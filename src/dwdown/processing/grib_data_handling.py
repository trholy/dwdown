import os

import bz2
import shutil
import xarray as xr
import glob

from ..utils import (
    Utilities, LogHandler,
    FileHandler,
    DateHandler,
    DataFrameOperator)


class GribFileManager(
    Utilities, LogHandler, FileHandler, DateHandler, DataFrameOperator):
    def __init__(
            self,
            files_path: str,
            extracted_files_path: str | None = None,
            converted_files_path: str | None = None,
            log_files_path: str | None = None
    ):
        """
        Initializes the DataProcessor with paths for files, extraction,
         conversion, and logging.

        :param files_path: Path to the directory containing the files to process.
        :param extracted_files_path: Path to the directory for decompressed files.
        :param converted_files_path: Path to the directory for converted files.
        :param log_files_path: Path to the directory for log files.
        """
        self.files_path = os.path.normpath(files_path or "download_files")
        self.extracted_files_path = os.path.normpath(
            extracted_files_path or "extracted_files")
        self.converted_files_path = os.path.normpath(
            converted_files_path or "converted_files")
        self.log_files_path = os.path.normpath(log_files_path or "log_files")

        Utilities.__init__(self)

        FileHandler.__init__(self)
        self._ensure_directories_exist([
            self.files_path, self.extracted_files_path,
            self.converted_files_path, self.log_files_path])

        LogHandler.__init__(self, self.log_files_path, True, True)
        self._logger = self.get_logger()

        DateHandler.__init__(self)
        DataFrameOperator.__init__(self)

        self.processed_download_files = []
        self.decompressed_files = []
        self.failed_files = []
        self.converted_files = []

    def _get_decompression_path(self, file_path: str) -> str:
        """
        Generates the path for the decompressed file.

        :param file_path: Path to the file to decompress.
        :return: Path to the decompressed file.
        """
        decompressed_file_path = file_path.replace('.bz2', '')
        decompressed_file_path = decompressed_file_path.replace(
            self.files_path, self.extracted_files_path)
        decompressed_file_path = os.path.normpath(decompressed_file_path)

        self._ensure_directory_exists(os.path.dirname(decompressed_file_path))

        return decompressed_file_path

    def _get_conversion_path(self, file_path: str) -> str:
        """
        Generates the path for the converted file.

        :param file_path: Path to the decompressed file.
        :return: Path to the converted file.
        """
        csv_file_path = file_path.replace(
            self.extracted_files_path, self.converted_files_path)
        csv_file_path = csv_file_path.replace('.grib2', '.csv')
        csv_file_path = os.path.normpath(csv_file_path)

        self._ensure_directory_exists(os.path.dirname(csv_file_path))

        return csv_file_path

    def _decompress_files(self, file_path: str) -> str:
        """
        Decompresses a .bz2 file.

        :param file_path: Path to the file to decompress.
        :return: Path to the decompressed file.
        """
        decompressed_file_path = self._get_decompression_path(file_path)

        try:
            if not os.path.exists(decompressed_file_path):
                with bz2.BZ2File(file_path, 'rb') as bz2_file:
                    with open(decompressed_file_path, 'wb') as output_file:
                        shutil.copyfileobj(bz2_file, output_file)
                self._logger.info(
                    f"Decompressed file: {os.path.basename(decompressed_file_path)}")
            else:
                self._logger.warning(
                    f"File already exists: {decompressed_file_path},"
                    f" skipping decompression.")
            return decompressed_file_path
        except PermissionError as e:
            self._logger.error(
                f"Permission error while decompressing"
                f" file {file_path}: {e}")
            raise
        except Exception as e:
            self._logger.error(
                f"Error decompressing file {file_path}: {e}")
            raise

    def _grib_to_df(
            self,
            file_path: str,
            apply_geo_filtering: bool,
            start_lat: float | None,
            end_lat: float | None,
            start_lon: float | None,
            end_lon: float | None
    ) -> None:
        """
        Reads a GRIB file into a DataFrame and optionally applies
         geographic filtering.

        :param file_path: Path to the decompressed GRIB file.
        :param apply_geo_filtering: Whether to apply geographic filtering.
        :param start_lat: Starting latitude for filtering.
        :param end_lat: Ending latitude for filtering.
        :param start_lon: Starting longitude for filtering.
        :param end_lon: Ending longitude for filtering.
        """
        csv_file_path = self._get_conversion_path(file_path)

        try:
            if not os.path.exists(csv_file_path):
                grib_data = xr.open_dataset(file_path, engine='cfgrib')
                df = grib_data.to_dataframe().reset_index()

                if apply_geo_filtering:
                    required_columns = {'latitude', 'longitude'}
                    if not required_columns.issubset(df.columns):
                        self._logger.error(
                            f"Missing required columns in GRIB file:"
                            f" {required_columns - set(df.columns)}")
                        return

                    df = self._filter_by_coordinates(
                        df,
                        start_lat, end_lat,
                        start_lon, end_lon)
                self._save_as_csv(df, csv_file_path)

                self.converted_files.append(csv_file_path)
            else:
                self._logger.warning(
                    f"File already exists: {file_path}, skipping converting.")
        except Exception as e:
            self._logger.error(
                f"Error while processing GRIB file {file_path}: {e}")

    def get_filenames(
            self,
            prefix: str | None = None,
            suffix: str | None = None,
            min_timestep: str | int | None = None,
            max_timestep: str | int | None = None,
            include_pattern: list[str] | None = None,
            exclude_pattern: list[str] | None = None,
            additional_patterns: dict | None = None,
            skip_time_step_filtering_variables: list[str] | None = None,
            variables: list[str] | None = None
    ) -> list:
        """
        Retrieves filenames based on specified criteria.

        :param prefix: Files must start with this string.
        :param suffix: Files must end with this string.
        :param min_timestep: Minimum timestep for filtering.
        :param max_timestep: Maximum timestep for filtering.
        :param include_pattern: Patterns to include in filenames.
        :param exclude_pattern: Patterns to exclude from filenames.
        :param additional_patterns: Additional patterns for filtering.
        :param skip_time_step_filtering_variables: Variables to skip
         timestep filtering for.
        :param variables: Variables to filter by.
        :return: List of filenames.
        """

        include_pattern = self._process_timesteps(
            min_timestep=min_timestep,
            max_timestep=max_timestep,
            include_pattern=include_pattern)

        filenames = self._search_directory(self.files_path)
        filenames = self._flatten_list(filenames)

        filtered_files = self._simple_filename_filter(
            filenames=filenames,
            prefix=prefix,
            suffix=suffix,
            include_pattern=include_pattern,
            exclude_pattern=exclude_pattern,
            skip_time_step_filtering_variables=skip_time_step_filtering_variables)

        filtered_files = self._advanced_filename_filter(
            filenames=filtered_files,
            patterns=additional_patterns,
            variables=variables)

        return filtered_files

    def get_csv(
            self,
            file_names: list[str],
            apply_geo_filtering: bool = False,
            start_lat: float | None = None,
            end_lat: float | None = None,
            start_lon: float | None = None,
            end_lon: float | None = None
    ) -> None:
        """
        Processes files to convert them to CSV.

        :param file_names: List of file names to process.
        :param apply_geo_filtering: Whether to apply geographic filtering.
        :param start_lat: Starting latitude for filtering.
        :param end_lat: Ending latitude for filtering.
        :param start_lon: Starting longitude for filtering.
        :param end_lon: Ending longitude for filtering.
        """
        if not file_names:
            self._logger.warning("No files provided. Exiting.")
            return

        self._logger.info(f"Processing {len(file_names)} files...")

        for idx, file in enumerate(file_names, start=1):
            try:
                self._logger.info(
                    f"[{idx}/{len(file_names)}] Processing {os.path.basename(file)}.")
                decompressed_file_path = self._decompress_files(file)

                self._grib_to_df(
                    decompressed_file_path,
                    apply_geo_filtering,
                    start_lat,
                    end_lat,
                    start_lon,
                    end_lon)

                self._logger.info(
                    f"Successfully processed {os.path.basename(file)}.")

                self.processed_download_files.append(file)
                self.decompressed_files.extend(
                    [decompressed_file_path] + glob.glob(
                        f"{decompressed_file_path}.*.idx"))

            except FileNotFoundError as e:
                self._logger.error(
                    f"File not found: {file}. Skipping. Error: {e}")
                self.failed_files.append(file)

            except Exception as e:
                self._logger.error(f"Error processing {file}: {e}")
                self.failed_files.append(file)

    def delete(
            self,
            delete_downloaded: bool = True,
            delete_decompressed: bool = True,
            converted_files: bool = False
    ) -> None:
        """
        Deletes local files after successful processing.

        :param delete_downloaded: Whether to delete downloaded files.
        :param delete_decompressed: Whether to delete decompressed files.
        :param converted_files: Whether to delete converted files.
        """
        if delete_downloaded:
            self._delete_files_safely(
                self.processed_download_files, "downloaded file")
            self._cleanup_empty_dirs(self.files_path)

        if delete_decompressed:
            self._delete_files_safely(
                self.decompressed_files, "decompressed file")
            self._cleanup_empty_dirs(self.extracted_files_path)

        if converted_files:
            self._delete_files_safely(
                self.converted_files, "converted file")
            self._cleanup_empty_dirs(self.converted_files_path)
