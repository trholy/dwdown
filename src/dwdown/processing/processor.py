import re
import os
import bz2
import shutil
import logging
import xarray as xr
import pandas as pd
from typing import List, Optional, Union, Set, Dict


# Configure logging to remove the default prefix
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
)


class DataProcessor:
    def __init__(
            self,
            search_path,
            extraction_path,
            converted_files_path
    ):
        """
        Initializes the DataProcessor.
        """
        self.search_path = search_path
        self.extraction_path = extraction_path
        self.converted_files_path = converted_files_path

        self._ensure_directory_exists(self.extraction_path)
        self._ensure_directory_exists(self.converted_files_path)

        self.failed_files = []

        self.logger = logging.getLogger(__name__)

    @staticmethod
    def _ensure_directory_exists(
            directory: str
    ) -> None:
        """
        Ensures that the directory exists. If not, it creates the directory.
        """
        if not os.path.exists(directory):
            os.makedirs(directory)
        elif os.path.isfile(directory):
            # In case the path was mistakenly assumed to be a directory
            raise FileExistsError(
                f"A file already exists with the name: {directory}")

    def _decompress_files(
            self,
            file_to_decompress: str
    ) -> str:
        """
        Decompresses a .bz2 file and returns the path to the decompressed file.
        """
        decompressed_file_path = self._get_decompressed_file_path(
            file_to_decompress
        )

        try:
            # Check if the decompressed file already exists
            if not os.path.exists(decompressed_file_path):
                # Decompress the .bz2 file
                with bz2.BZ2File(file_to_decompress, 'rb') as bz2_file:
                    with open(decompressed_file_path, 'wb') as output_file:
                        shutil.copyfileobj(bz2_file, output_file)

                logging.info(f"Decompressed file: {decompressed_file_path}")
            else:
                logging.warning(
                    f"File already exists: {decompressed_file_path},"
                    f" skipping decompression.")

            return decompressed_file_path

        except PermissionError as e:
            logging.error(
                f"Permission error while decompressing file"
                f" {file_to_decompress}: {e}"
            )
            raise
        except Exception as e:
            logging.error(f"Error decompressing file {file_to_decompress}: {e}")
            raise

    def _get_decompressed_file_path(
            self,
            file_to_decompress: str
    ) -> str:
        """
        Generates the path for the decompressed file.
        """
        # Replace .bz2 and adjust path
        decompressed_file_path = file_to_decompress.replace('.bz2', '')
        decompressed_file_path = os.path.join(
            self.extraction_path, decompressed_file_path
        )
        decompressed_file_path = decompressed_file_path.replace(
            self.search_path, ''
        )
        decompressed_file_path = os.path.normpath(decompressed_file_path)

        # Ensure the directory exists
        directory = os.path.dirname(decompressed_file_path)
        self._ensure_directory_exists(directory)

        return decompressed_file_path

    def _read_grib_to_dataframe(
            self,
            decompressed_file_path: str,
            apply_geo_filtering: bool,
            start_lat: Optional[float],
            end_lat: Optional[float],
            start_lon: Optional[float],
            end_lon: Optional[float]
    ) -> None:
        """
        Converts the decompressed GRIB file and
         converts it to a dataframe and CSV.

        Parameters:
        - file_to_decompress_path (str): Path to the GRIB file.
        - apply_geo_filtering (bool): Whether to apply geographic filtering.
        - start_lat (float): Minimum latitude.
        - end_lat (float): Maximum latitude.
        - start_lon (float): Minimum longitude.
        - end_lon (float): Maximum longitude.

        Returns:
        - None
        """

        # Define output csv path
        csv_file_path = self._get_converted_file_path(
            decompressed_file_path
        )

        try:
            # Check if the converted file already exists
            if not os.path.exists(csv_file_path):
                grib_data = xr.open_dataset(decompressed_file_path,
                                            engine='cfgrib')
                df = grib_data.to_dataframe().reset_index()

                # Apply geographic filtering if necessary
                if apply_geo_filtering:
                    # Validate required columns exist
                    required_columns = {'latitude', 'longitude'}
                    if not required_columns.issubset(df.columns):
                        logging.error(f"Missing required columns in GRIB file:"
                                      f" {required_columns - set(df.columns)}")
                        return

                    df = self._filter_by_coordinates(
                        df,
                        start_lat, end_lat,
                        start_lon, end_lon
                    )
                self._save_as_csv(df, csv_file_path)

                logging.info(f"Converted file saved: {csv_file_path}")
            else:
                logging.warning(
                    f"File already exists: {decompressed_file_path},"
                    f" skipping converting.")

        except Exception as e:
            logging.error(
                f"Error while processing GRIB file {decompressed_file_path}: "
                f"{e}")

    @staticmethod
    def _filter_by_coordinates(
            df: pd.DataFrame,
            start_lat: float,
            end_lat: float,
            start_lon: float,
            end_lon: float
    ) -> pd.DataFrame:
        """
        Filters the dataframe to include only rows within
         the given latitude and longitude range.

        Parameters:
        - df (pd.DataFrame): Input DataFrame.
        - start_lat (float): Minimum latitude.
        - end_lat (float): Maximum latitude.
        - start_lon (float): Minimum longitude.
        - end_lon (float): Maximum longitude.

        Returns:
        - pd.DataFrame: Filtered DataFrame.
        """
        if None in (start_lat, end_lat, start_lon, end_lon):
            logging.warning(
                "Geographic filtering is enabled but"
                " no valid coordinates provided. Skipping filter."
            )
            return df

        filtered_df = df[
            (df['latitude'].between(start_lat, end_lat)) &
            (df['longitude'].between(start_lon, end_lon))
        ]
        return filtered_df

    @staticmethod
    def _save_as_csv(
            df: pd.DataFrame,
            csv_file_path: str
    ) -> None:
        """
        Saves a DataFrame as a CSV file.

        Parameters:
        - df (pd.DataFrame): DataFrame to save.
        - original_file_path (str): Original GRIB file path.

        Returns:
        - None
        """
        try:
            df.to_csv(csv_file_path, index=False)
            logging.info(f"Filtered data saved as CSV: {csv_file_path}")
        except Exception as e:
            logging.error(f"Error saving CSV: {e}")

    def _get_converted_file_path(
            self,
            decompressed_file_path: str
    ) -> str:
        """
        Generates the path for the CSV file based on the decompressed file path.
        """
        # Replace extraction path with CSV path
        csv_file_path = decompressed_file_path.replace(
            self.extraction_path, self.converted_files_path
        )
        directory = os.path.dirname(csv_file_path)
        self._ensure_directory_exists(directory)

        # Add '.csv' extension
        return csv_file_path.replace('.grib2', '.csv')

    def flatten_list(
            self,
            nested_list: Union[List, str]
    ) -> List[str]:
        """Recursively flattens a nested list of filenames."""
        if isinstance(nested_list, str):
            return [nested_list]
        elif isinstance(nested_list, list):
            flattened = []
            for item in nested_list:
                flattened.extend(self.flatten_list(item))
            return flattened
        else:
            raise TypeError(
                "Expected a list or string, but got: " + str(type(nested_list)))

    def _search_directory(
            self,
            directory: str,
            include_pattern: List[str],
            exclude_pattern: List[str],
            name_startswith: str,
            name_endswith: str,
    ) -> List[str]:
        """Recursively search for matching files in the given directory."""
        filenames = []
        for entry in os.scandir(directory):
            if entry.is_file():
                # Check patterns and add matching files
                if entry.name.startswith(
                        name_startswith) and entry.name.endswith(name_endswith):
                    if any(pattern in entry.name for pattern in
                           include_pattern) and not any(
                            pattern in entry.name for pattern in
                            exclude_pattern):
                        filenames.append(entry.path)
            elif entry.is_dir():
                # Extend the list with results from subdirectories
                filenames.append(self._search_directory(
                    entry.path,
                    include_pattern,
                    exclude_pattern,
                    name_startswith,
                    name_endswith,
                ))

        return filenames

    def get_filenames(
            self,
            name_startswith: str = "",
            name_endswith: str = "",
            include_pattern: List[str] = None,
            exclude_pattern: List[str] = None,
    ) -> list:
        """
        Searches for files in the search path and includes subdirectory
         search if needed.

        :param name_startswith: String that filenames must start with
        :param name_endswith: String that filenames must end with
        :param include_pattern: List of substrings;
         at least one must be in the filename
        :param exclude_pattern: List of substrings;
         filenames with any of these are excluded
        :return: Sorted, filtered list of filenames
        """
        include_pattern = include_pattern or [""]
        exclude_pattern = exclude_pattern or []

        # Initial search
        filenames = self._search_directory(
            self.search_path,
            include_pattern,
            exclude_pattern,
            name_startswith,
            name_endswith,
        )

        return self.flatten_list(sorted(filenames))

    def get_csv(
            self,
            file_names: List[str],
            apply_geo_filtering: bool = False,
            start_lat: Optional[float] = None,
            end_lat: Optional[float] = None,
            start_lon: Optional[float] = None,
            end_lon: Optional[float] = None
    ) -> None:
        """
        Processes GRIB files: decompresses and converts them to CSV.

        Parameters:
        - file_names (List[str]): List of file names to process.
        - apply_geo_filtering (bool): Whether to filter data
        based on geographic coordinates.
        - start_lat (float, optional): Minimum latitude for filtering.
        - end_lat (float, optional): Maximum latitude for filtering.
        - start_lon (float, optional): Minimum longitude for filtering.
        - end_lon (float, optional): Maximum longitude for filtering.

        Returns:
        - None
        """

        if not file_names:
            logging.warning("No files provided. Exiting.")
            return

        logging.info(f"Processing {len(file_names)} files...")

        for idx, file in enumerate(file_names, start=1):
            try:
                logging.info(
                    f"[{idx}/{len(file_names)}] Decompressing {file}...")
                decompressed_file_path = self._decompress_files(file)

                logging.info(
                    f"[{idx}/{len(file_names)}]"
                    f" Converting {file} to CSV...")
                self._read_grib_to_dataframe(
                    decompressed_file_path,
                    apply_geo_filtering,
                    start_lat,
                    end_lat,
                    start_lon,
                    end_lon
                )

                logging.info(
                    f"[{idx}/{len(file_names)}] Successfully processed {file}.")

            except FileNotFoundError as e:
                logging.error(f"File not found: {file}. Skipping. Error: {e}")

            except Exception as e:
                logging.error(f"Error processing {file}: {e}")
                self.failed_files.append(file)


class DataEditor:
    def __init__(
            self,
            files_path: str,
            required_columns: Optional[Set[str]] = None,
            join_method: str = 'inner',
            sep: str = ',',
            index_col: str = None,
            mapping_dictionary: Optional[Dict[str, str]] = None,
            additional_pattern_selection: Optional[Dict[
                str, Union[List[int], int]
            ]] = None
    ):
        """
        Initializes the DataEditor.

        :param files_path: Root path of the CSV files.
        :param logger: Logger instance for logging warnings and errors.
        :param sep: CSV separator (default: ',').
        :param index_col: Column to be used as index (default: None).
        :param required_columns: Required columns in DataFrame.
        :param additional_pattern_selection: Dictionary specifying additional
         patterns for known variables.
        """
        self.required_columns = required_columns or {
            'latitude', 'longitude', 'valid_time'
        }
        self.join_method = join_method
        self.files_path = files_path
        self.index_col = index_col
        self.sep = sep

        # Allow external mapping dictionary
        self.mapping_dictionary = mapping_dictionary or {
            'aswdifd_s': 'ASWDIFD_S',
            'aswdir_s': 'ASWDIR_S',
            'cape_ml': 'CAPE_ML',
            'clch': 'CLCH',
            'clcl': 'CLCL',
            'clcm': 'CLCM',
            'clct': 'CLCT',
            'grau_gsp': 'tgrp',
            'h_snow': 'sde',
            'prg_gsp': 'PRG_GSP',
            'prr_gsp': 'lsrr',
            'prs_gsp': 'lssfr',
            'ps': 'sp',
            'q_sedim': 'Q_SEDIM',
            'rain_con': 'crr',
            'rain_gsp': 'lsrr',
            'relhum': 'r',
            'relhum_2m': 'r2',
            'rho_snow': 'rsn',
            'runoff_g': 'RUNOFF_G',
            'runoff_s': 'RUNOFF_S',
            'smi': 'SMI',
            'snow_con': 'csfwe',
            'snow_gsp': 'lsfwe',
            'soiltyp': 'SOILTYP',
            'td_2m': 'd2m',
            'tke': 'tke',
            'tmax_2m': 'mx2t',
            'tmin_2m': 'mn2t',
            'tot_prec': 'tp',
            'tqc': 'TQC',
            'tqg': 'TQG',
            'tqi': 'TQI',
            'tqr': 'tcolr',
            'tqs': 'tcols',
            'tqv': 'TQV',
            'twater': 'TWATER',
            't_2m': 't2m',
            't_g': 'T_G',
            't_snow': 'T_SNOW',
            't_so': 'T_SO',
            'u': 'u',
            'uh_max': 'UH_MAX',
            'uh_max_low': 'UH_MAX_LOW',
            'uh_max_med': 'UH_MAX_MED',
            'u_10m': 'u10',
            'v': 'v',
            'vis': 'vis',
            'vmax_10m': 'fg10',
            'vorw_ctmax': 'VORW_CTMAX',
            'v_10m': 'v10',
            'w': 'wz',
            'ww': 'WW',
            'w_ctmax': 'W_CTMAX',
            'w_so': 'W_SO',
            'w_so_ice': 'W_SO_ICE',
            'z0': 'fsr'
        }

        self.additional_pattern_selection = additional_pattern_selection or {}

        self.logger = logging.getLogger(__name__)

    def _validate_columns_exist(
            self,
            df: pd.DataFrame,
            required_columns: Set[str],
            variable: str
    ) -> bool:
        """
        Validates that the required columns exist in the DataFrame.
        """
        mapped_columns = {self.mapping_dictionary.get(col, col) for col in
                          required_columns}
        mapped_variable = self.mapping_dictionary.get(
            variable, variable
        )

        missing_columns = mapped_columns - set(df.columns)
        if missing_columns:
            self.logger.error(
                f"Missing required columns in CSV file: {missing_columns}. "
                f"Skipping variable: {variable} (expected: {mapped_variable})."
            )
            return False
        return True

    @staticmethod
    def _filter_dataframe(
            df: pd.DataFrame,
            required_columns: set,
            variable: str
    ) -> pd.DataFrame:
        """
        Filters the DataFrame to keep only required columns.
        """

        columns = required_columns.copy()
        selected_columns = list(columns) + [variable]

        return df[selected_columns]

    @staticmethod
    def _parse_datetime(
            df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Attempts to parse the 'valid_time' column to datetime.

        :param df (pd.DataFrame): The dataframe containing the
         'valid_time' column.
        :return: pd.DataFrame: The dataframe with the parsed
         'valid_time' column.
        """
        try:
            # Parse datetime with error coercion to handle any malformed data.
            df["valid_time"] = pd.to_datetime(df["valid_time"], errors='coerce')

            # Log the number of successfully parsed dates vs NaT (Not a Time)
            invalid_dates = df["valid_time"].isna().sum()
            if invalid_dates > 0:
                logging.warning(
                    f"{invalid_dates} invalid 'valid_time'"
                    f" entries were coerced to NaT.")

        except ValueError as e:
            logging.error(f"ValueError while parsing 'valid_time': {e}")
        except KeyError as e:
            logging.error(
                f"KeyError: 'valid_time' column not found"
                f" in the dataframe: {e}")
        except Exception as e:
            logging.error(f"Unexpected error while parsing 'valid_time': {e}")

        return df

    def _merge_dataframes(
            self,
            df1: pd.DataFrame,
            df2: pd.DataFrame,
            merge_on: Set[str],
    ) -> pd.DataFrame:
        """
        Merges two DataFrames based on specified columns.
        """
        common_columns = set(df1.columns) & set(df2.columns) & merge_on
        if not common_columns:
            self.logger.warning(
                f"No common columns found for merging."
                f" Returning df1 unchanged.")
            return df1

        return df1.merge(df2, on=list(common_columns), how=self.join_method)

    def _get_csv_file(
            self,
            variable: str
    ) -> List[str]:
        """
        Function to get CSV file path for a variable.
        """
        files_path = os.path.join(self.files_path, variable)

        files_to_read = []
        for root, _, files in os.walk(files_path):
            for file in files:
                local_file_path = os.path.join(root, file)
                files_to_read.append(local_file_path)

        if not files_to_read:
            self.logger.warning(f"No CSV files found for variable: {variable}")

        return files_to_read

    def _filter_file_names(
            self,
            filenames: List[str],
            name_startswith: str = "icon-d2_germany",
            name_endswith: str = ".csv",
            include_pattern: Union[str, List[str]] = None,
            exclude_pattern: List[str] = None,
            variable: Optional[str] = None
    ) -> Optional[List[str]]:
        """
        Filters filenames based on start, end, inclusion, exclusion patterns,
        and additional pattern selection.

        :param filenames: List of filenames to filter
        :param name_startswith: String that filenames must start with
        :param name_endswith: String that filenames must end with
        :param include_pattern: A substring or a list of substrings; at least
         one must be in the filename
        :param exclude_pattern: List of substrings; filenames with any of
         these are excluded
        :param variable: Variable name being processed
        :return: The selected filename(s) or None if no match is found.
        """
        if isinstance(include_pattern, str):
            include_pattern = [include_pattern]  # Convert single string to list
        include_pattern = include_pattern or [""]
        exclude_pattern = exclude_pattern or []

        valid_files = []
        detected_patterns = set()

        expected_patterns = self.additional_pattern_selection.get(
            variable, None
        )

        for file in filenames:
            filename = os.path.basename(file)

            # Check original filtering conditions
            if (filename.startswith(name_startswith) and filename.endswith(
                    name_endswith)
                    and any(pattern in filename for pattern in include_pattern)
                    and not any(
                        pattern in filename for pattern in exclude_pattern)):

                # Extract additional pattern (if exists)
                additional_pattern = self._extract_additional_pattern(filename)
                detected_patterns.add(additional_pattern)

                # If no additional pattern is detected, add file by default
                if additional_pattern is None:
                    valid_files.append(file)
                    continue

                # Check if this variable has a predefined pattern selection
                if expected_patterns:
                    if additional_pattern in expected_patterns:
                        valid_files.append(file)

        # Check if an expected pattern was **not** found in any filename
        if expected_patterns:
            missing_patterns = set(expected_patterns) - detected_patterns
            if missing_patterns:
                self.logger.error(
                    f"Expected additional pattern(s) {missing_patterns}"
                    f" for variable '{variable}', "
                    f"but none were found in available filenames!"
                )

        # Log warning only if unknown patterns were detected AND
        if detected_patterns and expected_patterns and not valid_files:
            self.logger.warning(
                f"Detected additional pattern(s) {detected_patterns}"
                f"for '{variable}', but no matching selection provided."
                f" Skipping."
            )
            return None

        if not valid_files:
            self.logger.warning(
                f"No valid file found for variable:"
                f" {variable} with required pattern.")
            return None

        return valid_files

    def _variable_mapping(
            self,
            variables: List[str]
    ) -> List[str]:
        """
        Maps manual variable names to actual CSV column names.
        Maintains the original order of variables.
        """
        return [self.mapping_dictionary.get(var, var) for var in variables]

    def _read_dataframe_from_csv(
            self,
            csv_file: str
    ) -> Optional[pd.DataFrame]:
        """
        Reads a CSV file into a DataFrame with error handling.
        """
        try:
            return pd.read_csv(csv_file, sep=self.sep, index_col=self.index_col)
        except FileNotFoundError as e:
            self.logger.error(f"File not found: {csv_file}", exc_info=True)
        except pd.errors.EmptyDataError as e:
            self.logger.error(f"File is empty: {csv_file}", exc_info=True)
        except Exception as e:
            self.logger.error(f"Error reading file {csv_file}: {e}",
                              exc_info=True)
        return None

    def merge_dfs(
            self,
            time_step: Union[str, int],
            variables: List[str],
            required_columns: Optional[Set[str]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Merges multiple CSV files into a DataFrame based on shared columns.
        """
        required_columns = required_columns or self.required_columns
        dataframes = []
        df_column_len = []

        variables_mapped = self._variable_mapping(variables)

        matching_id = f"_{str(time_step).zfill(3)}_"

        for variable, variable_mapped in zip(variables, variables_mapped):
            csv_files = self._get_csv_file(variable)
            selected_files = self._filter_file_names(
                filenames=csv_files,
                include_pattern=matching_id,
                variable=variable
            )

            if not selected_files:
                self.logger.warning(
                    f"No matching file found for variable: {variable}."
                    f" Skipping.")
                continue

            if isinstance(selected_files, str):
                selected_files = [selected_files]  # Ensure it's a list

            for csv_file in selected_files:
                additional_pattern = self._extract_additional_pattern(csv_file)

                df = self._read_dataframe_from_csv(csv_file)
                df_column_len.append(df.shape[0])
                if df is not None and self._validate_columns_exist(
                        df,
                        required_columns,
                        variable_mapped
                ):
                    df = self._parse_datetime(df)

                    # If additional pattern, rename the mapped variable column
                    if additional_pattern is not None:
                        new_variable_name = (f"{variable_mapped}"
                                             f"_{additional_pattern}")
                    else:
                        new_variable_name = variable_mapped

                    df = df.rename(
                        columns={variable_mapped: new_variable_name}
                    )
                    dataframes.append(self._filter_dataframe(
                        df,
                        required_columns,
                        new_variable_name
                    ))

        if not dataframes:
            self.logger.error("No valid dataframes found. Merging aborted.")
            return None

        if min(df_column_len) != max(df_column_len):
            self.logger.info(f"Dataframes have different lengths ranging from "
                             f"{min(df_column_len)} to {max(df_column_len)}. "
                             f"Keep effects of 'join_method' ="
                             f" {self.join_method} in mind!")

        merged_df = dataframes[0]
        for df in dataframes[1:]:
            merged_df = self._merge_dataframes(
                merged_df,
                df,
                required_columns
            )

        return merged_df

    @staticmethod
    def _extract_additional_pattern(
            filename: str
    ) -> Optional[int]:
        """
        Extracts the additional pattern from a filename.
        Returns an integer if a valid pattern exists; otherwise, None.
        """
        # Matches "_{digits}_" before variable name
        match = re.search(r"_(\d+)_([a-zA-Z_]+)\.csv$", filename)
        if match:
            pattern = match.group(1)  # Extract the numeric pattern
            return int(pattern) if pattern.isdigit() else None
        return None
