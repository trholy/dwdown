import os

import pandas as pd

from ..utils import (
    Utilities, LogHandler,
    FileHandler,
    DateHandler,
    DataFrameOperator)

from ..data import MappingStore


class DataMerger(
    Utilities, LogHandler, FileHandler, DateHandler, DataFrameOperator, MappingStore):
    def __init__(
            self,
            files_path: str,
            join_method: str | None =  None,
            additional_patterns: dict[str, list[int] | int] | None = None,
            mapping_dictionary: dict[str, str] | None = None,
            required_columns: set[str] | None = None,
            log_files_path: str | None = None,
            sep: str = ',',
            index_col: str | None = None
    ):
        """
        Initializes the DataEditor with necessary parameters.

        :param files_path: Path to the directory containing input files.
        :param join_method: Method to use for merging dataframes (default is 'outer').
        :param additional_patterns: Additional patterns for file filtering (optional).
        :param mapping_dictionary: Dictionary for mapping variable names (optional).
        :param required_columns: Set of required columns in the dataframes (optional).
        :param log_files_path: Path to the directory for log files (optional).
        :param sep: Separator used in CSV files (default is ',').
        :param index_col: Column to use as the index in dataframes (optional).
        """
        self.files_path = os.path.normpath(files_path or "converted_files")
        self.log_files_path = os.path.normpath(log_files_path or "log_files")

        Utilities.__init__(self)

        FileHandler.__init__(self)
        self._ensure_directories_exist([self.files_path, self.log_files_path])

        LogHandler.__init__(self, self.log_files_path, True, True)
        self._logger = self.get_logger()

        DateHandler.__init__(self)
        DataFrameOperator.__init__(self)
        MappingStore.__init__(self)

        self._required_columns = required_columns or {
            'latitude', 'longitude', 'valid_time'}
        self._join_method = join_method or 'outer'
        self._index_col = index_col
        self._sep = sep

        self.mapping_dict = self.get_mapping_dict()
        if mapping_dictionary is not None:
            self.mapping_dict.update(mapping_dictionary)

        self.additional_patterns = additional_patterns or {}

        self.selected_csv_files = []

    def merge(
            self,
            time_step: str | int,
            variables: list[str],
            prefix: str | None = None,
            suffix: str | None = None,
            include_pattern: list[str] | None = None,
            exclude_pattern: list[str] | None = None,
            skip_time_step_filtering_variables: list[str] | None = None
    ) -> pd.DataFrame | None:
        """
        Merges dataframes based on the provided parameters.

        :param time_step: Time step for filtering files.
        :param variables: List of variables to merge.
        :param prefix: Prefix for filtering filenames.
        :param suffix: Suffix for filtering filenames.
        :param include_pattern: Patterns to include in filenames.
        :param exclude_pattern: Patterns to exclude from filenames.
        :param skip_time_step_filtering_variables: Variables to skip time step filtering.
        :return: Merged dataframe or None if no valid dataframes are found.
        """
        dataframe_list = []
        df_column_len = []

        variables = self._string_to_list(variables)
        variables_mapped = self._variable_mapping(variables, self.mapping_dict)

        skip_time_step_filtering_variables = self._string_to_list(
            skip_time_step_filtering_variables)
        skip_time_step_filtering_variables_mapped = self._variable_mapping(
            skip_time_step_filtering_variables, self.mapping_dict)

        for variable, variable_mapped in zip(variables, variables_mapped, strict=False):
            variable_files_path = os.path.join(self.files_path, variable)
            variable_files_path = os.path.normpath(variable_files_path)
            csv_files_path = self._search_directory(variable_files_path, ".csv")

            if variable_mapped not in skip_time_step_filtering_variables_mapped:
                timesteps = self._process_timesteps(
                    min_timestep=time_step,
                    max_timestep=time_step)
            else:
                timesteps = []

            filtered_files = self._simple_filename_filter(
                filenames=csv_files_path,
                prefix=prefix,
                suffix=suffix,
                include_pattern=include_pattern,
                exclude_pattern=exclude_pattern,
                skip_time_step_filtering_variables=skip_time_step_filtering_variables,
                timesteps=timesteps)

            selected_files = self._match_filenames_by_patterns(
                filenames=filtered_files,
                variable=variable)

            if not selected_files:
                self._logger.warning(
                    f"No matching file found for variable: {variable}."
                    f" Skipping.")
                continue

            selected_files = self._string_to_list(selected_files)

            if isinstance(selected_files, (str, list)):
                self.selected_csv_files.extend(selected_files)

            for csv_file in selected_files:
                additional_pattern = self._extract_additional_pattern(csv_file)

                df = self._read_df_from_csv(csv_file)
                if df is None:
                    continue

                df_column_len.append(df.shape[0])
                columns_exist = self._validate_columns_exist(
                    df, self._required_columns, variable_mapped, self.mapping_dict)

                if columns_exist:
                    df['valid_time'] = self._parse_datetime(
                        df['valid_time'], 'valid_time')

                    # If additional pattern, rename the mapped variable column
                    variable_name = f"{variable_mapped}_{additional_pattern}" \
                         if additional_pattern else variable_mapped
                    #variable_name = f"{variable_mapped}_{additional_pattern}"
                    df = df.rename(columns={variable_mapped: variable_name})

                    filtered_df = self._filter_dataframe(
                        df,
                        self._required_columns,
                        variable_name)
                    dataframe_list.append(filtered_df)

        if not dataframe_list:
            self._logger.error("No valid dataframes found. Merging aborted.")
            return None

        if min(df_column_len) != max(df_column_len):
            self._logger.info(
                f"Dataframes have different lengths ranging from "
                f"{min(df_column_len)} to {max(df_column_len)}. "
                f"Keep effects of 'join_method' = {self._join_method} in mind!")

        merged_df = dataframe_list[0]
        for df in dataframe_list[1:]:
            merged_df = self._merge_dataframes(
                merged_df,
                df,
                self._required_columns,
                self._join_method)

        arranged_df = self._arrange_df(merged_df)

        return arranged_df.reset_index(drop=True)

    def _match_filenames_by_patterns(
            self,
            filenames: list[str],
            variable: str
    ) -> list[str] | None:
        """
        Filters filenames based on additional patterns.

        :param filenames: List of filenames to filter.
        :param variable: Variable for which to filter filenames.
        :return: List of filtered filenames or None if no valid files are found.
        """
        expected_patterns = self.additional_patterns.get(variable, None)

        matching_files = []
        detected_patterns = set()

        for file in filenames:
            filename = os.path.basename(file)
            additional_pattern = self._extract_additional_pattern(filename)
            detected_patterns.add(additional_pattern)

            if additional_pattern is None or expected_patterns is None:
                matching_files.append(file)
                continue

            if expected_patterns:
                if additional_pattern in expected_patterns:
                    matching_files.append(file)

        if expected_patterns:
            missing_patterns = set(expected_patterns) - detected_patterns
            if missing_patterns:
                self._logger.error(
                    f"Expected additional pattern(s) {missing_patterns}"
                    f" for variable '{variable}', "
                    f"but none were found in available filenames!")

        if detected_patterns and expected_patterns and not matching_files:
            self._logger.warning(
                f"Detected additional pattern(s) {detected_patterns}"
                f" for '{variable}', but no matching selection provided."
                f" Skipping.")
            return None

        if not matching_files:
            self._logger.warning(
                f"No valid file found for variable:"
                f" {variable} with required pattern.")
            return None

        return matching_files

    def delete(self) -> None:
        """
        Deletes local files after successful processing.

        """
        self._delete_files_safely(
            self.selected_csv_files, "csv file")
        self._cleanup_empty_dirs(self.files_path)
