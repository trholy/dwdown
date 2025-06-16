import logging
import os

import pandas as pd


class DataFrameOperator:
    def __init__(self):
        """
        Initializes the DataFrameOperator.

        """

    def _validate_columns_exist(
            self,
            df: pd.DataFrame,
            required_columns: set[str],
            variable: str,
            mapping_dictionary: dict[str, str]
    ) -> bool:
        """
        Validates that the required columns exist in the DataFrame after
         applying the mapping dictionary.

        :param df: DataFrame to validate.
        :param required_columns: Set of required column names.
        :param variable: Variable column name to check.
        :param mapping_dictionary: Dictionary to map column names.
        :return: True if all required columns exist, False otherwise.
        """
        mapped_columns = {
            mapping_dictionary.get(col, col) for col in required_columns}
        mapped_variable = mapping_dictionary.get(variable, variable)

        missing_columns = mapped_columns - set(df.columns)
        if missing_columns:
            self._logger.error(
                f"Missing required columns in DataFrame: {missing_columns}. "
                f"Skipping variable: {variable} (expected: {mapped_variable}).")
            return False
        return True

    @staticmethod
    def _filter_dataframe(
            df: pd.DataFrame,
            required_columns: set[str],
            variable: str
    ) -> pd.DataFrame:
        """
        Filters the DataFrame to include only the required columns and the variable column.

        :param df: DataFrame to filter.
        :param required_columns: Set of required column names.
        :param variable: Variable column name to include.
        :return: Filtered DataFrame.
        """
        selected_columns = list(required_columns) + [variable]
        return df[selected_columns]

    @staticmethod
    def _parse_datetime(
            series: pd.Series,
            column: str
    ) -> pd.Series:
        """
        Parses a Series to datetime format, coercing errors to NaT.

        :param series: Series to parse.
        :param column: Column name for logging purposes.
        :return: Parsed Series with datetime format.
        """
        try:
            parsed_series = pd.to_datetime(series, errors='coerce')
            invalid_dates = parsed_series.isna().sum()
            if invalid_dates > 0:
                logging.warning(
                    f"{invalid_dates} invalid '{column}'"
                    f" entries were coerced to NaT.")
        except ValueError as e:
            logging.error(f"ValueError while parsing '{column}': {e}")
            parsed_series = series  # Return the original series if parsing fails
        except Exception as e:
            logging.error(f"Unexpected error while parsing '{column}': {e}")
            parsed_series = series  # Return the original series if parsing fails

        return parsed_series

    @staticmethod
    def _filter_by_coordinates(
            df: pd.DataFrame,
            start_lat: float | None,
            end_lat: float | None,
            start_lon: float | None,
            end_lon: float | None
    ) -> pd.DataFrame:
        """
        Filters a DataFrame by geographic coordinates.

        :param df: DataFrame to filter.
        :param start_lat: Starting latitude.
        :param end_lat: Ending latitude.
        :param start_lon: Starting longitude.
        :param end_lon: Ending longitude.
        :return: Filtered DataFrame.
        """
        if None in (start_lat, end_lat, start_lon, end_lon):
            logging.warning(
                "Geographic filtering is enabled but no valid coordinates"
                " provided. Skipping filter.")
            return df

        filtered_df = df[
            (df['latitude'].between(start_lat, end_lat)) &
            (df['longitude'].between(start_lon, end_lon))]

        return filtered_df

    def _merge_dataframes(
            self,
            df1: pd.DataFrame,
            df2: pd.DataFrame,
            merge_on: set[str],
            join_method: str
    ) -> pd.DataFrame:
        """
        Merges two DataFrames on specified columns.

        :param df1: First DataFrame.
        :param df2: Second DataFrame.
        :param merge_on: Set of columns to merge on.
        :param join_method: Method to use for merging (e.g., 'inner',
         'outer', 'left', 'right').
        :return: Merged DataFrame.
        """
        # Validate input types
        if not isinstance(df1, pd.DataFrame):
            raise TypeError("df1 must be a pandas DataFrame.")
        if not isinstance(df2, pd.DataFrame):
            raise TypeError("df2 must be a pandas DataFrame.")
        if not isinstance(merge_on, set):
            raise TypeError("merge_on must be a set of column names.")
        if not isinstance(join_method, str):
            raise TypeError("join_method must be a string.")

        # Check if merge_on columns exist in both dataframes
        columns_in_df1 = set(df1.columns)
        columns_in_df2 = set(df2.columns)
        common_columns = columns_in_df1 & columns_in_df2 & merge_on

        if not common_columns:
            self._logger.warning(
                "No common columns found for merging on %s. "
                "Returning 'df1' unchanged.", merge_on)
            return df1

        # Log the merge operation details
        self._logger.info(
            "Merging dataframes on columns: %s using method: %s",
            common_columns, join_method)

        try:
            merged_df = df1.merge(
                df2, on=list(common_columns), how=join_method)
        except Exception as e:
            self._logger.error(
                "Error merging dataframes on columns: %s using method: %s. "
                "Returning 'df1' unchanged. Error: %s",
                common_columns, join_method, e)
            return df1

        # Log the result of the merge
        self._logger.info(
            "Merged dataframe shape: %s", merged_df.shape)

        return merged_df

    def _arrange_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Sorts and arranges the DataFrame columns.

        :param df: DataFrame to arrange.
        :return: Arranged DataFrame.
        """
        df = df.sort_values(by=['latitude', 'longitude', 'valid_time'])

        desired_order = ['valid_time', 'latitude', 'longitude']
        other_cols = [col for col in df.columns if col not in desired_order]

        return df[desired_order + other_cols]

    def _read_df_from_csv(
            self,
            csv_file: str,
            index_col: str | None = None,
            sep: str = ','
    ) -> pd.DataFrame | None:
        """
        Reads a CSV file into a DataFrame.

        :param csv_file: Path to the CSV file.
        :param index_col: Column to set as the index.
        :param sep: Separator used in the CSV file (default is ',').
        :return: DataFrame if successful, None otherwise.
        """
        try:
            return pd.read_csv(csv_file, sep=sep, index_col=index_col)
        except FileNotFoundError:
            self._logger.error(f"File not found: {csv_file}", exc_info=True)
        except pd.errors.EmptyDataError:
            self._logger.error(f"File is empty: {csv_file}", exc_info=True)
        except Exception as e:
            self._logger.error(
                f"Error reading file {csv_file}: {e}", exc_info=True)
        return None

    def _save_as_csv(
            self,
            df: pd.DataFrame,
            file_path: str,
            index: bool = False
    ) -> None:
        """
        Saves a DataFrame as a CSV file.

        :param df: DataFrame to save.
        :param file_path: Path to save the CSV file.
        :param index: Whether to include the index in the CSV file.
        """
        try:
            df.to_csv(file_path, index=index)
            self._logger.info(f"Saved CSV file: {os.path.basename(file_path)}")
        except Exception as e:
            self._logger.error(f"Error saving CSV file: {e}")
