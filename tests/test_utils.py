import logging
from unittest.mock import MagicMock

import pandas as pd
import pytest

from dwdown.utils.df_utilis import DataFrameOperator
from dwdown.utils.general_utilis import Utilities
from dwdown.utils.file_handling import FileHandler
from dwdown.utils.os_handling import OSHandler


class TestUtilities:

    @pytest.fixture
    def utilities(self):
        return Utilities()

    def test_string_to_list_with_string(self, utilities):
        result = utilities._string_to_list("hello")
        assert result == ["hello"]

    def test_string_to_list_with_list(self, utilities):
        result = utilities._string_to_list(["a", "b"])
        assert result == ["a", "b"]

    def test_string_to_list_with_flatten(self, utilities):
        result = utilities._string_to_list(["a", ["b", "c"]], flatten=True)
        assert result == ["a", "b", "c"]

    def test_flatten_list(self, utilities):
        nested = ["a", ["b", ["c", "d"]], "e"]
        flat = utilities._flatten_list(nested)
        assert flat == ["a", "b", "c", "d", "e"]
        
    def test_flatten_list_with_string(self, utilities):
        # Should raise error if not list or str, technically code implementation handles str recursion
        # but let's just test simple behavior
        assert utilities._flatten_list("abc") == ["abc"]

    def test_variable_mapping(self, utilities):
        mapping = {"T_2M": "temperature", "TOT_PREC": "precipitation"}
        variables = ["T_2M", "TOT_PREC", "UNKNOWN"]
        result = utilities._variable_mapping(variables, mapping)
        assert result == ["temperature", "precipitation", "UNKNOWN"]

    def test_variable_mapping_no_vars(self, utilities):
        result = utilities._variable_mapping(None, {})
        assert result == []

    def test_extract_additional_pattern_match(self, utilities):
        # Matches _digits_letters.csv
        # Note: variable name must NOT contain numbers as per current regex [a-zA-Z_]+
        filename = "icon_123_TEMP.csv"
        result = utilities._extract_additional_pattern(filename)
        assert result == 123

    def test_extract_additional_pattern_no_match(self, utilities):
        filename = "simple_file.csv"
        result = utilities._extract_additional_pattern(filename)
        assert result is None


class TestDataFrameOperator:

    @pytest.fixture
    def mock_log_handler(self):
        log_handler = MagicMock()
        log_handler.get_logger.return_value = MagicMock(spec=logging.Logger)
        return log_handler

    @pytest.fixture
    def df_op(self, mock_log_handler):
        return DataFrameOperator(log_handler=mock_log_handler)

    def test_filter_dataframe_columns(self, df_op):
        df = pd.DataFrame({
            'A': [1, 2],
            'B': [3, 4],
            'val': [10, 20]
        })
        required = {'A', 'B'}
        result = df_op._filter_dataframe(df, required, 'val')
        # Check set because order might differ
        assert set(result.columns) == {'A', 'B', 'val'}

    def test_parse_datetime(self, df_op):
        series = pd.Series(['2023-01-01', 'invalid', '2023-01-02'])
        result = df_op._parse_datetime(series, 'date_col')
        assert pd.notna(result[0])
        assert pd.isna(result[1])
        assert pd.notna(result[2])

    def test_arrange_df(self, df_op):
        # arrange_df sorts by ['latitude', 'longitude', 'valid_time']
        # so these must exist
        df = pd.DataFrame({
            'C': [1], 'A': [2], 'B': [3],
            'latitude': [50], 'longitude': [10], 'valid_time': [100]
        })
        result = df_op._arrange_df(df)
        # Expected order: ['valid_time', 'latitude', 'longitude'] + others sorted?
        # Implementation: desired_order + other_cols (which are list(df.columns) order excluding desired)
        assert 'valid_time' in result.columns
        assert 'latitude' in result.columns
        assert 'longitude' in result.columns
        assert 'A' in result.columns
        assert list(result.columns[:3]) == ['valid_time', 'latitude', 'longitude']

    def test_save_and_read_csv(self, df_op, tmp_path):
        df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
        csv_file = tmp_path / "test.csv"
        
        # Test Save
        df_op._save_as_csv(df, str(csv_file))
        assert csv_file.exists()
        
        # Test Read
        read_df = df_op._read_df_from_csv(str(csv_file))
        assert read_df is not None
        pd.testing.assert_frame_equal(df, read_df)

    def test_validate_columns_exist_success(self, df_op):
        df = pd.DataFrame({'lat': [1, 2], 'lon': [3, 4], 'temp': [20, 21]})
        required = {'latitude', 'longitude'}
        mapping = {'latitude': 'lat', 'longitude': 'lon', 'temperature': 'temp'}
        
        result = df_op._validate_columns_exist(df, required, 'temperature', mapping)
        assert result is True

    def test_validate_columns_exist_missing(self, df_op):
        df = pd.DataFrame({'lat': [1, 2], 'lon': [3, 4]})
        required = {'latitude', 'longitude'}
        mapping = {'latitude': 'lat', 'longitude': 'lon', 'temperature': 'temp'}
        
        result = df_op._validate_columns_exist(df, required, 'temperature', mapping)
        assert result is False

    def test_filter_by_coordinates(self, df_op):
        df = pd.DataFrame({
            'latitude': [10, 20, 30, 40, 50],
            'longitude': [5, 15, 25, 35, 45],
            'value': [1, 2, 3, 4, 5]
        })
        
        result = df_op._filter_by_coordinates(df, 15, 35, 10, 40)
        
        assert len(result) == 2
        assert result['latitude'].min() >= 15
        assert result['latitude'].max() <= 35

    def test_filter_by_coordinates_no_coords(self, df_op):
        df = pd.DataFrame({
            'latitude': [10, 20, 30],
            'longitude': [5, 15, 25],
            'value': [1, 2, 3]
        })
        
        result = df_op._filter_by_coordinates(df, None, None, None, None)
        
        pd.testing.assert_frame_equal(result, df)

    def test_merge_dataframes(self, df_op):
        df1 = pd.DataFrame({
            'time': [1, 2, 3],
            'temp': [20, 21, 22]
        })
        df2 = pd.DataFrame({
            'time': [1, 2, 3],
            'humidity': [60, 65, 70]
        })
        
        result = df_op._merge_dataframes(df1, df2, {'time'}, 'inner')
        
        assert 'temp' in result.columns
        assert 'humidity' in result.columns
        assert len(result) == 3

    def test_merge_dataframes_no_common_columns(self, df_op):
        df1 = pd.DataFrame({'a': [1, 2]})
        df2 = pd.DataFrame({'b': [3, 4]})
        
        result = df_op._merge_dataframes(df1, df2, {'time'}, 'inner')
        
        pd.testing.assert_frame_equal(result, df1)


class TestOSHandler:

    @pytest.fixture
    def mock_objects(self):
        log_handler = MagicMock()
        log_handler.get_logger.return_value = MagicMock(spec=logging.Logger)
        client = MagicMock() # Mock Minio client
        filehandler = MagicMock(spec=FileHandler)
        return log_handler, client, filehandler

    @pytest.fixture
    def os_handler(self, mock_objects):
        log_handler, client, filehandler = mock_objects
        return OSHandler(log_handler=log_handler, client=client, filehandler=filehandler)

    def test_ensure_bucket_exists(self, os_handler, mock_objects):
        _, client, _ = mock_objects
        client.bucket_exists.return_value = True
        
        os_handler._ensure_bucket("test-bucket")
        client.make_bucket.assert_not_called()

    def test_ensure_bucket_create(self, os_handler, mock_objects):
        _, client, _ = mock_objects
        client.bucket_exists.return_value = False
        
        os_handler._ensure_bucket("test-bucket", create_if_not_exists=True)
        client.make_bucket.assert_called_with("test-bucket")

    def test_fetch_existing_files(self, os_handler, mock_objects):
        _, client, _ = mock_objects
        mock_obj = MagicMock()
        mock_obj.object_name = "folder/file.txt"
        mock_obj.etag = "hash123"
        client.list_objects.return_value = [mock_obj]

        result = os_handler._fetch_existing_files("test-bucket", "folder/")
        assert result == {"folder/file.txt": "hash123"}

    def test_fetch_existing_files_basename(self, os_handler, mock_objects):
        _, client, _ = mock_objects
        mock_obj = MagicMock()
        mock_obj.object_name = "folder/file.txt"
        mock_obj.etag = "hash123"
        client.list_objects.return_value = [mock_obj]

        result = os_handler._fetch_existing_files("test-bucket", "folder/", return_basename=True)
        assert result == {"file.txt": "hash123"}
    
    def test_verify_file_integrity_match(self, os_handler, mock_objects):
        _, _, filehandler = mock_objects
        filehandler._calculate_md5.return_value = "hash123"
        
        result = os_handler._verify_file_integrity(
            local_file_path="local.txt",
            remote_hash="hash123"
        )
        assert result is True

    def test_verify_file_integrity_mismatch(self, os_handler, mock_objects):
        _, _, filehandler = mock_objects
        filehandler._calculate_md5.return_value = "hash123"
        
        result = os_handler._verify_file_integrity(
            local_file_path="local.txt",
            remote_hash="hash456"
        )
        assert result is False
