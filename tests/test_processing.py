from unittest.mock import MagicMock, mock_open, patch

import pandas as pd
import pytest

from dwdown.processing.data_merging import DataMerger
from dwdown.processing.grib_data_handling import GribFileManager


class TestGribFileManager:

    @pytest.fixture
    def mock_deps(self):
        with patch('dwdown.processing.grib_data_handling.LogHandler'), \
             patch('dwdown.processing.grib_data_handling.FileHandler') as MockFileHandler, \
             patch('dwdown.processing.grib_data_handling.DataFrameOperator') as MockDFOp, \
             patch('dwdown.processing.grib_data_handling.Utilities'):
            
            mock_filehandler = MagicMock()
            MockFileHandler.return_value = mock_filehandler
            
            mock_df_op = MagicMock()
            MockDFOp.return_value = mock_df_op
            
            yield {
                'filehandler': mock_filehandler,
                'df_op': mock_df_op,
            }

    @pytest.fixture
    def manager(self, mock_deps):
        return GribFileManager(files_path="files", extracted_files_path="extracted", converted_files_path="converted")

    def test_get_filenames(self, manager, mock_deps):
        # Mock the entire call chain needed by get_filenames
        manager._filehandler._search_directory.return_value = ["file1.bz2"]
        manager._utilities._flatten_list.return_value = ["file1.bz2"]
        manager._datehandler._process_timesteps.return_value = None
        manager._filehandler._simple_filename_filter.return_value = ["file1.bz2"]
        manager._filehandler._advanced_filename_filter.return_value = ["file1.bz2"]
        
        result = manager.get_filenames()
        assert result == ["file1.bz2"]

    def test_decompress_file(self, manager):
        # Mock os.path.exists to control the flow
        with patch('os.path.exists', return_value=False), \
             patch('bz2.BZ2File', mock_open(read_data=b'data')), \
             patch('builtins.open', mock_open()):
            
            result = manager._decompress_files("test.bz2")
            assert "test" in result or "grib2" in result

    def test_grib_to_df(self, manager, mock_deps):
        # Test that _grib_to_df can be called without crashing
        # Mock os.path.exists to ensure CSV doesn't exist yet
        with patch('os.path.exists', return_value=False), \
             patch('xarray.open_dataset') as mock_xr:
            
            mock_ds = MagicMock()
            mock_df = pd.DataFrame({
                'val': [1, 2], 
                'latitude': [50, 51], 
                'longitude': [10, 11]
            })
            
            mock_ds.to_dataframe.return_value = mock_df
            mock_xr.return_value.__enter__.return_value = mock_ds
            
            # Set up the mock dataframe_operator
            manager._dataframe_operator = mock_deps['df_op']
            mock_deps['df_op']._filter_by_coordinates.return_value = mock_df
            mock_deps['df_op']._save_as_csv.return_value = None
            
            # Just test it runs without error
            result = manager._grib_to_df(
                "file.grib2", apply_geo_filtering=True,
                start_lat=0, end_lat=10, start_lon=0, end_lon=10)
            
            # Verify the method completes
            assert result is None

    def test_get_csv_flow(self, manager, mock_deps):
        manager.get_filenames = MagicMock(return_value=["f1.bz2"])
        manager._decompress_files = MagicMock(return_value="f1.grib2")
        manager._grib_to_df = MagicMock(return_value=None)
        
        manager.get_csv(file_names=["f1.bz2"])
        
        manager._decompress_files.assert_called_with("f1.bz2")
        manager._grib_to_df.assert_called()

    def test_delete_operation(self, manager):
        manager.processed_download_files = ['down1.bz2']
        manager.decompressed_files = ['decomp1.grib2']
        manager.converted_files = ['conv1.csv']
        
        manager.delete(delete_downloaded=True, delete_decompressed=True, converted_files=True)
        
        # Verify all file types were deleted
        assert manager._filehandler._delete_files_safely.call_count == 3
        assert manager._filehandler._cleanup_empty_dirs.call_count == 3


class TestDataMerger:

    @pytest.fixture
    def mock_deps(self):
        with patch('dwdown.processing.data_merging.LogHandler'), \
             patch('dwdown.processing.data_merging.FileHandler') as MockFileHandler, \
             patch('dwdown.processing.data_merging.DataFrameOperator') as MockDFOp, \
             patch('dwdown.processing.data_merging.MappingStore'):
            
            mock_filehandler = MagicMock()
            MockFileHandler.return_value = mock_filehandler
            
            mock_df_op = MagicMock()
            MockDFOp.return_value = mock_df_op
            
            yield {
                'filehandler': mock_filehandler,
                'df_op': mock_df_op,
            }

    @pytest.fixture
    def merger(self, mock_deps):
        return DataMerger(files_path="files")

    def test_merge_flow(self, merger, mock_deps):
        merger._utilities._variable_mapping = MagicMock(return_value=["T_2M"])
        merger._filehandler.get_filenames.return_value = ["file_T_2M.csv"]
        merger._match_filenames_by_patterns = MagicMock(return_value=["file_T_2M.csv"])
        
        df_mock = pd.DataFrame({
            'timestamp': ['2023-01-01'], 
            'T_2M': [10],
            'latitude': [50],
            'longitude': [10],
            'valid_time': [100]
        })
        
        mock_deps['df_op']._read_df_from_csv.return_value = df_mock
        mock_deps['df_op']._filter_dataframe.return_value = df_mock
        mock_deps['df_op']._merge_dataframes.return_value = df_mock
        mock_deps['df_op']._arrange_df.return_value = df_mock
        
        result = merger.merge(time_step="000", variables=["T_2M"], prefix="icon", suffix=".csv")
        
        assert isinstance(result, pd.DataFrame)
        mock_deps['df_op']._read_df_from_csv.assert_called()

    def test_process_dataframe_success(self, merger, mock_deps):
        df = pd.DataFrame({
            'valid_time': ['2023-01-01'], 
            'latitude': [50],
            'longitude': [10],
            'T_2M': [15]
        })
        merger._required_columns = {'latitude', 'longitude', 'valid_time'}
        merger.mapping_dict = {'temperature': 'T_2M'}
        
        mock_deps['df_op']._validate_columns_exist.return_value = True
        mock_deps['df_op']._parse_datetime.return_value = pd.to_datetime(['2023-01-01'])
        mock_deps['df_op']._filter_dataframe.return_value = df
        
        result = merger._process_dataframe(df, 'T_2M')
        
        assert result is not None
        mock_deps['df_op']._validate_columns_exist.assert_called()

    def test_process_dataframe_missing_columns(self, merger, mock_deps):
        df = pd.DataFrame({'value': [1, 2]})
        merger._required_columns = {'latitude', 'longitude'}
        merger.mapping_dict = {}
        
        mock_deps['df_op']._validate_columns_exist.return_value = False
        
        result = merger._process_dataframe(df, 'temp', skip_variable_validation=False)
        
        assert result is None

    def test_match_filenames_by_patterns_with_pattern(self, merger):
        merger.additional_patterns = {'temp': [100, 200]}
        merger._utilities._extract_additional_pattern = MagicMock(side_effect=[100, 200])
        
        files = ['file_100_temp.csv', 'file_200_temp.csv']
        result = merger._match_filenames_by_patterns(files, 'temp')
        
        assert len(result) == 2

    def test_match_filenames_by_patterns_no_match(self, merger):
        merger.additional_patterns = {'temp': [100]}
        merger._utilities._extract_additional_pattern = MagicMock(return_value=200)
        
        files = ['file_200_temp.csv']
        result = merger._match_filenames_by_patterns(files, 'temp')
        
        assert result is None

    def test_delete(self, merger):
        merger.selected_csv_files = ['file1.csv', 'file2.csv']
        merger.files_path = 'test_path'
        
        merger.delete()
        
        merger._filehandler._delete_files_safely.assert_called_with(['file1.csv', 'file2.csv'], 'csv file')
        merger._filehandler._cleanup_empty_dirs.assert_called_with('test_path')
