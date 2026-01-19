from unittest.mock import MagicMock, patch

import pytest

from dwdown.download.os_download import OSDownloader


class TestOSDownloader:

    @pytest.fixture
    def mock_deps(self):
        with patch('dwdown.download.os_download.LogHandler') as MockLogHandler, \
             patch('dwdown.download.os_download.ClientHandler') as MockClientHandler, \
             patch('dwdown.download.os_download.FileHandler') as MockFileHandler, \
             patch('dwdown.download.os_download.OSHandler') as MockOSHandler:
            
            mock_logger = MagicMock()
            MockLogHandler.return_value.get_logger.return_value = mock_logger
            
            mock_client = MagicMock()
            MockClientHandler.return_value._client = mock_client
            
            yield {
                'LogHandler': MockLogHandler,
                'ClientHandler': MockClientHandler,
                'FileHandler': MockFileHandler,
                'OSHandler': MockOSHandler,
                'logger': mock_logger,
                'client': mock_client
            }

    @pytest.fixture
    def downloader(self, mock_deps):
        return OSDownloader(
            endpoint="test-endpoint",
            access_key="acc",
            secret_key="sec",
            files_path="downloads",
            bucket_name="bucket"
        )

    def test_init(self, downloader, mock_deps):
        assert downloader.bucket_name == "bucket"
        mock_deps['OSHandler'].assert_called()

    def test_build_download_list(self, downloader):
        remote_files = {"file1.csv": "hash1", "file2.csv": "hash2"}
        with patch.object(downloader._filehandler, 'file_exists', return_value=False):
            download_list = downloader._build_download_list(remote_files)
            assert len(download_list) == 2
            assert download_list[0][1] in ["file1.csv", "file2.csv"]

    def test_download_file_no_existence_check(self, downloader):
        downloader._client.fget_object = MagicMock()
        downloader._oshandler._verify_file_integrity.return_value = True
        
        result = downloader._download_file("local/path/f.csv", "remote/f.csv", False, "hash")
        
        downloader._client.fget_object.assert_called_with("bucket", "remote/f.csv", "local/path/f.csv")
        assert result is True

    def test_download_file_exists_and_valid(self, downloader):
        downloader._oshandler._verify_file_integrity.return_value = True
        
        with patch('os.path.exists', return_value=True):
            result = downloader._download_file("local/path/f.csv", "remote/f.csv", True, "hash")
        
        assert result is True

    def test_download_main_flow(self, downloader):
        # Mock the entire download flow including filtering
        downloader._oshandler._fetch_existing_files.return_value = {"f1.csv": "h1"}
        
        # Mock utilities and filtering to ensure files pass through
        downloader._utilities._string_to_list = MagicMock(side_effect=lambda x: x if isinstance(x, list) else [x] if x else [])
        downloader._datehandler._process_timesteps.return_value = None
        downloader._filehandler._simple_filename_filter.return_value = ["f1.csv"]
        downloader._filehandler._advanced_filename_filter.return_value = ["f1.csv"]
        
        # Mock build_download_list to return a list with items
        download_list = [("local/f1.csv", "f1.csv", "h1")]
        downloader._build_download_list = MagicMock(return_value=download_list)
        
        with patch('dwdown.download.os_download.ThreadPoolExecutor') as MockExecutor:
            mock_executor_instance = MockExecutor.return_value
            mock_executor_instance.__enter__.return_value = mock_executor_instance
            mock_future = MagicMock()
            mock_executor_instance.submit.return_value = mock_future
            
            with patch('dwdown.download.os_download.as_completed', return_value=[mock_future]):
                mock_future.result.return_value = True
                
                downloader.download()
                
                downloader._oshandler._ensure_bucket.assert_called()
                downloader._oshandler._fetch_existing_files.assert_called()
                # Verify submit was called
                assert mock_executor_instance.submit.call_count >= 1

    def test_delete_operation(self, downloader):
        downloader.downloaded_files = ['file1.csv', 'file2.csv']
        downloader.files_path = 'downloads'
        
        downloader.delete()
        
        downloader._filehandler._delete_files_safely.assert_called_with(['file1.csv', 'file2.csv'], 'downloaded file')
        downloader._filehandler._cleanup_empty_dirs.assert_called_with('downloads')
