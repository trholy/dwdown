from unittest.mock import MagicMock, patch

import pytest

from dwdown.upload.os_upload import OSUploader


class TestOSUploader:

    @pytest.fixture
    def mock_deps(self):
        with patch('dwdown.upload.os_upload.LogHandler') as MockLogHandler, \
             patch('dwdown.upload.os_upload.ClientHandler') as MockClientHandler, \
             patch('dwdown.upload.os_upload.FileHandler') as MockFileHandler, \
             patch('dwdown.upload.os_upload.OSHandler') as MockOSHandler:
            
            mock_logger = MagicMock()
            MockLogHandler.return_value.get_logger.return_value = mock_logger
            
            mock_client = MagicMock()
            MockClientHandler.return_value._client = mock_client
            
            mock_filehandler = MagicMock()
            MockFileHandler.return_value = mock_filehandler

            mock_oshandler = MagicMock()
            MockOSHandler.return_value = mock_oshandler

            yield {
                'LogHandler': MockLogHandler,
                'ClientHandler': MockClientHandler,
                'FileHandler': MockFileHandler,
                'OSHandler': MockOSHandler,
                'logger': mock_logger,
                'client': mock_client,
                'filehandler': mock_filehandler,
                'oshandler': mock_oshandler
            }

    @pytest.fixture
    def uploader(self, mock_deps):
        return OSUploader(
            endpoint="test-endpoint",
            access_key="acc",
            secret_key="sec",
            files_path="uploads",
            bucket_name="bucket"
        )

    def test_init(self, uploader, mock_deps):
        assert uploader.bucket_name == "bucket"
        mock_deps['OSHandler'].assert_called()

    def test_build_upload_list(self, uploader):
        local_files = {
            "uploads/f1.csv": "hash1",
            "uploads/f2.csv": "hash2"
        }
        existing_remote = {}
        
        upload_list = uploader._build_upload_list(local_files, "prefix/", existing_remote)
        
        assert len(upload_list) == 2
        remote_paths = [item[1] for item in upload_list]
        assert "prefix/f1.csv" in remote_paths
        assert "prefix/f2.csv" in remote_paths

    def test_build_upload_list_skips_existing(self, uploader):
        local_files = {"uploads/f1.csv": "hash1"}
        existing_remote = {"f1.csv": "hash1"}
        
        uploader._oshandler._verify_file_integrity.return_value = True
        
        upload_list = uploader._build_upload_list(local_files, "prefix/", existing_remote)
        assert len(upload_list) == 0

    def test_upload_file(self, uploader):
        # Mock the client directly on the uploader instance
        uploader._client.fput_object = MagicMock()
        uploader._client.stat_object = MagicMock()
        mock_stat = MagicMock()
        mock_stat.etag = "hash1"
        uploader._client.stat_object.return_value = mock_stat
        
        result = uploader._upload_file("local/f1.csv", "remote/f1.csv", "hash1")
        
        uploader._client.fput_object.assert_called_with(
            "bucket", "remote/f1.csv", "local/f1.csv"
        )
        assert result is True

    def test_upload_main_flow(self, uploader):
        uploader._filehandler._search_directory.return_value = ["uploads/f1.csv"]
        uploader._utilities = MagicMock()
        uploader._utilities._flatten_list.return_value = ["uploads/f1.csv"]
        
        uploader._datehandler = MagicMock()
        uploader._datehandler._process_timesteps.return_value = None
        
        uploader._filehandler._simple_filename_filter.return_value = ["uploads/f1.csv"]
        uploader._filehandler._advanced_filename_filter.return_value = ["uploads/f1.csv"]
        uploader._filehandler._calculate_md5.return_value = "hash1"
        
        uploader._oshandler._fetch_existing_files.return_value = {}
        
        uploader._build_upload_list = MagicMock(return_value=[("uploads/f1.csv", "remote/f1.csv", "hash1")])
        
        with patch('dwdown.upload.os_upload.ThreadPoolExecutor') as MockExecutor:
            mock_executor_instance = MockExecutor.return_value
            mock_executor_instance.__enter__.return_value = mock_executor_instance
            mock_future = MagicMock()
            mock_executor_instance.submit.return_value = mock_future
            
            with patch('dwdown.upload.os_upload.as_completed', return_value=[mock_future]):
                mock_future.result.return_value = True
                
                uploader.upload()
                
                uploader._oshandler._ensure_bucket.assert_called()
                mock_executor_instance.submit.assert_called()

    def test_delete_operation(self, uploader):
        uploader.uploaded_files = ['file1.csv', 'file2.csv']
        uploader.files_path = 'uploads'
        
        uploader.delete()
        
        uploader._filehandler._delete_files_safely.assert_called_with(['file1.csv', 'file2.csv'], 'uploaded file')
        uploader._filehandler._cleanup_empty_dirs.assert_called_with('uploads')
