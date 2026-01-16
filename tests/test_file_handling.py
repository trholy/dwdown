import unittest
from unittest.mock import patch, MagicMock, mock_open, call
import os
import hashlib
import re

from dwdown.utils.file_handling import FileHandler


class TestFileHandler(unittest.TestCase):

    def setUp(self):
        self.handler = FileHandler()
        # Add a mock logger to avoid AttributeErrors when calling _logger
        self.handler._logger = MagicMock()

    @patch("os.path.exists")
    @patch("os.makedirs")
    def test__ensure_directory_exists_creates_if_not_exists(self, mock_makedirs,
                                                            mock_exists):
        # Directory does not exist, should create
        mock_exists.return_value = False
        path = "/some/path"
        self.handler._ensure_directory_exists(path)
        mock_exists.assert_called_once_with(os.path.normpath(path))
        mock_makedirs.assert_called_once_with(os.path.normpath(path),
                                              exist_ok=True)

    @patch("os.path.exists")
    @patch("os.makedirs")
    def test__ensure_directory_exists_skips_if_exists(self, mock_makedirs,
                                                      mock_exists):
        # Directory exists, should not create
        mock_exists.return_value = True
        path = "/exists/path"
        self.handler._ensure_directory_exists(path)
        mock_makedirs.assert_not_called()

    @patch.object(FileHandler, "_ensure_directory_exists")
    def test__ensure_directories_exist_calls_for_all(self, mock_ensure_dir):
        paths = ["/path/one", "/path/two"]
        self.handler._ensure_directories_exist(paths)
        self.assertEqual(mock_ensure_dir.call_count, 2)
        mock_ensure_dir.assert_has_calls([call("/path/one"), call("/path/two")])

    @patch("os.scandir")
    def test__search_directory_finds_files_and_recurse_dirs(self, mock_scandir):
        # Setup mocks for root directory
        file_entry = MagicMock()
        file_entry.is_file.return_value = True
        file_entry.is_dir.return_value = False
        file_entry.path = "/dir/file.txt"

        dir_entry = MagicMock()
        dir_entry.is_file.return_value = False
        dir_entry.is_dir.return_value = True
        dir_entry.path = "/dir/subdir"

        # Setup mocks for subdirectory
        sub_file_entry = MagicMock()
        sub_file_entry.is_file.return_value = True
        sub_file_entry.is_dir.return_value = False
        sub_file_entry.path = "/dir/subdir/file2.txt"

        # Define behavior of os.scandir based on input path
        def scandir_side_effect(path):
            if path == "/dir":
                return [file_entry, dir_entry]
            elif path == "/dir/subdir":
                return [sub_file_entry]
            return []

        mock_scandir.side_effect = scandir_side_effect

        result = self.handler._search_directory("/dir", suffix=".txt")

        expected = [
            os.path.normpath("/dir/file.txt"),
            os.path.normpath("/dir/subdir/file2.txt")
        ]

        self.assertCountEqual(result, expected)

    def test__simple_filename_filter_various_conditions(self):
        filenames = [
            "/prefix_file_suffix.txt",
            "/prefix_file_other.log",
            "/skipvar/file1.txt",
            "/prefix_include_exclude_suffix.txt"
        ]
        prefix = "/prefix"
        suffix = ".txt"
        include_pattern = ["include"]
        exclude_pattern = ["exclude"]
        skip_vars = ["skipvar"]
        timesteps = ["file"]

        # Filename 0 starts with prefix and ends with suffix, but doesn't include 'include' pattern => excluded
        # Filename 1 does not end with suffix => excluded
        # Filename 2's parent dir is skipvar (should be included)
        # Filename 3 matches prefix, suffix, include, exclude pattern => excluded because of exclude
        filtered = self.handler._simple_filename_filter(
            filenames,
            prefix=prefix,
            suffix=suffix,
            include_pattern=include_pattern,
            exclude_pattern=exclude_pattern,
            skip_time_step_filtering_variables=skip_vars,
            timesteps=timesteps
        )
        # Only filename 2 should appear because skipvar directory is in skip list
        self.assertIn(filenames[2], filtered)
        # filename 0 and 3 should be excluded because of include/exclude filters
        self.assertNotIn(filenames[0], filtered)
        self.assertNotIn(filenames[3], filtered)

    def test__advanced_filename_filter_no_filters_returns_all(self):
        files = ["/var1/file_1_var1.txt", "/var2/file_2_var2.txt"]
        result = FileHandler._advanced_filename_filter(files)
        self.assertEqual(result, files)

    def test__advanced_filename_filter_with_variables_only(self):
        files = [
            "/var1/file_1_var1.txt",
            "/var2/file_2_var2.txt",
            "/var3/file_3_var3.txt"
        ]
        variables = ["var1", "var3"]
        filtered = FileHandler._advanced_filename_filter(files,
                                                         variables=variables)
        self.assertIn(files[0], filtered)
        self.assertIn(files[2], filtered)
        self.assertNotIn(files[1], filtered)

    def test__advanced_filename_filter_with_patterns_only(self):
        files = [
            "/var1/file_1_var1.txt",
            "/var1/file_2_var1.txt",
            "/var2/file_15_var2.txt"
        ]
        patterns = {
            "var1": [1],
            "var2": [15]
        }
        filtered = FileHandler._advanced_filename_filter(files,
                                                         patterns=patterns)
        self.assertIn(files[0], filtered)
        self.assertIn(files[2], filtered)
        self.assertIn(files[1],
                      filtered)  # Because of a logic detail: file appended anyway

    def test__advanced_filename_filter_with_both(self):
        files = [
            "/var1/file_1_var1.txt",
            "/var1/file_2_var1.txt",
            "/var2/file_15_var2.txt",
            "/var3/file_3_var3.txt"
        ]
        variables = ["var1", "var2"]
        patterns = {"var1": [1], "var2": [15]}
        filtered = FileHandler._advanced_filename_filter(files,
                                                         variables=variables,
                                                         patterns=patterns)
        self.assertIn(files[0], filtered)
        self.assertIn(files[2], filtered)
        self.assertNotIn(files[1], filtered)
        self.assertNotIn(files[3], filtered)

    @patch("builtins.open", new_callable=mock_open, read_data=b"abc")
    def test__calculate_md5_returns_correct_hash(self, mock_file):
        # MD5 for 'abc' is '900150983cd24fb0d6963f7d28e17f72'
        file_path = "/some/file.txt"
        md5 = FileHandler._calculate_md5(file_path)
        self.assertEqual(md5, "900150983cd24fb0d6963f7d28e17f72")
        mock_file.assert_called_once_with(os.path.normpath(file_path), "rb")

    @patch("os.remove")
    def test__delete_files_safely_success_and_exceptions(self, mock_remove):
        files = ["/path/file1.txt", "/path/file2.txt"]

        # Setup os.remove to raise FileNotFoundError on file2
        def side_effect(path):
            if path.endswith("file2.txt"):
                raise FileNotFoundError

        mock_remove.side_effect = side_effect

        self.handler._delete_files_safely(files, label="testfile")

        calls = [call(os.path.normpath(f)) for f in files]
        mock_remove.assert_has_calls(calls, any_order=True)
        self.handler._logger.info.assert_called_once_with(
            f"Deleted testfile: {os.path.normpath(files[0])}"
        )
        self.handler._logger.warning.assert_called_once_with(
            f"Testfile not found: {os.path.normpath(files[1])}"
        )

    @patch("os.listdir")
    @patch("os.rmdir")
    @patch("os.walk")
    def test__cleanup_empty_dirs_deletes_empty_dirs(self, mock_walk, mock_rmdir,
                                                    mock_listdir):
        base_path = "/base/path"
        mock_walk.return_value = [
            ("/base/path", ["empty_dir", "non_empty_dir"], []),
            ("/base/path/empty_dir", [], []),
            ("/base/path/non_empty_dir", [], []),
        ]

        # empty_dir is empty, non_empty_dir is not
        def listdir_side_effect(path):
            if path == "/base/path/empty_dir":
                return []
            else:
                return ["file"]

        mock_listdir.side_effect = listdir_side_effect

        self.handler._cleanup_empty_dirs(base_path)

        mock_rmdir.assert_called_once_with("/base/path/empty_dir")
        self.handler._logger.info.assert_called_once_with(
            "Deleted directory: /base/path/empty_dir")

    @patch("os.listdir", side_effect=FileNotFoundError)
    @patch("os.walk", return_value=[("/base", ["dir"], [])])
    def test__cleanup_empty_dirs_handles_file_not_found(self, mock_walk,
                                                        mock_listdir):
        self.handler._cleanup_empty_dirs("/base")
        self.handler._logger.warning.assert_called()

    @patch("os.listdir", side_effect=PermissionError)
    @patch("os.walk", return_value=[("/base", ["dir"], [])])
    def test__cleanup_empty_dirs_handles_permission_error(self, mock_walk,
                                                          mock_listdir):
        self.handler._cleanup_empty_dirs("/base")
        self.handler._logger.error.assert_called()

    @patch("os.listdir", side_effect=OSError("error"))
    @patch("os.walk", return_value=[("/base", ["dir"], [])])
    def test__cleanup_empty_dirs_handles_os_error(self, mock_walk,
                                                  mock_listdir):
        self.handler._cleanup_empty_dirs("/base")
        self.handler._logger.error.assert_called()


if __name__ == "__main__":
    unittest.main()
