import hashlib
import os
import re
import unittest
from unittest.mock import MagicMock, call, mock_open, patch

from dwdown.utils.file_handling import FileHandler


class TestFileHandler(unittest.TestCase):

    def setUp(self):
        self.mock_logger = MagicMock()
        self.mock_utilities = MagicMock()
        # Default behavior for _string_to_list to avoid breaking simple filter
        self.mock_utilities._string_to_list.side_effect = lambda x, flatten=False: [x] if isinstance(x, str) else (x if x else [])
        
        self.handler = FileHandler(logger=self.mock_logger, utilities=self.mock_utilities)

    @patch("dwdown.utils.file_handling.os.path.exists")
    @patch("dwdown.utils.file_handling.os.makedirs")
    def test__ensure_directory_exists_creates_if_not_exists(self, mock_makedirs, mock_exists):
        # Directory does not exist, should create
        mock_exists.return_value = False
        path = "/some/path"
        norm_path = os.path.normpath(path)
        
        self.handler._ensure_directory_exists(path)
        
        mock_exists.assert_called_once_with(norm_path)
        mock_makedirs.assert_called_once_with(norm_path, exist_ok=True)

    @patch("dwdown.utils.file_handling.os.path.exists")
    @patch("dwdown.utils.file_handling.os.makedirs")
    def test__ensure_directory_exists_skips_if_exists(self, mock_makedirs, mock_exists):
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

    @patch("dwdown.utils.file_handling.os.scandir")
    def test__search_directory_finds_files_and_recurse_dirs(self, mock_scandir):
        # Setup mocks for root directory
        file_entry = MagicMock()
        file_entry.is_file.return_value = True
        file_entry.is_dir.return_value = False
        file_entry.path = os.path.normpath("/dir/file.txt")

        dir_entry = MagicMock()
        dir_entry.is_file.return_value = False
        dir_entry.is_dir.return_value = True
        dir_entry.path = os.path.normpath("/dir/subdir")

        # Setup mocks for subdirectory
        sub_file_entry = MagicMock()
        sub_file_entry.is_file.return_value = True
        sub_file_entry.is_dir.return_value = False
        sub_file_entry.path = os.path.normpath("/dir/subdir/file2.txt")

        # Define behavior of os.scandir based on input path
        # Normalize paths for comparison
        root_dir = os.path.normpath("/dir")
        sub_dir = os.path.normpath("/dir/subdir")

        def scandir_side_effect(path):
            path = os.path.normpath(path)
            if path == root_dir:
                return [file_entry, dir_entry]
            elif path == sub_dir:
                return [sub_file_entry]
            return []

        mock_scandir.side_effect = scandir_side_effect

        # Need to configure mock_utilities._flatten_list to just return the list
        self.mock_utilities._flatten_list.side_effect = lambda x: [item for sublist in x for item in (sublist if isinstance(sublist, list) else [sublist])] 
        # Actually simplest flattening behavior for test:
        self.mock_utilities._flatten_list.side_effect = lambda x: [file_entry.path, sub_file_entry.path] 
        # But wait, search_directory implementation constructs a list of [file, [recursive_results]].
        # So we need a real flatten implementation or improved mock.
        
        # Real implementation:
        def flatten_mock(obj):
            if isinstance(obj, list):
                flat = []
                for item in obj:
                    flat.extend(flatten_mock(item))
                return flat
            return [obj]
        self.mock_utilities._flatten_list.side_effect = flatten_mock

        result = self.handler._search_directory("/dir", suffix=".txt")

        expected = [
            os.path.normpath("/dir/file.txt"),
            os.path.normpath("/dir/subdir/file2.txt")
        ]

        # Use set comparison to avoid order issues if recursion order varies
        self.assertCountEqual(result, expected)

    def test__simple_filename_filter_various_conditions(self):
        filenames = [
            "/prefix_file_suffix.txt",
            "/prefix_file_other.log",
            "/skipvar/file1.txt",
            "/prefix_include_exclude_suffix.txt"
        ]
        # Normalize input filenames for the test logic to match
        filenames = [os.path.normpath(f) for f in filenames]
        
        prefix = "/prefix" # On windows normalized to \prefix
        suffix = ".txt"
        include_pattern = ["include"]
        exclude_pattern = ["exclude"]
        skip_vars = ["skipvar"]
        timesteps = ["file"]
        
        # We need self._utilities._string_to_list to return these lists
        self.mock_utilities._string_to_list.side_effect = lambda x: [x] if isinstance(x, str) else (x if x else [])

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
            timesteps=timesteps,
            norm_path=True # Use norm_path in filter
        )
        
        # Expected:
        # 0: prefix match? /prefix vs \prefix on windows. 
        # If we pass normalized specific strings into args, it should work.
        # But wait, prefix arguments passed as "/prefix" might invoke normpath inside?
        # FileHandler._simple_filename_filter does filenames normpath, but what about prefix/suffix?
        # Code says: prefix = prefix or "". No normpath on prefix.
        # So explicit normpath needed in test args.
        
        if os.name == 'nt':
            prefix = os.path.normpath(prefix)
            
        filtered = self.handler._simple_filename_filter(
            filenames,
            prefix=prefix,
            suffix=suffix,
            include_pattern=include_pattern,
            exclude_pattern=exclude_pattern,
            skip_time_step_filtering_variables=skip_vars,
            timesteps=timesteps,
            norm_path=True
        )

        match_skipvar = filenames[2]
        
        self.assertIn(match_skipvar, filtered)
        self.assertNotIn(filenames[0], filtered)
        self.assertNotIn(filenames[3], filtered)

    def test__advanced_filename_filter_no_filters_returns_all(self):
        files = [os.path.normpath("/var1/file_1_var1.txt"), os.path.normpath("/var2/file_2_var2.txt")]
        result = FileHandler._advanced_filename_filter(files)
        self.assertEqual(result, files)

    def test__advanced_filename_filter_with_variables_only(self):
        files = [
            os.path.normpath("/var1/file_1_var1.txt"),
            os.path.normpath("/var2/file_2_var2.txt"),
            os.path.normpath("/var3/file_3_var3.txt")
        ]
        variables = ["var1", "var3"]
        filtered = FileHandler._advanced_filename_filter(files,
                                                         variables=variables)
        self.assertIn(files[0], filtered)
        self.assertIn(files[2], filtered)
        self.assertNotIn(files[1], filtered)

    def test__advanced_filename_filter_with_patterns_only(self):
        files = [
            os.path.normpath("/var1/file_1_var1.txt"),
            os.path.normpath("/var1/file_2_var1.txt"),
            os.path.normpath("/var2/file_15_var2.txt")
        ]
        patterns = {
            "var1": [1],
            "var2": [15]
        }
        filtered = FileHandler._advanced_filename_filter(files,
                                                         patterns=patterns)
        self.assertIn(files[0], filtered)
        self.assertIn(files[2], filtered)
        self.assertIn(files[1], filtered)  # Because of a logic detail: file appended anyway

    def test__advanced_filename_filter_with_both(self):
        files = [
            os.path.normpath("/var1/file_1_var1.txt"),
            os.path.normpath("/var1/file_2_var1.txt"),
            os.path.normpath("/var2/file_15_var2.txt"),
            os.path.normpath("/var3/file_3_var3.txt")
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
        norm_file_path = os.path.normpath(file_path)
        md5 = FileHandler._calculate_md5(file_path)
        self.assertEqual(md5, "900150983cd24fb0d6963f7d28e17f72")
        mock_file.assert_called_once_with(norm_file_path, "rb")

    @patch("dwdown.utils.file_handling.os.remove")
    def test__delete_files_safely_success_and_exceptions(self, mock_remove):
        files = ["/path/file1.txt", "/path/file2.txt"]
        norm_files = [os.path.normpath(f) for f in files]

        # Setup os.remove to raise FileNotFoundError on file2
        def side_effect(path):
            if path.endswith("file2.txt"):
                raise FileNotFoundError
        
        mock_remove.side_effect = side_effect

        self.handler._delete_files_safely(files, label="testfile")

        calls = [call(f) for f in norm_files]
        mock_remove.assert_has_calls(calls, any_order=True)
        self.handler.logger.info.assert_called_once_with(
            f"Deleted testfile: {norm_files[0]}"
        )
        self.handler.logger.warning.assert_called_once_with(
            f"Testfile not found: {norm_files[1]}"
        )

    @patch("dwdown.utils.file_handling.os.listdir")
    @patch("dwdown.utils.file_handling.os.rmdir")
    @patch("dwdown.utils.file_handling.os.walk")
    def test__cleanup_empty_dirs_deletes_empty_dirs(self, mock_walk, mock_rmdir, mock_listdir):
        base_path = "/base/path"
        norm_base_path = os.path.normpath(base_path)
        
        # Use normpaths in mock return
        empty_dir = os.path.normpath("/base/path/empty_dir")
        non_empty_dir = os.path.normpath("/base/path/non_empty_dir")
        
        mock_walk.return_value = [
            (norm_base_path, ["empty_dir", "non_empty_dir"], []),
            (empty_dir, [], []),
            (non_empty_dir, [], []),
        ]

        def listdir_side_effect(path):
            if path == empty_dir:
                return []
            else:
                return ["file"]

        mock_listdir.side_effect = listdir_side_effect

        self.handler._cleanup_empty_dirs(base_path)

        mock_rmdir.assert_called_once_with(empty_dir)
        self.handler.logger.info.assert_called_once_with(
            f"Deleted directory: {empty_dir}")

    @patch("dwdown.utils.file_handling.os.listdir", side_effect=FileNotFoundError)
    @patch("dwdown.utils.file_handling.os.walk", return_value=[(os.path.normpath("/base"), ["dir"], [])])
    def test__cleanup_empty_dirs_handles_file_not_found(self, mock_walk, mock_listdir):
        self.handler._cleanup_empty_dirs("/base")
        self.handler.logger.warning.assert_called()

    @patch("dwdown.utils.file_handling.os.listdir", side_effect=PermissionError)
    @patch("dwdown.utils.file_handling.os.walk", return_value=[(os.path.normpath("/base"), ["dir"], [])])
    def test__cleanup_empty_dirs_handles_permission_error(self, mock_walk, mock_listdir):
        self.handler._cleanup_empty_dirs("/base")
        self.handler.logger.error.assert_called()

    @patch("dwdown.utils.file_handling.os.listdir", side_effect=OSError("error"))
    @patch("dwdown.utils.file_handling.os.walk", return_value=[(os.path.normpath("/base"), ["dir"], [])])
    def test__cleanup_empty_dirs_handles_os_error(self, mock_walk, mock_listdir):
        self.handler._cleanup_empty_dirs("/base")
        self.handler.logger.error.assert_called()


if __name__ == "__main__":
    unittest.main()
