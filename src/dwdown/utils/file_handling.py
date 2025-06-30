import hashlib
import logging
import os
import re


class FileHandler:
    """
    A class to handle file operations such as directory creation, file filtering,
    MD5 calculation, file deletion, and directory cleanup.

    Attributes:
        logger (logging.Logger): Logger for logging messages.
    """

    def __init__(self):
        """
        Initializes the FileHandler.
        """

    @staticmethod
    def _ensure_directory_exists(path: str) -> None:
        """
        Ensures that the specified directory exists, creating it if necessary.

        :param path: The directory path to ensure.
        """
        path = os.path.normpath(path)
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)

    def _ensure_directories_exist(self, paths: list[str]) -> None:
        """
        Ensures that all specified directories exist.
         If any directory does not exist, it creates the directory and any
          necessary parent directories.

        param: paths; A list of paths to the directories to ensure existence.
        """
        for path in paths:
            self._ensure_directory_exists(path)

    def _search_directory(
            self,
            directory: str,
            suffix: str = ""
    ) -> list[str]:
        """
        Searches a directory for files.

        :param directory: Directory to search.
        :return: List of file paths.
        """
        filenames = []

        for entry in os.scandir(directory):
            if entry.is_file():
                if entry.path.endswith(suffix):
                    norm_path = os.path.normpath(entry.path)
                    filenames.append(norm_path)
            elif entry.is_dir():
                # Extend the list with results from subdirectories
                filenames.append(self._search_directory(entry.path))

        return filenames

    def _simple_filename_filter(
            self,
            filenames: list[str],
            prefix: str = "",
            suffix: str = "",
            include_pattern: list[str] | None = None,
            exclude_pattern: list[str] | None = None,
            skip_time_step_filtering_variables: list[str] | None = None,
            timesteps: list[str] | None = None,
            norm_path: bool = True
    ) -> list[str]:
        """
        Filters filenames based on specified criteria.

        :param filenames: List of filenames to filter.
        :param prefix: Prefix to filter filenames by.
        :param suffix: Suffix to filter filenames by.
        :param include_pattern: List of patterns to include in filenames.
        :param exclude_pattern: List of patterns to exclude from filenames.
        :param skip_time_step_filtering_variables: List of variables to skip time step filtering.
        :return: List of filtered filenames.
        """
        include_pattern = self._string_to_list(include_pattern)
        exclude_pattern = self._string_to_list(exclude_pattern)
        skip_time_step_filtering_variables = self._string_to_list(
            skip_time_step_filtering_variables)

        prefix = prefix or ""
        suffix = suffix or ""

        if norm_path:
            filenames = [os.path.normpath(filename) for filename in filenames]
        filtered_filenames = [
            filename for filename in filenames
            if filename.startswith(prefix)
               and filename.endswith(suffix)
               and all(pattern in filename for pattern in include_pattern)
               and any(ts in filename for ts in timesteps)
               and not any(pattern in filename for pattern in exclude_pattern)]

        if skip_time_step_filtering_variables:
            target_vars = set(v.lower() for v in skip_time_step_filtering_variables)
            filtered_filenames.extend(
                filename for filename in filenames
                if os.path.basename(os.path.dirname(filename)).lower() in target_vars)

        return filtered_filenames

    @staticmethod
    def _advanced_filename_filter(
        filenames: list[str],
        variables: set[str] | list[str] | None = None,
        patterns: dict[str, set[int] | list[int]] | None = None
    ) -> list[str]:
        """
        Filters remote files based on specified variables and patterns.

        :param filenames: List of filenames to filter.
        :param variables: List of variables to filter by.
        :param patterns: Dictionary of patterns to filter by.
        :return: List of filtered filenames.
        """
        variables_set: set[str] | None = None
        patterns_dict: dict[str, set[int]] | None = None

        if variables is not None:
            variables_set = set(v.lower() for v in variables)

        if patterns is not None:
            patterns_dict = {k.lower(): set(v) for k, v in patterns.items()}

        # If no filtering is needed at all
        if variables_set is None and patterns_dict is None:
            return filenames

        filtered_files = []

        for file_path in filenames:
            dir_path, file_name = os.path.split(file_path)
            variable_folder = os.path.basename(dir_path).lower()

            # Case 1: Only patterns are provided
            if variables_set is None and patterns_dict:
                if variable_folder in patterns_dict:
                    match = re.search(
                        rf"_([0-9]+)_{re.escape(variable_folder)}\.",
                        file_name.lower())
                    if match and int(match.group(1)) in patterns_dict[variable_folder]:
                        filtered_files.append(file_path)
                filtered_files.append(file_path)

            # Case 2: Only variables are provided
            elif patterns_dict is None and variables_set:
                if variable_folder in variables_set:
                    filtered_files.append(file_path)

            # Case 3: Both patterns and variables are provided
            elif patterns_dict and variables_set:
                if variable_folder in variables_set:
                    if variable_folder in patterns_dict:
                        match = re.search(
                            rf"_([0-9]+)_{re.escape(variable_folder)}\.",
                            file_name.lower())
                        if match and int(match.group(1)) in patterns_dict[variable_folder]:
                            filtered_files.append(file_path)
                    else:
                        # Variable is allowed and no pattern constraint
                        filtered_files.append(file_path)

        return filtered_files

    @staticmethod
    def _calculate_md5(file_path: str) -> str:
        """
        Calculates the MD5 hash of a file.

        :param file_path: The path to the file.
        :return: The MD5 hash of the file.
        """
        file_path = os.path.normpath(file_path)

        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def _delete_files_safely(
        self,
        files: list[str],
        label: str = "file"
    ) -> None:
        """
        Deletes files safely, handling potential errors.

        :param files: List of file paths to delete.
        :param label: Label for the type of file being deleted.
        """
        files = [os.path.normpath(file) for file in files]

        for file_path in files:
            try:
                os.remove(file_path)
                self._logger.info(f"Deleted {label}: {file_path}")
            except FileNotFoundError:
                self._logger.warning(
                    f"{label.capitalize()} not found: {file_path}")
            except PermissionError:
                self._logger.error(f"Permission denied: {file_path}")
            except OSError as e:
                self._logger.error(
                    f"Error deleting {label}: {file_path}: {e.strerror}")

    def _cleanup_empty_dirs(self, base_path: str) -> None:
        """
        Cleans up empty directories within the specified base path.

        :param base_path: The base directory path to clean up.
        """
        base_path = os.path.normpath(base_path)

        for root, dirs, _ in os.walk(base_path, topdown=False):
            for dir_name in dirs:
                dir_path = os.path.join(root, dir_name)
                try:
                    if not os.listdir(dir_path):
                        os.rmdir(dir_path)
                        self._logger.info(f"Deleted directory: {dir_path}")
                except FileNotFoundError:
                    self._logger.warning(f"Directory not found: {dir_path}")
                except PermissionError:
                    self._logger.error(
                        f"Permission denied while accessing"
                        f" or deleting: {dir_path}")
                except OSError as e:
                    self._logger.error(
                        f"Error deleting directory: {dir_path}: {e.strerror}")
