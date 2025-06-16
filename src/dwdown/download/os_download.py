import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from ..utils import (
    Utilities, LogHandler,
    FileHandler,
    DateHandler, TimeHandler,
    ClientHandler, OSHandler)


class OSDownloader(
    Utilities, LogHandler, FileHandler, TimeHandler, DateHandler, ClientHandler, OSHandler):
    def __init__(
            self,
            endpoint: str,
            access_key: str,
            secret_key: str,
            files_path: str,
            bucket_name: str,
            secure: bool = True,
            log_files_path: str | None = None,
            delay: int | float = 1,
            n_jobs: int = 1,
            retry: int = 0
    ):
        """
        Initializes the OSDownloader with necessary parameters.

        :param endpoint: The endpoint of the storage service.
        :param access_key: The access key for authentication.
        :param secret_key: The secret key for authentication.
        :param secure: Whether to use secure connection.
        :param files_path: The local path to the files to be downloaded.
        :param bucket_name: The name of the bucket to download files from.
        :param log_files_path: The path to store log files.
        :param delay: Optional delay between downloads (in seconds).
        :param n_jobs: Number of parallel jobs for downloading.
        :param retry: Number of retries for failed downloady.
        """
        self._endpoint = endpoint
        self._access_key = access_key
        self._secret_key = secret_key
        self._secure = secure

        self.files_path = os.path.normpath(files_path or "download_files")
        self.bucket_name = bucket_name
        self.log_files_path = os.path.normpath(log_files_path or "log_files")

        self._delay = delay
        self._n_jobs = n_jobs
        self._retry = retry

        FileHandler.__init__(self)
        self._ensure_directory_exists(self.log_files_path)

        LogHandler.__init__(self, self.log_files_path, True, True)
        self._logger = self.get_logger()

        TimeHandler.__init__(self)
        DateHandler.__init__(self)
        ClientHandler.__init__(self,
            endpoint=self._endpoint,
            access_key=self._access_key,
            secret_key=self._secret_key,
            secure=self._secure)
        self._client = self.get_client()

        OSHandler.__init__(self)
        Utilities.__init__(self)

        self.remote_files = []
        self.downloaded_files = []
        self.corrupted_files = []

    def _build_download_list(
            self,
            filtered_remote_files: dict[str, str]
    ) -> list[tuple[str, str, str]]:
        """
        Builds a list of files to download.

        :param filtered_remote_files: Dictionary of remote file paths and their ETags.
        :return: A list of tuples (local_file_path, remote_path, remote_hash).
        """
        files_to_download = []
        for remote_path, remote_hash in filtered_remote_files.items():
            self.remote_files.append(remote_path)
            local_file_path = os.path.join(self.files_path, remote_path)
            local_file_path = os.path.normpath(local_file_path)

            # Ensure the directory exists
            self._ensure_directory_exists(os.path.dirname(local_file_path))

            # Skip if the file already exists and matches hash
            if os.path.exists(local_file_path) and self._verify_file_integrity(
                    local_file_path, remote_path, self.bucket_name, remote_hash):
                self._logger.info(
                    f"Skipping already downloaded file: {remote_path}")
                continue

            files_to_download.append((local_file_path, remote_path, remote_hash))
        return files_to_download

    def _download_file(
            self,
            local_file_path: str,
            remote_path: str,
            check_for_existence: bool,
            remote_hash: str
    ) -> bool:
        """
        Downloads a single file with immediate logging and integrity check.

        :param local_file_path: The path to the local file.
        :param remote_path: The path to the remote file.
        :param check_for_existence: Whether to check for existing files.
        :param remote_hash: The ETag of the remote file.
        :return: True if the file was downloaded successfully, False otherwise.
        """
        try:
            # Respect the delay between downloads
            if self._delay > 0:
                time.sleep(self._delay)

            # Check if file already exists and verify integrity
            if check_for_existence and os.path.exists(local_file_path):
                if self._verify_file_integrity(
                        local_file_path, remote_path,
                        self.bucket_name, remote_hash):
                    self._logger.info(
                        f"Skipping already downloaded file: {remote_path}")
                    return True

            self._client.fget_object(
                self.bucket_name, remote_path, local_file_path)

            # Verify integrity after download
            if self._verify_file_integrity(
                local_file_path, remote_path, self.bucket_name, remote_hash):
                self._logger.info(f"Successfully downloaded: {remote_path}")
                return True
            else:
                self._logger.warning(
                    f"Hash mismatch: {remote_path} (possible corruption).")
                self.corrupted_files.append(remote_path)
                return False

        except Exception as e:
            self._logger.error(f"Failed to download {remote_path}: {e}")
            self.corrupted_files.append(remote_path)
            return False

    def _log_summary(self) -> None:
        """
        Logs the summary of the download process.
        """
        self._logger.info(
            f"Downloaded {len(self.downloaded_files)} files successfully.")
        if self.corrupted_files:
            self._logger.warning(
                f"{len(self.corrupted_files)} files may be corrupted.")

        self._write_log_file(self.downloaded_files, "downloaded_files")
        self._write_log_file(self.corrupted_files, "corrupted_files")

    def download(
            self,
            check_for_existence: bool = False,
            suffix: str | None = None,
            min_timestep: str | int | None = None,
            max_timestep: str | int | None = None,
            include_pattern: list[str] | None = None,
            exclude_pattern: list[str] | None = None,
            additional_patterns: dict | None = None,
            skip_time_step_filtering_variables: list[str] | None = None,
            variables: list[str] | None = None,
            remote_prefix: str | None = None,
    ) -> None:
        """
        Downloads files from a specified bucket based on given criteria.

        :param check_for_existence: If True, checks if the file already exists
         in the download directory and skips the download if it does.
        :param suffix: The file extension to filter by.
        :param min_timestep: Minimum timestep to include.
        :param max_timestep: Maximum timestep to include.
        :param include_pattern: List of patterns to include.
        :param exclude_pattern: List of patterns to exclude.
        :param additional_patterns: Additional patterns for filtering.
        :param skip_time_step_filtering_variables: Variables to skip timestep filtering.
        :param variables: List of variables to filter by.
        :param remote_prefix: Prefix for the folder in the bucket.
        """
        self._ensure_bucket(self.bucket_name)
        remote_files_with_hashes = self._fetch_existing_files(
            self.bucket_name, remote_prefix)

        include_pattern = self._string_to_list(include_pattern)
        exclude_pattern = self._string_to_list(exclude_pattern)

        include_pattern_with_timesteps = self._process_timesteps(
            min_timestep=min_timestep,
            max_timestep=max_timestep,
            include_pattern=include_pattern)

        filtered_files = self._simple_filename_filter(
            filenames=list(remote_files_with_hashes.keys()),
            prefix=remote_prefix,
            suffix=suffix,
            include_pattern=include_pattern_with_timesteps,
            exclude_pattern=exclude_pattern,
            skip_time_step_filtering_variables=skip_time_step_filtering_variables)

        filtered_files = self._advanced_filename_filter(
            filenames=filtered_files,
            patterns=additional_patterns,
            variables=variables)

        # Filter the remote files with hashes
        filtered_remote_files_with_hashes = {
            k: v for k, v in remote_files_with_hashes.items()
            if k in filtered_files}

        if not filtered_remote_files_with_hashes:
            self._logger.info(
                f"No files to download from bucket '{self.bucket_name}'"
                f" with prefix '{remote_prefix}'.")
            return  # Exit early if no files

        files_to_download = self._build_download_list(
            filtered_remote_files_with_hashes)

        if not files_to_download:
            self._logger.info("All files are already downloaded and verified.")
            return  # Exit early

        self._logger.info(
            f"Starting download of {len(files_to_download)} files...")

        # Parallel Download with Real-time Logging
        with ThreadPoolExecutor(max_workers=self._n_jobs) as executor:
            futures = {executor.submit(
                self._download_file, local, remote,
                check_for_existence, remote_hash): (local, remote)
                       for local, remote, remote_hash in files_to_download}

            for future in as_completed(futures):
                local_file_path, remote_path = futures[future]
                try:
                    if future.result():
                        self.downloaded_files.append(remote_path)
                    else:
                        self.corrupted_files.append(remote_path)
                except Exception as e:
                    self._logger.error(f"Error downloading {remote_path}: {e}")

        # Final summary logs
        self._log_summary()

    def delete(self) -> None:
        """
        Deletes local files after successful download.

        :return: None
        """
        self._delete_files_safely(self.downloaded_files, "downloaded file")
        self._cleanup_empty_dirs(self.files_path)
