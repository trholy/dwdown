import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin

from ..utils import (
    Utilities, LogHandler, FileHandler,
    TimeHandler, DateHandler,
    ClientHandler, OSHandler)


class OSUploader(
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
        Initializes the OSUploader with necessary parameters.

        :param endpoint: The endpoint of the storage service.
        :param access_key: The access key for authentication.
        :param secret_key: The secret key for authentication.
        :param secure: Whether to use secure connection.
        :param files_path: The local path to the files to be uploaded.
        :param bucket_name: The name of the bucket to upload files to.
        :param log_files_path: The path to store log files.
        :param delay: Optional delay between downloads (in seconds).
        :param n_jobs: Number of parallel jobs for uploading.
        :param retry: Number of retries for failed uploads.
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

        Utilities.__init__(self)
        TimeHandler.__init__(self)
        DateHandler.__init__(self)
        ClientHandler.__init__(self,
            endpoint=self._endpoint,
            access_key=self._access_key,
            secret_key=self._secret_key,
            secure=self._secure)
        self._client = self.get_client()

        OSHandler.__init__(self)

        self.uploaded_files = []
        self.corrupted_files = []

    """
    def _build_upload_list(
            self,
            remote_prefix: str
    ) -> list[tuple[str, str]]:

        files_to_upload = []
        for root, _, files in os.walk(self.files_path):
            for file in files:
                local_file_path = os.path.join(root, file)
                local_file_path = os.path.normpath(local_file_path)
                relative_path = os.path.relpath(local_file_path, self.files_path)
                remote_path = urljoin(remote_prefix, relative_path)
                files_to_upload.append((local_file_path, remote_path))
        return files_to_upload
    """
    def _build_upload_list(
            self,
            filtered_filenames,
            remote_prefix: str,
            filtered_remote_files: dict[str, str] | None = None
    ):
        """

        """
        files_to_upload = []
        for local_path, local_hash in filtered_filenames.items():
            local_file_path = os.path.normpath(local_path)

            # Skip if the file already exists and matches hash
            if (os.path.basename(local_file_path) in filtered_remote_files.keys()
                    and self._verify_file_integrity(
                        local_file_path, None, self.bucket_name,
                        filtered_remote_files[os.path.basename(local_file_path)], local_hash)):
                    self._logger.info(
                        f"Skipping already uploaded file: {os.path.basename(local_file_path)}")
                    continue
            else:
                relative_path = os.path.relpath(local_file_path, self.files_path)
                remote_path = urljoin(remote_prefix, relative_path)
                files_to_upload.append((local_file_path, remote_path))
        return files_to_upload

    def _upload_file(
            self,
            local_file_path: str,
            remote_path: str,
            check_for_existence: bool,
            remote_prefix: str,
            local_md5
    ) -> bool:
        """
        Uploads a single file and checks for integrity.

        :param local_file_path: The path to the local file.
        :param remote_path: The path to the remote file.
        :param check_for_existence: Whether to check for existing files.
        :return: True if the file was uploaded successfully, False otherwise.
        """
        try:
            """
            local_md5 = self._calculate_md5(local_file_path)

            if check_for_existence:
                existing_files = self._fetch_existing_files(
                    self.bucket_name, remote_prefix)
                if (remote_path in existing_files
                        and existing_files[remote_path] == local_md5):
                    self._logger.info(
                        f"Skipping already uploaded file: {remote_path}")
                    return True
            """
            # Respect the delay between downloads
            if self._delay > 0:
                time.sleep(self._delay)

            self._client.fput_object(
                self.bucket_name, remote_path, local_file_path)

            obj_stat = self._client.stat_object(self.bucket_name, remote_path)
            if obj_stat.etag == local_md5:
                self._logger.info(f"Successfully uploaded: {local_file_path}")
                return True
            else:
                self._logger.warning(
                    f"Hash mismatch: {local_file_path} (possible corruption).")
                self.corrupted_files.append(local_file_path)
                return False

        except Exception as e:
            self._logger.error(f"Failed to download {local_file_path}: {e}")
            self.corrupted_files.append(local_file_path)
            return False

    def _log_summary(self) -> None:
        """
        Logs the summary of the upload process.
        """
        self._logger.info(f"Uploaded {len(self.uploaded_files)} files successfully.")
        if self.corrupted_files:
            self._logger.warning(f"{len(self.corrupted_files)} files may be corrupted.")

        self._write_log_file(self.uploaded_files, "uploaded_files")
        self._write_log_file(self.corrupted_files, "corrupted_files")

    def upload(
            self,
            prefix: str | None = None,
            suffix: str | None = None,
            include_pattern: list[str] | None = None,
            exclude_pattern: list[str] | None = None,
            additional_patterns: dict | None = None,
            skip_time_step_filtering_variables: list[str] | None = None,
            variables: list[str] | None = None,
            min_timestep: str | int | None = None,
            max_timestep: str | int | None = None,
            remote_prefix: str = "",
            check_for_existence: bool = False
    ) -> None:
        """
        Uploads files from the local path to the specified bucket.

        :param remote_prefix: The prefix to use for remote paths.
        :param check_for_existence: Whether to check for existing files.
        """
        self._ensure_bucket(self.bucket_name, True)
        if check_for_existence:
            existing_remote_files_with_hashes = self._fetch_existing_files(
                self.bucket_name, remote_prefix, True)
        else:
            existing_remote_files_with_hashes = {}

        filenames = self._search_directory(self.files_path)
        filenames = self._flatten_list(filenames)

        include_pattern = self._process_timesteps(
            min_timestep=min_timestep,
            max_timestep=max_timestep,
            include_pattern=include_pattern)

        filtered_filenames = self._simple_filename_filter(
            filenames=filenames,
            prefix=prefix,
            suffix=suffix,
            include_pattern=include_pattern,
            exclude_pattern=exclude_pattern,
            skip_time_step_filtering_variables=skip_time_step_filtering_variables)

        filtered_filenames = self._advanced_filename_filter(
            filenames=filtered_filenames,
            patterns=additional_patterns,
            variables=variables)

        local_files_with_hashes = {}
        for obj in filtered_filenames:
            local_files_with_hashes[obj] = self._calculate_md5(obj)

        files_to_upload = self._build_upload_list(
            local_files_with_hashes,
            remote_prefix,
            existing_remote_files_with_hashes)

        # Step 3: Parallel Upload with Real-time Logging
        with ThreadPoolExecutor(max_workers=self._n_jobs) as executor:
            futures = {executor.submit(
                self._upload_file, local, remote,
                check_for_existence, remote_prefix, local_files_with_hashes[local]): (local, remote)
                       for local, remote in files_to_upload}

            for future in as_completed(futures):
                local_file_path, remote_path = futures[future]
                try:
                    if future.result():
                        self.uploaded_files.append(local_file_path)
                    else:
                        self.corrupted_files.append(local_file_path)
                except Exception as e:
                    self._logger.error(
                        f"Error downloading {local_file_path}: {e}")

        self._log_summary()

    def delete(self) -> None:
        """
        Deletes local files after successful upload.

        :return: None
        """
        self._delete_files_safely(self.uploaded_files, "uploaded file")
        self._cleanup_empty_dirs(self.files_path)
