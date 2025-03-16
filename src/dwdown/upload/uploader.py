import hashlib
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from minio import Minio
from minio.error import S3Error

# Configure logging to remove the default prefix
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
)


class MinioUploader:
    def __init__(
            self,
            endpoint: str,
            access_key: str,
            secret_key: str,
            files_path: str,
            bucket_name: str = 'my-bucket',
            secure: bool = False,
            log_uploads: bool = True,
            log_files_path: str = "log_files_MinioUploader",
            workers: int = 1,
    ):
        """
        Initializes the MinioUploader.
        """
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )
        self.workers = workers
        self.files_path = files_path
        self.bucket_name = bucket_name
        self.log_uploads = log_uploads
        self.log_files_path = log_files_path

        self.uploaded_files = []
        self.corrupted_files = []

        # Create directories if they don't exist
        self._ensure_directory_exists(self.log_files_path)

        self.logger = logging.getLogger(__name__)

    @staticmethod
    def _ensure_directory_exists(
            path: str
    ) -> None:
        """
        Helper function to ensure a directory exists, creates if not.
        """
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)

    def _ensure_bucket(
            self
    ) -> None:
        """
        Ensure the bucket exists, or create it if necessary.
        """
        if not self.client.bucket_exists(self.bucket_name):
            self.client.make_bucket(self.bucket_name)
            self.logger.info(f"Created bucket {self.bucket_name}")
        else:
            self.logger.info(f"Bucket {self.bucket_name} already exists")

    @staticmethod
    def calculate_md5(
            file_path: str
    ) -> str:
        """
        Computes the MD5 hash of a file.
        """
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    @staticmethod
    def _get_current_date() -> str:
        """
        Get the current system date, formatted as "DD-MMM-YYYY-HH-MM".

        :return: Current date in formatted datetime format.
        """
        # Get current datetime
        now = datetime.now()

        # Format as string and parse back to enforce "DD-MMM-YYYY-HH:MM" format
        formatted_str = now.strftime("%d-%b-%Y-%H-%M")

        return formatted_str

    def upload_directory(
            self,
            remote_prefix: str = "",
            check_for_existence: bool = False
    ) -> None:
        """
        Recursively uploads a directory to MinIO with real-time logging.
        """

        self._ensure_bucket()

        files_to_upload = []
        existing_files = {}  # Store {remote_path: etag}

        # Step 1: Fetch existing files in a single API call
        if check_for_existence:
            existing_files = self._fetch_existing_files(remote_prefix)

        # Step 2: Build the upload list
        for root, _, files in os.walk(self.files_path):
            for file in files:
                local_file_path = os.path.join(
                    root, file
                )
                relative_path = os.path.relpath(
                    local_file_path, self.files_path
                )
                remote_path = os.path.join(
                    remote_prefix, relative_path
                ).replace("\\", "/")
                files_to_upload.append((local_file_path, remote_path))

        # Step 3: Parallel Upload with Real-time Logging
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            futures = {
                executor.submit(
                    self._upload_file, local, remote, check_for_existence,
                    existing_files
                ): (local, remote) for local, remote in files_to_upload
            }

            for future in as_completed(futures):
                local_file_path, remote_path = futures[future]
                try:
                    if future.result():
                        self.uploaded_files.append(local_file_path)
                        self.logger.info(
                            f"Successfully uploaded: {local_file_path}")
                    else:
                        self.corrupted_files.append(local_file_path)
                        self.logger.warning(
                            f"Hash mismatch for {local_file_path}."
                            f" Upload might be corrupted!")

                except Exception as e:
                    self.logger.error(
                        f"Unexpected error uploading {local_file_path}: {e}")

        # Final summary logs
        self.logger.info(
            f"Uploaded {len(self.uploaded_files)} files successfully.")
        if self.corrupted_files:
            self.logger.warning(
                f"{len(self.corrupted_files)} files may be corrupted.")

        # Log uploads if enabled
        if self.log_uploads:
            time_stamp = self._get_current_date()
            formatted_time_stamp = re.sub(r"[-:\s]", "_", time_stamp)

            self._write_log_file(
                f"{self.log_files_path}/"
                f"MinioUploader_uploaded_files_{formatted_time_stamp}.log",
                self.uploaded_files
            )
            self._write_log_file(
                f"{self.log_files_path}/"
                f"MinioUploader_corrupted_files_{formatted_time_stamp}.log",
                self.corrupted_files
            )

    def _fetch_existing_files(self, remote_prefix: str) -> dict:
        """
        Fetches existing files in the bucket with their ETags.
        Returns a dictionary: {remote_path: etag}
        """
        existing_files = {}

        try:
            objects = self.client.list_objects(
                self.bucket_name,
                prefix=remote_prefix,
                recursive=True
            )
            for obj in objects:
                existing_files[
                    obj.object_name
                ] = obj.etag  # Store remote path and its hash

        except S3Error as e:
            self.logger.error(f"Failed to fetch existing files: {e}")

        return existing_files

    def _upload_file(
            self,
            local_file_path: str,
            remote_path: str,
            check_for_existence: bool,
            existing_files: dict
    ) -> bool:
        """
        Uploads a single file with immediate logging.
        """
        try:
            local_md5 = self.calculate_md5(local_file_path)

            # Step 1: Skip if file already exists and matches hash
            if check_for_existence and remote_path in existing_files:
                if existing_files[remote_path] == local_md5:
                    self.logger.info(
                        f"Skipping already uploaded file: {remote_path}")
                    return True  # Skip upload

            # Step 2: Upload the file
            self.client.fput_object(
                self.bucket_name, remote_path, local_file_path
            )

            # Step 3: Verify upload integrity
            obj_stat = self.client.stat_object(self.bucket_name, remote_path)
            return obj_stat.etag == local_md5

        except S3Error as e:
            self.logger.error(
                f"Failed to upload {local_file_path} due to S3 error: {e}")
        except Exception as e:
            self.logger.error(
                f"Unexpected error uploading {local_file_path}: {e}")

        return False

    def delete_local_files(
            self
    ) -> None:
        """
        Deletes local files after successful upload verification.
        """
        for file_path in self.uploaded_files:
            os.remove(file_path)
            self.logger.info(f"Deleted {file_path}")

        for root, dirs, _ in os.walk(self.files_path, topdown=False):
            for dir_name in dirs:
                dir_path = os.path.join(
                    root, dir_name
                )
                if not os.listdir(dir_path):  # Remove empty directories
                    os.rmdir(dir_path)
                    self.logger.info(f"Deleted directory {dir_path}")

    def _write_log_file(
            self,
            filename: str,
            data: list
    ) -> None:
        """
        Writes a list of file links to a log file,
         ensuring each entry is on a new line.
        """
        try:
            with open(filename, "w", encoding="utf-8") as file:
                file.write("\n".join(data) + "\n")
            self.logger.info(f"Saved log: {filename} ({len(data)} entries)")
        except Exception as e:
            self.logger.error(f"Error writing log file {filename}: {e}")
