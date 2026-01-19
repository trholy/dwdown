from __future__ import annotations

import logging
import os.path
from typing import TYPE_CHECKING

from minio import Minio
from minio.error import S3Error

from dwdown.utils.file_handling import FileHandler

if TYPE_CHECKING:
    from dwdown.utils.log_handling import LogHandler


class OSHandler:
    """
    A class to handle operations related to object storage, such as bucket management,
    file fetching, filtering, and integrity verification.

    Attributes:
        _client (Minio): The MinIO client for interacting with the object storage.
        _logger (logging.Logger): Logger for logging messages.
    """

    def __init__(
            self,
            log_handler: LogHandler,
            client: Minio,
            filehandler: FileHandler
    ):
        """
        Initializes the OSHandler with a MinIO client and a logger.
        :param log_handler: LogHandler instance.
        :param client: Minio client instance.
        :param filehandler: FileHandler instance.
        """
        self._logger = log_handler.get_logger()
        self._client = client
        self._filehandler = filehandler

    def _ensure_bucket(
        self,
        bucket_name: str,
        create_if_not_exists: bool = False
    ) -> None:
        """
        Ensures that the specified bucket exists, creating it if necessary.

        :param bucket_name: The name of the bucket to ensure.
        :param create_if_not_exists: Whether to create the bucket if it does not exist.
        """
        if not self._client.bucket_exists(bucket_name):
            if create_if_not_exists:
                self._client.make_bucket(bucket_name)
                self._logger.info(f"Created bucket {bucket_name}")
            else:
                self._logger.info(
                    f"Bucket {bucket_name} does not exist and will not be created")
        else:
            self._logger.info(f"Bucket {bucket_name} exists")

    def _fetch_existing_files(
            self,
            bucket_name: str,
            remote_prefix: str,
            return_basename: bool = False
    ) -> dict[str, str]:
        """
        Fetches existing files from the specified bucket and prefix.

        :param bucket_name: The name of the bucket.
        :param remote_prefix: The prefix to filter objects by.
        :param return_basename: Whether to return only the basename of the file.
        :return: A dictionary of object names and their ETags.
        :raises S3Error: If there is an error fetching the files.
        """
        remote_prefix = remote_prefix or ""

        existing_files = {}

        try:
            objects = self._client.list_objects(
                bucket_name,
                prefix=remote_prefix,
                recursive=True)

            for obj_stat in objects:
                obj_name = obj_stat.object_name
                if return_basename:
                    obj_name = os.path.basename(obj_name)
                existing_files[obj_name] = obj_stat.etag  # Store remote path and its hash

        except S3Error as e:
            self._logger.error(f"Failed to fetch existing files: {e}")
            raise

        return existing_files

    def _verify_file_integrity(
            self,
            local_file_path: str| None = None,
            remote_path: str| None = None,
            bucket_name: str | None = None,
            remote_hash: str | None = None,
            local_md5: str | None = None
    ) -> bool:
        """
        Verifies the integrity of a local file against a remote file using MD5 hash.

        :param local_file_path: The path to the local file.
        :param remote_path: The path to the remote file.
        :param bucket_name: The name of the bucket.
        :param remote_hash: The ETag of the remote file.
        :param local_md5: The MD5 hash of the local file.
        :return: True if the files match, False otherwise.
        """
        try:
            if local_md5 is None:
                local_md5 = self._filehandler._calculate_md5(local_file_path)
            if remote_hash is None:
                obj_stat = self._client.stat_object(bucket_name, remote_path)
                remote_hash = obj_stat.etag
            return remote_hash == local_md5

        except Exception as e:
            self._logger.error(
                f"Error verifying file integrity for {remote_path}: {e}"
            )
            return False
