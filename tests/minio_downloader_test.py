from dwdown.download import MinioDownloader

# Initialize MinioDownloader
minio_downloader = MinioDownloader(
    endpoint="your-minio-sever.com",
    access_key="your-access-key",
    secret_key="your-secret-key",
    files_path="download_files",  # Path for files to download
    secure=False,  # If ‘true’ API requests will be secure (HTTPS), and insecure (HTTP) otherwise
    log_downloads=True,  # Log upload status
    log_files_path="log_files",  # Path for log files
    workers=4  # Use 4 concurrent workers
)

# Download files from MinIO
minio_downloader.download_bucket(
    bucket_name="weather-forecasts",  # Name of the minio bucket
    folder_prefix=None
)

# Print status after upload
print("Successfully downloaded files:", minio_downloader.downloaded_files)
print("Download might be corrupted:", minio_downloader.corrupted_files)
