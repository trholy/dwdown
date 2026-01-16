from dwdown.download import OSDownloader

# Initialize OSDownloader
minio_downloader = OSDownloader(
    endpoint="your-minio-sever.com",
    access_key="your-access-key",
    secret_key="your-secret-key",
    files_path="download_files",  # Path for files to download
    bucket_name="weather-forecasts",  # Name of the minio bucket
    secure=False,  # If "true" API requests will be secure (HTTPS), and insecure (HTTP) otherwise
    log_files_path="log_files",  # Path for log files
    n_jobs=4  # Use 4 concurrent workers
)

# Download files from MinIO
minio_downloader.download(
    prefix=""  # Folder prefix, empty for root or None
)

# Print status after upload
print("Successfully downloaded files:", minio_downloader.downloaded_files)
print("Download might be corrupted:", minio_downloader.corrupted_files)
