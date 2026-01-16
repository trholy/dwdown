from dwdown.upload import OSUploader

# Initialize OSUploader
uploader = OSUploader(
    endpoint="your-minio-sever.com",
    access_key="your-access-key",
    secret_key="your-secret-key",
    files_path="download_files",  # Path for files to upload
    bucket_name="weather-forecasts",  # Name of the minio bucket
    secure=False,  # If "true" API requests will be secure (HTTPS), and insecure (HTTP) otherwise
    log_files_path="log_files",  # Path for log files
    n_jobs=4  # Use 4 concurrent workers
)

# Upload files to MinIO
uploader.upload()
uploader.delete()

# Print status after upload
print("Successfully uploaded files:", uploader.uploaded_files)
print("Upload might be corrupted:", uploader.corrupted_files)
