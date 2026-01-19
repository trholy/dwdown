# dwdown

`dwdown` is a Python package designed to download weather forecast data from the Deutscher Wetterdienst (DWD), process it, and upload it to a Object Storage Server. It supports downloading forecast files, uploading them to Object Storage, and processing them for analysis. Furthermore it keeps you informed about the status of downloads, uploads, and any errors.

## Features

- **ForecastDownloader**: Fetch weather forecast data from the DWD open data server.
- **OSDownloader**: Download files from a S3 compatible / [MinIO object storage server](https://github.com/minio/minio).
- **OSUploader**: Upload downloaded data to a S3 compatible / MinIO object storage server with parallel uploads and data integrity checks.
- **GribFileManager**: Extract BZ2 archives and convert GRIB2 files to CSV format.
- **DataMerger**: Filter and merge CSV dataframes.
- **Notifier**: Receive status messages of downloads, uploads, and any errors from a [Gotify server](https://github.com/gotify).
- **Logging**: Automatically log download and upload activities, and handle errors gracefully.
- **Parallel Processing**: Download, upload, and process files in parallel for faster performance.

## Installation

You can install `dwdown` via pip:

```bash
git clone https://github.com/trholy/dwdown.git 

pip install .
```

## Documentation

Read the documentation on [GitLab Pages](https://to82lod.gitpages.uni-jena.de/dwdown/).

## Usage

### ForecastDownloader: Download Data from DWD

The `ForecastDownloader` class allows you to download weather forecast files from the DWD open data server.

```python
from dwdown.download import ForecastDownloader

variables = [
    'aswdifd_s',
    'relhum',
    'smi',
]

for variable in variables:

    # Initialize ForecastDownloader
    dwd_downloader = ForecastDownloader(
        url=f"https://opendata.dwd.de/weather/nwp/icon-d2/grib/09/{variable}/",
        retry=0,  # Dont retry failed downloads
        delay=0.1,  # 0.1 seconds delay between downloads
        n_jobs=4,  # Use 4 concurrent workers
        files_path=f"download_files/09/{variable}",  # Path for downloaded files
        log_files_path="log_files"  # Path for log files
    )

    # Fetch download links
    dwd_downloader.get_links(exclude_pattern=["icosahedral"])

    # Download files
    dwd_downloader.download(check_for_existence=True)

    # Print status after download
    print("Successfully downloaded files:", dwd_downloader.downloaded_files)
    print("Failed downloads:", dwd_downloader.failed_files)
```

### OSUploader: Upload Data to Object Storage

The `OSUploader` class helps upload files to a [MinIO object storage server](https://github.com/minio/minio) or any S3-compatible storage, ensuring data integrity with MD5 hash verification.

```python
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
# Optional: Delete local files after upload
# uploader.delete()

# Print status after upload
print("Successfully uploaded files:", uploader.uploaded_files)
print("Upload might be corrupted:", uploader.corrupted_files)
```

### OSDownloader: Download Data from Object Storage

The `OSDownloader` class helps you download files from a [MinIO object storage server](https://github.com/minio/minio) or any S3-compatible storage.

```python
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
minio_downloader.download()

# Print status after download
print("Successfully downloaded files:", minio_downloader.downloaded_files)
print("Download might be corrupted:", minio_downloader.corrupted_files)
```

### GribFileManager & DataMerger: Process and Merge Data

The `GribFileManager` handles decompression and conversion of GRIB2 files, while `DataMerger` (formerly DataEditor) allows for merging and filtering CSV dataframes.

```python
from dwdown.processing import DataMerger, GribFileManager

# Initialize the GribFileManager
processor = GribFileManager(
    files_path="download_files",  # Path for files to process
    extracted_files_path="extracted_files",  # Path for extracted files
    converted_files_path="csv_files",  # Path for CSV files
)

# Retrieve the filenames that have been downloaded
file_names = processor.get_filenames()

# Convert downloaded files into CSV format
processor.get_csv(
    file_names=file_names,
    apply_geo_filtering=True,
    start_lat=50.840,
    end_lat=51.000,
    start_lon=11.470,
    end_lon=11.690,
)

# Variables to build merged dataframe from
variables = [
    'aswdifd_s',
    'relhum',
    'smi',
]

# External mapping dictionary
mapping_dictionary = {
    'aswdifd_s': 'ASWDIFD_S',
    'relhum': 'r',
    'smi': 'SMI',
}

# Pattern selection for known variables
additional_patterns = {
    "relhum": [200, 975, 1000],
    "smi": [0, 9, 27],
}

# Initialize DataMerger
data_editor = DataMerger(
    files_path='csv_files/09/',
    required_columns={
        'latitude', 'longitude', 'valid_time'
    },
    join_method='inner',
    mapping_dictionary=mapping_dictionary,
    additional_patterns=additional_patterns,
)

df = data_editor.merge(
    time_step=0,
    variables=variables
)
print("Processed DataFrame:", df)

df.to_csv('processed_dataframe.csv')
```

### Notifier: Send Status Updates

The `Notifier` class keeps you informed about the status of downloads, uploads, and any errors via a [Gotify server](https://github.com/gotify).

```python
from minio import Minio
from dwdown.notify import Notifier

# Initialize Notifier
notifier = Notifier(
    server_url="your-gotify-sever.com",
    token="your-access-token",
    priority=5,
    secure=False  # Set to True if your MinIO server is HTTPS
)

# Initialize minio client
minio_client = Minio(
    endpoint="your-minio-sever.com",
    access_key="your-access-key",
    secret_key="your-secret-key",
    secure=False  # Set to True if your MinIO server is HTTPS
)

# List all buckets
buckets = minio_client.list_buckets()

status_dict = {}

for bucket in buckets:
    bucket_name = bucket.name
    print(f"Processing bucket: {bucket_name}")

    # List all objects in the bucket
    objects = minio_client.list_objects(bucket_name, recursive=True)

    # Get number of objects in the bucket
    status_dict[bucket_name] = [len([obj.object_name for obj in objects])]

# Send notification
notifier.send_notification(
    message=status_dict,
    script_name="download-VM"
)
```

## Directory Structure

The package structure is as follows:

```
./
├── .git
├── .gitignore
├── .gitlab-ci.yml
├── LICENSE
├── README.md
├── THIRD_PARTY_LICENSES.txt
├── docs
│   ├── data
│   │   └── MappingStore.md
│   ├── download
│   │   ├── ForecastDownloader.md
│   │   └── OSDownloader.md
│   ├── notify
│   │   └── Notifier.md
│   ├── processing
│   │   ├── DataMerger.md
│   │   └── GribFileManager.md
│   ├── upload
│   │   └── OSUploader.md
│   └── utils
│       ├── DataFrameOperator.md
│       ├── DateTimeUtils.md
│       ├── FileHandler.md
│       ├── LogHandler.md
│       ├── NetworkHandlers.md
│       ├── OSHandler.md
│       └── Utilities.md
├── example_usage
│   ├── dwd_processing.py
│   ├── dwd_scraper.py
│   ├── minio_downloader.py
│   ├── minio_uploader.py
│   └── notifier.py
├── img
│   └── example_workflow.png
├── mkdocs.yml
├── pyproject.toml
├── setup.py
├── src
│   └── dwdown
│       ├── __init__.py
│       ├── data
│       │   ├── __init__.py
│       │   └── mapping.py
│       ├── download
│       │   ├── __init__.py
│       │   ├── forecast_download.py
│       │   └── os_download.py
│       ├── notify
│       │   ├── __init__.py
│       │   └── notifier.py
│       ├── processing
│       │   ├── __init__.py
│       │   ├── data_merging.py
│       │   ├── grib_data_handling.py
│       ├── upload
│       │   ├── __init__.py
│       │   └── os_upload.py
│       └── utils
│           ├── __init__.py
│           ├── date_time_utilis.py
│           ├── df_utilis.py
│           ├── file_handling.py
│           ├── general_utilis.py
│           ├── log_handling.py
│           ├── network_handling.py
│           └── os_handling.py
├── tests
│   ├── test_ForecastDownloader.py
│   ├── test_OSDownloader.py
│   ├── test_OSUploader.py
│   ├── test_date_time_utilis.py
│   ├── test_file_handling.py
│   ├── test_log_handling.py
│   ├── test_mapping.py
│   ├── test_network_handling.py
│   ├── test_notifier.py
│   ├── test_processing.py
│   └── test_utils.py
```

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for more details.

## Authors & Maintainers

- Thomas R. Holy, Ernst-Abbe-Hochschule Jena

## Contributing

Feel free to contribute to the development of `dwdown`!
