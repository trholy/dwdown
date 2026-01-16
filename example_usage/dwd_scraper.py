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
        retry=0,  # Dont retry failed downloads (formerly restart_failed_downloads)
        delay=0.1,  # 0.1 seconds delay between downloads
        n_jobs=4,  # Use 4 concurrent workers (formerly workers)
        files_path=f"download_files/09/{variable}",  # Path for downloaded files (formerly download_path)
        log_files_path="log_files"  # Path for log files
    )

    # Fetch download links
    dwd_downloader.get_links(exclude_pattern=["icosahedral"])

    # Download files
    dwd_downloader.download(check_for_existence=True)

    # Print status after download
    print("Successfully downloaded files:", dwd_downloader.downloaded_files)
    print("Failed downloads:", dwd_downloader.failed_files)
