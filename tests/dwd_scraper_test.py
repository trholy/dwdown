from dwdown.download import DWDDownloader

variables = [
    'aswdifd_s',
    'relhum',
    'smi',
]

for variable in variables:

    # Initialize DWDDownloader
    dwd_downloader = DWDDownloader(
        url=f"https://opendata.dwd.de/weather/nwp/icon-d2/grib/09/{variable}/",
        restart_failed_downloads=False,  # Dont retry failed downloads
        log_downloads=True,  # Log download status
        delay=0.1,  # 0.1 seconds delay between downloads
        workers=4,  # Use 4 concurrent workers
        download_path=f"download_files/09/{variable}",  # Path for downloaded files
        log_files_path="log_files"  # Path for log files
    )

    # Fetch download links
    dwd_downloader.get_links(exclude_pattern=["icosahedral"])

    # Download files
    dwd_downloader.download_files(check_for_existence=True)

    # Print status after download
    print("Successfully downloaded files:", dwd_downloader.downloaded_files)
    print("Failed downloads:", dwd_downloader.failed_files)
    print("Finally failed downloads:", dwd_downloader.finally_failed_files)
