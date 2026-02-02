from dwdown.download import HistoricalDownloader


# Initialize HistoricalDownloader
scraper = HistoricalDownloader(
    base_url=None,                       # Base URL for historical data (to be set)
    files_path=None,                     # Path for downloaded files
    extracted_files_path=None,           # Path for extracted files
    log_files_path="log_files",          # Path for log files
    encoding=None,                       # File encoding (to be set)
    station_description_file_name=None,  # Station description filename
    delay=1,                             # 1 second delay between downloads
    retry=0,                             # Don't retry failed downloads
    timeout=30                           # 30 second timeout for requests
)

# Download station descriptions
scraper.download_station_description()

# Read station descriptions
station_descriptions = scraper.read_station_description()
print(station_descriptions)

# Get download links for specific stations
links = scraper.get_links(
    station_ids=['00001','00003'],   # Get links for stations 1 and 3
    prefix="tageswerte_KL",          # File prefix for daily weather data
    suffix="_hist.zip"               # File suffix for historical zip files
)
print(links)

# Download files
scraper.download(check_for_existence=True)

# Unpack ZIP files
scraper.extract(unpack_hist_data_only=True, check_for_existence=True)

# Read and save data as CSV
df = scraper.read_data(save_as_csv=True)
print(df)
