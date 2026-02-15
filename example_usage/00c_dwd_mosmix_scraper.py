from dwdown.download import MOSMIXDownloader

# Initialize MOSMIX_Downloader
# mosmix_type can be "MOSMIX_L" (single stations available) or "MOSMIX_S" (all stations)
scraper = MOSMIXDownloader(
    mosmix_type="MOSMIX_L",              
    base_url=None,                       # Base URL constructed automatically based on type
    files_path=None,                     # Path for downloaded files (defaults to download_files)
    extracted_files_path=None,           # Path for extracted files (defaults to extracted_files)
    log_files_path="log_files",          # Path for log files
    delay=1,                             # 1 second delay between downloads
    retry=0,                             # Don't retry failed downloads
    timeout=30                           # 30 second timeout for requests
)

# Get download links for specific stations
# For MOSMIX_L, we can specify station IDs. 
# Station "01001" is Jan Mayen (as an example)
links = scraper.get_links(
    station_ids=['01001'],
    # prefix/suffix can also be used if needed, but usually redundant with station_ids logic
)
print(f"Found {len(links)} links:", links)

# Download files
# By default checks for existence and skips if present
scraper.download(check_for_existence=True)

# Unpack KMZ files
# Unpacks .kmz files to .kml in extracted_files_path
scraper.extract(check_for_existence=True)

# Read and save data as CSV
# Parses the KML files and converts them to DataFrames
# save_as_csv=True saves a CSV copy in the extracted folder
data = scraper.read_data(save_as_csv=True)

# Print result summary
if data:
    for filename, df in data.items():
        print(f"Processed {filename}:")
        print(df.head())
else:
    print("No data processed.")
