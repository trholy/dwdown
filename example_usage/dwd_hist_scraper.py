from dwdown.download import HistDataDownloader
import os
import re

downloader = HistDataDownloader()

# Step 1: Download the station description file
downloader.download_station_description()

# Step 2: Read station description into pandas DataFrame
df = downloader.read_station_description()
print(df.head())

# Step 3: Download zip files for a given station ID
station_id = '00011'  # Example station ID
downloader.download_zip_files(station_id)

# Step 4: Unpack the zip file
zip_file = f"^tageswerte_KL_{station_id}_.*\\.zip$"
for file in os.listdir(downloader.download_path):
    if re.match(zip_file, file):
        downloader.unpack_zip(file, station_id, unpack_hist_data_only=True)
        break

# Step 5: Read station data into DataFrame
station_data = downloader.read_station_data(station_id)
print(station_data)
