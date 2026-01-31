from dwdown.processing import DataMerger, GribFileManager


# Initialize the GribFileManager (formerly DataProcessor)
processor = GribFileManager(
    files_path="download_files",  # Path for files to process (formerly search_path)
    extracted_files_path="extracted_files",  # Path for extracted files (formerly extraction_path)
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

# Initialize DataMerger (formerly DataEditor)
data_editor = DataMerger(
    files_path='csv_files/09/',
    required_columns={
        'latitude', 'longitude', 'valid_time'
    },
    join_method='inner',
    mapping_dictionary=mapping_dictionary,
    additional_patterns=additional_patterns, # formerly additional_pattern_selection
)

df = data_editor.merge(
    time_step=0,
    variables=variables
)
print("Processed DataFrame:", df)

df.to_csv('processed_dataframe.csv')
