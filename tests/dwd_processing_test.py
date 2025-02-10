from dwdown.processing import DataProcessor
from dwdown.processing import DataEditor

# Initialize the DataProcessor
editor = DataProcessor(
    search_path="download_files",  # Path for files to process
    extraction_path="extracted_files",  # Path for extracted files
    converted_files_path="csv_files",  # Path for CSV files
)

# Retrieve the filenames that have been downloaded
file_names = editor.get_filenames()

# Convert downloaded files into CSV format
editor.get_csv(
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

# Initialize DataEditor
data_editor = DataEditor(
    files_path='csv_files/09/',
    required_columns={
        'latitude', 'longitude', 'valid_time'
    },
    join_method='inner',
    mapping_dictionary=mapping_dictionary,
    additional_pattern_selection=additional_patterns,

)

df = data_editor.merge_dfs(
    time_step=0,
    variables=variables
)
print("Processed DataFrame:", df)
