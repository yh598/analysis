import os
import gzip
import pandas as pd
from datetime import datetime

# Directory where the files are stored
directory = "/mnt/data/"

# Define start and end date (modify these values)
start_date = "2025-02-16"
end_date = "2025-03-06"

# Convert to datetime objects for comparison
start_dt = datetime.strptime(start_date, "%Y-%m-%d")
end_dt = datetime.strptime(end_date, "%Y-%m-%d")

# List all files in the directory
files = os.listdir(directory)

# Filter only csv.gz files that fall within the date range
gz_files = sorted([
    f for f in files if f.endswith(".csv.gz") and
    start_dt <= datetime.strptime(f.split('-')[1].split('.')[0], "%Y-%m-%d") <= end_dt
])

# Function to read and extract csv.gz files
def load_gz_files(file_list, directory):
    data_frames = []
    for file in file_list:
        file_path = os.path.join(directory, file)
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            df = pd.read_csv(f, dtype=str)  # Ensure all data is loaded as string for consistency
            data_frames.append(df)
    return data_frames

# Load filtered csv.gz files
data_frames = load_gz_files(gz_files, directory)

# Combine into a single DataFrame (if needed)
if data_frames:
    combined_df = pd.concat(data_frames, ignore_index=True)
else:
    combined_df = pd.DataFrame()  # Empty DataFrame if no files match criteria

# Display the loaded DataFrame
import ace_tools as tools
tools.display_dataframe_to_user(name="Filtered Combined Data", dataframe=combined_df)

# Generate the GML filename based on the date range
gml_filename = f"audience_{start_date}_to_{end_date}.gml"

@app.cell
def save_gml(G, nx):
    nx.write_gml(G, gml_filename)
    return f"GML file saved as {gml_filename}"


import os
import gzip
import pandas as pd
from datetime import datetime

# Directory where the files are stored
directory = "/mnt/data/"

# Define start and end date (modify these values)
start_date = "2025-02-16"
end_date = "2025-03-06"

# Convert to datetime objects for comparison
start_dt = datetime.strptime(start_date, "%Y-%m-%d")
end_dt = datetime.strptime(end_date, "%Y-%m-%d")

# List all files in the directory
files = os.listdir(directory)

# Filter only csv.gz files that fall within the date range
gz_files = sorted([
    f for f in files if f.endswith(".csv.gz") and
    start_dt <= datetime.strptime(f.split('-')[1].split('.')[0], "%Y-%m-%d") <= end_dt
])

# Function to read and extract csv.gz files
def load_gz_files(file_list, directory):
    data_frames = []
    for file in file_list:
        file_path = os.path.join(directory, file)
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            df = pd.read_csv(f, dtype=str)  # Ensure all data is loaded as string for consistency
            data_frames.append(df)
    return data_frames

# Load filtered csv.gz files
data_frames = load_gz_files(gz_files, directory)

# Combine into a single DataFrame (if needed)
if data_frames:
    combined_df = pd.concat(data_frames, ignore_index=True)
else:
    combined_df = pd.DataFrame()  # Empty DataFrame if no files match criteria

# Display the loaded DataFrame
import ace_tools as tools
tools.display_dataframe_to_user(name="Filtered Combined Data", dataframe=combined_df)

# Generate the CSV filename using the end date
csv_filename = f"syntheticData-{end_date}.csv"
csv_filepath = os.path.join(directory, csv_filename)

# Save the combined DataFrame as CSV
combined_df.to_csv(csv_filepath, index=False)

print(f"CSV file saved as: {csv_filename}")

import os
import gzip
import pandas as pd
from datetime import datetime

# Directory where the files are stored
directory = "/mnt/data/"

# Define start and end date (modify these values)
start_date = "2025-02-16"
end_date = "2025-03-06"

# Convert to datetime objects for comparison
start_dt = datetime.strptime(start_date, "%Y-%m-%d")
end_dt = datetime.strptime(end_date, "%Y-%m-%d")

# List all files in the directory
files = os.listdir(directory)

# Filter only csv.gz files that fall within the date range
gz_files = sorted([
    f for f in files if f.endswith(".csv.gz") 
    and len(f.split('-')) > 1  # Ensure proper split
    and start_dt <= datetime.strptime(f.split('-')[1].split('.')[0], "%Y-%m-%d") <= end_dt
])

# Function to read and extract csv.gz files
def load_gz_files(file_list, directory):
    data_frames = []
    for file in file_list:
        file_path = os.path.join(directory, file)
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            df = pd.read_csv(f, dtype=str)  # Ensure all data is loaded as string for consistency
            data_frames.append(df)
    return data_frames

# Load filtered csv.gz files
data_frames = load_gz_files(gz_files, directory)

# Combine into a single DataFrame (if needed)
if data_frames:
    combined_df = pd.concat(data_frames, ignore_index=True)
else:
    combined_df = pd.DataFrame()  # Empty DataFrame if no files match criteria

# Display the loaded DataFrame
import ace_tools as tools
tools.display_dataframe_to_user(name="Filtered Combined Data", dataframe=combined_df)

# Generate the CSV filename using the end date
csv_filename = f"syntheticData-{end_date}.csv"
csv_filepath = os.path.join(directory, csv_filename)

# Save the combined DataFrame as CSV
combined_df.to_csv(csv_filepath, index=False)

print(f"CSV file saved as: {csv_filename}")