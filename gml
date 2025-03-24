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
gz_files = []
for f in files:
    if f.endswith(".csv.gz"):
        try:
            # Extract the YYYY-MM-DD part from filename
            date_part = f.replace("syntheticData-", "").replace(".csv.gz", "")
            file_date = datetime.strptime(date_part, "%Y-%m-%d")
            
            # Check if the file falls within the date range
            if start_dt <= file_date <= end_dt:
                gz_files.append((f, date_part))  # Store file and extracted date
        except ValueError:
            print(f"Skipping file {f} - Invalid date format")

# Sort filtered files
gz_files.sort()

# Function to read and extract csv.gz files
def load_gz_file(file_name, directory):
    file_path = os.path.join(directory, file_name)
    with gzip.open(file_path, 'rt', encoding='utf-8') as f:
        df = pd.read_csv(f, dtype=str)  # Ensure all data is loaded as string for consistency
    return df

# Process each file separately and save as CSV
for file_name, date_str in gz_files:
    df = load_gz_file(file_name, directory)
    
    # Generate individual CSV filenames using each date
    csv_filename = f"syntheticData-{date_str}.csv"
    csv_filepath = os.path.join(directory, csv_filename)

    # Save each file separately
    df.to_csv(csv_filepath, index=False)

    print(f"CSV file saved: {csv_filename}")

# Display a sample DataFrame for verification
if gz_files:
    sample_df = load_gz_file(gz_files[0][0], directory)  # Load first file for preview
    import ace_tools as tools
    tools.display_dataframe_to_user(name="Sample Data Preview", dataframe=sample_df)
else:
    print("No files found within the specified date range.")

@app.cell
def load_data(pd, date_str):
    # Construct the filename dynamically using date_str
    filename = f"syntheticData-{date_str}.csv"
    
    # Load the corresponding CSV file
    dat = pd.read_csv(filename, dtype=str)
    
    return dat

@app.cell
def save_html(G, Sigma, date_str):
    # Generate dynamic filename using date_str
    html_filename = f"audience-{date_str}.html"
    
    Sigma.write_html(
        graph=G,
        path=html_filename,
        fullscreen=True,
        node_size=G.degree,
        # raw_node_pictogram=get_node_icon,
        # node_color=get_node_color,
        node_border_color_from="node",
        edge_size="count"
    )
    return f"HTML file saved as {html_filename}"

@app.cell
def save_gml(G, nx, date_str):
    # Generate dynamic filename using date_str
    gml_filename = f"audience-{date_str}.gml"
    
    nx.write_gml(G, gml_filename)
    return f"GML file saved as {gml_filename}"

@app.cell
def save_pickle(pickle, rg, date_str):
    # Generate dynamic filename using date_str
    pkl_filename = f"audience-{date_str}.pkl"
    
    with open(pkl_filename, 'wb') as file:
        pickle.dump(rg, file)
    
    return f"Pickle file saved as {pkl_filename}"


from datetime import datetime, timedelta

# Define start and end date (modify these values)
start_date = "2025-02-16"
end_date = "2025-03-06"

# Convert to datetime objects
start_dt = datetime.strptime(start_date, "%Y-%m-%d")
end_dt = datetime.strptime(end_date, "%Y-%m-%d")

# Generate a list of date strings for all dates in the range
date_strings = [(start_dt + timedelta(days=i)).strftime("%Y-%m-%d") 
                for i in range((end_dt - start_dt).days + 1)]

# Print all generated date strings for verification
print("Generated date strings:", date_strings)


for date_str in date_strings:
    print(f"Processing data for {date_str}...")
    
    # Load Data
    df = load_data(pd, date_str)
    
    # If data exists, display it
    if not df.empty:
        display(df, mo, quak)

        # Process Graph
        G = process_graph(df, nx)

        # Save Outputs
        save_gml(G, nx, date_str)
        save_html(G, Sigma, date_str)
        save_pickle(pickle, G, date_str)