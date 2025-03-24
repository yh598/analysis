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


import os
import marimo
import pandas as pd
import json
import re
from ipysigma import Sigma
import networkx as nx
import rustworkx as rx
import quak
import pickle
from datetime import datetime, timedelta

# Initialize Marimo app
app = marimo.App(width="medium")

# Define start and end date
start_date = "2025-02-16"
end_date = "2025-03-06"

# Generate list of date strings for each date in the range
start_dt = datetime.strptime(start_date, "%Y-%m-%d")
end_dt = datetime.strptime(end_date, "%Y-%m-%d")
date_strings = [(start_dt + timedelta(days=i)).strftime("%Y-%m-%d") 
                for i in range((end_dt - start_dt).days + 1)]

# Directory where files are stored
directory = "/mnt/data/"

# Function to display DataFrame
@app.cell
def display(df, mo, quak):
    return mo.ui.anywidget(quak.widget(df))

# Function to load CSV data
@app.cell
def load_data(pd, date_str):
    filename = f"{directory}syntheticData-{date_str}.csv"
    
    try:
        dat = pd.read_csv(filename, dtype=str)
        return dat
    except FileNotFoundError:
        print(f"File {filename} not found. Skipping...")
        return pd.DataFrame()

# Function to extract relationships
@app.cell
def extract_relationships(dat):
    return dat['relationships'] if "relationships" in dat.columns else []

# Function to process the graph
@app.cell
def process_graph(dat, nx):
    G = nx.Graph()
    
    for index, row in dat.iterrows():
        if 'source' in row and 'target' in row:
            G.add_edge(row['source'], row['target'], weight=row.get('weight', 1))
    
    return G

# Function to save HTML
@app.cell
def save_html(G, Sigma, date_str):
    html_filename = f"{directory}audience-{date_str}.html"
    
    Sigma.write_html(
        graph=G,
        path=html_filename,
        fullscreen=True,
        node_size=G.degree,
        node_border_color_from="node",
        edge_size="count"
    )
    
    return f"HTML file saved: {html_filename}"

# Function to save GML
@app.cell
def save_gml(G, nx, date_str):
    gml_filename = f"{directory}audience-{date_str}.gml"
    
    nx.write_gml(G, gml_filename)
    
    return f"GML file saved: {gml_filename}"

# Function to save Pickle file
@app.cell
def save_pickle(pickle, G, date_str):
    pkl_filename = f"{directory}audience-{date_str}.pkl"
    
    with open(pkl_filename, 'wb') as file:
        pickle.dump(G, file)
    
    return f"Pickle file saved: {pkl_filename}"

# Function to extract and process subgraph
@app.cell
def extract_subgraph(G, nx, super_nodes):
    nodes = super_nodes

    neighbors = [list(nx.all_neighbors(G, v)) for v in super_nodes[:10]]
    nodes.extend([item for sublist in neighbors for item in sublist])

    sg = G.subgraph(nodes)
    
    return neighbors, sg

# Process all dates
for date_str in date_strings:
    print(f"Processing data for {date_str}...")

    # Load data
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

print("Processing complete for all dates.")

@app.cell
def load_data(pd, date_str):
    filename = f"/mnt/data/syntheticData-{date_str}.csv"
    
    try:
        dat = pd.read_csv(filename, dtype=str)
        return dat  # Ensure this is inside the function
    except FileNotFoundError:
        print(f"File {filename} not found. Skipping...")
        return pd.DataFrame()  # Return an empty DataFrame if the file is missing