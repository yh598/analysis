import marimo

__generated_with = "0.9.32"
app = marimo.App(width="medium")


@app.cell
def __():
    import marimo as mo
    return (mo,)


@app.cell
def __():
    import pandas as pd
    import json
    import re
    from ipysigma import Sigma
    import networkx as nx
    import rustworkx as rx
    import quak
    import pickle
    from datetime import datetime, timedelta
    return Sigma, json, nx, pd, pickle, quak, re, rx, datetime, timedelta


@app.cell
def __(mo, quak):
    def display(df):
        return mo.ui.anywidget(quak.Widget(df))
    return (display,)


@app.cell
def __(pd, datetime, timedelta):
    start_date_str = '2025-03-01'
    end_date_str = '2025-03-05'
    
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    if start_date > end_date:
        raise ValueError(f"Start date {start_date_str} is later than end date {end_date_str}. Please provide a valid range.")
    
    data_frames = {}
    current_date = start_date
    
    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        csv_filename = f"syntheticData-{date_str}.csv"
        try:
            data_frames[date_str] = pd.read_csv(csv_filename, dtype=str)
        except FileNotFoundError:
            print(f"Warning: Data file {csv_filename} not found. Skipping {date_str}.")
        except Exception as e:
            print(f"Warning: Could not read {csv_filename} due to {e}. Skipping {date_str}.")
        
        current_date += timedelta(days=1)
    
    return (data_frames, start_date, end_date)


@app.cell
def __(data_frames, start_date, end_date, nx, timedelta):
    current_date = start_date
    
    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        if date_str not in data_frames:
            current_date += timedelta(days=1)
            continue
        
        print(f"Processing data for {date_str}")
        dat = data_frames[date_str]
        G = nx.DiGraph()

        def proc_record(fraudster_nd, potential_fraudster_nd, id_type, id_val):
            node_key = f"{id_type}_{id_val}"
            G.add_node(node_key, type=id_type, label=node_key)
            G.add_edge(fraudster_nd, node_key, has=id_type)
            G.add_edge(potential_fraudster_nd, node_key, has=id_type)
        
        for index, row in dat.iterrows():
            fraudster_id = row['fraudster']
            potential_fraudster_id = row['potential_fraudster']
            fraudster_nd = f"fraudster_{fraudster_id}"
            potential_fraudster_nd = f"member_{potential_fraudster_id}"
            G.add_node(fraudster_nd, type='fraudster', label=f"fraudster_{fraudster_id}")
            G.add_node(potential_fraudster_nd, type='member', label=f"member_{potential_fraudster_id}")

            for id_type, val in row['relationships'].items():
                if isinstance(val, list):
                    for id_val in val:
                        proc_record(fraudster_nd, potential_fraudster_nd, id_type, id_val)
                else:
                    proc_record(fraudster_nd, potential_fraudster_nd, id_type, val)
        
        gml_filename = f"audience-{date_str.replace('-', '')}.gml"
        nx.write_gml(G, gml_filename)
        print(f"Saved {gml_filename}")
        
        current_date += timedelta(days=1)
    
    return



import marimo

__generated_with = "0.9.32"
app = marimo.App(width="medium")


@app.cell
def __():
    import marimo as mo
    return (mo,)


@app.cell
def __():
    import pandas as pd
    import json
    import re
    from ipysigma import Sigma
    import networkx as nx
    import rustworkx as rx
    import quak
    import pickle
    from datetime import datetime, timedelta
    return Sigma, json, nx, pd, pickle, quak, re, rx, datetime, timedelta


@app.cell
def __(mo, quak):
    def display(df):
        return mo.ui.anywidget(quak.Widget(df))
    return (display,)


@app.cell
def __(pd, datetime, timedelta):
    start_date_str = '2025-03-01'
    end_date_str = '2025-03-05'
    
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    if start_date > end_date:
        raise ValueError(f"Start date {start_date_str} is later than end date {end_date_str}. Please provide a valid range.")
    
    data_frames = {}
    date_strings = [(start_date + timedelta(days=i)).strftime("%Y-%m-%d") for i in range((end_date - start_date).days + 1)]
    
    for date_str in date_strings:
        csv_filename = f"syntheticData-{date_str}.csv"
        try:
            data_frames[date_str] = pd.read_csv(csv_filename, dtype=str)
        except FileNotFoundError:
            print(f"Warning: Data file {csv_filename} not found. Skipping {date_str}.")
        except Exception as e:
            print(f"Warning: Could not read {csv_filename} due to {e}. Skipping {date_str}.")
    
    return (data_frames, date_strings)


@app.cell
def __(data_frames, date_strings, nx):
    for date_str in date_strings:
        if date_str not in data_frames:
            continue
        
        print(f"Processing data for {date_str}")
        dat = data_frames[date_str]
        G = nx.DiGraph()

        def proc_record(fraudster_nd, potential_fraudster_nd, id_type, id_val):
            node_key = f"{id_type}_{id_val}"
            G.add_node(node_key, type=id_type, label=node_key)
            G.add_edge(fraudster_nd, node_key, has=id_type)
            G.add_edge(potential_fraudster_nd, node_key, has=id_type)
        
        for index, row in dat.iterrows():
            fraudster_id = row['fraudster']
            potential_fraudster_id = row['potential_fraudster']
            fraudster_nd = f"fraudster_{fraudster_id}"
            potential_fraudster_nd = f"member_{potential_fraudster_id}"
            G.add_node(fraudster_nd, type='fraudster', label=f"fraudster_{fraudster_id}")
            G.add_node(potential_fraudster_nd, type='member', label=f"member_{potential_fraudster_id}")

            for id_type, val in row['relationships'].items():
                if isinstance(val, list):
                    for id_val in val:
                        proc_record(fraudster_nd, potential_fraudster_nd, id_type, id_val)
                else:
                    proc_record(fraudster_nd, potential_fraudster_nd, id_type, val)
        
        gml_filename = f"audience-{date_str.replace('-', '')}.gml"
        nx.write_gml(G, gml_filename)
        print(f"Saved {gml_filename}")
    
    return


import pandas as pd
import networkx as nx
import os
import re
from datetime import datetime

# Directory containing CSV files
data_dir = "./"  # Adjust if files are stored in a different location

# Find the latest CSV file matching the pattern `syntheticData-YYYY-MM-DD.csv`
csv_files = [f for f in os.listdir(data_dir) if re.match(r"syntheticData-\d{4}-\d{2}-\d{2}\.csv", f)]

# Extract dates from filenames
valid_files = []
for file in csv_files:
    match = re.search(r"syntheticData-(\d{4}-\d{2}-\d{2})\.csv", file)
    if match:
        file_date = datetime.strptime(match.group(1), "%Y-%m-%d")
        if file_date >= datetime(2025, 1, 1):  # Filter only files from 2025-01-01 onwards
            valid_files.append((file_date, file))

# Select the most recent file
if valid_files:
    latest_file = max(valid_files)[1]  # Get the latest date's file
    print(f"Using latest file: {latest_file}")
else:
    raise FileNotFoundError("No valid CSV files found from 2025-01-01 onwards.")

# Load dataset
dat = pd.read_csv(os.path.join(data_dir, latest_file), dtype=str)

# Convert date column to datetime format
dat['date_column'] = pd.to_datetime(dat['date_column'])

# Define date range for daily GML file generation
start_date = '2025-01-01'
end_date = '2025-03-12'
date_range = pd.date_range(start=start_date, end=end_date)

# Create output directory if it doesn't exist
output_dir = "daily_gml_files"
os.makedirs(output_dir, exist_ok=True)

# Function to process and save graphs
def process_and_save_graph(filtered_data, date_str):
    G = nx.DiGraph()

    # Function to add edges
    def proc_record(fraudster_nd, potential_fraudster_nd, id_type, id_val):
        node_key = f"{id_type}_{id_val}"
        G.add_node(node_key, type=id_type, label=node_key)
        G.add_edge(fraudster_nd, node_key, has=id_type)
        G.add_edge(potential_fraudster_nd, node_key, has=id_type)    

    # Process each row
    for _, row in filtered_data.iterrows():
        fraudster_id = row['fraudster']
        potential_fraudster_id = row['potential_fraudster']

        fraudster_nd = f"fraudster_{fraudster_id}"
        potential_fraudster_nd = f"member_{potential_fraudster_id}"

        G.add_node(fraudster_nd, type='fraudster', label=fraudster_nd)
        G.add_node(potential_fraudster_nd, type='member', label=potential_fraudster_nd)

        # Extract relationships
        relationships = eval(row['relationships'])  # Convert string dictionary to actual dictionary
        for id_type, val in relationships.items():
            if isinstance(val, list):
                for id_val in val:
                    proc_record(fraudster_nd, potential_fraudster_nd, id_type, id_val)
            else:
                proc_record(fraudster_nd, potential_fraudster_nd, id_type, val)

    # Save GML file
    gml_filename = f"{output_dir}/audience-{date_str}.gml"
    nx.write_gml(G, gml_filename)
    print(f"Saved: {gml_filename}")

# Iterate over each date and process the data
for date in date_range:
    date_str = date.strftime("%Y-%m-%d")
    daily_data = dat[dat['date_column'] == date]
    
    if not daily_data.empty:
        process_and_save_graph(daily_data, date_str)

