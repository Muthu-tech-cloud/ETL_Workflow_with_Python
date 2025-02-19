import pandas as pd
import json
import xml.etree.ElementTree as ET
import logging

# configure logging
logging.basicConfig(filename='log_file.txt', level=logging.INFO, format='%(message)s')

def log_message(message):
    """Log a message."""
    logging.info(message)

def extract_csv(file_path):
    """Extract data from a CSV file into a DataFrame."""
    log_message(f"Extracting: {file_path}")
    return pd.read_csv(file_path)

def extract_json(file_path):
    """Extract data from a JSON file into a DataFrame."""
    log_message(f"Extracting: {file_path}")
    with open(file_path, 'r') as file:
        try:
            data = json.load(file)  # Try to load a single valid JSON structure
            if isinstance(data, list):  # If JSON is a list of dictionaries
                return pd.DataFrame(data)
            else:
                return pd.DataFrame([data])  # Convert a single dictionary into a DataFrame
        except json.JSONDecodeError:
            # Handle a newline-separated JSON (JSONL)
            file.seek(0)  # Reset file read position
            data = [json.loads(line) for line in file]  # Read line by line
            return pd.DataFrame(data)

def extract_xml(file_path):
    """Extract data from an XML file into a DataFrame."""
    log_message(f"Extracting: {file_path}")
    tree = ET.parse(file_path)
    root = tree.getroot()

    data = []
    columns = [elem.tag for elem in root[0]] if len(root) > 0 else []

    for record in root:
        data.append([elem.text for elem in record])

    return pd.DataFrame(data, columns=columns)

def master_extract(file_path):
    """Master function to extract data based on file type and combine into a single DataFrame."""
    log_message("Starting extraction")
    extracted_data = []

    for file_path in file_path:
        if isinstance(file_path, str):
            if file_path.endswith('.csv'):
                extracted_data.append(extract_csv(file_path))
            elif file_path.endswith('.json'):
                extracted_data.append(extract_json(file_path))
            elif file_path.endswith('.xml'):
                extracted_data.append(extract_xml(file_path))
            else:
                log_message(f"Unsupported file type: {file_path}")
        else:
            log_message(f"Invalid file path type: {file_path}")

    log_message("Extraction completed")
    return pd.concat(extracted_data, ignore_index=True) if extracted_data else pd.DataFrame()

def transform_data(df):
    """Transform data: Convert heights from inches to meters and weights from pounds to kilograms."""
    log_message("Starting transformation")
    if 'Height(in)' in df.columns:
        df['Height(m)'] = df['Height(in)'].astype(float) * 0.0254
    if 'Weight(lb)' in df.columns:
        df['Weight(kg)'] = df['Weight(lb)'].astype(float) * 0.453592
    log_message("Transformation completed")
    return df

def load_data(df, output_file):
    """Load the transformed data into a CSV file."""
    log_message(f"Saving to: {output_file}")
    df.to_csv(output_file, index=False)
    log_message("Data saved")

# Example Usage
file_path = [r"D:\\Muthu\\source\\source1.csv", r"D:\\Muthu\\source\\source1.json", r"D:\\Muthu\\source\\source1.xml"]
combined_df = master_extract(file_path)
transformed_df = transform_data(combined_df)
load_data(transformed_df, 'transformed_data.csv')
print(transformed_df)
