import pandas as pd
from sqlalchemy import create_engine
import requests
import os
from urllib.parse import urlparse

# Function to download files from URLs if not already downloaded
def download_files(urls, output_dir):
    print("Downloading files...")
    os.makedirs(output_dir, exist_ok=True)  # Create the directory if it doesn't exist
    for url in urls:
        file_name = os.path.basename(urlparse(url).path)
        file_path = os.path.join(output_dir, file_name)
        # Check if file already exists
        if os.path.exists(file_path):
            print(f"File '{file_name}' already exists. Skipping download.")
            continue
        response = requests.get(url)
        with open(file_path, 'wb') as file:
            file.write(response.content)
    print("Files downloaded successfully.")

# URLs of the files to download
urls = [
    'https://github.com/anbento0490/tutorials/raw/master/sales_csv/sales_records_n1.csv',
    'https://github.com/anbento0490/tutorials/raw/master/sales_csv/sales_records_n2.csv',
    'https://github.com/anbento0490/tutorials/raw/master/sales_csv/sales_records_n3.csv',
    'https://github.com/anbento0490/tutorials/raw/master/sales_csv/sales_records_n4.csv',
    'https://github.com/anbento0490/tutorials/raw/master/sales_csv/sales_records_n5.csv'
]

# Directory to save downloaded files
output_dir = 'Sales'

# Function to perform transformations on date and numeric columns
def transform_data(data):
    print("Transforming data...")
    # Check if 'Order Date' column is present in the data
    if 'Order Date' in data.columns:
        data['Order Date'] = pd.to_datetime(data['Order Date']).dt.strftime('%d/%m/%Y')
    else:
        print("Error: 'Order Date' not found")
    # Check if 'Total Revenue' column is present in the data
    if 'Total Revenue' in data.columns:
        data['Total Revenue'] = data['Total Revenue'].apply(lambda x: '${:,.2f}'.format(x))
    else:
        print("Error: 'Total Revenue' not found")
    print("Data transformation complete.")
    return data

# Function to load data into a relational database
def load_data(data, database_url):
    print("Loading data into database...")
    engine = create_engine(database_url)
    data.to_sql('Sales', con=engine, if_exists='replace', index=False)
    print("Data loaded successfully.")

# Get list of file paths in the output directory
download_files(urls, output_dir)
file_paths = [os.path.join(output_dir, file_name) for file_name in os.listdir(output_dir) if file_name.endswith('.csv')]
try:
    all_data = pd.concat([pd.read_csv(file_path) for file_path in file_paths], ignore_index=True)
    print("CSV files read successfully; data extracted!")
except FileNotFoundError as e:
    print(f"Error: {e}")

# Transform data
try:
    transformed_data = transform_data(all_data)
except KeyError as e:
    print(f"Error: {e}")

# Load data into relational database
database_url = 'postgresql://postgres:Diza@localhost:5432/Sales'
load_data(transformed_data, database_url)
