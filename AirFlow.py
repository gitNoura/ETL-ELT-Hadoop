from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.dummy import DummyOperator
import pandas as pd
import psycopg2
import csv
import xml.etree.ElementTree as ET
import requests
import io

@task
def extract_task():
    url = 'https://raw.githubusercontent.com/anbento0490/tutorials/master/sales_xlsx/'
    files = [
        'sales_records_n1.xlsx',
        'sales_records_n2.xlsx',
        'sales_records_n3.xlsx',
        'sales_records_n4.xlsx',
        'sales_records_n5.xlsx'
    ]

    dataframes = []
    for file in files:
        url = url + file
        response = requests.get(url)
        if response.status_code == 200:
            excel_file = io.BytesIO(response.content)
            df = pd.read_excel(excel_file)
            dataframes.append(df)
        else:
            raise Exception(f'Failed to download {file} from {url}')

    combined_df = pd.concat(dataframes, ignore_index=True)
    combined_df.to_csv('sales_data.csv', index=False)
    return 'sales.csv'

@task
def save_xml(xml_file_path):
    tree = ET.parse(xml_file_path)
    root = tree.getroot()

    data = []
    for record in root.findall('record'):
        row = [
            record.find('Region').text,
            record.find('Country').text,
            record.find('Item_Type').text,
            record.find('Sales_Channel').text,
            record.find('Order_Priority').text,
            record.find('Order_Date').text,
            record.find('Order_ID').text,
            record.find('Ship_Date').text,
            record.find('Units_Sold').text,
            record.find('Unit_Price').text,
            record.find('Unit_Cost').text,
            record.find('Total_Revenue').text,
            record.find('Total_Cost').text,
            record.find('Total_Profit').text
        ]
        data.append(row)

    with open('xml_data.csv', 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow([
            'Region', 'Country', 'Item_Type', 'Sales_Channel', 'Order_Priority',
            'Order_Date', 'Order_ID', 'Ship_Date', 'Units_Sold', 'Unit_Price',
            'Unit_Cost', 'Total_Revenue', 'Total_Cost', 'Total_Profit'
        ])
        csv_writer.writerows(data)

    return 'xml_data.csv'

@task
def transform_task(csv_file_path):
    df = pd.read_csv(csv_file_path)
    transformed_df = transform_data(df)
    return transformed_df

# Function to perform transformations on date and numeric columns
def transform_data(data):
    print("Transforming data...")

    # Check if 'Order Date' column is present in the data
    if 'Order Date' in data.columns:
        data['Order Date'] = pd.to_datetime(data['Order Date']).dt.strftime('%d/%m/%Y')
    else:
        print("Error: 'Order Date' not found")

    # Check if 'Total Revenue' and 'Total Cost' columns are present in the data
    if 'Total Revenue' in data.columns:
        data['Total Revenue'] = data['Total Revenue'].apply(lambda x: '${:,.2f}'.format(x))
    else:
        print("Error: 'Total Revenue' not found")

    print("Data transformation complete.")
    return data

@task
def save_csv(transformed_data):
    conn = connect_db()
    cursor = conn.cursor()

    for _, row in transformed_data.iterrows():
        cursor.execute("INSERT INTO SalesDB (Region, Country, Item_Type, Sales_Channel, Order_Priority, Order_Date, Order_ID, Ship_Date, Units_Sold, Unit_Price, Unit_Cost, Total_Revenue, Total_Cost, Total_Profit) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                       (row['Region'], row['Country'], row['Item_Type'], row['Sales_Channel'], row['Order_Priority'], row['Order_Date'], row['Order_ID'], row['Ship_Date'], row['Units_Sold'], row['Unit_Price'], row['Unit_Cost'], row['Total_Revenue'], row['Total_Cost'], row['Total_Profit']))

    conn.commit()
    conn.close()

# Function to connect to PostgreSQL
def connect_db():
    db_config = {
        'user': 'postgres',
        'password': 'Fall2023',
        'host': 'localhost',
        'port': 5432,
        'database': 'Sales'
    }
    return psycopg2.connect(**db_config)

# DAG configurations
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=10),  # Set the retry delay to 10 seconds
}


# Define the DAG
with DAG('ETL',
         default_args=default_args,
         schedule_interval='@daily') as dag:

    start_task = DummyOperator(task_id='start')

    read_excel_task = extract_task()
    extract_xml_task = save_xml('/path/to/xml_file.xml')  # Update the path
    transform_task = transform_task(read_excel_task)
    load_task = save_csv(transform_task)

    end_task = DummyOperator(task_id='end')

    start_task >> read_excel_task >> transform_task >> load_task >> end_task
    start_task >> extract_xml_task >> end_task