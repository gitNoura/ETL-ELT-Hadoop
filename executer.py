import subprocess
import os

# List of input file paths
input_paths = [
    '/sales_records_n1.csv'
    '/sales_records_n2.csv'
    '/sales_records_n3.csv'
    '/sales_records_n4.csv'
    '/sales_records_n5.csv'
]

# Loop through input paths and process each file
for input_path in input_paths:
    # Define the static output file name
    output_file = os.path.splitext(os.path.basename(input_path))[0] + '_out.csv'
    output_path = '/' + output_file

    # Run the hadoop jar command
    subprocess.call([
        'mapred', 'streaming',
        '-input', input_path,
        '-output', output_path,
        '-mapper', 'mapper.py',
        '-file', 'mapper.py'
    ])
