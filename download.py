import os
import subprocess
# List of URLs to download
urls = [
    'https://raw.githubusercontent.com/anbento0490/code_tutorials/master/sales_csv/sales_records_n1.csv',
    'https://raw.githubusercontent.com/anbento0490/code_tutorials/master/sales_csv/sales_records_n2.csv',
    'https://raw.githubusercontent.com/anbento0490/code_tutorials/master/sales_csv/sales_records_n3.csv',
    'https://raw.githubusercontent.com/anbento0490/code_tutorials/master/sales_csv/sales_records_n4.csv',
    'https://raw.githubusercontent.com/anbento0490/code_tutorials/master/sales_csv/sales_records_n5.csv'

]
for url in urls:
    # Get the file name from the URL
    file_name = url.split('/')[-1]

    # Check if the file exists locally and delete it if it does
    if os.path.isfile(file_name):
        os.remove(file_name)

    # Check if the file exists in HDFS and delete it if it does
    if subprocess.call(['hadoop', 'fs', '-test', '-e', '/'+file_name]) == 0:
        subprocess.call(['hadoop', 'fs', '-rm', '/'+file_name])

    # Download the file and save it locally
    subprocess.call(['curl', '-O', url])

    # Copy the file to HDFS
    subprocess.call(['hadoop', 'fs', '-put', file_name, '/'])
    # Execute the executer that will call mapper.py as a mapper
    import subprocess; subprocess.call(['/usr/bin/sudo', 'python', '/executer.py'])