#!/usr/bin/python3
import sys
import csv

# Define a function to transform dates to MM/DD/YYYY format
def transform_date(order_date):
    parts = order_date.split('/')
    if len(parts) == 3:  # Ensure that there are three parts
        return '/'.join([parts[1], parts[0], parts[2]])
    else:
        return order_date

# Define a function to transform money by adding currency symbol ($)
def transform_money(amount_str):
    return f"${amount_str}"

# Read input from standard input (STDIN)
for line in csv.reader(sys.stdin):
    # Extract relevant columns
    order_date = line[6]
    revenue = line[12]

    # Perform transformations
    transformed_date = transform_date(order_date)
    transformed_revenue = transform_money(revenue)

    # Emit key-value pairs to STDOUT
    print(f"{transformed_date}\t{transformed_revenue}")