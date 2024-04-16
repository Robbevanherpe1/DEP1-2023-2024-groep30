#!/usr/bin/env python3

import csv
import logging

# Setup basic configuration for logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Filenames for the input and output CSV files
input_filename = '/home/vicuser/data/bets.csv'
output_filename = '/home/vicuser/data/betsCorrect.csv'

try:
    unique_rows = {}
    with open(input_filename, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        header = next(reader)  # Retrieve the header row
        for row in reader:
            unique_key = tuple(row[:-1])  # Use all fields except the timestamp to create a unique key
            timestamp = row[-1]  # Store the timestamp

            # Check if unique_key exists and if not or if existing timestamp is older, update the record
            if unique_key not in unique_rows or unique_rows[unique_key][1] > timestamp:
                unique_rows[unique_key] = (row[:-1], timestamp)

    with open(output_filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(header)  # Write the header row first
        for row_data in sorted(unique_rows.values(), key=lambda x: (x[0][0], x[0][2])):  # Sort by ID and start time
            writer.writerow(list(row_data[0]) + [row_data[1]])  # Combine row with its timestamp

    logging.info(f'{len(unique_rows)} unique rows have been written to {output_filename}.')
except Exception as e:
    logging.error(f"Failed to write to {output_filename}: {e}")
