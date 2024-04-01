#!/usr/bin/env python3

import csv
from datetime import datetime
import logging

# Set up logging to print to console
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def filter_bets():
    input_filename = '/home/vicuser/data/bets.csv'
    output_filename = '/home/vicuser/data/betsCorrect.csv'

    try:
        unique_rows = {}
        with open(input_filename, 'r', newline='') as csvfile:
            reader = csv.reader(csvfile)
            header = next(reader)  # Save the header row
            for row in reader:
                unique_key = tuple(row[:-1])  # Use all except the timestamp as the key
                timestamp = row[-1]  # Save the original timestamp
                # If the row is not in unique_rows or if it is but the new timestamp is later, update it
                if unique_key not in unique_rows or unique_rows[unique_key] < timestamp:
                    unique_rows[unique_key] = timestamp
    except Exception as e:
        logging.error(f"Failed to read or process {input_filename}: {e}")
        return

    try:
        with open(output_filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(header)  # Write the original header row
            for row, timestamp in sorted(unique_rows.items(), key=lambda x: (x[0][0], x[0][2])):  # Sort by ID and Starttijd
                writer.writerow(row + (timestamp,))  # Use the original timestamp
        logging.info(f'{len(unique_rows)} unique rows have been written to {output_filename}.')
    except Exception as e:
        logging.error(f"Failed to write to {output_filename}: {e}")

# Example function placeholder for processing and writing data
def process_and_write_data():
    # Placeholder for data processing and writing logic
    logging.info("Data processing and writing logic goes here.")

# Call your function to process and write data
try:
    process_and_write_data()
    filter_bets()
except Exception as e:
    logging.error(f"Unexpected error: {e}")
