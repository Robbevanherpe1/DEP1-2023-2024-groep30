#!/usr/bin/env python3

import csv
import logging

# Setup basic configuration for logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Filenames for the input and output CSV files
input_filename = '/home/vicuser/data/bets.csv'
output_filename = '/home/vicuser/data/betsCorrect.csv'

def read_and_filter_data(input_filename):
    # reads data from input CSV file and filters out rows based on ID field
    # keep the row with the earliest timestamp if there are duplicates

    unique_rows = {}
    with open(input_filename, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            # Use ID as the unique key
            key = row['ID']
            # Check if the key exists and compare timestamps
            if key in unique_rows:
                if unique_rows[key]['Timestamp'] > row['Timestamp']:
                    unique_rows[key] = row
            else:
                unique_rows[key] = row
    
    return unique_rows

def write_filtered_data(output_filename, unique_rows):
    # Writes the filtered data to output CSV file.
    
    fieldnames = ['ID', 'Wedstrijd', 'Starttijd', 'Thuisploeg', 'Uitploeg', 'ThuisPloegWint', 'Gelijk',
                  'UitPloegWint', 'OnderXGoals', 'OverXGoals', 'BeideTeamsScoren', 'NietBeideTeamsScoren', 'Timestamp']
    with open(output_filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for row in unique_rows.values():
            writer.writerow(row)

def main():
    logging.info("Starting the process to filter unique rows based on 'ID'.")
    unique_rows = read_and_filter_data(input_filename)
    logging.info(f"Found {len(unique_rows)} unique entries by 'ID'. Writing to output file.")
    write_filtered_data(output_filename, unique_rows)
    logging.info("Finished writing the filtered data.")

if __name__ == "__main__":
    main()