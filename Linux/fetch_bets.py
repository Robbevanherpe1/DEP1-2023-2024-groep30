#!/usr/bin/env python3

import requests
import csv
from datetime import datetime

def filter_bets():
    input_filename = '/home/vicuser/data/bets.csv'
    output_filename = '/home/vicuser/data/betsCorrect.csv'

    # Read the data and save unique rows
    unique_rows = {}
    with open(input_filename, 'r', newline='') as csvfile:
        reader = csv.reader(csvfile)
        header = next(reader) # Save the header row
        for row in reader:
            unique_key = tuple(row[:-1]) # Use all except the timestamp as the key
            timestamp = row[-1] # Save the original timestamp
            # If the row is not in unique_rows or if it is but the new timestamp is later, update it
            if unique_key not in unique_rows or unique_rows[unique_key] < timestamp:
                unique_rows[unique_key] = timestamp

    with open(output_filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(header) # Write the original header row
        for row, timestamp in sorted(unique_rows.items(), key=lambda x: (x[0][0], x[0][2])): # Sort by ID and Starttijd
            writer.writerow(row + (timestamp,)) # Use the original timestamp

    print(f'{len(unique_rows)} unique rows have been written to {output_filename}.')

def process_and_write_data(data):
    filename = '/home/vicuser/data/bets.csv'
    with open(filename, 'a', newline='', encoding='utf-8') as file: # Change mode to 'a' for append
        writer = csv.writer(file)
        # Write the header only if the file is empty (new file)
        if file.tell() == 0:
            writer.writerow(['ID', 'Wedstrijd', 'Starttijd', 'Thuisploeg', 'Uitploeg', 'Vraag', 'Keuze', 'Kans', 'Timestamp'])

        for sport in data.get('tree', []):
            for competition in sport.get('competitions', []):
                for event in competition.get('events', []):
                    event_id = event.get('id')
                    event_name = event.get('name')
                    start_time = event.get('starts_at')
                    home_team = event.get('home_team')
                    away_team = event.get('away_team')

                    for market in event.get('markets', []):
                        market_name = market.get('name')
                        for outcome in market.get('outcomes', []):
                            outcome_name = outcome.get('name')
                            odds = outcome.get('odds')
                            # Append the current timestamp
                            current_time = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
                            row = [event_id, event_name, start_time, home_team, away_team, market_name, outcome_name, odds, current_time]
                            writer.writerow(row)

# API call and data processing
api_url = 'https://api.sportify.bet/echo/v1/events?sport=voetbal&competition=belgium-first-division-a&_cached=true&key=market_type&lang=nl&bookmaker=bet777'

try:
    response = requests.get(api_url, timeout=30)
    response.raise_for_status()
    data = response.json()

    # Process and write the data to CSV
    process_and_write_data(data)
    print("Data has been successfully written to betsCorrect.csv.")
    filter_bets()

except requests.exceptions.RequestException as e:
    print(f"Error fetching data: {e}")