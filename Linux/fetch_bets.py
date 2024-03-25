#!/usr/bin/env python3

import requests
import csv
from datetime import datetime

# Function to check if a row (except for the timestamp) already exists in the CSV
def row_exists(filename, row):
    with open(filename, 'r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        existing_rows = [line[:-1] for line in reader]  # Exclude the timestamp column
        return row[:-1] in existing_rows

# Function to write data to the CSV, including a new timestamp column
def write_data_to_csv(data):
    filename = '/home/vicuser/data/bets.csv'
    with open(filename, 'a', newline='', encoding='utf-8') as file:  # Change mode to 'a' for append
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
                            # Only write if the row doesn't already exist
                            if not row_exists(filename, row):
                                writer.writerow(row)

# API call and data processing
api_url = 'https://api.sportify.bet/echo/v1/events?sport=voetbal&competition=belgium-first-division-a&_cached=true&key=market_type&lang=nl&bookmaker=bet777'

try:
    response = requests.get(api_url, timeout=30)
    response.raise_for_status()
    data = response.json()

    # Write the data to CSV
    write_data_to_csv(data)
    print("Data has been successfully written to bets.csv.")

except requests.exceptions.RequestException as e:
    print(f"Error fetching data: {e}")