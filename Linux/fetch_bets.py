#!/usr/bin/env python3

import csv
import logging
import requests
from datetime import datetime

# Set up logging to print to console
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_and_write_data(api_url, output_path):
    try:
        response = requests.get(api_url, timeout=30)
        response.raise_for_status()  # Raise an exception for HTTP errors
        data = response.json()

        with open(output_path, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['ID', 'Wedstrijd', 'Starttijd', 'Thuisploeg', 'Uitploeg', 'ThuisPloegWint', 'Gelijk', 'UitPloegWint', 'OnderXGoals', 'OverXGoals', 'BeideTeamsScoren', 'NietBeideTeamsScoren'])

            for sport in data.get('tree', []):
                for competition in sport.get('competitions', []):
                    for event in competition.get('events', []):
                        event_id = event.get('id')
                        event_name = event.get('name')
                        start_time = event.get('starts_at')
                        home_team = event.get('home_team')
                        away_team = event.get('away_team')

                        odds_dict = {
                            'ThuisPloegWint': None,
                            'Gelijk': None,
                            'UitPloegWint': None,
                            'OnderXGoals': None,
                            'OverXGoals': None,
                            'BeideTeamsScoren': None,
                            'NietBeideTeamsScoren': None
                        }

                        for market in event.get('markets', []):
                            market_name = market.get('name')
                            for outcome in market.get('outcomes', []):
                                odds = outcome.get('odds')
                                if market_name == "Wedstrijduitslag":
                                    if outcome.get('name') == "1":
                                        odds_dict['ThuisPloegWint'] = odds
                                    elif outcome.get('name') == "Gelijkspel":
                                        odds_dict['Gelijk'] = odds
                                    elif outcome.get('name') == "2":
                                        odds_dict['UitPloegWint'] = odds
                                elif market_name == "Totaal Aantal Goals":
                                    if outcome.get('name') == "Meer dan (2.5)":
                                        odds_dict['OverXGoals'] = odds
                                    elif outcome.get('name') == "Onder (2.5)":
                                        odds_dict['OnderXGoals'] = odds
                                elif market_name == "Beide teams zullen scoren":
                                    if outcome.get('name') == "Ja":
                                        odds_dict['BeideTeamsScoren'] = odds
                                    elif outcome.get('name') == "Nee":
                                        odds_dict['NietBeideTeamsScoren'] = odds

                        writer.writerow([event_id, event_name, start_time, home_team, away_team] + list(odds_dict.values()))
        logging.info("Data has been successfully written to bets.csv.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data: {e}")

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

        with open(output_filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(header)  # Write the original header
            for row, timestamp in sorted(unique_rows.items(), key=lambda x: (x[0][0], x[0][2])):  # Sort by ID and Starttijd
                writer.writerow(row + (timestamp,))  # Use the original timestamp
        logging.info(f'{len(unique_rows)} unique rows have been written to {output_filename}.')
    except Exception as e:
        logging.error(f"Failed to write to {output_filename}: {e}")

if __name__ == "__main__":
    api_url = 'https://api.sportify.bet/echo/v1/events?sport=voetbal&competition=belgium-first-division-a&_cached=true&key=market_type&lang=nl&bookmaker=bet777'
    output_path = '/home/vicuser/data/bets.csv'

    try:
        fetch_and_write_data(api_url, output_path)
        filter_bets()
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
