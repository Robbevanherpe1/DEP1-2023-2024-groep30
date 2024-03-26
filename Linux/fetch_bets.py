#!/usr/bin/env python3

import requests
import csv
from datetime import datetime

def filter_bets():
    input_filename = '/home/vicuser/data/bets.csv'
    output_filename = '/home/vicuser/data/betsCorrect.csv'

    # Lees de data en sla de unieke regels op, met uitzondering van de timestamp
    unique_rows = set()
    with open(input_filename, 'r', newline='') as csvfile:
        reader = csv.reader(csvfile)
        header = next(reader)  # Sla de kopregel op
        for row in reader:
            # Negeer de timestamp (laatste kolom) voor uniciteit check
            unique_key = tuple(row[:-1])
            unique_rows.add(unique_key)

    with open(output_filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(header + ['Timestamp'])  # Voeg 'Timestamp' toe aan de kopregel
        current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for row in sorted(unique_rows, key=lambda x: (x[0], x[2])):  # Sorteer op ID en Starttijd
            writer.writerow(row + (current_timestamp,))  # Voeg de huidige timestamp toe aan elke rij

    print(f'{len(unique_rows)} unieke regels zijn geschreven naar {output_filename}.')

def write_unique_data_to_csv(unique_rows):
    output_filename = '/home/vicuser/data/betsCorrect.csv'
    with open(output_filename, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        # Voeg 'Timestamp' toe aan de header
        writer.writerow(['ID', 'Wedstrijd', 'Starttijd', 'Thuisploeg', 'Uitploeg', 'Vraag', 'Keuze', 'Kans', 'Timestamp'])
        for row in sorted(unique_rows, key=lambda x: (x[0], x[2])):  # Sorteer op ID en Starttijd
            # Voeg de huidige timestamp toe aan elke rij
            current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            writer.writerow(list(row) + [current_timestamp])  # Voeg de timestamp toe aan het einde van elke rij

def process_and_write_data(data):
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
                            writer.writerow(row)

# API-aanroep en gegevensverwerking
api_url = 'https://api.sportify.bet/echo/v1/events?sport=voetbal&competition=belgium-first-division-a&_cached=true&key=market_type&lang=nl&bookmaker=bet777'

try:
    response = requests.get(api_url, timeout=30)
    response.raise_for_status()
    data = response.json()

    # Verwerk en schrijf de data naar CSV
    process_and_write_data(data)
    print("Data is succesvol geschreven naar betsCorrect.csv.")
    filter_bets()

except requests.exceptions.RequestException as e:
    print(f"Fout bij het ophalen van gegevens: {e}")
