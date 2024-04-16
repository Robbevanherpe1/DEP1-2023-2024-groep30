import requests
import csv
from datetime import datetime

api_url = 'https://api.sportify.bet/echo/v1/events?sport=voetbal&competition=belgium-first-division-a&_cached=true&key=market_type&lang=nl&bookmaker=bet777'
output_path = r'DEP\DEP1-2023-2024-groep30\bet777\data\bets.csv'
header = ['ID', 'Wedstrijd', 'Starttijd', 'Thuisploeg', 'Uitploeg', 'ThuisPloegWint', 'Gelijk', 'UitPloegWint', 'OnderXGoals', 'OverXGoals', 'BeideTeamsScoren', 'NietBeideTeamsScoren', 'Timestamp']

# Controleer of het bestand een header heeft
def file_needs_header(filepath):
    try:
        with open(filepath, 'r', newline='', encoding='utf-8') as file:
            reader = csv.reader(file)
            existing_header = next(reader, None)
            return not existing_header or existing_header != header
    except FileNotFoundError:
        return True  # Het bestand bestaat niet, dus heeft een header nodig

needs_header = file_needs_header(output_path)

# API data ophalen
try:
    response = requests.get(api_url, timeout=30)
    response.raise_for_status()  # Controleer op HTTP-fouten
    data = response.json()

    with open(output_path, 'a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        if needs_header:
            writer.writerow(header)  # Schrijf de header als deze nodig is

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
                                    odds_dict['OnderXGoals'] = odds
                                elif outcome.get('name') == "Onder (2.5)":
                                    odds_dict['OverXGoals'] = odds
                            elif market_name == "Beide teams zullen scoren":
                                if outcome.get('name') == "Ja":
                                    odds_dict['BeideTeamsScoren'] = odds
                                elif outcome.get('name') == "Nee":
                                    odds_dict['NietBeideTeamsScoren'] = odds

                    new_row = [event_id, event_name, start_time, home_team, away_team] + list(odds_dict.values())
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    row_with_timestamp = new_row + [timestamp]
                    writer.writerow(row_with_timestamp)

except requests.exceptions.RequestException as e:
    print(f"Error fetching data: {e}")
