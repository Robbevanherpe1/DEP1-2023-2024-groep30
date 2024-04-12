import requests
import csv

api_url = 'https://api.sportify.bet/echo/v1/events?sport=voetbal&competition=belgium-first-division-a&_cached=true&key=market_type&lang=nl&bookmaker=bet777'

try:
    response = requests.get(api_url, timeout=30)
    response.raise_for_status()  # Raise an exception for 4xx and 5xx status codes
    data = response.json()

    with open(r'DEP\DEP1-2023-2024-groep30\bet777\data\bets.csv', 'w', newline='', encoding='utf-8') as file:
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
                                    odds_dict['OnderXGoals'] = odds  # Omwisselen van de waarden
                                elif outcome.get('name') == "Onder (2.5)":
                                    odds_dict['OverXGoals'] = odds  # Omwisselen van de waarden
                            elif market_name == "Beide teams zullen scoren":
                                if outcome.get('name') == "Ja":
                                    odds_dict['BeideTeamsScoren'] = odds
                                elif outcome.get('name') == "Nee":
                                    odds_dict['NietBeideTeamsScoren'] = odds

                    writer.writerow([event_id, event_name, start_time, home_team, away_team] + list(odds_dict.values()))

    print("Data has been successfully written to bets.csv.")

except requests.exceptions.RequestException as e:
    print(f"Error fetching data: {e}")
