import requests
import csv

api_url = 'https://api.sportify.bet/echo/v1/events?sport=voetbal&competition=belgium-first-division-a&_cached=true&key=market_type&lang=nl&bookmaker=bet777'

try:
    response = requests.get(api_url, timeout=30)
    response.raise_for_status()  # Raise an exception for 4xx and 5xx status codes
    data = response.json()

    with open(r'DEP-G30\bet777\data\bets.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['ID', 'Wedstrijd', 'Starttijd', 'Thuisploeg', 'Uitploeg', 'Vraag', 'Keuze', 'Kans'])

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
                            writer.writerow([event_id, event_name, start_time, home_team, away_team, market_name, outcome_name, odds])

    print("Data has been successfully written to bets.csv.")

except requests.exceptions.RequestException as e:
    print(f"Error fetching data: {e}")
